//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/workers"
)

const testScope = "integration-test"

func newAPI(t *testing.T) *httptest.Server {
	t.Helper()
	srv := api.NewServer(
		jobs.NewStore(testPool),
		api.Config{MaxRequestBytes: 256 * 1024, DevScope: testScope},
		discardLogger(),
		api.ReadinessCheck{
			Name:  "postgres",
			Check: func(ctx context.Context) error { return database.Ping(ctx, testPool) },
		},
	).WithWorkerControl(workers.NewStore(testPool, 30*time.Second))
	s := httptest.NewServer(srv.Handler())
	t.Cleanup(s.Close)
	return s
}

func submit(t *testing.T, base, key, body string) (*http.Response, api.JobResponse) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/jobs", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	var job api.JobResponse
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&job))
	}
	return resp, job
}

const jobBody = `{"queue":"default","job_type":"demo.echo","payload":{"n":1},"priority":70,"max_attempts":5,"timeout_seconds":45}`

func TestSubmit_CreatesJobAndOutboxEventAtomically(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	resp, job := submit(t, srv.URL, "key-atomic", jobBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NotEmpty(t, job.ID)
	require.Equal(t, "QUEUED", job.Status)
	require.Equal(t, 70, job.Priority)
	require.Equal(t, 5, job.MaxAttempts)
	require.Equal(t, 45, job.TimeoutSeconds)
	require.Equal(t, []string{}, job.RequiredCapabilities, "must serialize as [] and never null")

	// All three rows exist, and they exist together.
	require.Equal(t, 1, countRows(t, "jobs"))
	require.Equal(t, 1, countRows(t, "idempotency_records"))
	require.Equal(t, 1, countRows(t, "outbox_events"))

	var outboxJobID string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT payload->>'job_id' FROM outbox_events`).Scan(&outboxJobID))
	require.Equal(t, job.ID, outboxJobID)

	// The job and its notification committed in the same transaction, so their
	// creation timestamps come from the same transaction snapshot.
	var sameTransaction bool
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT (SELECT created_at FROM jobs) = (SELECT created_at FROM outbox_events)`).Scan(&sameTransaction))
	require.True(t, sameTransaction, "job and outbox event must commit in one transaction")
}

func TestGetJob_ReturnsThePersistedState(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	_, created := submit(t, srv.URL, "key-get", jobBody)

	resp, err := http.Get(srv.URL + "/v1/jobs/" + created.ID)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var fetched api.JobResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&fetched))
	require.Equal(t, created, fetched)

	// The internal auth scope must never be exposed.
	body, err := json.Marshal(fetched)
	require.NoError(t, err)
	require.NotContains(t, string(body), testScope)
}

func TestGetJob_IsScopedToTheCaller(t *testing.T) {
	reset(t)
	srv := newAPI(t)
	_, created := submit(t, srv.URL, "key-scope", jobBody)

	// Move the job to a different scope: the same id must stop being visible.
	_, err := testPool.Exec(context.Background(),
		`UPDATE jobs SET scope = 'someone-else' WHERE id = $1`, created.ID)
	require.NoError(t, err)

	resp, err := http.Get(srv.URL + "/v1/jobs/" + created.ID)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetJob_UnknownIDIsNotFound(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	resp, err := http.Get(srv.URL + "/v1/jobs/" + uuid.NewString())
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// This is the invariant the whole idempotency design exists for. Concurrency is
// driven through separate HTTP connections and therefore separate database
// connections — sharing one connection would serialize the requests and prove
// nothing.
func TestSubmit_ConcurrentIdenticalRequestsCreateExactlyOneJob(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	const concurrency = 24
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     []string
		created int
		replays int
		others  []int
	)

	start := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all goroutines at once to maximize contention

			req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/jobs", strings.NewReader(jobBody))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "key-concurrent")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var job api.JobResponse
			decodeErr := json.NewDecoder(resp.Body).Decode(&job)

			mu.Lock()
			defer mu.Unlock()
			switch resp.StatusCode {
			case http.StatusCreated:
				created++
			case http.StatusOK:
				replays++
			default:
				others = append(others, resp.StatusCode)
				return
			}
			if decodeErr == nil {
				ids = append(ids, job.ID)
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Empty(t, others, "every concurrent duplicate must succeed as create or replay")
	require.Equal(t, 1, created, "exactly one request may create the job")
	require.Equal(t, concurrency-1, replays)
	require.Len(t, ids, concurrency)

	for _, id := range ids {
		require.Equal(t, ids[0], id, "every response must reference the same job")
	}

	// The durable state is what actually matters.
	require.Equal(t, 1, countRows(t, "jobs"))
	require.Equal(t, 1, countRows(t, "idempotency_records"))
	require.Equal(t, 1, countRows(t, "outbox_events"),
		"losing submissions must not leave an orphan notification behind")
}

func TestSubmit_SameKeyDifferentRequestIsAConflict(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	resp, original := submit(t, srv.URL, "key-conflict", jobBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	different := `{"queue":"default","job_type":"demo.sleep","payload":{"n":1}}`
	resp2, _ := submit(t, srv.URL, "key-conflict", different)
	require.Equal(t, http.StatusConflict, resp2.StatusCode)

	var body api.ErrorBody
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&body))
	require.Equal(t, api.CodeIdempotencyConflict, body.Error.Code)

	// The conflicting attempt must not have created anything.
	require.Equal(t, 1, countRows(t, "jobs"))
	require.Equal(t, 1, countRows(t, "outbox_events"))

	resp3, replayed := submit(t, srv.URL, "key-conflict", jobBody)
	require.Equal(t, http.StatusOK, resp3.StatusCode)
	require.Equal(t, original.ID, replayed.ID, "the original request must still replay")
}

// Reordering fields or restating a default is the same request, so it must
// replay rather than conflict.
func TestSubmit_EquivalentRequestReplaysRatherThanConflicts(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	first := `{"queue":"default","job_type":"demo.echo","payload":{"a":1,"b":2}}`
	resp, original := submit(t, srv.URL, "key-equivalent", first)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Same job: keys reordered, defaults stated explicitly, capabilities empty.
	equivalent := `{"payload":{"b":2,"a":1},"job_type":"demo.echo","queue":"default",` +
		`"priority":50,"max_attempts":3,"timeout_seconds":300,"required_capabilities":[],"scheduled_at":null}`
	resp2, replayed := submit(t, srv.URL, "key-equivalent", equivalent)
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	require.Equal(t, original.ID, replayed.ID)
	require.Equal(t, 1, countRows(t, "jobs"))
}

func TestSubmit_DifferentKeysCreateDifferentJobs(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	_, a := submit(t, srv.URL, "key-a", jobBody)
	_, b := submit(t, srv.URL, "key-b", jobBody)
	require.NotEqual(t, a.ID, b.ID)
	require.Equal(t, 2, countRows(t, "jobs"))
	require.Equal(t, 2, countRows(t, "outbox_events"))
}

// A rejected submission must leave nothing behind: no job, no idempotency
// record, and above all no notification for a job that does not exist.
func TestSubmit_RejectedRequestLeavesNoPartialState(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	cases := map[string]struct {
		key, body string
		want      int
	}{
		"unknown queue":      {"key-unknown-queue", `{"queue":"does-not-exist","job_type":"demo.echo","payload":{}}`, http.StatusUnprocessableEntity},
		"invalid priority":   {"key-bad-priority", `{"queue":"default","job_type":"demo.echo","payload":{},"priority":500}`, http.StatusUnprocessableEntity},
		"scheduled job":      {"key-scheduled", `{"queue":"default","job_type":"demo.echo","payload":{},"scheduled_at":"2030-01-01T00:00:00Z"}`, http.StatusUnprocessableEntity},
		"payload not object": {"key-bad-payload", `{"queue":"default","job_type":"demo.echo","payload":[1,2]}`, http.StatusUnprocessableEntity},
		"malformed json":     {"key-malformed", `{"queue":`, http.StatusBadRequest},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resp, _ := submit(t, srv.URL, tc.key, tc.body)
			require.Equal(t, tc.want, resp.StatusCode)
		})
	}

	require.Equal(t, 0, countRows(t, "jobs"))
	require.Equal(t, 0, countRows(t, "idempotency_records"))
	require.Equal(t, 0, countRows(t, "outbox_events"))
}

func TestSubmit_OversizedPayloadIsRejected(t *testing.T) {
	reset(t)
	srv := api.NewServer(jobs.NewStore(testPool),
		api.Config{MaxRequestBytes: 2048, DevScope: testScope}, discardLogger())
	s := httptest.NewServer(srv.Handler())
	defer s.Close()

	huge := fmt.Sprintf(`{"queue":"default","job_type":"demo.echo","payload":{"blob":"%s"}}`, strings.Repeat("x", 8192))
	resp, _ := submit(t, s.URL, "key-huge", huge)
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	require.Equal(t, 0, countRows(t, "jobs"))
}

// The API holds nothing in memory: a completely new server, store, and pool must
// see the same job and be able to replay its idempotency key.
func TestSubmit_SurvivesAPIRestart(t *testing.T) {
	reset(t)

	first := newAPI(t)
	_, created := submit(t, first.URL, "key-restart", jobBody)
	first.Close()

	pool, err := database.Connect(context.Background(), dsn())
	require.NoError(t, err)
	defer pool.Close()

	restarted := httptest.NewServer(api.NewServer(jobs.NewStore(pool),
		api.Config{MaxRequestBytes: 256 * 1024, DevScope: testScope}, discardLogger()).Handler())
	defer restarted.Close()

	resp, err := http.Get(restarted.URL + "/v1/jobs/" + created.ID)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var fetched api.JobResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&fetched))
	require.Equal(t, created.ID, fetched.ID)

	replayResp, replayed := submit(t, restarted.URL, "key-restart", jobBody)
	require.Equal(t, http.StatusOK, replayResp.StatusCode, "idempotency must not depend on process memory")
	require.Equal(t, created.ID, replayed.ID)
	require.Equal(t, 1, countRows(t, "jobs"))
}

func TestReadiness_ReportsRealDatabaseHealth(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	resp, err := http.Get(srv.URL + "/readyz")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Status     string            `json:"status"`
		Components map[string]string `json:"components"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "ready", body.Status)
	require.Equal(t, "ok", body.Components["postgres"])
}

func TestSubmit_PayloadIsStoredCanonicallyAndReturnedIntact(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	body := `{"queue":"default","job_type":"demo.echo","payload":{"z":1,"a":{"n":9007199254740993}}}`
	_, job := submit(t, srv.URL, "key-canonical", body)

	// Large integers must survive: decoding through float64 would corrupt this.
	require.Contains(t, string(job.Payload), "9007199254740993")

	var stored string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT payload->'a'->>'n' FROM jobs WHERE id = $1`, job.ID).Scan(&stored))
	require.Equal(t, "9007199254740993", stored)

}
