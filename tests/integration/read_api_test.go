//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/auth"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// otherScope is a second tenant, used to prove every read is scope-filtered.
const otherScope = "integration-other"

// getJSON issues an authenticated GET and decodes the body into v. The
// suite's authorizingTransport supplies the credential.
func getJSON(t *testing.T, base, path string, v any) *http.Response {
	t.Helper()
	resp, err := http.Get(base + path)
	require.NoError(t, err)
	defer resp.Body.Close()
	if v != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(v))
	}
	return resp
}

// getRaw returns the status and the undecoded body, for assertions about the
// exact JSON a client receives rather than about a Go struct.
func getRaw(t *testing.T, base, path string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(base + path)
	require.NoError(t, err)
	defer resp.Body.Close()
	body := make([]byte, 0)
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		body = append(body, buf[:n]...)
		if err != nil {
			break
		}
	}
	return resp.StatusCode, body
}

// mintKeyFor creates a live API key for a scope and returns a client
// presenting it.
func mintKeyFor(t *testing.T, scope string) *http.Client {
	t.Helper()
	created, err := auth.NewStore(testPool).Create(context.Background(), scope, "read-api-test")
	require.NoError(t, err)
	return clientWithKey(created.Raw)
}

// createJobInScope submits one job directly through the store, for a scope
// other than testScope. The HTTP path is scope-bound by its credential, so a
// second tenant's fixture is built here.
func createJobInScope(t *testing.T, scope, key, queueName, jobType string) uuid.UUID {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"key": key})
	require.NoError(t, err)
	priority, maxAttempts, timeout := 50, 3, 300
	request := jobs.SubmitRequest{
		Queue: queueName, Type: jobType, Payload: payload, Priority: &priority,
		MaxAttempts: &maxAttempts, TimeoutSeconds: &timeout,
	}
	normalized, err := request.Normalize()
	require.NoError(t, err)
	result, err := jobs.NewStore(testPool).Submit(context.Background(), scope, key, normalized)
	require.NoError(t, err)
	return result.Job.ID
}

// insertJobInStatus writes one job directly in a chosen status.
//
// It exists because jobs_cancellation_consistent requires cancel_requested_at
// to be set for exactly CANCEL_REQUESTED and CANCELED and unset otherwise --
// the schema refuses to record a cancellation with no instant, so a fixture
// spanning every status has to honor that rather than work around it.
func insertJobInStatus(t *testing.T, scope, queueName, status string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	now := time.Now().UTC()

	var cancelRequestedAt *time.Time
	if status == "CANCEL_REQUESTED" || status == "CANCELED" {
		cancelRequestedAt = &now
	}
	// jobs_pending_requires_schedule: PENDING means "durable but not yet
	// eligible", which is only meaningful with a schedule to be waiting for.
	var scheduledAt *time.Time
	if status == "PENDING" {
		future := now.Add(time.Hour)
		scheduledAt = &future
	}

	_, err := testPool.Exec(context.Background(), `
		INSERT INTO jobs (
			id, scope, queue, job_type, payload, status, priority,
			max_attempts, timeout_seconds, available_at, created_at, updated_at,
			notification_generation, last_notification_at,
			cancel_requested_at, scheduled_at
		) VALUES ($1, $2, $3, 'demo.echo', '{"m":1}', $4, 50, 3, 300,
		          COALESCE($6, now()), now(), now(), 1, now(), $5, $6)`,
		id, scope, queueName, status, cancelRequestedAt, scheduledAt)
	require.NoError(t, err)
	return id
}

// ---------------------------------------------------------------------------
// Criterion 1 & 2: scope isolation and response shape
// ---------------------------------------------------------------------------

// TestReadAPI_EveryReadIsScopeFiltered is the load-bearing isolation
// assertion: one tenant's key never sees another tenant's jobs, workers, or
// queue depth, and the durable rows prove both tenants really have data.
func TestReadAPI_EveryReadIsScopeFiltered(t *testing.T) {
	reset(t)
	ctx := context.Background()
	srv := newAPI(t)
	store := controlStore()

	// testScope: two jobs and a worker.
	createJob(t, "scope-mine-1", "demo.echo", 50, nil)
	createJob(t, "scope-mine-2", "demo.echo", 50, nil)
	registerWorker(t, store, workerRegistration("mine-worker", 2, nil, []string{"demo.echo"}))

	// otherScope: three jobs and a worker of its own.
	for i := 0; i < 3; i++ {
		createJobInScope(t, otherScope, fmt.Sprintf("scope-theirs-%d", i), "default", "demo.echo")
	}
	_, err := store.Register(ctx, otherScope, workerRegistration("theirs-worker", 2, nil, []string{"demo.echo"}))
	require.NoError(t, err)

	// Both tenants really have rows; otherwise "sees nothing" would pass vacuously.
	require.Equal(t, 5, countRows(t, "jobs"))
	require.Equal(t, 2, countRows(t, "workers"))

	mine := mintKeyFor(t, testScope)
	theirs := mintKeyFor(t, otherScope)

	t.Run("jobs", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			client *http.Client
			want   int
		}{{"mine", mine, 2}, {"theirs", theirs, 3}} {
			resp, err := tc.client.Get(srv.URL + "/v1/jobs")
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var page api.JobPageResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
			require.Lenf(t, page.Jobs, tc.want, "%s must see exactly its own jobs", tc.name)
		}
	})

	t.Run("workers", func(t *testing.T) {
		for _, tc := range []struct {
			client *http.Client
			want   string
		}{{mine, "mine-worker"}, {theirs, "theirs-worker"}} {
			resp, err := tc.client.Get(srv.URL + "/v1/workers")
			require.NoError(t, err)
			defer resp.Body.Close()

			var page api.WorkerPageResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
			require.Len(t, page.Workers, 1)
			require.Equal(t, tc.want, page.Workers[0].Name)
		}
	})

	t.Run("queue depth", func(t *testing.T) {
		for _, tc := range []struct {
			client *http.Client
			want   int
		}{{mine, 2}, {theirs, 3}} {
			resp, err := tc.client.Get(srv.URL + "/v1/queues")
			require.NoError(t, err)
			defer resp.Body.Close()

			var list api.QueueListResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&list))
			require.Len(t, list.Queues, 1)
			require.Equalf(t, tc.want, list.Queues[0].Depth["QUEUED"],
				"depth must count only the calling scope's jobs")
		}
	})
}

// TestReadAPI_ListingCarriesNoPayload asserts on the raw JSON with a large
// payload present. A struct-level assertion would pass even if the server
// serialized a payload field the Go type happened not to model.
func TestReadAPI_ListingCarriesNoPayload(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	big := make(map[string]any, 1)
	// Printable filler: a NUL byte is not a valid JSONB escape sequence.
	big["blob"] = strings.Repeat("payloadblob", 800)
	payload, err := json.Marshal(big)
	require.NoError(t, err)
	priority, maxAttempts, timeout := 50, 3, 300
	request := jobs.SubmitRequest{
		Queue: "default", Type: "demo.echo", Payload: payload,
		Priority: &priority, MaxAttempts: &maxAttempts, TimeoutSeconds: &timeout,
	}
	normalized, err := request.Normalize()
	require.NoError(t, err)
	_, err = jobs.NewStore(testPool).Submit(context.Background(), testScope, "big-payload", normalized)
	require.NoError(t, err)

	status, body := getRaw(t, srv.URL, "/v1/jobs")
	require.Equal(t, http.StatusOK, status)
	require.NotContains(t, string(body), "payload",
		"a list endpoint must not return payloads; one request would pull unbounded user data")
	require.NotContains(t, string(body), "blob")

	// The same job's payload IS available one job at a time.
	var page api.JobPageResponse
	require.NoError(t, json.Unmarshal(body, &page))
	require.Len(t, page.Jobs, 1)
	_, single := getRaw(t, srv.URL, "/v1/jobs/"+page.Jobs[0].ID)
	require.Contains(t, string(single), "blob", "GET /v1/jobs/{id} still returns the payload")
}

// ---------------------------------------------------------------------------
// Criteria 4-7: pagination
// ---------------------------------------------------------------------------

// TestReadAPI_KeysetPaginationHasNoDuplicatesOrOmissions collapses every
// timestamp onto one instant so the id is the only tiebreak -- the same
// technique TestDLQ_KeysetPaginationHasNoDuplicatesOrOmissions uses, and the
// only way to actually exercise the composite comparison.
func TestReadAPI_KeysetPaginationHasNoDuplicatesOrOmissions(t *testing.T) {
	reset(t)
	ctx := context.Background()
	srv := newAPI(t)

	const total = 7
	expected := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		id := createJob(t, fmt.Sprintf("page-%02d", i), "demo.echo", 50, nil)
		expected[id.String()] = struct{}{}
	}
	_, err := testPool.Exec(ctx,
		`UPDATE jobs SET created_at = TIMESTAMPTZ '2026-09-21 12:00:00+00'`)
	require.NoError(t, err)

	seen := make(map[string]int, total)
	var order []string
	cursor := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, 10, "pagination did not terminate")

		path := "/v1/jobs?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		var page api.JobPageResponse
		resp := getJSON(t, srv.URL, path, &page)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		for _, job := range page.Jobs {
			seen[job.ID]++
			order = append(order, job.ID)
		}
		if page.NextCursor == "" {
			break
		}
		require.LessOrEqual(t, len(page.Jobs), 2)
		cursor = page.NextCursor
	}

	require.Len(t, seen, total, "every job must appear")
	for id := range expected {
		require.Equalf(t, 1, seen[id], "job %s appeared %d times", id, seen[id])
	}
	for i := 1; i < len(order); i++ {
		require.Greater(t, order[i-1], order[i],
			"ids must be strictly descending so a page boundary is unambiguous")
	}
}

// TestReadAPI_ConcurrentInsertsNeverDuplicateOrReorder walks pages while
// other connections insert, and pins BOTH halves of the guarantee: no job
// present throughout is duplicated or skipped, and a job that commits mid-walk
// may legitimately be missed by that walk while appearing in a fresh one.
//
// The second half is the limitation jobs.created_at's transaction-start
// semantics create. Documenting it without testing it would leave a reader
// unable to tell a deliberate property from a bug.
//
// The inserters are deliberately BOUNDED -- a fixed budget each, with a short
// yield between commits -- rather than spinning in a tight loop. An unbounded
// loop saturates the shared pgx pool for as long as the walk takes, and under
// the race detector that starves later timing-sensitive tests in this suite
// into failing. The property under test needs commits to interleave with the
// walk, not the pool to be exhausted.
func TestReadAPI_ConcurrentInsertsNeverDuplicateOrReorder(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	const preexisting = 12
	stable := make(map[string]struct{}, preexisting)
	for i := 0; i < preexisting; i++ {
		id := createJob(t, fmt.Sprintf("stable-%02d", i), "demo.echo", 50, nil)
		stable[id.String()] = struct{}{}
	}

	// Inserters run on their own connections, not the walker's: a shared
	// connection would serialize them and prove nothing (AGENTS.md section 7).
	const inserters, perInserter = 3, 8
	var wg sync.WaitGroup
	started := make(chan struct{})
	for i := 0; i < inserters; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-started
			for n := 0; n < perInserter; n++ {
				payload, err := json.Marshal(map[string]any{"w": worker, "n": n})
				if err != nil {
					return
				}
				priority, maxAttempts, timeout := 50, 3, 300
				request := jobs.SubmitRequest{
					Queue: "default", Type: "demo.echo", Payload: payload,
					Priority: &priority, MaxAttempts: &maxAttempts, TimeoutSeconds: &timeout,
				}
				normalized, err := request.Normalize()
				if err != nil {
					return
				}
				_, _ = jobs.NewStore(testPool).Submit(context.Background(), testScope,
					fmt.Sprintf("concurrent-%d-%d", worker, n), normalized)
				// Yield rather than spin: commits must interleave with the
				// walk, not monopolize the pool.
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	close(started) // release every inserter at once, so the walk really races them
	seen := make(map[string]int)
	var order []string
	cursor := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, 200, "pagination did not terminate")

		path := "/v1/jobs?limit=3"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		var page api.JobPageResponse
		getJSON(t, srv.URL, path, &page)
		for _, job := range page.Jobs {
			seen[job.ID]++
			order = append(order, job.ID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	wg.Wait()

	// No duplicate, anywhere.
	for id, count := range seen {
		require.Equalf(t, 1, count, "job %s appeared %d times during a concurrent walk", id, count)
	}
	// Every job that existed before the walk started appears in it.
	for id := range stable {
		require.Containsf(t, seen, id, "job %s existed throughout the walk and must appear", id)
	}
	require.GreaterOrEqual(t, len(order), preexisting)

	// The stated limitation, asserted rather than merely described: a fresh
	// listing sees everything, including rows the walk could legitimately have
	// missed because their transaction-start timestamp fell behind the cursor.
	total := countRows(t, "jobs")
	require.Equal(t, preexisting+inserters*perInserter, total,
		"every inserter must actually have committed its whole budget")
	require.LessOrEqual(t, len(seen), total)

	var fresh api.JobPageResponse
	getJSON(t, srv.URL, "/v1/jobs?limit=100", &fresh)
	require.Len(t, fresh.Jobs, total,
		"a listing taken after every commit sees them all, whatever the racing walk saw")
}

// TestReadAPI_NextCursorIsAbsentOnlyOnTheLastPage pins the LIMIT n+1
// property: a full page that happens to be last must NOT hand out a cursor to
// an empty page.
func TestReadAPI_NextCursorIsAbsentOnlyOnTheLastPage(t *testing.T) {
	reset(t)
	srv := newAPI(t)
	for i := 0; i < 4; i++ {
		createJob(t, fmt.Sprintf("cursor-%d", i), "demo.echo", 50, nil)
	}

	t.Run("exactly limit rows remain", func(t *testing.T) {
		var page api.JobPageResponse
		getJSON(t, srv.URL, "/v1/jobs?limit=4", &page)
		require.Len(t, page.Jobs, 4)
		require.Empty(t, page.NextCursor,
			"a full page that is genuinely last must not advertise another")
	})

	t.Run("more rows remain", func(t *testing.T) {
		var page api.JobPageResponse
		getJSON(t, srv.URL, "/v1/jobs?limit=3", &page)
		require.Len(t, page.Jobs, 3)
		require.NotEmpty(t, page.NextCursor)

		var next api.JobPageResponse
		getJSON(t, srv.URL, "/v1/jobs?limit=3&cursor="+page.NextCursor, &next)
		require.Len(t, next.Jobs, 1)
		require.Empty(t, next.NextCursor)
	})
}

// TestReadAPI_RejectsInvalidPaginationAndFilters covers criterion 7 end to
// end, over HTTP, including the status code the CLI and SDK both branch on.
func TestReadAPI_RejectsInvalidPaginationAndFilters(t *testing.T) {
	reset(t)
	srv := newAPI(t)
	createJob(t, "reject-1", "demo.echo", 50, nil)

	t.Run("garbage cursors", func(t *testing.T) {
		for _, path := range []string{
			"/v1/jobs?cursor=not-base64!",
			"/v1/jobs?cursor=bm90LWEtY3Vyc29y",
			"/v1/workers?cursor=not-base64!",
			"/v1/workers?cursor=" + jobs.EncodeJobCursor(time.Now(), uuid.New()),
		} {
			status, body := getRaw(t, srv.URL, path)
			require.Equalf(t, http.StatusUnprocessableEntity, status, "path %s", path)
			require.Contains(t, string(body), "invalid_cursor")
		}
	})

	t.Run("out-of-range limits", func(t *testing.T) {
		for _, path := range []string{
			"/v1/jobs?limit=0", "/v1/jobs?limit=101", "/v1/jobs?limit=abc",
			"/v1/workers?limit=0", "/v1/workers?limit=101",
		} {
			status, body := getRaw(t, srv.URL, path)
			require.Equalf(t, http.StatusUnprocessableEntity, status, "path %s", path)
			require.Contains(t, string(body), "validation_failed")
		}
	})

	t.Run("invalid filters", func(t *testing.T) {
		for _, path := range []string{"/v1/jobs?status=NOPE", "/v1/jobs?queue=UPPER"} {
			status, body := getRaw(t, srv.URL, path)
			require.Equalf(t, http.StatusUnprocessableEntity, status, "path %s", path)
			require.Contains(t, string(body), "validation_failed")
		}
	})

	t.Run("a queue that does not exist is an empty page, not an error", func(t *testing.T) {
		var page api.JobPageResponse
		resp := getJSON(t, srv.URL, "/v1/jobs?queue=no-such-queue", &page)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Empty(t, page.Jobs)
	})
}

// TestReadAPI_ACursorFromAnotherScopeReturnsOnlyTheCallersRows proves a
// cursor is a position and not an authorization: presenting one minted by a
// different tenant's listing cannot reach that tenant's rows.
func TestReadAPI_ACursorFromAnotherScopeReturnsOnlyTheCallersRows(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	for i := 0; i < 4; i++ {
		createJob(t, fmt.Sprintf("mine-%d", i), "demo.echo", 50, nil)
		createJobInScope(t, otherScope, fmt.Sprintf("theirs-%d", i), "default", "demo.echo")
	}

	mine := mintKeyFor(t, testScope)
	theirs := mintKeyFor(t, otherScope)

	// A cursor issued to the other scope's walk.
	resp, err := theirs.Get(srv.URL + "/v1/jobs?limit=2")
	require.NoError(t, err)
	var theirPage api.JobPageResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&theirPage))
	resp.Body.Close()
	require.NotEmpty(t, theirPage.NextCursor)

	theirIDs := make(map[string]struct{}, len(theirPage.Jobs))
	for _, job := range theirPage.Jobs {
		theirIDs[job.ID] = struct{}{}
	}

	// Presented by this scope: accepted as a position, but it can only ever
	// select from this caller's own rows.
	resp, err = mine.Get(srv.URL + "/v1/jobs?limit=10&cursor=" + theirPage.NextCursor)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var minePage api.JobPageResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&minePage))
	for _, job := range minePage.Jobs {
		require.NotContainsf(t, theirIDs, job.ID,
			"a cursor must not carry another scope's rows across the boundary")
	}
}

// ---------------------------------------------------------------------------
// Criterion 8: attempts
// ---------------------------------------------------------------------------

// TestReadAPI_AttemptTimelineIncludesTheAttemptsThatFailed is the point of
// the route: a job on its last attempt is interesting because of the earlier
// ones that did not work.
func TestReadAPI_AttemptTimelineIncludesTheAttemptsThatFailed(t *testing.T) {
	reset(t)
	ctx := context.Background()
	srv := newAPI(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("attempt-worker", 4, nil, []string{"demo.echo"}))

	jobID := createJobWithOptions(t, "attempts-timeline", "default", "demo.echo", 50, nil, 3, 300, nil)

	// Attempt 1: a retryable failure.
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	first := assignmentFence(claim.Assignment)
	startAttempt(t, store, first)
	_, err = store.Fail(ctx, testScope, workers.FailureReport{
		Fence: first, OutcomeRequestID: uuid.New(),
		Class: lifecycle.ClassRetryable, ErrorCode: "handler_error",
		ErrorMessage: "the trusted handler reported an error",
	})
	require.NoError(t, err)

	// Make it eligible again, then abandon attempt 2 by expiring its lease.
	_, err = testPool.Exec(ctx,
		`UPDATE jobs SET status = 'QUEUED', available_at = now() WHERE id = $1`, jobID)
	require.NoError(t, err)
	claim, err = store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	second := assignmentFence(claim.Assignment)
	// Both instants move, because leases_expiry_after_acquisition requires
	// expires_at > acquired_at -- an expired lease is one acquired earlier
	// still, not one that expired before it was taken.
	_, err = testPool.Exec(ctx, `
		UPDATE leases
		SET acquired_at = now() - interval '10 minutes',
		    renewed_at  = now() - interval '10 minutes',
		    expires_at  = now() - interval '1 minute'
		WHERE id = $1`, second.LeaseID)
	require.NoError(t, err)
	_, err = store.ReconcileExpiredLeases(ctx, 20)
	require.NoError(t, err)

	var list api.AttemptListResponse
	resp := getJSON(t, srv.URL, "/v1/jobs/"+jobID.String()+"/attempts", &list)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, list.Attempts, 2, "both attempts must be visible, including the abandoned one")

	require.Equal(t, 1, list.Attempts[0].AttemptNumber, "oldest attempt first")
	require.Equal(t, 2, list.Attempts[1].AttemptNumber)

	failed := list.Attempts[0]
	require.Equal(t, "FAILED", failed.Status)
	require.NotNil(t, failed.FailureClass)
	require.Equal(t, "RETRYABLE", *failed.FailureClass)
	require.NotNil(t, failed.ErrorCode)
	require.Equal(t, "handler_error", *failed.ErrorCode)
	require.NotNil(t, failed.StartedAt)
	require.NotNil(t, failed.FinishedAt)
	require.Equal(t, "attempt-worker", failed.WorkerName)

	abandoned := list.Attempts[1]
	require.Equal(t, "ABANDONED", abandoned.Status,
		"a crashed worker's attempt is exactly what an operator opens this view for")
	require.Nil(t, abandoned.StartedAt, "this attempt never started")

	// And no fencing identifier reached the wire.
	_, body := getRaw(t, srv.URL, "/v1/jobs/"+jobID.String()+"/attempts")
	for _, forbidden := range []string{"session", "lease", "outcome_request_id", "scope"} {
		require.NotContainsf(t, string(body), forbidden,
			"%q must never appear in a public attempt read", forbidden)
	}
}

// TestReadAPI_AttemptsOfAJobWithNoneIsAnEmptyList distinguishes the two cases
// the single LEFT JOIN exists to keep apart.
func TestReadAPI_AttemptsOfAJobWithNoneIsAnEmptyList(t *testing.T) {
	reset(t)
	srv := newAPI(t)
	jobID := createJob(t, "no-attempts", "demo.echo", 50, nil)

	var list api.AttemptListResponse
	resp := getJSON(t, srv.URL, "/v1/jobs/"+jobID.String()+"/attempts", &list)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotNil(t, list.Attempts)
	require.Empty(t, list.Attempts)

	status, body := getRaw(t, srv.URL, "/v1/jobs/"+jobID.String()+"/attempts")
	require.Equal(t, http.StatusOK, status)
	require.JSONEq(t, `{"attempts":[]}`, string(body), "empty is [] and never null")
}

// TestReadAPI_AttemptsOfAnotherScopesJobIs404 keeps the anti-oracle rule:
// another scope's job and a nonexistent one are indistinguishable.
func TestReadAPI_AttemptsOfAnotherScopesJobIs404(t *testing.T) {
	reset(t)
	srv := newAPI(t)
	theirJob := createJobInScope(t, otherScope, "theirs", "default", "demo.echo")

	for _, path := range []string{
		"/v1/jobs/" + theirJob.String() + "/attempts",
		"/v1/jobs/" + uuid.New().String() + "/attempts",
		"/v1/jobs/not-a-uuid/attempts",
	} {
		status, body := getRaw(t, srv.URL, path)
		require.Equalf(t, http.StatusNotFound, status, "path %s", path)
		require.Contains(t, string(body), "not_found")
		require.Contains(t, string(body), "job not found")
	}
}

// ---------------------------------------------------------------------------
// Criterion 9: workers
// ---------------------------------------------------------------------------

// TestReadAPI_WorkersIncludeCrashedAndReplacedSessions is the test that
// catches the naive implementation.
//
// A listing built on worker_sessions_one_current_per_worker_idx -- whose
// predicate is status IN ('STARTING','HEALTHY','DRAINING') -- would silently
// omit both workers below. They are the ones an operator is looking for.
func TestReadAPI_WorkersIncludeCrashedAndReplacedSessions(t *testing.T) {
	reset(t)
	ctx := context.Background()
	srv := newAPI(t)
	store := controlStore()

	// A healthy worker, for contrast.
	registerWorker(t, store, workerRegistration("aaa-healthy", 4, nil, []string{"demo.echo"}))

	// A crashed worker: reconciliation marks its stale session UNHEALTHY.
	registerWorker(t, store, workerRegistration("bbb-crashed", 4, nil, []string{"demo.echo"}))
	// registered_at moves back with it: worker_sessions_timeline_order requires
	// last_heartbeat_at >= registered_at, and a session cannot have last been
	// heard from before it existed.
	_, err := testPool.Exec(ctx, `
		UPDATE worker_sessions
		SET registered_at      = now() - interval '2 hours',
		    last_heartbeat_at  = now() - interval '1 hour'
		WHERE worker_id = (SELECT id FROM workers WHERE name = 'bbb-crashed')`)
	require.NoError(t, err)
	marked, err := store.MarkStaleSessions(ctx, time.Second, 20)
	require.NoError(t, err)
	require.Equal(t, 1, marked)

	// A replaced worker: registering the same name again ends the prior boot.
	replaced := workerRegistration("ccc-replaced", 4, nil, []string{"demo.echo"})
	registerWorker(t, store, replaced)
	second := workerRegistration("ccc-replaced", 8, nil, []string{"demo.echo"})
	registerWorker(t, store, second)

	// Durable state really is what the test assumes.
	var unhealthy, offline int
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT count(*) FROM worker_sessions WHERE status = 'UNHEALTHY'`).Scan(&unhealthy))
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT count(*) FROM worker_sessions WHERE status = 'OFFLINE'`).Scan(&offline))
	require.Equal(t, 1, unhealthy)
	require.Equal(t, 1, offline)

	var page api.WorkerPageResponse
	resp := getJSON(t, srv.URL, "/v1/workers", &page)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, page.Workers, 3,
		"a crashed worker and a replaced one must both still be listed")

	byName := make(map[string]api.WorkerResponse, 3)
	for _, worker := range page.Workers {
		byName[worker.Name] = worker
	}
	require.Equal(t, "HEALTHY", byName["aaa-healthy"].Status)
	require.Equal(t, "UNHEALTHY", byName["bbb-crashed"].Status,
		"reconciliation marked this session stale; the listing must show it")
	require.NotNil(t, byName["bbb-crashed"].EndedAt)

	// The replaced worker reports its NEWEST session, not the OFFLINE one.
	require.Equal(t, "HEALTHY", byName["ccc-replaced"].Status)
	require.Equal(t, 8, byName["ccc-replaced"].ConcurrencyLimit,
		"the latest session is the one reported, so its concurrency limit is too")

	// Ordered by name, ascending.
	require.Equal(t, []string{"aaa-healthy", "bbb-crashed", "ccc-replaced"},
		[]string{page.Workers[0].Name, page.Workers[1].Name, page.Workers[2].Name})

	// Heartbeat age is measured by PostgreSQL and never negative.
	for _, worker := range page.Workers {
		require.GreaterOrEqualf(t, worker.HeartbeatAgeSeconds, 0.0,
			"worker %s reported a negative heartbeat age", worker.Name)
	}
	require.Greater(t, byName["bbb-crashed"].HeartbeatAgeSeconds, 60.0,
		"the crashed worker's heartbeat really is an hour old")
}

// TestReadAPI_WorkerActiveLeasesMatchesADirectCount pins the occupancy
// number against the database rather than against the claim count the test
// happens to remember.
func TestReadAPI_WorkerActiveLeasesMatchesADirectCount(t *testing.T) {
	reset(t)
	ctx := context.Background()
	srv := newAPI(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("lease-worker", 4, nil, []string{"demo.echo"}))

	for i := 0; i < 3; i++ {
		createJob(t, fmt.Sprintf("lease-job-%d", i), "demo.echo", 50, nil)
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, claim.Disposition)
	}

	var direct int
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT count(*) FROM leases WHERE status = 'ACTIVE'`).Scan(&direct))
	require.Equal(t, 3, direct)

	var page api.WorkerPageResponse
	getJSON(t, srv.URL, "/v1/workers", &page)
	require.Len(t, page.Workers, 1)
	require.Equal(t, direct, page.Workers[0].ActiveLeases)
	require.Equal(t, 4, page.Workers[0].ConcurrencyLimit)
}

// ---------------------------------------------------------------------------
// Criterion 10: queue depth
// ---------------------------------------------------------------------------

// TestReadAPI_QueueDepthMatchesDirectCounts seeds a fixture spanning every
// job status, with another scope's jobs in the same queue, and compares the
// endpoint against SQL rather than against the fixture's own bookkeeping.
func TestReadAPI_QueueDepthMatchesDirectCounts(t *testing.T) {
	reset(t)
	ctx := context.Background()
	srv := newAPI(t)

	// One job in each of the nine statuses, written directly so every state is
	// reachable without driving nine different lifecycles.
	counts := map[string]int{
		"PENDING": 2, "QUEUED": 3, "LEASED": 1, "RUNNING": 1, "RETRY_WAIT": 2,
		"CANCEL_REQUESTED": 1, "SUCCEEDED": 4, "CANCELED": 2, "DEAD_LETTERED": 3,
	}
	for status, n := range counts {
		for i := 0; i < n; i++ {
			insertJobInStatus(t, testScope, "default", status)
		}
	}
	// Another scope's jobs in the SAME queue must not be counted.
	for i := 0; i < 5; i++ {
		insertJobInStatus(t, otherScope, "default", "QUEUED")
	}

	var list api.QueueListResponse
	resp := getJSON(t, srv.URL, "/v1/queues", &list)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, list.Queues, 1)
	queue := list.Queues[0]

	require.Equal(t, "default", queue.Name)
	require.Equal(t, "default", queue.WorkerGroup)
	require.Equal(t, 100, queue.MaxConcurrency)

	for _, status := range jobs.NonTerminalStatuses() {
		var direct int
		require.NoError(t, testPool.QueryRow(ctx,
			`SELECT count(*) FROM jobs WHERE scope = $1 AND queue = 'default' AND status = $2`,
			testScope, status.String()).Scan(&direct))
		require.Equalf(t, direct, queue.Depth[status.String()],
			"depth for %s must equal a direct count", status)
		require.Equalf(t, counts[status.String()], queue.Depth[status.String()],
			"depth for %s must match the fixture", status)
	}

	// Terminal statuses are absent, and no global figure is reported.
	for _, status := range jobs.AllStatuses() {
		if !status.Terminal() {
			continue
		}
		require.NotContainsf(t, queue.Depth, status.String(),
			"%s is terminal and is not depth", status)
	}
	require.Len(t, queue.Depth, 6)

	_, body := getRaw(t, srv.URL, "/v1/queues")
	for _, forbidden := range []string{"in_flight", "utilization", "total", "available"} {
		require.NotContainsf(t, string(body), forbidden,
			"%q would mix a queue-wide number with a scope-local one", forbidden)
	}
}

// TestReadAPI_QueueDepthIsZeroFilled proves a status with no jobs is still a
// key, so a client never has to tell "none" from "not reported".
func TestReadAPI_QueueDepthIsZeroFilled(t *testing.T) {
	reset(t)
	srv := newAPI(t)

	var list api.QueueListResponse
	getJSON(t, srv.URL, "/v1/queues", &list)
	require.Len(t, list.Queues, 1)

	for _, status := range jobs.NonTerminalStatuses() {
		value, present := list.Queues[0].Depth[status.String()]
		require.Truef(t, present, "%s must be present even with no jobs at all", status)
		require.Equal(t, 0, value)
	}
}

// TestReadAPI_QueueDepthStatusesMatchTheIndexPredicate is named by migration
// 0017's own comment. It lives here rather than beside the other migration
// assertions because it is about the Go/SQL agreement the depth query depends
// on, and it fails loudly if either side gains a status the other lacks.
func TestReadAPI_QueueDepthStatusesMatchTheIndexPredicate(t *testing.T) {
	def := indexDefinition(t, "jobs_scope_queue_depth_idx")
	require.NotEmpty(t, def)
	for _, status := range jobs.NonTerminalStatuses() {
		require.Containsf(t, def, "'"+status.String()+"'",
			"jobs_scope_queue_depth_idx must cover %s", status)
	}
}

// ---------------------------------------------------------------------------
// Criterion 3: authentication, over HTTP
// ---------------------------------------------------------------------------

// TestReadAPI_EveryReadRefusesAnUnauthenticatedCaller is the HTTP-level
// counterpart to the unit test's publicRoutes walk.
func TestReadAPI_EveryReadRefusesAnUnauthenticatedCaller(t *testing.T) {
	reset(t)
	srv := newAPI(t)
	anonymous := unauthenticatedClient()

	for _, path := range []string{
		"/v1/jobs",
		"/v1/jobs/" + uuid.New().String() + "/attempts",
		"/v1/workers",
		"/v1/queues",
	} {
		resp, err := anonymous.Get(srv.URL + path)
		require.NoError(t, err)
		require.Equalf(t, http.StatusUnauthorized, resp.StatusCode, "path %s", path)
		require.Equal(t, "Bearer", resp.Header.Get("WWW-Authenticate"))
		resp.Body.Close()
	}
}

// TestReadAPI_ARevokedKeyStopsWorkingOnTheNextRequest extends M5A's
// revocation guarantee to the new routes rather than assuming it carries.
func TestReadAPI_ARevokedKeyStopsWorkingOnTheNextRequest(t *testing.T) {
	reset(t)
	ctx := context.Background()
	srv := newAPI(t)
	createJob(t, "revocation", "demo.echo", 50, nil)

	store := auth.NewStore(testPool)
	created, err := store.Create(ctx, testScope, "revocation-test")
	require.NoError(t, err)
	client := clientWithKey(created.Raw)

	resp, err := client.Get(srv.URL + "/v1/jobs")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	_, err = store.Revoke(ctx, created.Key.ID)
	require.NoError(t, err)

	for _, path := range []string{"/v1/jobs", "/v1/workers", "/v1/queues"} {
		resp, err := client.Get(srv.URL + path)
		require.NoError(t, err)
		require.Equalf(t, http.StatusUnauthorized, resp.StatusCode,
			"a revoked key must stop working on %s", path)
		resp.Body.Close()
	}
}
