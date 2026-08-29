//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/outbox"
	"github.com/co-rtex/TaskForge/internal/queue"
)

func publisherConfig() outbox.PublisherConfig {
	return outbox.PublisherConfig{
		BatchSize:    50,
		PollInterval: 50 * time.Millisecond,
		// Short so a test can observe a reclaim without waiting 30 seconds.
		ClaimTimeout: 2 * time.Second,
		Backoff: outbox.BackoffPolicy{
			Base: 50 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: 0.2,
		},
	}
}

func newPublisher(t *testing.T, broker queue.Publisher) *outbox.Publisher {
	t.Helper()
	// A fixed seed keeps jittered backoff deterministic across runs.
	return outbox.NewPublisher(outbox.NewStore(testPool), broker, publisherConfig(),
		discardLogger(), rand.New(rand.NewSource(1)))
}

// submitJobs creates n distinct durable jobs through the real API.
func submitJobs(t *testing.T, n int) []string {
	t.Helper()
	srv := newAPI(t)
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		body := `{"queue":"default","job_type":"demo.echo","payload":{"i":` + itoa(i) + `}}`
		resp, job := submit(t, srv.URL, "key-"+uuid.NewString(), body)
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		ids = append(ids, job.ID)
	}
	return ids
}

func itoa(i int) string {
	return strings.TrimSpace(json.Number(jsonInt(i)).String())
}

func jsonInt(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}

func decodeEnvelope(t *testing.T, body []byte) (outbox.Envelope, outbox.WorkAvailableData) {
	t.Helper()
	var env outbox.Envelope
	require.NoError(t, json.Unmarshal(body, &env))
	var data outbox.WorkAvailableData
	require.NoError(t, json.Unmarshal(env.Data, &data))
	return env, data
}

func TestOutbox_PublishesVersionedNotificationToRealBroker(t *testing.T) {
	reset(t)
	broker := newBroker(t, "")
	jobIDs := submitJobs(t, 1)

	stats, err := newPublisher(t, broker).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Claimed)
	require.Equal(t, 1, stats.Published)
	require.Equal(t, 0, stats.Failed)

	bodies := receiveAll(t, broker, 300*time.Millisecond)
	require.Len(t, bodies, 1, "exactly one notification must reach the real broker")

	env, data := decodeEnvelope(t, bodies[0])
	require.Equal(t, "work.available", env.EventType)
	require.Equal(t, 1, env.SchemaVersion)
	require.NotEmpty(t, env.EventID)
	require.False(t, env.OccurredAt.IsZero())
	require.Equal(t, "default", data.Queue)
	require.Equal(t, jobIDs[0], data.JobID)

	// The broker is a notification channel, not a store.
	require.NotContains(t, string(bodies[0]), `"payload"`)

	require.Equal(t, 0, countPendingOutbox(t))
	require.Equal(t, 1, countPublishedOutbox(t))
}

// The core durability claim: a broker outage after commit is a latency problem,
// never a loss problem.
func TestOutbox_BrokerOutageKeepsJobDurableThenRecovers(t *testing.T) {
	reset(t)
	proxy := newFlakyProxy(t, brokerEndpoint())

	// Construct the client while the broker is reachable, then break the network.
	brokerThroughProxy := newBroker(t, proxy.URL())
	directBroker := newBroker(t, "")
	publisher := newPublisher(t, brokerThroughProxy)

	jobIDs := submitJobs(t, 3)
	proxy.Stop()

	stats, err := publisher.RunOnce(context.Background())
	require.NoError(t, err, "a broker outage must not fail the whole pass")
	require.Equal(t, 3, stats.Claimed)
	require.Equal(t, 0, stats.Published)
	require.Equal(t, 3, stats.Failed)

	// The jobs are still durable and their notifications are still pending.
	require.Equal(t, 3, countRows(t, "jobs"))
	require.Equal(t, 3, countPendingOutbox(t))
	require.Equal(t, 0, countPublishedOutbox(t))

	// The failure was recorded and a retry was scheduled.
	var withError int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE last_error IS NOT NULL AND attempts >= 1`).Scan(&withError))
	require.Equal(t, 3, withError)

	require.Empty(t, receiveAll(t, directBroker, 200*time.Millisecond),
		"nothing may reach the broker while it is unavailable")

	// Restore the broker. Publication must resume with no resubmission.
	proxy.Start()
	eventually(t, 20*time.Second, "pending events publish after recovery", func() bool {
		if _, err := publisher.RunOnce(context.Background()); err != nil {
			return false
		}
		return countPendingOutbox(t) == 0
	})

	require.Equal(t, 3, countPublishedOutbox(t))
	require.Equal(t, 3, countRows(t, "jobs"), "recovery must not create or duplicate jobs")

	bodies := receiveAll(t, directBroker, 500*time.Millisecond)
	require.Len(t, bodies, 3)

	got := map[string]bool{}
	for _, b := range bodies {
		_, data := decodeEnvelope(t, b)
		got[data.JobID] = true
	}
	for _, id := range jobIDs {
		require.True(t, got[id], "job %s was never notified after recovery", id)
	}
}

// SKIP LOCKED is what makes publisher replicas safe. Without it, two publishers
// scanning at the same instant would both claim the same rows.
func TestOutbox_ConcurrentPublishersPublishEachEventExactlyOnce(t *testing.T) {
	reset(t)
	broker := newBroker(t, "")

	const events = 40
	submitJobs(t, events)

	const publishers = 4
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		total outbox.Stats
	)
	start := make(chan struct{})
	for i := 0; i < publishers; i++ {
		p := newPublisher(t, broker)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Several passes each, so publishers genuinely interleave.
			for pass := 0; pass < 5; pass++ {
				stats, err := p.RunOnce(context.Background())
				if err != nil {
					return
				}
				mu.Lock()
				total.Claimed += stats.Claimed
				total.Published += stats.Published
				total.Failed += stats.Failed
				total.AlreadyPublished += stats.AlreadyPublished
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, 0, total.Failed)
	require.Equal(t, events, total.Published, "each event may be marked published exactly once")
	require.Equal(t, 0, countPendingOutbox(t))
	require.Equal(t, events, countPublishedOutbox(t))

	bodies := receiveAll(t, broker, 700*time.Millisecond)
	require.Len(t, bodies, events, "concurrent publishers must not duplicate deliveries")

	seen := map[string]int{}
	for _, b := range bodies {
		env, _ := decodeEnvelope(t, b)
		seen[env.EventID]++
	}
	require.Len(t, seen, events)
	for id, n := range seen {
		require.Equal(t, 1, n, "event %s was delivered %d times", id, n)
	}
}

// A claimed event is invisible to other publishers for the claim timeout, so a
// second publisher cannot duplicate work that is still in flight.
func TestOutbox_ClaimedEventIsInvisibleToOtherPublishers(t *testing.T) {
	reset(t)
	submitJobs(t, 2)
	store := outbox.NewStore(testPool)

	first, err := store.ClaimDue(context.Background(), 10, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, first, 2)

	second, err := store.ClaimDue(context.Background(), 10, 30*time.Second)
	require.NoError(t, err)
	require.Empty(t, second, "already-claimed events must not be claimable again")

	require.Equal(t, 1, first[0].Attempts, "claiming records an attempt")
}

// The publish-before-mark crash window, reproduced deterministically.
//
// This duplicate is expected and documented (docs/adr/0004-transactional-outbox.md):
// notifications are advisory, so a duplicate cannot cause duplicate execution.
// The test exists to prove the behavior is understood, not to prevent it.
func TestOutbox_PublishBeforeMarkWindowRepublishesExactlyThatEvent(t *testing.T) {
	reset(t)
	broker := newBroker(t, "")
	submitJobs(t, 1)
	store := outbox.NewStore(testPool)

	// Claim and publish, then "crash" before marking.
	claimed, err := store.ClaimDue(context.Background(), 10, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	body, err := claimed[0].Body()
	require.NoError(t, err)
	require.NoError(t, broker.Publish(context.Background(), body))

	require.Equal(t, 1, countPendingOutbox(t), "the event is published but still unmarked")

	// The visibility window expires and another publisher picks it up.
	require.NoError(t, store.ReleaseClaim(context.Background(), claimed[0].ID))

	stats, err := newPublisher(t, broker).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Published)

	bodies := receiveAll(t, broker, 500*time.Millisecond)
	require.Len(t, bodies, 2, "the documented at-least-once window republishes the event")

	env1, _ := decodeEnvelope(t, bodies[0])
	env2, _ := decodeEnvelope(t, bodies[1])
	require.Equal(t, env1.EventID, env2.EventID, "the duplicate is the same event, not a second one")

	// Whatever happened on the wire, the durable record settles exactly once.
	require.Equal(t, 0, countPendingOutbox(t))
	require.Equal(t, 1, countPublishedOutbox(t))
	require.Equal(t, 1, countRows(t, "jobs"))
}

// Delivery state lives in PostgreSQL, so a publisher process is disposable.
func TestOutbox_SurvivesPublisherRestart(t *testing.T) {
	reset(t)
	proxy := newFlakyProxy(t, brokerEndpoint())
	brokerThroughProxy := newBroker(t, proxy.URL())
	directBroker := newBroker(t, "")

	submitJobs(t, 5)

	// A publisher fails everything, then goes away entirely.
	proxy.Stop()
	doomed := newPublisher(t, brokerThroughProxy)
	stats, err := doomed.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 5, stats.Failed)
	require.Equal(t, 5, countPendingOutbox(t))

	// A brand new publisher, with no shared memory, finishes the work.
	proxy.Start()
	replacement := newPublisher(t, newBroker(t, proxy.URL()))
	eventually(t, 20*time.Second, "replacement publisher drains the outbox", func() bool {
		if _, err := replacement.RunOnce(context.Background()); err != nil {
			return false
		}
		return countPendingOutbox(t) == 0
	})

	require.Equal(t, 5, countPublishedOutbox(t))
	require.Len(t, receiveAll(t, directBroker, 500*time.Millisecond), 5)
}

// A failed publish must schedule a retry rather than spin, and must never mark
// the event terminally failed: the job it refers to is already durable and would
// otherwise sit queued forever.
func TestOutbox_FailedPublishSchedulesABoundedRetry(t *testing.T) {
	reset(t)
	proxy := newFlakyProxy(t, brokerEndpoint())
	publisher := newPublisher(t, newBroker(t, proxy.URL()))
	submitJobs(t, 1)

	proxy.Stop()
	_, err := publisher.RunOnce(context.Background())
	require.NoError(t, err)

	var (
		status      string
		attempts    int
		lastError   *string
		notYetDue   bool
		withinBound bool
	)
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT status, attempts, last_error,
		       available_at > now(),
		       available_at <= now() + interval '1 second'
		FROM outbox_events`).Scan(&status, &attempts, &lastError, &notYetDue, &withinBound))

	require.Equal(t, "PENDING", status, "an undeliverable notification stays retryable")
	require.Equal(t, 1, attempts)
	require.NotNil(t, lastError)
	require.True(t, notYetDue, "a failed event must back off instead of being retried immediately")
	require.True(t, withinBound, "backoff must stay within the configured maximum")

	// It becomes due again on its own once the backoff elapses.
	proxy.Start()
	eventually(t, 10*time.Second, "event becomes due and publishes", func() bool {
		if _, err := publisher.RunOnce(context.Background()); err != nil {
			return false
		}
		return countPublishedOutbox(t) == 1
	})
}

// The publisher loop keeps running when the broker is down and drains the
// backlog once it returns, without anyone resubmitting a job.
func TestOutbox_RunLoopDrainsBacklogAfterRecovery(t *testing.T) {
	reset(t)
	proxy := newFlakyProxy(t, brokerEndpoint())
	directBroker := newBroker(t, "")
	publisher := newPublisher(t, newBroker(t, proxy.URL()))

	proxy.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- publisher.Run(ctx) }()

	submitJobs(t, 4)
	eventually(t, 10*time.Second, "the loop attempts delivery while the broker is down", func() bool {
		var attempted int
		_ = testPool.QueryRow(context.Background(),
			`SELECT count(*) FROM outbox_events WHERE attempts > 0`).Scan(&attempted)
		return attempted == 4
	})
	require.Equal(t, 4, countPendingOutbox(t))

	proxy.Start()
	eventually(t, 30*time.Second, "the loop drains the backlog after recovery", func() bool {
		return countPendingOutbox(t) == 0
	})

	cancel()
	require.NoError(t, <-done)

	require.Equal(t, 4, countPublishedOutbox(t))
	require.Len(t, receiveAll(t, directBroker, 500*time.Millisecond), 4)
}

// The API and the publisher are separate processes; a job submitted through HTTP
// becomes a broker notification with no shared memory between them.
func TestOutbox_EndToEndFromHTTPSubmissionToBrokerNotification(t *testing.T) {
	reset(t)
	broker := newBroker(t, "")

	srv := httptest.NewServer(api.NewServer(jobs.NewStore(testPool),
		api.Config{MaxRequestBytes: 256 * 1024, DevScope: testScope}, discardLogger()).Handler())
	defer srv.Close()

	resp, job := submit(t, srv.URL, "key-e2e", jobBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	publisher := newPublisher(t, broker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = publisher.Run(ctx) }()

	eventually(t, 15*time.Second, "the notification is published", func() bool {
		return countPublishedOutbox(t) == 1
	})

	bodies := receiveAll(t, broker, 500*time.Millisecond)
	require.Len(t, bodies, 1)
	_, data := decodeEnvelope(t, bodies[0])
	require.Equal(t, job.ID, data.JobID)
	require.Equal(t, "default", data.Queue)
}
