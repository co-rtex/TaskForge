//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/reconciler"
	"github.com/co-rtex/TaskForge/internal/scheduler"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// e2eLease is short enough that a stalled attempt's lease lapses inside a test,
// and long enough that a healthy worker never lapses by accident.
const e2eLease = 4 * time.Second

// e2eStack wires the real components a running deployment has: the real API
// over real PostgreSQL, the real outbox publisher over real ElasticMQ, the real
// scheduler, the real reconciler, and a DB-less worker talking HTTP.
//
// Nothing here is simulated. A test that hand-wrote the recovery rows would
// prove something about the test rather than about TaskForge.
type e2eStack struct {
	baseURL  string
	broker   queue.Broker
	control  *workerruntime.Client
	registry *workerruntime.Registry

	stop       context.CancelFunc
	background sync.WaitGroup
}

// startE2EStack brings up everything except the worker, which each test wires
// with its own handlers.
//
// renotifyAfter is explicit per test rather than shared, because it is the one
// setting whose value changes what a test is about. A long interval keeps
// bounded re-notification out of the way of tests that are about something
// else; the stranded-work test sets it short precisely so the repair happens
// inside the test.
func startE2EStack(t *testing.T, registry *workerruntime.Registry, renotifyAfter time.Duration) *e2eStack {
	t.Helper()
	reset(t)

	control := workers.NewStore(testPool, workers.StoreConfig{
		LeaseDuration: e2eLease,
		RetryPolicy:   integrationRetryPolicy(),
	})
	httpServer := newControlServer(t, control)

	// Its own queue: these tests run real workers that legitimately decline to
	// acknowledge some notifications — for work another attempt already took, or
	// for a job that was canceled — and an unacknowledged message stays in
	// flight for the queue's visibility timeout. reset() cannot drain what it
	// cannot see, so sharing a queue would leak those deliveries into whichever
	// test ran next.
	broker := newBrokerForQueue(t, createIsolatedBrokerQueue(t, "taskforge-e2e-"))
	stack := &e2eStack{
		baseURL:  httpServer.URL,
		broker:   broker,
		control:  workerruntime.NewClient(httpServer.URL, &http.Client{Timeout: 10 * time.Second}),
		registry: registry,
	}

	ctx, cancel := context.WithCancel(context.Background())
	stack.stop = cancel
	t.Cleanup(func() {
		cancel()
		stack.background.Wait()
	})

	// The real publisher, scheduler, and reconciler, each on its own loop.
	publisher := newPublisher(t, broker)
	stack.background.Add(1)
	go func() {
		defer stack.background.Done()
		for ctx.Err() == nil {
			if _, err := publisher.RunOnce(ctx); err != nil && ctx.Err() == nil {
				t.Logf("publisher pass failed: %v", err)
			}
			if !sleepCtx(ctx, 25*time.Millisecond) {
				return
			}
		}
	}()

	schedulerEngine := scheduler.New(jobStore(), scheduler.Config{
		PollInterval: 25 * time.Millisecond, BatchSize: 50, RenotifyAfter: renotifyAfter,
	}, discardLogger())
	stack.background.Add(1)
	go func() {
		defer stack.background.Done()
		for ctx.Err() == nil {
			if _, err := schedulerEngine.RunOnce(ctx); err != nil && ctx.Err() == nil {
				t.Logf("scheduler pass failed: %v", err)
			}
			if !sleepCtx(ctx, 25*time.Millisecond) {
				return
			}
		}
	}()

	reconcilerEngine := reconciler.New(control, reconciler.Config{
		StaleAfter: 3 * time.Second, PollInterval: 25 * time.Millisecond, BatchSize: 50,
	}, discardLogger())
	stack.background.Add(1)
	go func() {
		defer stack.background.Done()
		for ctx.Err() == nil {
			if _, err := reconcilerEngine.RunOnce(ctx); err != nil && ctx.Err() == nil {
				t.Logf("reconciler pass failed: %v", err)
			}
			if !sleepCtx(ctx, 25*time.Millisecond) {
				return
			}
		}
	}()

	return stack
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// startWorker runs a real DB-less worker against the stack until the test ends.
func (s *e2eStack) startWorker(t *testing.T, name string, concurrency int) {
	t.Helper()
	runner := workerruntime.NewRunner(s.control, s.broker, s.registry, workerruntime.RunnerConfig{
		Registration: workers.Registration{
			SessionID: uuid.New(), Name: name, Hostname: name + ".local",
			WorkerGroup: "default", ConcurrencyLimit: concurrency, Capabilities: []string{"cpu"},
		},
		Queue: "default", PollWait: time.Second,
		RetryAttempts: 3, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond, SessionStaleAfter: 3 * time.Second,
		RenewInterval:   time.Second,
		ShutdownTimeout: 2 * time.Second,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s.background.Add(1)
	go func() {
		defer s.background.Done()
		done <- runner.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop after cancellation")
		}
	})
}

func (s *e2eStack) submit(t *testing.T, key, body string) api.JobResponse {
	t.Helper()
	response, job := submit(t, s.baseURL, key, body)
	require.Equal(t, http.StatusCreated, response.StatusCode)
	return job
}

func (s *e2eStack) post(t *testing.T, path, key string) (*http.Response, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, s.baseURL+path, strings.NewReader(""))
	require.NoError(t, err)
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	require.NoError(t, err)
	return response, body
}

// consumeAndDiscard takes every message off the broker and throws it away,
// returning how many it saw. It polls to a deadline rather than sampling once,
// because an SQS-style short poll legitimately returns an arbitrary subset.
//
// This is how a lost delivery is reproduced honestly: the notification really
// was published and really was received, and then nothing acted on it.
func consumeAndDiscard(t *testing.T, broker queue.Receiver, within time.Duration) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()

	seen := 0
	deadline := time.Now().Add(within)
	quietUntil := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		messages, err := broker.Receive(ctx, 10, 0)
		require.NoError(t, err)
		for _, message := range messages {
			require.NoError(t, broker.Delete(ctx, message.ReceiptHandle))
			seen++
		}
		if len(messages) > 0 {
			quietUntil = time.Now().Add(500 * time.Millisecond)
			continue
		}
		if seen > 0 && time.Now().After(quietUntil) {
			return seen
		}
		time.Sleep(20 * time.Millisecond)
	}
	return seen
}

func awaitJobStatus(t *testing.T, jobID uuid.UUID, want string, within time.Duration) {
	t.Helper()
	eventually(t, within, fmt.Sprintf("job %s reaches %s", jobID, want), func() bool {
		var status string
		err := testPool.QueryRow(context.Background(),
			`SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&status)
		return err == nil && status == want
	})
}

// flakyHandler fails its first n invocations and then succeeds, which is what a
// transient dependency outage looks like from inside a handler.
type flakyHandler struct {
	failures int32
	calls    atomic.Int32
}

func (h *flakyHandler) Execute(context.Context, workerruntime.Execution) (json.RawMessage, error) {
	if h.calls.Add(1) <= h.failures {
		return nil, workerruntime.Retryable("upstream_5xx", "upstream returned 502")
	}
	return json.RawMessage(`{}`), nil
}

// TestE2E_RetryRecoversThroughTheRealSchedulerAndBroker is the whole retry
// story with nothing simulated: a real handler fails, the real control plane
// schedules a backoff, the real scheduler promotes the job when it comes due,
// the real publisher delivers the notification through real ElasticMQ, and the
// real worker claims a second attempt and finishes it.
func TestE2E_RetryRecoversThroughTheRealSchedulerAndBroker(t *testing.T) {
	handler := &flakyHandler{failures: 1}
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", handler))

	stack := startE2EStack(t, registry, time.Minute)
	stack.startWorker(t, "e2e-retry-worker", 2)

	job := stack.submit(t, "e2e-retry",
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1},"max_attempts":3}`)
	jobID := uuid.MustParse(job.ID)

	awaitJobStatus(t, jobID, "SUCCEEDED", 45*time.Second)

	require.Equal(t, []string{"FAILED", "SUCCEEDED"}, attemptHistory(t, jobID),
		"the first attempt's failure stays visible in history")
	require.EqualValues(t, 2, handler.calls.Load())

	// The failed attempt carries the durable decision that produced the retry.
	var failedAttempt uuid.UUID
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT id FROM job_attempts WHERE job_id = $1 AND attempt_number = 1`, jobID).Scan(&failedAttempt))
	row := readAttemptOutcome(t, failedAttempt)
	require.Equal(t, "RETRYABLE", *row.failureClass)
	require.Equal(t, "upstream_5xx", *row.errorCode)
	require.Equal(t, "upstream returned 502", *row.errorMessage)
	require.NotNil(t, row.retryAt)
	require.NotNil(t, row.outcomeID)

	require.Equal(t, []string{"RELEASED", "COMPLETED"}, leaseHistory(t, jobID))
	require.Equal(t, 0, countActiveLeases(t))
	require.Empty(t, dlqRows(t, jobID))

	// Two notifications: the submission's, and the one the scheduler wrote when
	// the retry became due. Their generations say which is which.
	events := eventsForJob(t, jobID)
	require.GreaterOrEqual(t, len(events), 2)
	require.Equal(t, 1, events[0].Generation)
	require.Equal(t, 2, events[len(events)-1].Generation)
}

// TestE2E_DelayedJobIsPromotedAndExecuted proves a delayed submission is durable
// while it waits, invisible to the broker, and then runs once PostgreSQL says it
// is due — with the real scheduler making that decision.
func TestE2E_DelayedJobIsPromotedAndExecuted(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))

	stack := startE2EStack(t, registry, time.Minute)
	stack.startWorker(t, "e2e-delayed-worker", 2)

	scheduled := time.Now().Add(2 * time.Second).UTC()
	job := stack.submit(t, "e2e-delayed", fmt.Sprintf(
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1},"scheduled_at":%q}`,
		scheduled.Format(time.RFC3339Nano)))
	jobID := uuid.MustParse(job.ID)
	require.Equal(t, "PENDING", job.Status)

	// Nothing may reach the broker before it is due.
	require.Equal(t, "PENDING", readJob(t, jobID).status)
	require.Empty(t, eventsForJob(t, jobID))

	awaitJobStatus(t, jobID, "SUCCEEDED", 45*time.Second)
	require.Equal(t, []string{"SUCCEEDED"}, attemptHistory(t, jobID))

	events := eventsForJob(t, jobID)
	require.Len(t, events, 1, "the scheduler wrote exactly one notification")
	require.Equal(t, 1, events[0].Generation)

	var startedAt time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT started_at FROM job_attempts WHERE job_id = $1`, jobID).Scan(&startedAt))
	require.False(t, startedAt.Before(scheduled),
		"a delayed job must not begin executing before the instant it was scheduled for")
}

// TestE2E_LostNotificationIsRepairedByBoundedRenotification is the reachability
// repair, exercised against real ElasticMQ.
//
// The submission's notification is genuinely published and then genuinely
// consumed and discarded without claiming — the shape of a delivery that was
// lost. Nothing else will wake a worker, so the job sits claimable and unclaimed
// until the scheduler notices and writes a replacement.
func TestE2E_LostNotificationIsRepairedByBoundedRenotification(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	// Short on purpose: this test is about the repair firing.
	stack := startE2EStack(t, registry, 750*time.Millisecond)

	job := stack.submit(t, "e2e-stranded",
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1}}`)
	jobID := uuid.MustParse(job.ID)

	// Wait for the real publisher to deliver it, then take the message off the
	// broker and throw it away. No worker is running yet, so nothing claimed it.
	eventually(t, 20*time.Second, "the submission notification is published", func() bool {
		events := eventsForJob(t, jobID)
		return len(events) == 1 && events[0].Status == "PUBLISHED"
	})
	require.Positive(t, consumeAndDiscard(t, stack.broker, 20*time.Second),
		"the delivery really happened before it was discarded")
	require.Equal(t, "QUEUED", readJob(t, jobID).status,
		"the job is claimable and unclaimed: reachable by nothing")

	// Now a worker starts. It cannot find the job on its own — the only
	// notification is gone — so only re-notification can make it reachable.
	stack.startWorker(t, "e2e-stranded-worker", 2)
	awaitJobStatus(t, jobID, "SUCCEEDED", 45*time.Second)

	events := eventsForJob(t, jobID)
	require.GreaterOrEqual(t, len(events), 2, "a replacement notification was written")
	require.Equal(t, events[0].Generation, events[1].Generation,
		"a re-notification advertises the same eligibility transition, not a new one")
	require.Equal(t, 1, readJob(t, jobID).generation,
		"re-notification must not open a new generation")
	require.Equal(t, []string{"SUCCEEDED"}, attemptHistory(t, jobID))
	require.Equal(t, 1, countRows(t, "job_attempts"),
		"however many notifications exist, one job produces one attempt")
}

// blockingCooperativeHandler blocks until its context is canceled, then returns.
// It is a test-only trusted handler injected through the existing registry seam;
// no production handler was made slow to widen a window.
type blockingCooperativeHandler struct {
	entered chan struct{}
	once    atomic.Bool
	cause   atomic.Value
}

func (h *blockingCooperativeHandler) Execute(ctx context.Context, _ workerruntime.Execution) (json.RawMessage, error) {
	if h.once.CompareAndSwap(false, true) {
		close(h.entered)
	}
	<-ctx.Done()
	h.cause.Store(context.Cause(ctx).Error())
	return nil, ctx.Err()
}

// TestE2E_CancellationReachesARunningHandlerThroughTheHeartbeat is active
// cancellation end to end: an operator calls the public API, the directive rides
// the worker's own heartbeat loop, the handler stops cooperatively, and the
// worker reports the fenced acknowledgment.
//
// No broker delivery is involved in any of that, which is the point.
func TestE2E_CancellationReachesARunningHandlerThroughTheHeartbeat(t *testing.T) {
	handler := &blockingCooperativeHandler{entered: make(chan struct{})}
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", handler))

	stack := startE2EStack(t, registry, time.Minute)
	stack.startWorker(t, "e2e-cancel-worker", 2)

	job := stack.submit(t, "e2e-cancel",
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1},"timeout_seconds":3600}`)
	jobID := uuid.MustParse(job.ID)

	select {
	case <-handler.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the handler never started executing")
	}
	awaitJobStatus(t, jobID, "RUNNING", 10*time.Second)

	// Drain the broker so nothing else could possibly wake the worker: whatever
	// reaches it from here reaches it on the heartbeat.
	drainBroker(t, stack.broker)

	eventsBeforeCancel := len(eventsForJob(t, jobID))

	response, body := stack.post(t, "/v1/jobs/"+job.ID+"/cancel", "")
	require.Equal(t, http.StatusOK, response.StatusCode)
	var cancelBody api.CancelResponse
	require.NoError(t, json.Unmarshal(body, &cancelBody))
	require.Equal(t, "CANCEL_REQUESTED", cancelBody.Status,
		"a running job is asked to stop rather than declared canceled")

	awaitJobStatus(t, jobID, "CANCELED", 30*time.Second)

	require.Equal(t, []string{"CANCELED"}, attemptHistory(t, jobID))
	require.Equal(t, []string{"RELEASED"}, leaseHistory(t, jobID),
		"a cooperative acknowledgment hands authority back rather than losing it")
	require.Equal(t, 0, countActiveLeases(t))
	require.Empty(t, dlqRows(t, jobID), "cancellation never creates a dead-letter entry")

	var attemptID uuid.UUID
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT id FROM job_attempts WHERE job_id = $1`, jobID).Scan(&attemptID))
	row := readAttemptOutcome(t, attemptID)
	require.Equal(t, "CANCELED", *row.failureClass)
	require.NotNil(t, row.outcomeID, "the worker acknowledged under a retained identity")
	require.Equal(t, "the job was canceled", handler.cause.Load(),
		"the handler saw user cancellation, not shutdown or authority loss")

	// Cancelling creates no notification of its own. The count is compared
	// against the snapshot taken before the cancel rather than pinned to one,
	// because a harmless re-notification while the job was still queued is
	// legitimate and says nothing about cancellation.
	require.Len(t, eventsForJob(t, jobID), eventsBeforeCancel,
		"a canceled job must never be advertised again")
}

// stallingHandler ignores its context entirely until released. It is the
// uncooperative case Go cannot terminate, injected only here.
type stallingHandler struct {
	entered chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (h *stallingHandler) Execute(context.Context, workerruntime.Execution) (json.RawMessage, error) {
	if h.once.CompareAndSwap(false, true) {
		close(h.entered)
	}
	<-h.release
	return json.RawMessage(`{}`), nil
}

// TestE2E_TimeoutIsRecordedByReconciliationAndTheHandlerCannotCommit is the
// timeout story end to end, including the limitation TaskForge documents rather
// than hides: an uncooperative handler keeps running, and what is guaranteed is
// that nothing it produces afterwards can be committed.
func TestE2E_TimeoutIsRecordedByReconciliationAndTheHandlerCannotCommit(t *testing.T) {
	handler := &stallingHandler{entered: make(chan struct{}), release: make(chan struct{})}
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", handler))

	stack := startE2EStack(t, registry, time.Minute)
	stack.startWorker(t, "e2e-timeout-worker", 2)

	// timeout_seconds is 1: the smallest budget the schema allows, so the
	// deadline arrives while the handler is still stalled.
	job := stack.submit(t, "e2e-timeout",
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1},"timeout_seconds":1,"max_attempts":1}`)
	jobID := uuid.MustParse(job.ID)

	select {
	case <-handler.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the handler never started executing")
	}

	// The real reconciler records the outcome, with the handler still running.
	eventually(t, 45*time.Second, "the attempt is recorded TIMED_OUT", func() bool {
		return len(attemptHistory(t, jobID)) == 1 && attemptHistory(t, jobID)[0] == "TIMED_OUT"
	})
	awaitJobStatus(t, jobID, "DEAD_LETTERED", 20*time.Second)

	var attemptID uuid.UUID
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT id FROM job_attempts WHERE job_id = $1`, jobID).Scan(&attemptID))
	row := readAttemptOutcome(t, attemptID)
	require.Equal(t, "TIMED_OUT", row.status, "not ABANDONED, and not FAILED")
	require.Equal(t, "TIMED_OUT", *row.failureClass)
	require.NotNil(t, row.timeoutAt)
	require.Equal(t, []string{"ATTEMPTS_EXHAUSTED"}, dlqRows(t, jobID))
	frozen := readAttemptOutcome(t, attemptID)

	// Now let the uncooperative handler finish. It will try to report success,
	// and every fence rejects it.
	close(handler.release)
	time.Sleep(500 * time.Millisecond)

	require.Equal(t, frozen, readAttemptOutcome(t, attemptID),
		"a handler that returns after its timeout must not move a single stored field")
	require.Equal(t, "DEAD_LETTERED", readJob(t, jobID).status)
	require.Equal(t, 1, countRows(t, "job_attempts"))
	require.Equal(t, 1, countRows(t, "dlq_entries"))
}

// TestE2E_DeadLetteredJobIsListedAndReplayedThroughThePublicAPI closes the
// operator loop: a job fails permanently, appears in the DLQ, is replayed
// through the public API, and the replacement runs to success.
func TestE2E_DeadLetteredJobIsListedAndReplayedThroughThePublicAPI(t *testing.T) {
	// The first invocation fails permanently, so the original dead-letters
	// immediately without burning its remaining budget. Every later invocation
	// succeeds, so the replacement created by the replay runs to completion.
	var invocations atomic.Int32
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo",
		workerruntime.HandlerFunc(func(context.Context, workerruntime.Execution) (json.RawMessage, error) {
			if invocations.Add(1) == 1 {
				return nil, workerruntime.Permanent("invalid_payload", "the payload names no known account")
			}
			return json.RawMessage(`{}`), nil
		})))

	stack := startE2EStack(t, registry, time.Minute)
	stack.startWorker(t, "e2e-dlq-worker", 2)

	job := stack.submit(t, "e2e-dlq",
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1},"max_attempts":3}`)
	jobID := uuid.MustParse(job.ID)

	awaitJobStatus(t, jobID, "DEAD_LETTERED", 45*time.Second)
	require.Equal(t, []string{"FAILED"}, attemptHistory(t, jobID),
		"a permanent failure must not burn the remaining attempt budget")

	// It is listed, through the public endpoint, with bounded metadata.
	request, err := http.NewRequest(http.MethodGet, stack.baseURL+"/v1/dlq", nil)
	require.NoError(t, err)
	listResponse, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer listResponse.Body.Close()
	require.Equal(t, http.StatusOK, listResponse.StatusCode)
	var page api.DLQPageResponse
	require.NoError(t, json.NewDecoder(listResponse.Body).Decode(&page))
	require.Len(t, page.Entries, 1)
	require.Equal(t, job.ID, page.Entries[0].JobID)
	require.Equal(t, "PERMANENT_FAILURE", page.Entries[0].Reason)
	require.Equal(t, "invalid_payload", *page.Entries[0].ErrorCode)

	// Replayed through the public endpoint, and the replacement runs.
	response, body := stack.post(t, "/v1/dlq/"+job.ID+"/replay", "e2e-replay-key")
	require.Equal(t, http.StatusCreated, response.StatusCode)
	var replay api.ReplayResponse
	require.NoError(t, json.Unmarshal(body, &replay))
	require.NotEqual(t, job.ID, replay.Replacement.ID)
	require.Equal(t, job.ID, *replay.Replacement.ReplayedFromJobID)

	replacementID := uuid.MustParse(replay.Replacement.ID)
	awaitJobStatus(t, replacementID, "SUCCEEDED", 45*time.Second)

	// The original is untouched, and is still the DLQ's.
	require.Equal(t, "DEAD_LETTERED", readJob(t, jobID).status)
	require.Equal(t, []string{"FAILED"}, attemptHistory(t, jobID))
	require.Equal(t, 1, countRows(t, "dlq_entries"))

	// An ambiguous retry of the replay returns the same replacement.
	response, body = stack.post(t, "/v1/jobs/"+job.ID+"/retry", "e2e-replay-key")
	require.Equal(t, http.StatusOK, response.StatusCode)
	var repeat api.ReplayResponse
	require.NoError(t, json.Unmarshal(body, &repeat))
	require.True(t, repeat.Replayed)
	require.Equal(t, replay.Replacement.ID, repeat.Replacement.ID)
}

// TestE2E_CancellingAQueuedJobStopsItBeforeAnyWorkerClaimsIt is the other
// cancellation shape, and it deliberately runs with no worker at all: the job is
// terminal before anything could have claimed it.
func TestE2E_CancellingAQueuedJobStopsItBeforeAnyWorkerClaimsIt(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	stack := startE2EStack(t, registry, time.Minute)

	job := stack.submit(t, "e2e-cancel-queued",
		`{"queue":"default","job_type":"demo.echo","payload":{"m":1}}`)
	jobID := uuid.MustParse(job.ID)

	response, body := stack.post(t, "/v1/jobs/"+job.ID+"/cancel", "")
	require.Equal(t, http.StatusOK, response.StatusCode)
	var cancelBody api.CancelResponse
	require.NoError(t, json.Unmarshal(body, &cancelBody))
	require.Equal(t, "CANCELED", cancelBody.Status)

	// Only now does a worker start. The notification the publisher already
	// delivered is harmless: the claim predicate finds no QUEUED job.
	stack.startWorker(t, "e2e-cancel-queued-worker", 2)
	time.Sleep(2 * time.Second)

	require.Equal(t, "CANCELED", readJob(t, jobID).status)
	require.Equal(t, 0, countRows(t, "job_attempts"),
		"a job canceled before any claim must never produce an attempt")
	require.Equal(t, 0, countActiveLeases(t))
}

// startWorkerVia is startWorker pointed at a different control-plane URL, so a
// test can interpose on the worker's own HTTP traffic without changing what the
// control plane, the API, or the runner actually do.
func (s *e2eStack) startWorkerVia(t *testing.T, name string, concurrency int, controlURL string) {
	t.Helper()
	control := workerruntime.NewClient(controlURL, &http.Client{Timeout: 10 * time.Second})
	runner := workerruntime.NewRunner(control, s.broker, s.registry, workerruntime.RunnerConfig{
		Registration: workers.Registration{
			SessionID: uuid.New(), Name: name, Hostname: name + ".local",
			WorkerGroup: "default", ConcurrencyLimit: concurrency, Capabilities: []string{"cpu"},
		},
		Queue: "default", PollWait: time.Second,
		RetryAttempts: 3, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond, SessionStaleAfter: 3 * time.Second,
		RenewInterval:   time.Second,
		ShutdownTimeout: 2 * time.Second,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s.background.Add(1)
	go func() {
		defer s.background.Done()
		done <- runner.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop after cancellation")
		}
	})
}

// startGate holds the worker's first Start request in flight and reports what
// the control plane eventually answered.
//
// It is a transparent proxy in front of the real API. It does not fabricate a
// response, shortcut a call, or change a body — it only delays requests long
// enough for a real cancellation to commit first, which widens a window that is
// otherwise microseconds wide and therefore untestable. Everything a request
// meets after the delay is the production path.
//
// Heartbeats are held for the same window, and that is not incidental. A
// heartbeat that landed in it would carry the cancellation directive back to
// the worker, and the worker would then acknowledge from the directive rather
// than from Start's answer — leaving the typed refusal untested while the test
// still passed. Holding both makes Start the only thing this process can learn
// the cancellation from, which is the case the typed refusal exists for.
type startGate struct {
	url     string
	arrived chan struct{}
	// Two releases, because the two holds end at different moments: Start is
	// released so the refusal can happen, and heartbeats stay held until the
	// outcome has been observed.
	releaseStart     chan struct{}
	releaseHeartbeat chan struct{}

	status   atomic.Int64
	code     atomic.Value
	requests atomic.Int64
	// forwardedHeartbeats counts heartbeats forwarded after Start was parked.
	// It must still be zero when the job reaches CANCELED, or a directive could
	// have arrived that way and the typed refusal would be untested.
	forwardedHeartbeats atomic.Int64
}

func newStartGate(t *testing.T, upstream string) *startGate {
	t.Helper()
	target, err := url.Parse(upstream)
	require.NoError(t, err)

	gate := &startGate{
		arrived:          make(chan struct{}, 1),
		releaseStart:     make(chan struct{}),
		releaseHeartbeat: make(chan struct{}),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(response *http.Response) error {
		if !strings.HasSuffix(response.Request.URL.Path, "/start") {
			return nil
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			return err
		}
		_ = response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = int64(len(body))
		gate.status.Store(int64(response.StatusCode))
		var envelope api.ErrorBody
		if json.Unmarshal(body, &envelope) == nil {
			gate.code.Store(envelope.Error.Code)
		}
		return nil
	}

	parked := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/start") && gate.requests.Add(1) == 1:
			close(parked)
			select {
			case gate.arrived <- struct{}{}:
			default:
			}
			<-gate.releaseStart
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			select {
			case <-parked:
				<-gate.releaseHeartbeat
				gate.forwardedHeartbeats.Add(1)
			default:
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		// Nothing may stay blocked after the test, however it ended.
		select {
		case <-gate.releaseHeartbeat:
		default:
			close(gate.releaseHeartbeat)
		}
		server.Close()
	})
	gate.url = server.URL
	return gate
}

// TestE2E_CancellationWinningBeforeStartIsAcknowledgedByTheWorker is the
// cancel-first race run through every real component.
//
// The window is real and narrow: a claim commits, and before the worker's Start
// reaches the control plane an operator cancels the job. Everything here is
// production code — the public API commits the cancellation, the control plane
// refuses Start, the API renders the refusal, and a DB-less worker over HTTP
// decides what to do with it. Only the arrival time of one HTTP request is
// controlled, because otherwise the window is too short to observe.
//
// What must not happen is the worker treating the refusal as an ordinary
// conflict and dropping the attempt. The job would then sit in CANCEL_REQUESTED
// for the rest of its lease window with a cooperative worker holding it and
// nothing to do — the exact wait the acknowledgment exists to avoid.
func TestE2E_CancellationWinningBeforeStartIsAcknowledgedByTheWorker(t *testing.T) {
	var handlerRan atomic.Bool
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.HandlerFunc(
		func(context.Context, workerruntime.Execution) (json.RawMessage, error) {
			handlerRan.Store(true)
			return json.RawMessage(`{}`), nil
		})))

	stack := startE2EStack(t, registry, time.Hour)
	gate := newStartGate(t, stack.baseURL)
	stack.startWorkerVia(t, "cancel-before-start", 1, gate.url)

	job := stack.submit(t, "e2e-cancel-before-start",
		`{"queue":"default","job_type":"demo.echo","payload":{"n":1},"timeout_seconds":3600}`)
	jobID := uuid.MustParse(job.ID)

	// The claim has committed and the attempt exists; Start is held at the gate.
	select {
	case <-gate.arrived:
	case <-time.After(20 * time.Second):
		t.Fatal("the worker never reached Start")
	}
	awaitJobStatus(t, jobID, "LEASED", 10*time.Second)

	// A real operator cancellation through the real public API, which wins
	// because Start has not been accepted yet.
	response, body := stack.post(t, "/v1/jobs/"+job.ID+"/cancel", "")
	require.Equalf(t, http.StatusOK, response.StatusCode, "cancel returned %s", body)
	var canceled api.CancelResponse
	require.NoError(t, json.Unmarshal(body, &canceled))
	require.Equal(t, "CANCEL_REQUESTED", canceled.Status,
		"a claimed job cancels through CANCEL_REQUESTED, because an attempt still holds authority")

	close(gate.releaseStart)

	// The worker acknowledges rather than waiting for the lease to lapse, so the
	// job is terminal long before reconciliation would have reached it.
	awaitJobStatus(t, jobID, "CANCELED", 10*time.Second)

	// Checked before heartbeats are let through: no heartbeat completed between
	// the cancellation committing and the acknowledgment landing, so Start's own
	// refusal is the only thing this worker could have learned it from.
	require.Zero(t, gate.forwardedHeartbeats.Load(),
		"no heartbeat may deliver the directive in this test, or the typed refusal is untested")
	close(gate.releaseHeartbeat)

	require.EqualValues(t, http.StatusConflict, gate.status.Load(),
		"the control plane must refuse a cancel-first Start")
	require.Equal(t, "cancellation_requested", gate.code.Load(),
		"the refusal must be typed, or the worker cannot tell it from an unrelated conflict")

	var attemptStatus string
	var startedAt, finishedAt *time.Time
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT status, started_at, finished_at FROM job_attempts WHERE job_id = $1`, jobID,
	).Scan(&attemptStatus, &startedAt, &finishedAt))
	require.Equal(t, "CANCELED", attemptStatus)
	require.Nil(t, startedAt, "the attempt never started")
	require.NotNil(t, finishedAt, "the acknowledgment finalizes the attempt")
	require.False(t, handlerRan.Load(), "an attempt that never started has nothing to run")

	// The lease is released, so the worker's capacity came back immediately
	// instead of at the end of the lease window.
	var leaseStatus string
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT l.status FROM leases l
		JOIN job_attempts a ON a.id = l.attempt_id
		WHERE a.job_id = $1`, jobID).Scan(&leaseStatus))
	require.Equal(t, "RELEASED", leaseStatus)
}
