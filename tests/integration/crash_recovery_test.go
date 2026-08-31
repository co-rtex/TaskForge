//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/reconciler"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// crashRecoveryLease is short enough that a killed worker's lease lapses inside
// the test, and long enough that a healthy worker never lapses by accident.
const crashRecoveryLease = 2 * time.Second

// killableAPI fronts the real control plane and can sever one worker session's
// authority path, which is how this test "kills" worker A.
//
// Severing at the transport is deliberate. Killing the goroutine is not
// something Go offers, and asserting on hand-written recovery rows would prove
// nothing about the reconciler. What a real crash looks like to PostgreSQL is
// precisely this: heartbeats stop, renewals stop, and the lease lapses on server
// time while the process is still nominally around.
type killableAPI struct {
	control  *workers.Store
	severed  atomic.Pointer[uuid.UUID]
	rejected atomic.Int32
}

func (k *killableAPI) isSevered(sessionID uuid.UUID) bool {
	severed := k.severed.Load()
	if severed == nil || *severed != sessionID {
		return false
	}
	k.rejected.Add(1)
	return true
}

func (k *killableAPI) Register(ctx context.Context, scope string, req workers.Registration) (workers.Session, error) {
	return k.control.Register(ctx, scope, req)
}

func (k *killableAPI) Heartbeat(ctx context.Context, scope string, req workers.HeartbeatRequest) (workers.HeartbeatResult, error) {
	if k.isSevered(req.SessionID) {
		return workers.HeartbeatResult{}, context.DeadlineExceeded
	}
	return k.control.Heartbeat(ctx, scope, req)
}

func (k *killableAPI) Claim(ctx context.Context, scope string, req workers.ClaimRequest) (workers.ClaimResult, error) {
	if k.isSevered(req.SessionID) {
		return workers.ClaimResult{}, context.DeadlineExceeded
	}
	return k.control.Claim(ctx, scope, req)
}

func (k *killableAPI) RenewLease(ctx context.Context, scope string, req workers.RenewalRequest) (workers.RenewalResult, error) {
	if k.isSevered(req.Fence.SessionID) {
		return workers.RenewalResult{}, context.DeadlineExceeded
	}
	return k.control.RenewLease(ctx, scope, req)
}

func (k *killableAPI) Start(ctx context.Context, scope string, fence workers.Fence) error {
	if k.isSevered(fence.SessionID) {
		return context.DeadlineExceeded
	}
	return k.control.Start(ctx, scope, fence)
}

func (k *killableAPI) Succeed(ctx context.Context, scope string, fence workers.Fence) error {
	if k.isSevered(fence.SessionID) {
		return context.DeadlineExceeded
	}
	return k.control.Succeed(ctx, scope, fence)
}

var _ api.WorkerControl = (*killableAPI)(nil)

// blockingEcho is a trusted, test-only handler. It behaves exactly like a real
// cooperative handler that happens to take a long time, and it adds no new
// production handler surface: it is registered only by this test.
type blockingEcho struct {
	started chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (h *blockingEcho) Execute(ctx context.Context, execution workerruntime.Execution) (json.RawMessage, error) {
	if h.once.CompareAndSwap(false, true) {
		close(h.started)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.release:
		result := make(json.RawMessage, len(execution.Payload))
		copy(result, execution.Payload)
		return result, nil
	}
}

// TestWorkerCrash_RecoversThroughTheRealOutboxAndBrokerPath is the load-bearing
// M3 story, end to end and with nothing hand-written:
//
//	worker A reaches RUNNING -> its authority path dies -> its session goes stale
//	and its lease expires -> the reconciler records EXPIRED + ABANDONED and
//	releases capacity -> a new outbox event travels through the real publisher
//	and real ElasticMQ -> worker B claims attempt 2 and completes it -> every
//	late mutation from worker A is rejected.
func TestWorkerCrash_RecoversThroughTheRealOutboxAndBrokerPath(t *testing.T) {
	reset(t)
	broker := newBroker(t, "")
	store := workers.NewStore(testPool, crashRecoveryLease)
	control := &killableAPI{control: store}

	server := newControlServer(t, control)
	submitted := submitCrashRecoveryJob(t, server.URL)

	// 1. Deliver the submission notification through the real outbox and broker.
	stats, err := newPublisher(t, broker).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Published)

	// 2. Worker A claims and starts the job, then blocks inside its handler.
	handler := &blockingEcho{started: make(chan struct{}), release: make(chan struct{})}
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", handler))

	workerA := workerruntime.NewClient(server.URL, &http.Client{Timeout: 5 * time.Second})
	sessionA := uuid.New()
	runnerA := workerruntime.NewRunner(workerA, broker, registry, workerruntime.RunnerConfig{
		Registration: workers.Registration{
			SessionID: sessionA, Name: "crash-worker-a", Hostname: "a.local",
			WorkerGroup: "default", ConcurrencyLimit: 1, Capabilities: []string{"cpu"},
		},
		Queue: "default", PollWait: time.Second,
		RetryAttempts: 2, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		SessionStaleAfter: 600 * time.Millisecond,
		RenewInterval:     200 * time.Millisecond,
		ShutdownTimeout:   2 * time.Second,
	}, discardLogger())

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	doneA := make(chan error, 1)
	go func() { doneA <- runnerA.Run(ctxA) }()

	select {
	case <-handler.started:
	case <-time.After(30 * time.Second):
		t.Fatal("worker A never began executing the job")
	}

	// Durable state, not a local flag, is what says worker A is really RUNNING.
	var fence workers.Fence
	eventually(t, 15*time.Second, "worker A's attempt is durably RUNNING", func() bool {
		err := testPool.QueryRow(context.Background(), `
			SELECT j.id, a.id, l.id, a.worker_id, a.worker_session_id
			FROM jobs j
			JOIN job_attempts a ON a.job_id = j.id
			JOIN leases l ON l.attempt_id = a.id
			WHERE j.id = $1 AND j.status = 'RUNNING' AND a.status = 'RUNNING'`, submitted,
		).Scan(&fence.JobID, &fence.AttemptID, &fence.LeaseID, &fence.WorkerID, &fence.SessionID)
		return err == nil
	})
	require.Equal(t, 1, countActiveLeases(t))
	require.Equal(t, sessionA, fence.SessionID)

	// 3. Kill worker A's authority path mid-handler. Heartbeats and renewals now
	//    fail, exactly as they would if the process had been SIGKILLed.
	control.severed.Store(&sessionA)

	engine := reconciler.New(store, reconciler.Config{
		StaleAfter: 600 * time.Millisecond, PollInterval: 50 * time.Millisecond, BatchSize: 50,
	}, discardLogger())

	// 4. Its session goes stale on PostgreSQL receipt time.
	eventually(t, 30*time.Second, "worker A's session is detected as stale", func() bool {
		if _, err := engine.RunOnce(context.Background()); err != nil {
			return false
		}
		return sessionStatus(t, sessionA) == "UNHEALTHY"
	})

	// 5, 6, 7. Its lease expires, attempt 1 is abandoned, and capacity returns.
	eventually(t, 30*time.Second, "the expired lease is reconciled", func() bool {
		if _, err := engine.RunOnce(context.Background()); err != nil {
			return false
		}
		state := readState(t, fence)
		return state.lease == "EXPIRED" && state.attempt == "ABANDONED" && state.job == "QUEUED"
	})
	require.Equal(t, 0, countActiveLeases(t), "abandonment must return the reserved capacity")

	// Worker A's runner gives up on its own, without needing the test to stop it.
	select {
	case <-doneA:
	case <-time.After(30 * time.Second):
		t.Fatal("worker A never stopped after losing its session")
	}
	require.False(t, runnerA.Ready(), "a worker that lost its session must not report ready")
	close(handler.release) // let the stranded goroutine finish

	// 8. Publish the transactionally created recovery event through the real
	//    outbox publisher and the real ElasticMQ queue.
	recovery, err := newPublisher(t, broker).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, recovery.Published, "reconciliation must have written exactly one recovery event")

	// 9. Worker B, a genuinely different logical worker, picks the job back up.
	completed := make(chan struct{})
	var closeOnce atomic.Bool
	registryB := workerruntime.NewRegistry()
	require.NoError(t, registryB.Register("demo.echo",
		workerruntime.HandlerFunc(func(_ context.Context, execution workerruntime.Execution) (json.RawMessage, error) {
			if closeOnce.CompareAndSwap(false, true) {
				close(completed)
			}
			return execution.Payload, nil
		})))
	workerB := workerruntime.NewClient(server.URL, &http.Client{Timeout: 5 * time.Second})
	runnerB := workerruntime.NewRunner(workerB, broker, registryB, workerruntime.RunnerConfig{
		Registration: workers.Registration{
			SessionID: uuid.New(), Name: "crash-worker-b", Hostname: "b.local",
			WorkerGroup: "default", ConcurrencyLimit: 1, Capabilities: []string{"cpu"},
		},
		Queue: "default", PollWait: time.Second,
		RetryAttempts: 2, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		SessionStaleAfter: 10 * time.Second,
		RenewInterval:     200 * time.Millisecond,
		ShutdownTimeout:   2 * time.Second,
	}, discardLogger())

	ctxB, cancelB := context.WithCancel(context.Background())
	doneB := make(chan error, 1)
	go func() { doneB <- runnerB.Run(ctxB) }()

	select {
	case <-completed:
	case <-time.After(30 * time.Second):
		t.Fatal("worker B never executed the recovered job")
	}

	// 10. The durable end state.
	eventually(t, 30*time.Second, "the recovered job reaches SUCCEEDED", func() bool {
		var status string
		return testPool.QueryRow(context.Background(),
			`SELECT status FROM jobs WHERE id = $1`, submitted).Scan(&status) == nil && status == "SUCCEEDED"
	})
	cancelB()
	select {
	case err := <-doneB:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("worker B did not stop cleanly")
	}

	require.Equal(t, []string{"ABANDONED", "SUCCEEDED"}, attemptHistory(t, submitted))
	require.Equal(t, []string{"EXPIRED", "COMPLETED"}, leaseHistory(t, submitted))
	require.Equal(t, 0, countActiveLeases(t))

	// 11. Every late mutation from worker A is rejected and changes nothing.
	control.severed.Store(nil)
	settled := readState(t, fence)

	_, err = store.Heartbeat(context.Background(), testScope,
		workers.HeartbeatRequest{WorkerID: fence.WorkerID, SessionID: sessionA})
	require.ErrorIs(t, err, workers.ErrSessionUnavailable, "a dead session cannot heartbeat")

	_, err = store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
	require.ErrorIs(t, err, workers.ErrFenceRejected, "a dead session cannot renew")

	require.ErrorIs(t, store.Succeed(context.Background(), testScope, fence),
		workers.ErrFenceRejected, "a dead session cannot commit an outcome")

	require.Equal(t, settled, readState(t, fence),
		"no late mutation from the dead worker may change durable state")
	require.Equal(t, "UNHEALTHY", sessionStatus(t, sessionA))
	require.Positive(t, control.rejected.Load(),
		"the test must actually have severed worker A's authority path")
}

// newControlServer serves the real API over an injectable worker-control store,
// which is the seam this test uses to sever one session's authority.
func newControlServer(t *testing.T, control api.WorkerControl) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(api.NewServer(
		jobs.NewStore(testPool),
		api.Config{MaxRequestBytes: 256 * 1024, DevScope: testScope},
		discardLogger(),
		api.ReadinessCheck{
			Name:  "postgres",
			Check: func(ctx context.Context) error { return database.Ping(ctx, testPool) },
		},
	).WithWorkerControl(control).Handler())
	t.Cleanup(server.Close)
	return server
}

func submitCrashRecoveryJob(t *testing.T, base string) uuid.UUID {
	t.Helper()
	response, job := submit(t, base, "crash-recovery",
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"recover me"},"max_attempts":2}`)
	require.Equal(t, http.StatusCreated, response.StatusCode)
	id, err := uuid.Parse(job.ID)
	require.NoError(t, err)
	require.Equal(t, 2, job.MaxAttempts, "recovery needs a budget for a second attempt")
	return id
}
