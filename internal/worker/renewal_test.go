package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// renewingRunner builds a runner whose loops tick fast enough for a test to
// observe them. The assertions below are all driven by channel barriers or by
// the fake control plane being called, never by sleeping for a guessed duration.
func renewingRunner(control ControlPlane, broker queue.Broker, registry *Registry, cfg RunnerConfig) *Runner {
	cfg.Queue = "default"
	cfg.PollWait = time.Second
	if cfg.RetryAttempts == 0 {
		cfg.RetryAttempts = 1
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 2 * time.Second
	}
	if cfg.RenewInterval == 0 {
		cfg.RenewInterval = 2 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 2 * time.Millisecond
	}
	if cfg.SessionStaleAfter == 0 {
		cfg.SessionStaleAfter = time.Minute
	}
	return NewRunner(control, broker, registry, cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

// TestRenewal_LongHandlerSurvivesSeveralLeaseWindows is the property M3 exists
// for: cooperative work outliving the lease it started under, but only for as
// long as renewal keeps succeeding.
//
// The handler blocks until the third renewal has committed, which is a barrier,
// not a timeout: if renewal ever stopped, the handler would never be released
// and the test would fail on its own deadline instead of passing by luck.
func TestRenewal_LongHandlerSurvivesSeveralLeaseWindowsWhileRenewalSucceeds(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	// A window far shorter than the handler will take. Without renewal this
	// attempt could never report success.
	assignment.LeaseRemaining = 40 * time.Millisecond
	assignment.ExecutionDeadline = time.Now().Add(40 * time.Millisecond)

	renewed := make(chan int, 16)
	var succeeded atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		renew: func(_ context.Context, req workers.RenewalRequest) (workers.RenewalResult, error) {
			version := req.ExpectedVersion + 1
			select {
			case renewed <- version:
			default:
			}
			return workers.RenewalResult{
				LeaseID: req.Fence.LeaseID, RenewalVersion: version,
				ExpiresAt: time.Now().Add(40 * time.Millisecond),
				Remaining: 40 * time.Millisecond,
			}, nil
		},
		succeed: func(context.Context, workers.Fence) error {
			succeeded.Store(true)
			return nil
		},
	}

	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
		// Wait for three committed renewals, then return. Three windows of 40ms
		// is more than double the original lease.
		for seen := 0; seen < 3; {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-renewed:
				seen++
			}
		}
		return nil, nil
	})))

	runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
	require.NoError(t, runner.processMessage(context.Background(), session,
		queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")}))
	require.True(t, succeeded.Load(),
		"work that kept renewing must be allowed to report a fenced success")
}

// TestRenewal_RetriesReuseOneIdentityAndGeneration pins the contract that makes
// an ambiguous renewal safe. A retry that invented a new request id, or that
// advanced the expected generation, would extend the lease a second time.
func TestRenewal_RetriesReuseOneIdentityAndGeneration(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)

	type observed struct {
		id       uuid.UUID
		expected int
	}
	var mu sync.Mutex
	var calls []observed
	release := make(chan struct{})
	var once sync.Once

	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		renew: func(_ context.Context, req workers.RenewalRequest) (workers.RenewalResult, error) {
			mu.Lock()
			calls = append(calls, observed{req.RenewalRequestID, req.ExpectedVersion})
			count := len(calls)
			mu.Unlock()

			// The first two attempts fail ambiguously; the third succeeds. All
			// three must carry the same identity and the same expected generation.
			if count < 3 {
				return workers.RenewalResult{}, &RemoteError{Status: 503, Code: "service_unavailable"}
			}
			once.Do(func() { close(release) })
			return workers.RenewalResult{
				LeaseID: req.Fence.LeaseID, RenewalVersion: req.ExpectedVersion + 1,
				ExpiresAt: time.Now().Add(time.Minute), Remaining: time.Minute,
			}, nil
		},
		succeed: func(context.Context, workers.Fence) error { return nil },
	}

	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return nil, nil
		}
	})))

	runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
	require.NoError(t, runner.processMessage(context.Background(), session,
		queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")}))

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(calls), 3)
	for i := 0; i < 3; i++ {
		require.Equal(t, calls[0].id, calls[i].id,
			"an ambiguous renewal must be retried under its original identity")
		require.Equal(t, 0, calls[i].expected,
			"a retry must not advance the expected generation; that would double-extend")
	}
}

// TestRenewal_DefinitiveLossCancelsTheHandlerAndPreventsSuccess covers every
// rejection that means some other authority already won.
func TestRenewal_DefinitiveLossCancelsTheHandlerAndPreventsSuccess(t *testing.T) {
	// Fence rejection is deliberately absent: this repository already classifies
	// it as session loss (isSessionLost), which is fatal for the whole boot and
	// is covered by TestRenewal_SessionLossIsFatalForTheWholeBoot.
	for name, rejection := range map[string]error{
		"lease expired":    workers.ErrLeaseExpired,
		"renewal conflict": workers.ErrRenewalConflict,
		"state conflict":   workers.ErrStateConflict,
	} {
		t.Run(name, func(t *testing.T) {
			session := testSession(1)
			assignment := testAssignment(session)
			var canceled, succeeded atomic.Bool
			control := &fakeControl{
				claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
					return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
				},
				renew: func(context.Context, workers.RenewalRequest) (workers.RenewalResult, error) {
					return workers.RenewalResult{}, rejection
				},
				succeed: func(context.Context, workers.Fence) error {
					succeeded.Store(true)
					return nil
				},
			}
			registry := NewRegistry()
			require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
				<-ctx.Done() // released only by the renewal loop cancelling authority
				canceled.Store(true)
				return nil, nil
			})))

			runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
			// Only session loss is fatal for the whole process; losing one lease
			// ends one attempt and the worker keeps running.
			require.NoError(t, runner.processMessage(context.Background(), session,
				queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")}))
			require.True(t, canceled.Load(), "losing lease authority must cancel the handler")
			require.False(t, succeeded.Load(),
				"a handler that returned nil after authority was lost must not report success")
		})
	}
}

// Session loss during renewal is different in kind: it ends the process boot, not
// just this attempt, so it must surface as a fatal runner error.
func TestRenewal_SessionLossIsFatalForTheWholeBoot(t *testing.T) {
	for name, rejection := range map[string]error{
		"session replaced": &RemoteError{Status: 409, Code: "worker_session_unavailable"},
		// The repository already treats a rejected fence as session loss, because
		// lockFence answers it both for a stale tuple and for a replaced session.
		"fence rejected": &RemoteError{Status: 409, Code: "fence_rejected"},
	} {
		t.Run(name, func(t *testing.T) {
			session := testSession(1)
			assignment := testAssignment(session)
			var succeeded atomic.Bool
			control := &fakeControl{
				claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
					return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
				},
				renew: func(context.Context, workers.RenewalRequest) (workers.RenewalResult, error) {
					return workers.RenewalResult{}, rejection
				},
				succeed: func(context.Context, workers.Fence) error {
					succeeded.Store(true)
					return nil
				},
			}
			registry := NewRegistry()
			require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
				<-ctx.Done()
				return nil, nil
			})))

			runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
			err := runner.processMessage(context.Background(), session,
				queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")})
			require.ErrorIs(t, err, ErrSessionLost)
			require.False(t, succeeded.Load())
		})
	}
}

// TestRenewal_UnresolvedTransientFailureStopsAtTheSafetyDeadline is the
// conservative half. Renewal never definitively fails, it just never confirms;
// once the local authority deadline passes, execution is cancelled and no
// outcome is reported. Durable recovery is left to expiry and reconciliation.
func TestRenewal_UnresolvedTransientFailureStopsAtTheSafetyDeadline(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	assignment.LeaseRemaining = 30 * time.Millisecond
	assignment.ExecutionDeadline = time.Now().Add(30 * time.Millisecond)

	var attempted atomic.Int32
	var canceled, succeeded atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		renew: func(context.Context, workers.RenewalRequest) (workers.RenewalResult, error) {
			attempted.Add(1)
			return workers.RenewalResult{}, &RemoteError{Status: 503, Code: "service_unavailable"}
		},
		succeed: func(context.Context, workers.Fence) error {
			succeeded.Store(true)
			return nil
		},
	}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
		<-ctx.Done()
		canceled.Store(true)
		return nil, nil // a cooperative handler that returns cleanly after cancellation
	})))

	runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
	require.NoError(t, runner.processMessage(context.Background(), session,
		queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")}))
	require.Positive(t, attempted.Load(), "the loop must actually have tried to renew")
	require.True(t, canceled.Load())
	require.False(t, succeeded.Load(),
		"work that could not confirm authority before its deadline must not report success")
}

// TestRenewal_DoesNotResetTheOverallJobTimeout is the boundary between the two
// budgets. The lease is renewed indefinitely here — every renewal returns a
// fresh hour — so the only thing that can stop this handler is the attempt's own
// execution deadline, which renewal must never extend.
//
// M4 changes where that budget comes from: it is the PostgreSQL-measured
// remaining window Start returned, not a fresh timer started from
// timeout_seconds once the response landed. A Start that reports no budget left
// is therefore the exhausted case, and the handler must not run at all.
func TestRenewal_DoesNotResetTheOverallJobTimeout(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	assignment.LeaseRemaining = time.Minute
	assignment.ExecutionDeadline = time.Now().Add(time.Minute)

	var renewals atomic.Int32
	var handled, succeeded atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start: func(_ context.Context, fence workers.Fence) (workers.StartResult, error) {
			now := time.Now()
			return workers.StartResult{
				AttemptID: fence.AttemptID, StartedAt: now,
				TimeoutAt: now, Remaining: 0,
			}, nil
		},
		renew: func(_ context.Context, req workers.RenewalRequest) (workers.RenewalResult, error) {
			renewals.Add(1)
			return workers.RenewalResult{
				LeaseID: req.Fence.LeaseID, RenewalVersion: req.ExpectedVersion + 1,
				ExpiresAt: time.Now().Add(time.Hour), Remaining: time.Hour,
			}, nil
		},
		succeed: func(context.Context, workers.Fence) error {
			succeeded.Store(true)
			return nil
		},
	}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		handled.Store(true)
		return nil, nil
	})))

	runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
	require.NoError(t, runner.processMessage(context.Background(), session,
		queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")}))
	require.False(t, handled.Load(),
		"an exhausted job timeout must stop execution no matter how healthy the lease is")
	require.False(t, succeeded.Load())
}

// TestHeartbeat_IdleSessionLossIsFatalAndDropsReadiness proves the worker
// discovers replacement without any broker delivery at all: the broker here
// never produces a message.
func TestHeartbeat_IdleSessionLossIsFatalAndDropsReadiness(t *testing.T) {
	session := testSession(1)
	beat := make(chan struct{}, 1)
	control := &fakeControl{
		register: func(context.Context, workers.Registration) (workers.Session, error) {
			return session, nil
		},
		heartbeat: func(context.Context, workers.HeartbeatRequest) (workers.HeartbeatResult, error) {
			select {
			case beat <- struct{}{}:
			default:
			}
			return workers.HeartbeatResult{}, &RemoteError{Status: 409, Code: "worker_session_unavailable"}
		},
	}
	runner := renewingRunner(control, &fakeBroker{messages: make(chan queue.Message)}, NewRegistry(), RunnerConfig{})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()

	select {
	case <-beat:
	case <-time.After(5 * time.Second):
		t.Fatal("the heartbeat loop never ran")
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrSessionLost)
	case <-time.After(5 * time.Second):
		t.Fatal("a fenced session must end the runner without needing a broker delivery")
	}
	require.False(t, runner.Ready(), "a fenced session must remove readiness")
}

// TestHeartbeat_UnconfirmedLivenessStopsTheWorkerAtTheStaleThreshold covers the
// ambiguous case: the control plane never answers definitively, so the worker
// cannot know whether it has already been fenced. Once the staleness threshold
// has passed it must stop presenting itself as ready.
func TestHeartbeat_UnconfirmedLivenessStopsTheWorkerAtTheStaleThreshold(t *testing.T) {
	session := testSession(1)
	control := &fakeControl{
		register: func(context.Context, workers.Registration) (workers.Session, error) {
			return session, nil
		},
		heartbeat: func(context.Context, workers.HeartbeatRequest) (workers.HeartbeatResult, error) {
			return workers.HeartbeatResult{}, errors.New("dial tcp: connection refused")
		},
	}
	runner := renewingRunner(control, &fakeBroker{messages: make(chan queue.Message)}, NewRegistry(),
		RunnerConfig{HeartbeatInterval: 2 * time.Millisecond, SessionStaleAfter: 20 * time.Millisecond})

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrHeartbeatStale)
	case <-time.After(5 * time.Second):
		t.Fatal("a worker that cannot confirm liveness must stop accepting work")
	}
	require.False(t, runner.Ready())
}

// TestHeartbeat_ContinuesThroughAGracefulDrain proves the heartbeat runs on the
// completion context, not the intake context. If it stopped at cancellation, a
// long drain would let the session go stale and the reconciler would abandon the
// very work the drain is trying to finish.
func TestHeartbeat_ContinuesThroughAGracefulDrain(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	var beatsDuringDrain atomic.Int32
	draining := make(chan struct{})
	handlerRunning := make(chan struct{})
	var handlerOnce, drainOnce sync.Once

	control := &fakeControl{
		register: func(context.Context, workers.Registration) (workers.Session, error) {
			return session, nil
		},
		heartbeat: func(context.Context, workers.HeartbeatRequest) (workers.HeartbeatResult, error) {
			select {
			case <-draining:
				beatsDuringDrain.Add(1)
			default:
			}
			return workers.HeartbeatResult{
				SessionID: session.ID, Status: workers.SessionHealthy, LastHeartbeatAt: time.Now(),
			}, nil
		},
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		succeed: func(context.Context, workers.Fence) error { return nil },
	}

	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		handlerOnce.Do(func() { close(handlerRunning) })
		// Hold until several heartbeats have been observed after the drain began.
		for beatsDuringDrain.Load() < 2 {
			select {
			case <-draining:
				time.Sleep(time.Millisecond)
			default:
				time.Sleep(time.Millisecond)
			}
		}
		return nil, nil
	})))

	messages := make(chan queue.Message, 1)
	messages <- queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")}
	runner := renewingRunner(control, &fakeBroker{messages: messages}, registry, RunnerConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case <-handlerRunning:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never started")
	}
	// Begin the graceful drain while the handler is still executing.
	drainOnce.Do(func() { close(draining) })
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the runner never finished draining")
	}
	require.GreaterOrEqual(t, beatsDuringDrain.Load(), int32(2),
		"heartbeats must keep proving liveness while in-flight work drains")
}

// --- blocking control calls -------------------------------------------------
//
// The tests above return their failures immediately, so they never exercise the
// case that actually matters: a control-plane call that does not come back. A
// hung call used to hold the loop, so neither the staleness timer nor the lease
// authority timer could be selected, and the worker kept going with no provable
// authority. Each test below therefore blocks until its call's own context is
// cancelled — if the call were not bounded by the safety deadline, nothing would
// ever cancel it and the test would hang rather than fail.

// blockingCall records that a call arrived, then blocks until its context is
// cancelled. Returning ctx.Err() is what a real transport does when the caller's
// deadline elapses mid-request.
type blockingCall struct {
	entered chan struct{}
	once    sync.Once
}

func newBlockingCall() *blockingCall {
	return &blockingCall{entered: make(chan struct{})}
}

func (b *blockingCall) enter(ctx context.Context) error {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func TestHeartbeat_AHungCallCannotOutliveTheStalenessWindow(t *testing.T) {
	session := testSession(1)
	blocked := newBlockingCall()
	control := &fakeControl{
		register: func(context.Context, workers.Registration) (workers.Session, error) {
			return session, nil
		},
		heartbeat: func(ctx context.Context, _ workers.HeartbeatRequest) (workers.HeartbeatResult, error) {
			return workers.HeartbeatResult{}, blocked.enter(ctx)
		},
	}
	runner := renewingRunner(control, &fakeBroker{messages: make(chan queue.Message)}, NewRegistry(),
		RunnerConfig{HeartbeatInterval: 2 * time.Millisecond, SessionStaleAfter: 40 * time.Millisecond})

	done := make(chan error, 1)
	// The runner context is deliberately never cancelled: only the per-call
	// deadline can release this heartbeat.
	go func() { done <- runner.Run(context.Background()) }()

	select {
	case <-blocked.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the heartbeat loop never issued a call")
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrHeartbeatStale,
			"a heartbeat that cannot complete inside the staleness window must stop the worker")
	case <-time.After(10 * time.Second):
		t.Fatal("a hung heartbeat outlived the staleness window without stopping the worker")
	}
	require.False(t, runner.Ready())
}

func TestRenewal_AHungCallCannotOutliveTheAuthorityDeadline(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	assignment.LeaseRemaining = 60 * time.Millisecond
	assignment.ExecutionDeadline = time.Now().Add(60 * time.Millisecond)

	blocked := newBlockingCall()
	var canceled, succeeded atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		renew: func(ctx context.Context, _ workers.RenewalRequest) (workers.RenewalResult, error) {
			return workers.RenewalResult{}, blocked.enter(ctx)
		},
		succeed: func(context.Context, workers.Fence) error {
			succeeded.Store(true)
			return nil
		},
	}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
		<-ctx.Done() // released only when the renewal loop cancels authority
		canceled.Store(true)
		return nil, nil
	})))

	runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
	done := make(chan error, 1)
	go func() {
		done <- runner.processMessage(context.Background(), session,
			queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")})
	}()

	select {
	case <-blocked.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the renewal loop never issued a call")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("a hung renewal outlived the authority deadline without cancelling the handler")
	}
	require.True(t, canceled.Load(), "a hung renewal must still cancel the cooperative handler")
	require.False(t, succeeded.Load(),
		"work that could not prove its lease must never report success")
}

// TestRenewal_ALateSuccessCannotRestoreExpiredLocalAuthority is the other half.
// The call is cancelled at the deadline, but the fake answers anyway with a
// perfectly valid renewal — exactly what a slow round trip looks like when the
// response finally lands. Accepting it would silently un-expire a window the
// handler has already outlived.
func TestRenewal_ALateSuccessCannotRestoreExpiredLocalAuthority(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	assignment.LeaseRemaining = 60 * time.Millisecond
	assignment.ExecutionDeadline = time.Now().Add(60 * time.Millisecond)

	entered := make(chan struct{})
	var once sync.Once
	var renewals atomic.Int32
	var canceled, succeeded atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		renew: func(ctx context.Context, req workers.RenewalRequest) (workers.RenewalResult, error) {
			renewals.Add(1)
			once.Do(func() { close(entered) })
			<-ctx.Done() // the call is cancelled at the authority deadline
			// ...and the control plane answers successfully anyway, late.
			return workers.RenewalResult{
				LeaseID: req.Fence.LeaseID, RenewalVersion: req.ExpectedVersion + 1,
				ExpiresAt: time.Now().Add(time.Hour), Remaining: time.Hour,
			}, nil
		},
		succeed: func(context.Context, workers.Fence) error {
			succeeded.Store(true)
			return nil
		},
	}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
		<-ctx.Done()
		canceled.Store(true)
		return nil, nil
	})))

	runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
	done := make(chan error, 1)
	go func() {
		done <- runner.processMessage(context.Background(), session,
			queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")})
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the renewal loop never issued a call")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("a late renewal success restored authority instead of ending the attempt")
	}
	require.Positive(t, renewals.Load())
	require.True(t, canceled.Load())
	require.False(t, succeeded.Load(),
		"a renewal confirmed after the deadline it was racing must not authorize success")
}
