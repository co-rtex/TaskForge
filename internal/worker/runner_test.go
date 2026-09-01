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

	"github.com/co-rtex/TaskForge/internal/outbox"
	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/workers"
)

type fakeControl struct {
	register  func(context.Context, workers.Registration) (workers.Session, error)
	heartbeat func(context.Context, workers.HeartbeatRequest) (workers.HeartbeatResult, error)
	claim     func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error)
	renew     func(context.Context, workers.RenewalRequest) (workers.RenewalResult, error)
	start     func(context.Context, workers.Fence) error
	succeed   func(context.Context, workers.Fence) error
}

func (f *fakeControl) Register(ctx context.Context, req workers.Registration) (workers.Session, error) {
	return f.register(ctx, req)
}

// Heartbeat and RenewLease default to succeeding when a test does not care:
// every Run-based test starts a heartbeat loop, and most of them are about
// something else entirely.
func (f *fakeControl) Heartbeat(ctx context.Context, req workers.HeartbeatRequest) (workers.HeartbeatResult, error) {
	if f.heartbeat == nil {
		return workers.HeartbeatResult{
			SessionID: req.SessionID, Status: workers.SessionHealthy, LastHeartbeatAt: time.Now(),
		}, nil
	}
	return f.heartbeat(ctx, req)
}
func (f *fakeControl) Claim(ctx context.Context, req workers.ClaimRequest) (workers.ClaimResult, error) {
	return f.claim(ctx, req)
}
func (f *fakeControl) RenewLease(ctx context.Context, req workers.RenewalRequest) (workers.RenewalResult, error) {
	if f.renew == nil {
		return workers.RenewalResult{
			LeaseID: req.Fence.LeaseID, RenewalVersion: req.ExpectedVersion + 1,
			ExpiresAt: time.Now().Add(time.Minute), Remaining: time.Minute,
		}, nil
	}
	return f.renew(ctx, req)
}
func (f *fakeControl) Start(ctx context.Context, fence workers.Fence) error {
	return f.start(ctx, fence)
}
func (f *fakeControl) Succeed(ctx context.Context, fence workers.Fence) error {
	return f.succeed(ctx, fence)
}
func (f *fakeControl) Ping(context.Context) error { return nil }

type fakeBroker struct {
	messages     chan queue.Message
	deleteErr    error
	receiveCount atomic.Int32
	mu           sync.Mutex
	deleted      []string
	deletedCh    chan string
	onDelete     func()
}

func (b *fakeBroker) Publish(context.Context, []byte) error { return nil }
func (b *fakeBroker) Ping(context.Context) error            { return nil }
func (b *fakeBroker) Receive(ctx context.Context, _ int, _ time.Duration) ([]queue.Message, error) {
	b.receiveCount.Add(1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case message := <-b.messages:
		return []queue.Message{message}, nil
	}
}
func (b *fakeBroker) Delete(_ context.Context, receipt string) error {
	b.mu.Lock()
	b.deleted = append(b.deleted, receipt)
	observer := b.deletedCh
	b.mu.Unlock()
	if b.onDelete != nil {
		b.onDelete()
	}
	// deletedCh lets a test use acknowledgement as a barrier instead of a sleep.
	// It stays nil for every test that does not need that synchronization.
	if observer != nil {
		observer <- receipt
	}
	return b.deleteErr
}

func (b *fakeBroker) deletedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.deleted)
}

func (b *fakeBroker) deletedReceipts() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.deleted...)
}

func testSession(concurrency int) workers.Session {
	return workers.Session{
		ID: uuid.New(), WorkerID: uuid.New(), Name: "worker", Hostname: "host",
		WorkerGroup: "default", ConcurrencyLimit: concurrency,
		SupportedJobTypes: []string{"demo.echo"}, Status: workers.SessionHealthy,
	}
}

func testAssignment(session workers.Session) *workers.Assignment {
	return &workers.Assignment{
		JobID: uuid.New(), Queue: "default", JobType: "demo.echo",
		Payload: json.RawMessage(`{"message":"hello"}`), TimeoutSeconds: 30,
		AttemptID: uuid.New(), AttemptNumber: 1, LeaseID: uuid.New(),
		LeaseExpiresAt: time.Now().Add(time.Minute), WorkerID: session.WorkerID,
		LeaseRemaining: time.Minute, ExecutionDeadline: time.Now().Add(time.Minute),
		SessionID: session.ID,
	}
}

func testRunner(control ControlPlane, broker queue.Broker, registry *Registry) *Runner {
	return NewRunner(control, broker, registry, RunnerConfig{
		Queue: "default", PollWait: time.Second, RetryAttempts: 2,
		ShutdownTimeout: time.Second,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func TestProcessMessage_OrdersClaimAckStartHandlerAndSuccess(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	var events []string
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			events = append(events, "claim")
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start: func(context.Context, workers.Fence) error {
			events = append(events, "start")
			return nil
		},
		succeed: func(context.Context, workers.Fence) error {
			events = append(events, "succeed")
			return nil
		},
	}
	broker := &fakeBroker{onDelete: func() { events = append(events, "ack") }}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		events = append(events, "handler")
		return nil, nil
	})))

	require.NoError(t, testRunner(control, broker, registry).processMessage(context.Background(), session, queue.Message{
		ID: "m1", ReceiptHandle: "r1", Body: notificationBody(t, "default"),
	}))
	require.Equal(t, []string{"claim", "ack", "start", "handler", "succeed"}, events)
}

func TestProcessMessage_DeleteFailureStillExecutesDurableClaim(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	var handled, succeeded bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start: func(context.Context, workers.Fence) error { return nil },
		succeed: func(context.Context, workers.Fence) error {
			succeeded = true
			return nil
		},
	}
	broker := &fakeBroker{deleteErr: errors.New("delete failed")}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		handled = true
		return nil, nil
	})))

	require.NoError(t, testRunner(control, broker, registry).processMessage(context.Background(), session, queue.Message{
		ReceiptHandle: "r1", Body: notificationBody(t, "default"),
	}))
	require.True(t, handled)
	require.True(t, succeeded)
}

func TestProcessMessage_CooperativeDeadlineCannotBeReportedAsSuccess(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	assignment.ExecutionDeadline = time.Now().Add(50 * time.Millisecond)
	var handled, succeeded atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start: func(context.Context, workers.Fence) error { return nil },
		succeed: func(context.Context, workers.Fence) error {
			succeeded.Store(true)
			return nil
		},
	}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
		handled.Store(true)
		<-ctx.Done()
		return nil, nil
	})))

	require.NoError(t, testRunner(control, &fakeBroker{}, registry).processMessage(
		context.Background(), session, queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")},
	))
	require.True(t, handled.Load())
	require.False(t, succeeded.Load(), "a nil handler result after its deadline must not complete the job")
}

func TestProcessMessage_ExpiredExecutionWindowDoesNotInvokeHandler(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	assignment.ExecutionDeadline = time.Now().Add(-time.Millisecond)
	var handled, succeeded atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start: func(context.Context, workers.Fence) error { return nil },
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

	require.NoError(t, testRunner(control, &fakeBroker{}, registry).processMessage(
		context.Background(), session, queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")},
	))
	require.False(t, handled.Load())
	require.False(t, succeeded.Load())
}

func TestProcessMessage_AcknowledgesOnlyClaimedOrGloballyEmpty(t *testing.T) {
	for _, test := range []struct {
		disposition workers.ClaimDisposition
		wantDeletes int
	}{
		{workers.QueueEmpty, 1},
		{workers.NoEligibleJob, 0},
		{workers.CapacityExhausted, 0},
	} {
		t.Run(string(test.disposition), func(t *testing.T) {
			session := testSession(1)
			control := &fakeControl{
				claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
					return workers.ClaimResult{Disposition: test.disposition}, nil
				},
			}
			broker := &fakeBroker{}
			require.NoError(t, testRunner(control, broker, NewRegistry()).processMessage(context.Background(), session, queue.Message{
				ReceiptHandle: "r1", Body: notificationBody(t, "default"),
			}))
			require.Equal(t, test.wantDeletes, broker.deletedCount())
		})
	}
}

func TestProcessMessage_RetriesOneLogicalClaimWithTheSameRequestID(t *testing.T) {
	session := testSession(1)
	var ids []uuid.UUID
	control := &fakeControl{
		claim: func(_ context.Context, request workers.ClaimRequest) (workers.ClaimResult, error) {
			ids = append(ids, request.ClaimRequestID)
			if len(ids) == 1 {
				return workers.ClaimResult{}, errors.New("ambiguous transport failure")
			}
			return workers.ClaimResult{Disposition: workers.QueueEmpty}, nil
		},
	}
	broker := &fakeBroker{}
	require.NoError(t, testRunner(control, broker, NewRegistry()).processMessage(context.Background(), session, queue.Message{
		ReceiptHandle: "r1", Body: notificationBody(t, "default"),
	}))
	require.Len(t, ids, 2)
	require.Equal(t, ids[0], ids[1])
	require.Equal(t, 1, broker.deletedCount())
}

func TestProcessMessage_RedeliveryRecoversAnAmbiguousCommittedClaim(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	body := notificationBody(t, "default")
	var ids []uuid.UUID
	var calls int
	control := &fakeControl{
		claim: func(_ context.Context, request workers.ClaimRequest) (workers.ClaimResult, error) {
			ids = append(ids, request.ClaimRequestID)
			calls++
			if calls <= 2 {
				// Model a committed claim whose response is lost through the
				// runner's entire immediate retry budget.
				return workers.ClaimResult{}, errors.New("ambiguous transport failure")
			}
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment, Replayed: true}, nil
		},
		start:   func(context.Context, workers.Fence) error { return nil },
		succeed: func(context.Context, workers.Fence) error { return nil },
	}
	broker := &fakeBroker{}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		return nil, nil
	})))
	runner := testRunner(control, broker, registry)
	message := queue.Message{ReceiptHandle: "r1", Body: body}

	require.NoError(t, runner.processMessage(context.Background(), session, message))
	require.Equal(t, 0, broker.deletedCount())
	message.ReceiptHandle = "r2"
	require.NoError(t, runner.processMessage(context.Background(), session, message))
	require.Equal(t, 1, broker.deletedCount())
	require.Len(t, ids, 3)
	require.Equal(t, ids[0], ids[1])
	require.Equal(t, ids[0], ids[2], "broker redelivery must reuse the durable event identity")
}

func TestProcessMessage_ConcurrentDuplicateDeliveryExecutesOneLocalHandler(t *testing.T) {
	session := testSession(2)
	assignment := testAssignment(session)
	var claims, starts, handlers, succeeds atomic.Int32
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			claims.Add(1)
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start: func(context.Context, workers.Fence) error {
			starts.Add(1)
			return nil
		},
		succeed: func(context.Context, workers.Fence) error {
			succeeds.Add(1)
			return nil
		},
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		handlers.Add(1)
		close(entered)
		<-release
		return nil, nil
	})))
	broker := &fakeBroker{}
	runner := testRunner(control, broker, registry)
	body := notificationBody(t, "default")
	notification, err := decodeWorkNotification(body)
	require.NoError(t, err)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			errs <- runner.processMessage(context.Background(), session, queue.Message{
				ID: uuid.NewString(), ReceiptHandle: uuid.NewString(), Body: body,
			})
		}(i)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	require.Equal(t, int32(1), claims.Load())
	require.Eventually(t, func() bool {
		runner.flightsMu.Lock()
		defer runner.flightsMu.Unlock()
		flight := runner.flights[notification.EventID]
		return flight != nil && flight.followers == 1
	}, time.Second, time.Millisecond, "the duplicate must join the in-flight delivery")
	close(release)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.Equal(t, int32(1), claims.Load())
	require.Equal(t, int32(1), starts.Load())
	require.Equal(t, int32(1), handlers.Load())
	require.Equal(t, int32(1), succeeds.Load())
	require.Equal(t, 2, broker.deletedCount())
}

func TestRunner_SessionReplacementIsFatalAndDropsReadiness(t *testing.T) {
	session := testSession(1)
	claimEntered := make(chan struct{})
	releaseClaim := make(chan struct{})
	control := &fakeControl{
		register: func(context.Context, workers.Registration) (workers.Session, error) { return session, nil },
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			close(claimEntered)
			<-releaseClaim
			return workers.ClaimResult{}, workers.ErrSessionUnavailable
		},
	}
	broker := &fakeBroker{messages: make(chan queue.Message, 1)}
	broker.messages <- queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", DemoEcho{}))
	runner := NewRunner(control, broker, registry, RunnerConfig{
		Registration: workers.Registration{SessionID: session.ID}, Queue: "default",
		PollWait: time.Second, RetryAttempts: 1, ShutdownTimeout: time.Second,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	select {
	case <-claimEntered:
	case <-time.After(time.Second):
		t.Fatal("worker did not attempt a claim")
	}
	require.True(t, runner.Ready())
	close(releaseClaim)
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrSessionLost)
	case <-time.After(time.Second):
		t.Fatal("fenced worker did not stop")
	}
	require.False(t, runner.Ready())
	require.Equal(t, int32(1), broker.receiveCount.Load())
}

func TestRunner_ShutdownDrainsAndReportsInFlightSuccess(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	entered := make(chan struct{})
	release := make(chan struct{})
	succeeded := make(chan struct{}, 1)
	control := &fakeControl{
		register: func(context.Context, workers.Registration) (workers.Session, error) { return session, nil },
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start: func(context.Context, workers.Fence) error { return nil },
		succeed: func(ctx context.Context, _ workers.Fence) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			succeeded <- struct{}{}
			return nil
		},
	}
	broker := &fakeBroker{messages: make(chan queue.Message, 1)}
	broker.messages <- queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		close(entered)
		<-release
		return nil, nil
	})))
	runner := NewRunner(control, broker, registry, RunnerConfig{
		Registration: workers.Registration{SessionID: session.ID}, Queue: "default",
		PollWait: time.Second, RetryAttempts: 1, ShutdownTimeout: time.Second,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	require.Eventually(t, func() bool { return !runner.Ready() }, time.Second, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("runner returned before its handler drained: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("runner did not finish its bounded drain")
	}
	select {
	case <-succeeded:
	default:
		t.Fatal("in-flight success was not reported")
	}
}

func TestRunner_ShutdownTimeoutBoundsUncooperativeHandler(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	entered := make(chan struct{})
	release := make(chan struct{})
	control := &fakeControl{
		register: func(context.Context, workers.Registration) (workers.Session, error) { return session, nil },
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start:   func(context.Context, workers.Fence) error { return nil },
		succeed: func(context.Context, workers.Fence) error { return nil },
	}
	broker := &fakeBroker{messages: make(chan queue.Message, 1)}
	broker.messages <- queue.Message{ReceiptHandle: "r1", Body: notificationBody(t, "default")}
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
		close(entered)
		<-release // intentionally ignore cancellation until the test releases us
		return nil, ctx.Err()
	})))
	runner := NewRunner(control, broker, registry, RunnerConfig{
		Registration: workers.Registration{SessionID: session.ID}, Queue: "default",
		PollWait: time.Second, RetryAttempts: 1, ShutdownTimeout: 30 * time.Millisecond,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrShutdownTimeout)
	case <-time.After(time.Second):
		t.Fatal("shutdown timeout did not bound the runner")
	}
	close(release)
}

func TestRunner_LocalPoolNeverPollsPastItsBound(t *testing.T) {
	session := testSession(2)
	control := &fakeControl{}
	control.register = func(context.Context, workers.Registration) (workers.Session, error) { return session, nil }
	var sequence atomic.Int32
	control.claim = func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
		assignment := testAssignment(session)
		assignment.AttemptNumber = int(sequence.Add(1))
		return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
	}
	control.start = func(context.Context, workers.Fence) error { return nil }
	control.succeed = func(context.Context, workers.Fence) error { return nil }

	broker := &fakeBroker{messages: make(chan queue.Message, 3)}
	for i := 0; i < 3; i++ {
		broker.messages <- queue.Message{
			ID: uuid.NewString(), ReceiptHandle: uuid.NewString(), Body: notificationBody(t, "default"),
		}
	}

	entered := make(chan struct{}, 3)
	release := make(chan struct{}, 3)
	var active, maximum atomic.Int32
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo", HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
		current := active.Add(1)
		for {
			seen := maximum.Load()
			if current <= seen || maximum.CompareAndSwap(seen, current) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-ctx.Done():
		case <-release:
		}
		active.Add(-1)
		return nil, nil
	})))

	runner := NewRunner(control, broker, registry, RunnerConfig{
		Registration: workers.Registration{SessionID: session.ID},
		Queue:        "default", PollWait: time.Second, RetryAttempts: 1,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("two local slots did not begin execution")
		}
	}
	require.Equal(t, int32(2), broker.receiveCount.Load(), "no third receive may occur while both slots execute")
	release <- struct{}{}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("released slot did not poll the third notification")
	}
	require.Equal(t, int32(2), maximum.Load())

	cancel()
	release <- struct{}{}
	release <- struct{}{}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}
}

var _ ControlPlane = (*fakeControl)(nil)
var _ queue.Broker = (*fakeBroker)(nil)

// notificationBodyForEvent builds a work notification carrying a caller-chosen
// durable event id so a test can deliver true duplicates of one event alongside
// an unrelated one.
func notificationBodyForEvent(t *testing.T, eventID uuid.UUID, queueName string) []byte {
	t.Helper()
	data, err := json.Marshal(outbox.WorkAvailableData{Queue: queueName, JobID: "non-authoritative-hint"})
	require.NoError(t, err)
	event := outbox.Event{
		ID: eventID, Type: outbox.EventWorkAvailable,
		SchemaVersion: outbox.WorkAvailableSchemaVersion,
		Data:          data, CreatedAt: time.Now(),
	}
	body, err := event.Body()
	require.NoError(t, err)
	return body
}

// TestRunner_DuplicateDeliveryReleasesItsSlotBeforeLeaderExecution is the
// bounded-pool regression for duplicate work notifications.
//
// A duplicate used to wait on the leader's entire Start/handler/Succeed path,
// so N copies of one event could starve an N-slot pool for one job's duration.
// The pool here has exactly two slots: the leader holds one inside a blocked
// handler, so unrelated work can only progress if the duplicate returned its
// slot after the claim decision. Every step is a channel barrier.
func TestRunner_DuplicateDeliveryReleasesItsSlotBeforeLeaderExecution(t *testing.T) {
	session := testSession(2)
	slowEventID, fastEventID := uuid.New(), uuid.New()
	slowAssignment := testAssignment(session)
	slowAssignment.JobType = "demo.slow"
	fastAssignment := testAssignment(session)
	fastAssignment.JobType = "demo.fast"

	var claims, starts, succeeds sync.Map // event/attempt id -> *atomic.Int32
	countFor := func(m *sync.Map, key uuid.UUID) *atomic.Int32 {
		counter, _ := m.LoadOrStore(key, &atomic.Int32{})
		return counter.(*atomic.Int32)
	}
	assignmentFor := func(claimID uuid.UUID) *workers.Assignment {
		if claimID == slowEventID {
			return slowAssignment
		}
		return fastAssignment
	}

	slowSucceeded := make(chan struct{})
	control := &fakeControl{
		register: func(context.Context, workers.Registration) (workers.Session, error) {
			return session, nil
		},
		claim: func(_ context.Context, request workers.ClaimRequest) (workers.ClaimResult, error) {
			countFor(&claims, request.ClaimRequestID).Add(1)
			return workers.ClaimResult{
				Disposition: workers.Claimed, Assignment: assignmentFor(request.ClaimRequestID),
			}, nil
		},
		start: func(_ context.Context, fence workers.Fence) error {
			countFor(&starts, fence.AttemptID).Add(1)
			return nil
		},
		succeed: func(_ context.Context, fence workers.Fence) error {
			countFor(&succeeds, fence.AttemptID).Add(1)
			if fence.AttemptID == slowAssignment.AttemptID {
				close(slowSucceeded)
			}
			return nil
		},
	}

	slowEntered := make(chan struct{})
	releaseSlow := make(chan struct{})
	fastRan := make(chan struct{})
	var slowRuns, fastRuns atomic.Int32
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.slow", HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		slowRuns.Add(1)
		close(slowEntered)
		<-releaseSlow
		return nil, nil
	})))
	require.NoError(t, registry.Register("demo.fast", HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		fastRuns.Add(1)
		close(fastRan)
		return nil, nil
	})))

	broker := &fakeBroker{messages: make(chan queue.Message), deletedCh: make(chan string, 8)}
	runner := testRunner(control, broker, registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	deliver := func(receipt string, body []byte) {
		select {
		case broker.messages <- queue.Message{ID: receipt, ReceiptHandle: receipt, Body: body}:
		case <-time.After(5 * time.Second):
			t.Errorf("no free slot accepted receipt %q", receipt)
		}
	}
	acknowledged := map[string]struct{}{}
	awaitAck := func(receipt string) {
		t.Helper()
		for {
			if _, ok := acknowledged[receipt]; ok {
				return
			}
			select {
			case got := <-broker.deletedCh:
				acknowledged[got] = struct{}{}
			case <-time.After(5 * time.Second):
				t.Fatalf("receipt %q was never acknowledged", receipt)
			}
		}
	}
	await := func(signal <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-signal:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s", what)
		}
	}

	slowBody := notificationBodyForEvent(t, slowEventID, "default")
	deliver("slow-original", slowBody)
	await(slowEntered, "the leader's handler to start")

	// The duplicate must join the existing flight rather than lead a second one.
	deliver("slow-duplicate", slowBody)
	awaitAck("slow-duplicate")

	// The only way this can be received is if the duplicate already returned its
	// slot: the other slot is still parked inside the blocked leader handler.
	deliver("fast-unrelated", notificationBodyForEvent(t, fastEventID, "default"))
	await(fastRan, "unrelated work to progress on the freed slot")

	// The leader is still mid-execution, and still owns the event's flight entry.
	require.Equal(t, int32(0), countFor(&succeeds, slowAssignment.AttemptID).Load(),
		"the leader must still be blocked while unrelated work progressed")
	runner.flightsMu.Lock()
	flight := runner.flights[slowEventID]
	require.NotNil(t, flight, "execution ownership must be held until the leader finishes")
	require.Equal(t, 1, flight.followers, "the duplicate must have joined as a follower")
	runner.flightsMu.Unlock()

	close(releaseSlow)
	await(slowSucceeded, "the leader to commit its outcome")
	awaitAck("slow-original")
	awaitAck("fast-unrelated")

	cancel()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not stop after cancellation")
	}

	// The duplicated event resolved to exactly one of everything.
	require.Equal(t, int32(1), countFor(&claims, slowEventID).Load(), "claims for the duplicated event")
	require.Equal(t, int32(1), countFor(&starts, slowAssignment.AttemptID).Load(), "Start for the duplicated event")
	require.Equal(t, int32(1), slowRuns.Load(), "handler executions for the duplicated event")
	require.Equal(t, int32(1), countFor(&succeeds, slowAssignment.AttemptID).Load(), "Succeed for the duplicated event")
	require.Equal(t, int32(1), fastRuns.Load(), "the unrelated event ran exactly once")

	// Both copies of the safe duplicate, plus the unrelated receipt, were acked.
	require.ElementsMatch(t,
		[]string{"slow-original", "slow-duplicate", "fast-unrelated"}, broker.deletedReceipts())

	runner.flightsMu.Lock()
	require.Empty(t, runner.flights, "execution ownership must be released once the leader finishes")
	runner.flightsMu.Unlock()
}

// TestProcessMessage_UnsafeDispositionsNeverReleaseAFollowerReceipt proves the
// prompt follower release did not widen what counts as a safe acknowledgement.
// A follower that joins an in-flight delivery and then observes an unsafe or
// unresolved leader decision must leave its own receipt on the broker.
func TestProcessMessage_UnsafeDispositionsNeverReleaseAFollowerReceipt(t *testing.T) {
	tests := map[string]struct {
		result workers.ClaimResult
		err    error
	}{
		"NO_ELIGIBLE_JOB":        {result: workers.ClaimResult{Disposition: workers.NoEligibleJob}},
		"CAPACITY_EXHAUSTED":     {result: workers.ClaimResult{Disposition: workers.CapacityExhausted}},
		"unresolved claim error": {err: errors.New("control plane unreachable")},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			session := testSession(2)
			eventID := uuid.New()
			body := notificationBodyForEvent(t, eventID, "default")
			leaderClaiming := make(chan struct{})
			releaseClaim := make(chan struct{})
			var claims atomic.Int32
			control := &fakeControl{
				claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
					if claims.Add(1) == 1 {
						close(leaderClaiming)
						<-releaseClaim
					}
					return test.result, test.err
				},
			}
			broker := &fakeBroker{}
			runner := testRunner(control, broker, NewRegistry())

			errs := make(chan error, 2)
			go func() {
				errs <- runner.processMessage(context.Background(), session,
					queue.Message{ID: "leader", ReceiptHandle: "leader", Body: body})
			}()
			select {
			case <-leaderClaiming:
			case <-time.After(5 * time.Second):
				t.Fatal("leader never reached its claim")
			}
			go func() {
				errs <- runner.processMessage(context.Background(), session,
					queue.Message{ID: "follower", ReceiptHandle: "follower", Body: body})
			}()
			// Barrier on durable flight state, not on elapsed time: release the
			// leader only once the second delivery has actually joined as a follower.
			require.Eventually(t, func() bool {
				runner.flightsMu.Lock()
				defer runner.flightsMu.Unlock()
				flight := runner.flights[eventID]
				return flight != nil && flight.followers == 1
			}, 5*time.Second, time.Millisecond, "the duplicate must join the in-flight delivery")
			close(releaseClaim)
			require.NoError(t, <-errs)
			require.NoError(t, <-errs)

			require.Equal(t, 0, broker.deletedCount(),
				"an unsafe or unresolved decision must leave both receipts on the broker")
		})
	}
}
