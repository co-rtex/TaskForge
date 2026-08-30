package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// RunnerConfig bounds all local concurrency and retry behavior.
type RunnerConfig struct {
	Registration    workers.Registration
	Queue           string
	PollWait        time.Duration
	RetryAttempts   int
	RetryDelay      time.Duration
	ErrorBackoff    time.Duration
	ShutdownTimeout time.Duration
}

var (
	// ErrSessionLost means the control plane fenced this process boot. Continuing
	// to consume notifications would only hide work from the current session.
	ErrSessionLost = errors.New("worker session is no longer current")
	// ErrShutdownTimeout means at least one trusted handler did not drain within
	// the configured process bound. Go cannot forcibly stop that goroutine, so
	// the process must exit and let durable lease recovery handle it in M3.
	ErrShutdownTimeout = errors.New("worker shutdown drain timed out")
)

type deliveryFlight struct {
	done      chan struct{}
	safeAck   bool
	err       error
	followers int
}

// Runner polls only from fixed local slot goroutines. A slot never polls while
// its handler is executing, so local concurrency cannot exceed registration's
// declared limit even if the broker duplicates every notification.
type Runner struct {
	control   ControlPlane
	broker    queue.Broker
	registry  *Registry
	cfg       RunnerConfig
	log       *slog.Logger
	ready     atomic.Bool
	flightsMu sync.Mutex
	flights   map[uuid.UUID]*deliveryFlight
}

func NewRunner(control ControlPlane, broker queue.Broker, registry *Registry, cfg RunnerConfig, log *slog.Logger) *Runner {
	if cfg.RetryAttempts < 1 {
		cfg.RetryAttempts = 1
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}
	return &Runner{
		control: control, broker: broker, registry: registry, cfg: cfg, log: log,
		flights: make(map[uuid.UUID]*deliveryFlight),
	}
}

// Ready reports whether this process registered successfully and its slot loops
// are accepting work. M3 heartbeats will make current-session loss observable
// even while an idle worker receives no notifications.
func (r *Runner) Ready() bool { return r.ready.Load() }

// Run registers one process session, starts the bounded slot pool, and blocks
// until cancellation or a fatal registration error.
func (r *Runner) Run(ctx context.Context) error {
	registration := r.cfg.Registration
	registration.SupportedJobTypes = r.registry.Types()
	var session workers.Session
	err := r.retry(ctx, func() error {
		var err error
		session, err = r.control.Register(ctx, registration)
		return err
	})
	if err != nil {
		return fmt.Errorf("register worker session: %w", err)
	}

	r.ready.Store(true)
	defer r.ready.Store(false)
	r.log.Info("worker session ready",
		slog.String("worker_id", session.WorkerID.String()),
		slog.String("worker_session_id", session.ID.String()),
		slog.Int("concurrency_limit", session.ConcurrencyLimit),
		slog.String("queue", r.cfg.Queue))

	intakeCtx, stopIntake := context.WithCancel(ctx)
	defer stopIntake()
	completionCtx, stopCompletion := context.WithCancel(context.WithoutCancel(ctx))
	defer stopCompletion()

	var slots sync.WaitGroup
	fatal := make(chan error, 1)
	slotsDone := make(chan struct{})
	for slot := 0; slot < session.ConcurrencyLimit; slot++ {
		slots.Add(1)
		go func(slot int) {
			defer slots.Done()
			if err := r.runSlot(intakeCtx, completionCtx, session, slot); err != nil {
				select {
				case fatal <- err:
				default:
				}
			}
		}(slot)
	}
	go func() {
		slots.Wait()
		close(slotsDone)
	}()

	var runErr error
	select {
	case <-ctx.Done():
		// A requested shutdown is successful if in-flight work drains below.
	case runErr = <-fatal:
		// This session has lost authority. Cancel in-flight handlers because no
		// later transition from this process can be accepted.
		stopCompletion()
	case <-slotsDone:
		select {
		case runErr = <-fatal:
			return runErr
		default:
			return nil
		}
	}
	r.ready.Store(false)
	stopIntake()

	timer := time.NewTimer(r.cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-slotsDone:
		if runErr == nil {
			select {
			case runErr = <-fatal:
			default:
			}
		}
		return runErr
	case <-timer.C:
		stopCompletion()
		return fmt.Errorf("%w after %s", ErrShutdownTimeout, r.cfg.ShutdownTimeout)
	}
}

func (r *Runner) runSlot(intakeCtx, completionCtx context.Context, session workers.Session, slot int) error {
	for intakeCtx.Err() == nil {
		messages, err := r.broker.Receive(intakeCtx, 1, r.cfg.PollWait)
		if err != nil {
			if intakeCtx.Err() != nil {
				return nil
			}
			r.log.Warn("receive work notification", slog.Int("slot", slot), slog.String("error", err.Error()))
			if !waitContext(intakeCtx, r.cfg.ErrorBackoff) {
				return nil
			}
			continue
		}
		if len(messages) == 0 {
			continue
		}
		if err := r.processMessage(completionCtx, session, messages[0]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) processMessage(ctx context.Context, session workers.Session, message queue.Message) error {
	notification, err := decodeWorkNotification(message.Body)
	if err != nil {
		r.log.Warn("reject malformed work notification",
			slog.String("broker_message_id", message.ID), slog.String("error", err.Error()))
		return nil // not safe to acknowledge without a trustworthy queue decision
	}
	if notification.Queue != r.cfg.Queue {
		return nil // another queue's worker must receive it
	}

	flight, leader := r.beginDelivery(notification.EventID)
	if !leader {
		select {
		case <-ctx.Done():
			return nil
		case <-flight.done:
			if flight.safeAck {
				r.acknowledge(ctx, message)
			}
			return flight.err
		}
	}
	safeAck := false
	var flightErr error
	defer func() { r.finishDelivery(notification.EventID, flight, safeAck, flightErr) }()

	request := workers.ClaimRequest{
		WorkerID: session.WorkerID, SessionID: session.ID,
		// The durable outbox event identity survives broker redelivery and an
		// ambiguous HTTP response, allowing the same session to recover the one
		// committed assignment instead of issuing a different claim.
		ClaimRequestID: notification.EventID, Queue: notification.Queue,
	}
	var claim workers.ClaimResult
	err = r.retry(ctx, func() error {
		var err error
		claim, err = r.control.Claim(ctx, request)
		return err
	})
	if err != nil {
		if isSessionLost(err) {
			flightErr = fmt.Errorf("%w: %v", ErrSessionLost, err)
			return flightErr
		}
		r.log.Warn("claim failed",
			slog.String("worker_id", session.WorkerID.String()),
			slog.String("worker_session_id", session.ID.String()),
			slog.String("claim_request_id", request.ClaimRequestID.String()),
			slog.String("error", err.Error()))
		return nil
	}

	if claim.SafeToAcknowledge() {
		safeAck = true
		r.acknowledge(ctx, message)
	}
	if claim.Disposition != workers.Claimed || claim.Assignment == nil {
		return nil
	}

	assignment := claim.Assignment
	handler, ok := r.registry.Lookup(assignment.JobType)
	if !ok {
		// The control plane filters on the immutable registered handler set. This
		// branch is a defensive guard against contract drift, not routing logic.
		r.log.Error("claimed job has no trusted handler",
			slog.String("job_id", assignment.JobID.String()),
			slog.String("job_type", assignment.JobType))
		return nil
	}
	fence := workers.Fence{
		JobID: assignment.JobID, AttemptID: assignment.AttemptID,
		LeaseID: assignment.LeaseID, WorkerID: assignment.WorkerID,
		SessionID: assignment.SessionID,
	}
	if err := r.retry(ctx, func() error { return r.control.Start(ctx, fence) }); err != nil {
		if isSessionLost(err) {
			flightErr = fmt.Errorf("%w: %v", ErrSessionLost, err)
			return flightErr
		}
		r.log.Warn("start attempt rejected", fenceLog(fence, slog.String("error", err.Error()))...)
		return nil
	}

	// Until M3 adds renewal, the client converts PostgreSQL's remaining lease
	// window into a conservative monotonic deadline measured from before the
	// claim request, with completion margin reserved for Succeed. This avoids
	// trusting worker wall-clock alignment with the database.
	leaseCtx := ctx
	cancelLease := func() {}
	if !assignment.ExecutionDeadline.IsZero() {
		leaseCtx, cancelLease = context.WithDeadline(ctx, assignment.ExecutionDeadline)
	}
	executionCtx, cancelExecution := context.WithTimeout(leaseCtx, time.Duration(assignment.TimeoutSeconds)*time.Second)
	if executionErr := executionCtx.Err(); executionErr != nil {
		cancelExecution()
		cancelLease()
		r.log.Error("trusted handler execution window unavailable",
			fenceLog(fence, slog.String("error", executionErr.Error()))...)
		return nil
	}
	_, handlerErr := invokeHandler(executionCtx, handler, Execution{
		JobID: assignment.JobID, AttemptID: assignment.AttemptID, Payload: assignment.Payload,
	})
	// Capture expiry before canceling the derived contexts: canceling them first
	// would make every successful handler look canceled, while ignoring the
	// value would let a cooperative timeout that returns nil be marked SUCCEEDED.
	executionErr := executionCtx.Err()
	cancelExecution()
	cancelLease()
	if handlerErr != nil || executionErr != nil {
		// Failure classification, retry, timeout, and DLQ transitions belong to
		// M4. Leaving this lease active is honest; M3 reconciliation will first
		// make the crashed/error path recoverable.
		failure := handlerErr
		if failure == nil {
			failure = executionErr
		}
		r.log.Error("trusted handler failed", fenceLog(fence, slog.String("error", failure.Error()))...)
		return nil
	}

	if err := r.retry(ctx, func() error { return r.control.Succeed(ctx, fence) }); err != nil {
		if isSessionLost(err) {
			flightErr = fmt.Errorf("%w: %v", ErrSessionLost, err)
			return flightErr
		}
		r.log.Warn("report successful outcome", fenceLog(fence, slog.String("error", err.Error()))...)
		return nil
	}
	r.log.Info("job succeeded", fenceLog(fence)...)
	return nil
}

func (r *Runner) beginDelivery(eventID uuid.UUID) (*deliveryFlight, bool) {
	r.flightsMu.Lock()
	defer r.flightsMu.Unlock()
	if flight, ok := r.flights[eventID]; ok {
		flight.followers++
		return flight, false
	}
	flight := &deliveryFlight{done: make(chan struct{})}
	r.flights[eventID] = flight
	return flight, true
}

func (r *Runner) finishDelivery(eventID uuid.UUID, flight *deliveryFlight, safeAck bool, err error) {
	r.flightsMu.Lock()
	flight.safeAck = safeAck
	flight.err = err
	close(flight.done)
	delete(r.flights, eventID)
	r.flightsMu.Unlock()
}

func (r *Runner) acknowledge(ctx context.Context, message queue.Message) {
	if err := r.broker.Delete(ctx, message.ReceiptHandle); err != nil {
		r.log.Warn("acknowledge work notification",
			slog.String("broker_message_id", message.ID), slog.String("error", err.Error()))
	}
}

func isSessionLost(err error) bool {
	if errors.Is(err, workers.ErrSessionUnavailable) || errors.Is(err, workers.ErrFenceRejected) {
		return true
	}
	var remote *RemoteError
	return errors.As(err, &remote) &&
		(remote.Code == "worker_session_unavailable" || remote.Code == "fence_rejected")
}

func invokeHandler(ctx context.Context, handler Handler, execution Execution) (result []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler.Execute(ctx, execution)
}

func (r *Runner) retry(ctx context.Context, operation func() error) error {
	var err error
	for attempt := 1; attempt <= r.cfg.RetryAttempts; attempt++ {
		err = operation()
		if err == nil {
			return nil
		}
		if !retryable(err) || attempt == r.cfg.RetryAttempts {
			return err
		}
		if !waitContext(ctx, r.cfg.RetryDelay) {
			return ctx.Err()
		}
	}
	return err
}

func retryable(err error) bool {
	var classified interface{ Retryable() bool }
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	return true // transport and unclassified dependency errors are ambiguous
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func fenceLog(fence workers.Fence, extra ...slog.Attr) []any {
	attrs := []any{
		slog.String("job_id", fence.JobID.String()),
		slog.String("attempt_id", fence.AttemptID.String()),
		slog.String("lease_id", fence.LeaseID.String()),
		slog.String("worker_id", fence.WorkerID.String()),
		slog.String("worker_session_id", fence.SessionID.String()),
	}
	for _, attr := range extra {
		attrs = append(attrs, attr)
	}
	return attrs
}
