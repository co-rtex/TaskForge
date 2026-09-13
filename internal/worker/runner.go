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

	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// RunnerConfig bounds all local concurrency, liveness, and retry behavior.
type RunnerConfig struct {
	Registration    workers.Registration
	Queue           string
	PollWait        time.Duration
	RetryAttempts   int
	RetryDelay      time.Duration
	ErrorBackoff    time.Duration
	ShutdownTimeout time.Duration

	// HeartbeatInterval is how often this process proves its session is alive.
	// SessionStaleAfter is the server-side threshold it is racing; the process
	// stops accepting work once it has gone that long without a confirmation.
	HeartbeatInterval time.Duration
	SessionStaleAfter time.Duration
	// RenewInterval is how often an executing attempt renews its lease. It must
	// leave room for several attempts inside one lease window; the relationship
	// is validated in internal/config.
	RenewInterval time.Duration
}

var (
	// ErrSessionLost means the control plane fenced this process boot. Continuing
	// to consume notifications would only hide work from the current session.
	ErrSessionLost = errors.New("worker session is no longer current")
	// ErrShutdownTimeout means at least one trusted handler did not drain within
	// the configured process bound. Go cannot forcibly stop that goroutine, so
	// the process exits and leaves any unresolved active lease to server-time
	// expiry and reconciliation.
	ErrShutdownTimeout = errors.New("worker shutdown drain timed out")
)

// deliveryFlight separates two lifetimes that a duplicated notification would
// otherwise conflate.
//
//   - The claim/ack decision is published as soon as the control plane commits a
//     durable disposition. Followers wait only on this, so a duplicate releases
//     its slot instead of idling for the leader's whole handler.
//   - Execution ownership is the map entry itself. It is held until the leader's
//     Start/handler/Succeed path ends, so a duplicate that arrives mid-execution
//     still joins as a follower and can never become a second leader, replay
//     Start against a RUNNING attempt, or invoke the handler again.
type deliveryFlight struct {
	decided chan struct{}
	// safeAck and err are written once under Runner.flightsMu before decided is
	// closed, so the close is the happens-before edge every follower reads through.
	safeAck   bool
	err       error
	settled   bool
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
	// attempts is the concurrency-safe registry that lets a cancellation
	// directive delivered on the heartbeat loop reach the handler goroutine
	// executing that attempt.
	attempts *attemptRegistry
	// now is injected so deadline arithmetic is testable without a real clock.
	// It is only ever used for local monotonic reasoning: PostgreSQL remains the
	// authority for every durable decision.
	now func() time.Time
}

func NewRunner(control ControlPlane, broker queue.Broker, registry *Registry, cfg RunnerConfig, log *slog.Logger) *Runner {
	if cfg.RetryAttempts < 1 {
		cfg.RetryAttempts = 1
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}
	// Defensive fallbacks matching the documented development defaults. A wired
	// process always supplies these from validated configuration; these exist so
	// a zero value can never produce a ticker of zero duration.
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.SessionStaleAfter <= 0 {
		cfg.SessionStaleAfter = 15 * time.Second
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = 10 * time.Second
	}
	return &Runner{
		control: control, broker: broker, registry: registry, cfg: cfg, log: log,
		flights:  make(map[uuid.UUID]*deliveryFlight),
		attempts: newAttemptRegistry(),
		now:      time.Now,
	}
}

// Ready reports whether this process registered successfully and its slot loops
// are accepting work. The heartbeat loop makes current-session loss observable
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

	// The heartbeat runs on the completion context, so it keeps proving liveness
	// through a graceful drain and only stops when in-flight work is finished or
	// abandoned. Losing the session immediately removes readiness and stops
	// intake; the fatal channel then cancels any handler still executing.
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		if err := r.runHeartbeat(completionCtx, session); err != nil {
			r.ready.Store(false)
			stopIntake()
			select {
			case fatal <- err:
			default:
			}
		}
	}()
	// Registered after stopCompletion so it runs first: cancel the loop, then
	// wait for it, so Run never returns while a heartbeat goroutine is live.
	defer func() {
		stopCompletion()
		<-heartbeatDone
	}()
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
		// A follower waits only for the durable claim decision, never for the
		// leader's execution. It acknowledges its own receipt and frees its slot.
		select {
		case <-ctx.Done():
			return nil
		case <-flight.decided:
			if flight.safeAck {
				r.acknowledge(ctx, message)
			}
			return flight.err
		}
	}
	defer r.endDelivery(notification.EventID, flight)

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
			sessionLost := fmt.Errorf("%w: %v", ErrSessionLost, err)
			r.decideDelivery(flight, false, sessionLost)
			return sessionLost
		}
		r.log.Warn("claim failed",
			slog.String("worker_id", session.WorkerID.String()),
			slog.String("worker_session_id", session.ID.String()),
			slog.String("claim_request_id", request.ClaimRequestID.String()),
			slog.String("error", err.Error()))
		// An unresolved claim error is never a safe acknowledgement: the receipt
		// must return to the broker so the still-queued work stays reachable.
		r.decideDelivery(flight, false, nil)
		return nil
	}

	// The durable disposition is committed, so publish it before execution
	// begins. NO_ELIGIBLE_JOB and capacity exhaustion report false here exactly
	// as before; only SafeToAcknowledge outcomes release a receipt.
	safeAck := claim.SafeToAcknowledge()
	r.decideDelivery(flight, safeAck, nil)
	if safeAck {
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
	// Registered BEFORE Start, so a cancellation that wins in the window between
	// the claim committing and the handler being invoked is retained rather than
	// dropped. Unregistering is deferred to the end of the whole outcome path,
	// including outcome reporting, so a directive that arrives while the report
	// is in flight is still observable.
	r.attempts.register(fence)
	defer r.attempts.unregister(fence.AttemptID)

	// Captured before the Start round trip, so the local deadline derived from
	// the server-measured remaining budget cannot silently include time the round
	// trip itself consumed.
	timeoutStarted := r.now()
	var start workers.StartResult
	if err := r.retry(ctx, func() error {
		var err error
		start, err = r.control.Start(ctx, fence)
		return err
	}); err != nil {
		if isSessionLost(err) {
			// The claim decision was already published; this session-loss is the
			// leader's own fatal signal and does not change any follower's ack.
			return fmt.Errorf("%w: %v", ErrSessionLost, err)
		}
		// Cancellation can win between the claim committing and Start being
		// accepted, and this is the one branch where dropping the attempt would
		// be wrong. This worker still holds it, so the job would sit in
		// CANCEL_REQUESTED until its lease lapsed even though a cooperative
		// worker was right here — the exact wait the acknowledgment exists to
		// avoid.
		//
		// Two independent signals, because either can arrive alone: the
		// directive may have reached the registry over the heartbeat before
		// Start was even sent, or Start itself may be the first this process
		// hears of it. The handler never ran, so there is nothing to cancel
		// locally; the acknowledgment is the whole response, and the deferred
		// unregister above runs only after it.
		if r.attempts.wasCanceled(fence.AttemptID) || isCancellationRequested(err) {
			r.log.Info("cancellation won before the attempt started", fenceLog(fence)...)
			r.acknowledgeCancellation(ctx, fence)
			return nil
		}
		r.log.Warn("start attempt rejected", fenceLog(fence, slog.String("error", err.Error()))...)
		return nil
	}

	// Lease authority is a cancelable context rather than a fixed deadline: the
	// renewal loop owns it and cancels it the moment it can no longer prove this
	// lease is still ours. The starting deadline is the conservative monotonic
	// window the client already derived from PostgreSQL's own measurement, so
	// worker wall-clock alignment with the database is never assumed.
	authorityDeadline := assignment.ExecutionDeadline
	if authorityDeadline.IsZero() {
		authorityDeadline = r.now().Add(executionBudget(assignment.LeaseRemaining))
	}
	// A window that is already gone is not something renewal can rescue: the
	// handler must not run at all, because nothing it produces could be committed.
	if !r.now().Before(authorityDeadline) {
		r.log.Error("trusted handler execution window unavailable",
			fenceLog(fence, slog.String("error", "lease authority window had already elapsed"))...)
		return nil
	}
	leaseCtx, cancelLease := context.WithCancelCause(ctx)
	renewal := r.startRenewal(leaseCtx, func() { cancelLease(errAuthorityLost) }, fence, authorityDeadline)

	// The attempt's execution budget comes from the server-measured remaining
	// duration Start returned, NOT from a fresh timeout_seconds timer started
	// once the response landed. Two things depend on that:
	//
	//   - the round trip is not silently given back to the handler; and
	//   - an ambiguous Start retry inherits the ORIGINAL deadline, because the
	//     control plane returns the deadline it stamped the first time.
	//
	// The local deadline is conservative — a margin earlier than the server's —
	// so a cooperative handler is asked to stop before the instant at which
	// nothing it produces could commit. PostgreSQL remains authoritative:
	// reconciliation, not this timer, records TIMED_OUT.
	//
	// It is derived from leaseCtx and is never reset by renewal. Renewal extends
	// lease authority; the job's timeout_seconds budget is measured once.
	timeoutDeadline := timeoutStarted.Add(executionBudget(start.Remaining))
	executionCtx, cancelExecution := context.WithDeadlineCause(leaseCtx, timeoutDeadline, errAttemptTimedOut)
	if executionErr := executionCtx.Err(); executionErr != nil {
		cancelExecution()
		renewal.Stop()
		cancelLease(errAuthorityLost)
		r.log.Error("trusted handler execution window unavailable",
			fenceLog(fence, slog.String("error", executionErr.Error()))...)
		return nil
	}

	// A separate cancellable layer under the deadline, so a cancellation
	// directive can stop the handler with its own distinguishable cause.
	handlerCtx, cancelHandler := context.WithCancelCause(executionCtx)
	r.attempts.bind(fence.AttemptID, cancelHandler)

	_, handlerErr := invokeHandler(handlerCtx, handler, Execution{
		JobID: assignment.JobID, AttemptID: assignment.AttemptID, Payload: assignment.Payload,
	})
	// Captured before the derived contexts are cancelled: cancelling first would
	// make every successful handler look cancelled, while ignoring the value
	// would let a cooperative timeout that returns nil be reported as success.
	cause := context.Cause(handlerCtx)
	canceled := r.attempts.wasCanceled(fence.AttemptID)
	cancelHandler(nil)
	cancelExecution()
	// Stopping the renewal loop before reporting the outcome is what serializes
	// renewal against a terminal report from this process: exactly one of them is
	// in flight at a time, and the control plane resolves any remaining race with
	// reconciliation.
	authorityLost, fatalErr := renewal.Stop()
	cancelLease(errAuthorityLost)
	if fatalErr != nil {
		// This whole process boot lost its session. Report it as fatal so intake
		// stops and readiness drops, not just this one attempt.
		return fatalErr
	}

	// Precedence. Engine-owned causes outrank whatever the handler returned,
	// because a handler cancelled by a timeout is very likely to return an error
	// of its own and that error is not what happened.
	switch {
	case canceled || errors.Is(cause, errUserCanceled):
		// Cooperative cancellation. One outcome identity is generated here and
		// reused by every retry, so an ambiguous acknowledgment cannot become two.
		r.acknowledgeCancellation(ctx, fence)
		return nil

	case errors.Is(cause, errAttemptTimedOut):
		// The attempt outlived its budget. Report NOTHING: only reconciliation
		// may record TIMED_OUT, and sending an ordinary failure here would
		// mislabel the attempt. If the handler was uncooperative and is still
		// running, it cannot commit anything either — the deadline is persisted
		// and every fenced operation checks it.
		r.log.Warn("trusted handler exceeded its attempt deadline",
			fenceLog(fence, slog.String("outcome", "left to timeout reconciliation"))...)
		return nil

	case authorityLost || errors.Is(cause, errAuthorityLost):
		// The lease, session, or generation stopped being provable, and nobody
		// canceled this job. Report nothing and let M3 recovery happen: the lease
		// expires on server time and reconciliation abandons the attempt.
		r.log.Warn("lease authority lost before an outcome could be reported",
			fenceLog(fence)...)
		return nil

	case ctx.Err() != nil && handlerErr != nil:
		// Shutdown, not cancellation. Reporting this as either a failure or a
		// cancellation would attribute a local process decision to the job.
		r.log.Warn("handler interrupted by worker shutdown",
			fenceLog(fence, slog.String("cause", errWorkerShutdown.Error()))...)
		return nil

	case handlerErr != nil:
		r.reportFailure(ctx, fence, handlerErr)
		return nil
	}

	if err := r.retry(ctx, func() error { return r.control.Succeed(ctx, fence) }); err != nil {
		if isSessionLost(err) {
			// The claim decision was already published; this session-loss is the
			// leader's own fatal signal and does not change any follower's ack.
			return fmt.Errorf("%w: %v", ErrSessionLost, err)
		}
		r.log.Warn("report successful outcome", fenceLog(fence, slog.String("error", err.Error()))...)
		return nil
	}
	r.log.Info("job succeeded", fenceLog(fence)...)
	return nil
}

// reportFailure classifies a handler error and reports it under ONE outcome
// identity that every retry reuses.
//
// The identity is generated before the retry loop, not inside it. Generating a
// fresh one per attempt is precisely the bug the retained identity exists to
// prevent: a committed-but-lost failure response would then consume a second
// place in the attempt budget and draw fresh jitter for a different retry
// instant.
func (r *Runner) reportFailure(ctx context.Context, fence workers.Fence, handlerErr error) {
	report := workers.FailureReport{
		Fence:            fence,
		OutcomeRequestID: uuid.New(),
	}
	report.Class, report.ErrorCode, report.ErrorMessage = classifyHandlerError(handlerErr)

	var result workers.OutcomeResult
	if err := r.retry(ctx, func() error {
		var err error
		result, err = r.control.Fail(ctx, report)
		return err
	}); err != nil {
		// Nothing else to do: the lease stays as it is and expires on server
		// time, and reconciliation recovers the attempt. The raw handler error is
		// deliberately absent from this line — only the classification and the
		// stable code are logged.
		r.log.Warn("report failed outcome",
			fenceLog(fence,
				slog.String("failure_class", string(report.Class)),
				slog.String("error_code", report.ErrorCode),
				slog.String("error", err.Error()))...)
		return
	}
	r.log.Info("job attempt failed",
		fenceLog(fence,
			slog.String("failure_class", string(report.Class)),
			slog.String("error_code", report.ErrorCode),
			slog.String("job_status", result.JobStatus))...)
}

// acknowledgeCancellation reports the dedicated fenced cancellation outcome,
// reusing one identity across retries for the same reason a failure report does.
func (r *Runner) acknowledgeCancellation(ctx context.Context, fence workers.Fence) {
	ack := workers.CancelAcknowledgment{Fence: fence, OutcomeRequestID: uuid.New()}
	if err := r.retry(ctx, func() error {
		_, err := r.control.AcknowledgeCancellation(ctx, ack)
		return err
	}); err != nil {
		// A rejection here is not a problem to solve locally. The job is already
		// CANCEL_REQUESTED, the lease will lapse, and reconciliation finalizes the
		// cancellation without this worker's help.
		r.log.Warn("acknowledge cancellation",
			fenceLog(fence, slog.String("error", err.Error()))...)
		return
	}
	r.log.Info("job attempt canceled", fenceLog(fence)...)
}

// classifyHandlerError turns a handler error into bounded, safe, typed failure
// detail.
//
// A trusted handler may declare its own classification, stable code, and safe
// message through Retryable or Permanent. Anything else — a plain error, a
// wrapped dependency error, a recovered panic — becomes a generic retryable
// failure with a generic message, and its raw text is neither stored, returned,
// nor logged. That text is the one place payload fragments, credentials, driver
// output, and stack traces reliably show up.
func classifyHandlerError(err error) (lifecycle.FailureClass, string, string) {
	var failure *FailureError
	if errors.As(err, &failure) && failure.Class.ReportableByHandler() {
		return failure.Class, failure.Code, lifecycle.SafeMessage(failure.Message)
	}
	return lifecycle.ClassRetryable, lifecycle.CodeHandlerError, lifecycle.MessageHandlerError
}

func (r *Runner) beginDelivery(eventID uuid.UUID) (*deliveryFlight, bool) {
	r.flightsMu.Lock()
	defer r.flightsMu.Unlock()
	if flight, ok := r.flights[eventID]; ok {
		flight.followers++
		return flight, false
	}
	flight := &deliveryFlight{decided: make(chan struct{})}
	r.flights[eventID] = flight
	return flight, true
}

// decideDelivery publishes the durable claim decision to current and future
// followers without releasing execution ownership.
func (r *Runner) decideDelivery(flight *deliveryFlight, safeAck bool, err error) {
	r.flightsMu.Lock()
	defer r.flightsMu.Unlock()
	r.settleLocked(flight, safeAck, err)
}

// endDelivery releases execution ownership once the leader's whole path has
// ended. The defensive settle covers any leader return that never reached a
// durable decision: followers then learn the receipt is not safe to acknowledge
// rather than blocking forever.
func (r *Runner) endDelivery(eventID uuid.UUID, flight *deliveryFlight) {
	r.flightsMu.Lock()
	defer r.flightsMu.Unlock()
	r.settleLocked(flight, false, nil)
	delete(r.flights, eventID)
}

func (r *Runner) settleLocked(flight *deliveryFlight, safeAck bool, err error) {
	if flight.settled {
		return
	}
	flight.settled = true
	flight.safeAck = safeAck
	flight.err = err
	close(flight.decided)
}

func (r *Runner) acknowledge(ctx context.Context, message queue.Message) {
	if err := r.broker.Delete(ctx, message.ReceiptHandle); err != nil {
		r.log.Warn("acknowledge work notification",
			slog.String("broker_message_id", message.ID), slog.String("error", err.Error()))
	}
}

// isCancellationRequested reports the one Start rejection a worker must answer
// rather than abandon.
func isCancellationRequested(err error) bool {
	if errors.Is(err, workers.ErrCancellationRequested) {
		return true
	}
	var remote *RemoteError
	return errors.As(err, &remote) && remote.Code == "cancellation_requested"
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
