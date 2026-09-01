// Package workers owns worker identities, process sessions, attempts, leases,
// and the fenced control-plane operations used by trusted worker processes.
package workers

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
)

// Stable domain errors exposed to the HTTP boundary.
var (
	ErrUnknownQueue       = errors.New("unknown queue")
	ErrSessionConflict    = errors.New("worker session id reused with different registration")
	ErrSessionUnavailable = errors.New("worker session is not current and healthy")
	ErrClaimConflict      = errors.New("claim request id reused for a different claim")
	ErrFenceRejected      = errors.New("attempt fence rejected")
	ErrLeaseExpired       = errors.New("lease expired")
	ErrStateConflict      = errors.New("state transition conflict")
	// ErrRenewalConflict reports that a renewal named a generation that is no
	// longer current, or reused a renewal identity that is currently recorded on a
	// different lease. Only each lease's live identity is retained, so reusing one
	// a lease has already superseded is deliberately not a conflict; see ADR-0008's
	// scope note. It is distinct from ErrLeaseExpired: the lease may still be
	// perfectly valid and simply owned by a newer generation.
	ErrRenewalConflict = errors.New("lease renewal generation or identity conflict")
	// ErrDeadlineExceeded reports that a database call in this operation failed
	// because the operation's own deadline elapsed. It is produced only by
	// classifyDatabaseError, from the failing call's returned error — never from
	// an ambient context check — so an unrelated failure that merely happens to
	// coincide with an expired deadline is never reported as this.
	ErrDeadlineExceeded = errors.New("operation deadline exceeded")
	// ErrAttemptTimedOut reports that PostgreSQL time has reached the attempt's
	// persisted execution deadline, so this attempt may no longer commit
	// anything. It is distinct from ErrLeaseExpired: the lease may still be
	// perfectly valid and freshly renewed. Renewal extends lease authority; it
	// never extends the job's timeout_seconds budget.
	//
	// The transition to TIMED_OUT is owned by reconciliation, not by the caller
	// that received this error. A worker cannot declare its own timeout.
	ErrAttemptTimedOut = errors.New("attempt execution deadline reached")
	// ErrOutcomeConflict reports that a terminal outcome request id was reused
	// for a different attempt, or replayed against its own attempt with a
	// different classification, code, or message. Outcome identities are retained
	// for the lifetime of attempt history, so this is a stable domain conflict
	// rather than a leaked uniqueness error.
	ErrOutcomeConflict = errors.New("outcome request id reused for a different outcome")
)

// SessionStatus is the server-owned health state of one process lifetime.
type SessionStatus string

const (
	SessionStarting  SessionStatus = "STARTING"
	SessionHealthy   SessionStatus = "HEALTHY"
	SessionDraining  SessionStatus = "DRAINING"
	SessionUnhealthy SessionStatus = "UNHEALTHY"
	SessionOffline   SessionStatus = "OFFLINE"
)

// AttemptStatus is one attempt's lifecycle, separate from the job lifecycle.
type AttemptStatus string

const (
	AttemptLeased    AttemptStatus = "LEASED"
	AttemptRunning   AttemptStatus = "RUNNING"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptTimedOut  AttemptStatus = "TIMED_OUT"
	AttemptCanceled  AttemptStatus = "CANCELED"
	AttemptAbandoned AttemptStatus = "ABANDONED"
)

// LeaseStatus records whether a fencing lease can still authorize a transition.
type LeaseStatus string

const (
	LeaseActive    LeaseStatus = "ACTIVE"
	LeaseCompleted LeaseStatus = "COMPLETED"
	LeaseExpired   LeaseStatus = "EXPIRED"
	LeaseReleased  LeaseStatus = "RELEASED"
)

// Registration describes one trusted worker process boot. SessionID is created
// once by the process, so retrying a registration after a lost response is
// idempotent.
type Registration struct {
	SessionID         uuid.UUID
	Name              string
	Hostname          string
	WorkerGroup       string
	ConcurrencyLimit  int
	Capabilities      []string
	SupportedJobTypes []string
}

// Session is the durable identity returned by registration.
type Session struct {
	ID                uuid.UUID
	WorkerID          uuid.UUID
	Name              string
	Hostname          string
	WorkerGroup       string
	ConcurrencyLimit  int
	Capabilities      []string
	SupportedJobTypes []string
	Status            SessionStatus
	RegisteredAt      time.Time
	LastHeartbeatAt   time.Time
}

// ClaimRequest identifies one retryable claim operation. ClaimRequestID is the
// durable outbox event id and is reused across HTTP and broker redelivery.
type ClaimRequest struct {
	WorkerID       uuid.UUID
	SessionID      uuid.UUID
	ClaimRequestID uuid.UUID
	Queue          string
}

// ClaimDisposition tells a broker consumer whether a notification is safe to
// acknowledge. A committed assignment, a confirmed empty queue, and a globally
// consumed duplicate notification are the only safe acknowledgements.
type ClaimDisposition string

const (
	Claimed               ClaimDisposition = "CLAIMED"
	QueueEmpty            ClaimDisposition = "QUEUE_EMPTY"
	DuplicateNotification ClaimDisposition = "DUPLICATE_NOTIFICATION"
	NoEligibleJob         ClaimDisposition = "NO_ELIGIBLE_JOB"
	CapacityExhausted     ClaimDisposition = "CAPACITY_EXHAUSTED"
)

// Assignment is the authoritative job data and fencing tuple returned by a
// committed claim. Broker contents are never used as the job definition.
type Assignment struct {
	Scope                string
	JobID                uuid.UUID
	Queue                string
	JobType              string
	Payload              json.RawMessage
	Priority             int
	TimeoutSeconds       int
	RequiredCapabilities []string
	AttemptID            uuid.UUID
	AttemptNumber        int
	LeaseID              uuid.UUID
	LeaseExpiresAt       time.Time
	// LeaseRemaining is sampled from PostgreSQL during the claim response. A
	// DB-less worker converts it into a conservative monotonic local deadline.
	LeaseRemaining time.Duration
	// ExecutionDeadline is worker-local metadata and is never authoritative at
	// the control plane. The PostgreSQL fence remains the final decision.
	ExecutionDeadline time.Time
	WorkerID          uuid.UUID
	SessionID         uuid.UUID
}

// ClaimResult is one committed control-plane decision.
type ClaimResult struct {
	Disposition ClaimDisposition
	Assignment  *Assignment
	Replayed    bool
}

// SafeToAcknowledge reports whether deleting the advisory broker notification
// can leave no eligible work stranded.
func (r ClaimResult) SafeToAcknowledge() bool {
	return (r.Disposition == Claimed && r.Assignment != nil) ||
		((r.Disposition == QueueEmpty || r.Disposition == DuplicateNotification) && r.Assignment == nil)
}

// Fence identifies the only attempt and lease allowed to mutate a job.
type Fence struct {
	JobID     uuid.UUID
	AttemptID uuid.UUID
	LeaseID   uuid.UUID
	WorkerID  uuid.UUID
	SessionID uuid.UUID
}

// HeartbeatRequest identifies the one process session reporting liveness. It
// deliberately carries no timestamp: PostgreSQL receipt time is authoritative
// for staleness, and a worker-supplied clock never is.
type HeartbeatRequest struct {
	WorkerID  uuid.UUID
	SessionID uuid.UUID
}

// HeartbeatResult reports the PostgreSQL time the control plane accepted, so a
// caller can confirm a heartbeat actually advanced rather than assuming it did.
//
// It also carries this session's outstanding cancellation directives. Delivering
// them here rather than on a work notification is deliberate: the heartbeat loop
// already runs unconditionally, while idle and through a graceful drain, so
// cancellation reaches a worker that is busy executing and one that is waiting
// on an empty broker queue alike. Nothing about cancellation delivery depends on
// the broker.
type HeartbeatResult struct {
	SessionID       uuid.UUID
	Status          SessionStatus
	LastHeartbeatAt time.Time
	Cancellations   []CancellationDirective
}

// CancellationDirective tells one worker session that a specific attempt it may
// still be executing has been canceled.
//
// It is advisory in exactly the way a broker notification is: the durable
// decision already committed when the job moved to CANCEL_REQUESTED, and the
// worker cannot make anything true by acting or failing to act on it. What it
// buys is cooperative termination — the handler gets its context canceled and a
// chance to stop — instead of waiting out lease expiry.
type CancellationDirective struct {
	JobID             uuid.UUID
	AttemptID         uuid.UUID
	LeaseID           uuid.UUID
	CancelRequestedAt time.Time
}

// StartResult is the committed LEASED -> RUNNING transition.
//
// It replaces M2's empty response because the attempt's execution deadline is
// now durable state a worker must be told about rather than recompute. A worker
// that derived its own deadline from timeout_seconds after the response arrived
// would silently grant itself the round trip's duration back, and an ambiguous
// Start retry would restart the clock entirely.
type StartResult struct {
	AttemptID uuid.UUID
	StartedAt time.Time
	// TimeoutAt is the persisted per-attempt deadline. Lease renewal never moves
	// it.
	TimeoutAt time.Time
	// Remaining is measured by PostgreSQL after every authority lock. A DB-less
	// worker converts it into a conservative monotonic local deadline instead of
	// comparing its own wall clock with TimeoutAt.
	Remaining time.Duration
	// Replayed is true when this attempt was already RUNNING. The original
	// started_at and timeout_at are returned unchanged; a replay never restarts
	// the timeout.
	Replayed bool
}

// FailureReport is one fenced terminal failure from a trusted worker.
//
// OutcomeRequestID is generated once per logical outcome and reused by every
// retry of it, exactly as a renewal identity is. Retaining it on the attempt for
// the lifetime of history is what makes an ambiguous failure response safe:
// the replay returns the decision that already committed rather than consuming
// another attempt, drawing fresh jitter, or creating a second DLQ entry.
type FailureReport struct {
	Fence            Fence
	OutcomeRequestID uuid.UUID
	Class            lifecycle.FailureClass
	ErrorCode        string
	ErrorMessage     string
}

// CancelAcknowledgment is a worker confirming it stopped a canceled attempt.
type CancelAcknowledgment struct {
	Fence            Fence
	OutcomeRequestID uuid.UUID
}

// OutcomeResult is the committed decision for one terminal attempt outcome.
type OutcomeResult struct {
	JobID uuid.UUID
	// JobStatus and AttemptStatus are the states this outcome produced. They are
	// returned so a worker can log what actually happened rather than assume, and
	// so an exact replay can prove it saw the same answer.
	JobStatus     string
	AttemptStatus AttemptStatus
	// RetryAt and RetryDelay are set when the job entered RETRY_WAIT. They are
	// read back from the attempt on a replay, never recomputed: recomputing would
	// draw fresh jitter and answer a different instant every time.
	RetryAt    *time.Time
	RetryDelay *time.Duration
	// DeadLetterReason is set when the job entered DEAD_LETTERED.
	DeadLetterReason lifecycle.DLQReason
	// Replayed is true when this outcome identity had already committed.
	Replayed bool
}

// RenewalRequest extends one lease's authority window.
//
// Fence is the complete existing five-part fence. RenewalRequestID is generated
// once per logical renewal and reused by every retry of it, and ExpectedVersion
// names the generation the caller believes is current. Together they make an
// ambiguous retry recoverable and a duplicate harmless: renewing twice with one
// identity cannot extend authority twice, and a delayed older generation cannot
// extend it at all.
type RenewalRequest struct {
	Fence            Fence
	RenewalRequestID uuid.UUID
	ExpectedVersion  int
}

// RenewalResult is the committed renewal window.
//
// Remaining is derived from PostgreSQL time sampled after every authority lock,
// so a worker converts a server-measured duration into a local monotonic
// deadline instead of comparing its own wall clock with ExpiresAt.
type RenewalResult struct {
	LeaseID        uuid.UUID
	RenewalVersion int
	ExpiresAt      time.Time
	Remaining      time.Duration
	// Replayed is true when this exact renewal identity had already committed
	// this generation. The stored result is returned unchanged; nothing moved.
	Replayed bool
}

// ReconcileStats counts what one reconciliation pass durably changed.
type ReconcileStats struct {
	StaleSessions int
	ExpiredLeases int
	// TimedOutAttempts counts attempts recorded TIMED_OUT, whether by the
	// dedicated due-timeout scan or by the expired-lease scan recognizing that a
	// lapsed lease's attempt had in fact already timed out.
	TimedOutAttempts int
	// CanceledAttempts counts cancellations finalized by reconciliation after a
	// worker failed to acknowledge one cooperatively.
	CanceledAttempts int
	RequeuedJobs     int
	RetryWaitingJobs int
	DeadLetteredJobs int
	// Skipped counts candidates that no longer qualified once their authority
	// rows were locked and PostgreSQL time was resampled — a renewal moved the
	// expiry forward, or another reconciler got there first.
	Skipped int
}

// Add accumulates one scan's counts into another's.
func (s *ReconcileStats) Add(other ReconcileStats) {
	s.StaleSessions += other.StaleSessions
	s.ExpiredLeases += other.ExpiredLeases
	s.TimedOutAttempts += other.TimedOutAttempts
	s.CanceledAttempts += other.CanceledAttempts
	s.RequeuedJobs += other.RequeuedJobs
	s.RetryWaitingJobs += other.RetryWaitingJobs
	s.DeadLetteredJobs += other.DeadLetteredJobs
	s.Skipped += other.Skipped
}

// FieldError is one registration or control-request validation problem.
type FieldError struct {
	Field   string
	Message string
}

// ValidationError reports every problem in a control request.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string { return "worker control request validation failed" }
