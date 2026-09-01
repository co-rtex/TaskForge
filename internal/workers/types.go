// Package workers owns worker identities, process sessions, attempts, leases,
// and the fenced control-plane operations used by trusted worker processes.
package workers

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
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
type HeartbeatResult struct {
	SessionID       uuid.UUID
	Status          SessionStatus
	LastHeartbeatAt time.Time
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
	StaleSessions    int
	ExpiredLeases    int
	RequeuedJobs     int
	DeadLetteredJobs int
	// Skipped counts candidates that no longer qualified once their authority
	// rows were locked and PostgreSQL time was resampled — a renewal moved the
	// expiry forward, or another reconciler got there first.
	Skipped int
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
