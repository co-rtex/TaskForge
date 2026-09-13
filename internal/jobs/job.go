// Package jobs owns the authoritative job model: its typed state machine, its
// submission contract, and the single transaction that accepts a job durably
// and idempotently.
package jobs

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Status is a job's position in the V1 state machine
// (docs/ARCHITECTURE.md section 4).
//
// There is deliberately no job-level FAILED status: one failed attempt is not a
// job outcome. Permanent and exhausted failure are represented by
// StatusDeadLettered, and individual failures stay visible in attempt history.
type Status string

const (
	StatusPending         Status = "PENDING"          // durable, not yet eligible
	StatusQueued          Status = "QUEUED"           // eligible to be claimed
	StatusLeased          Status = "LEASED"           // attempt and active lease exist
	StatusRunning         Status = "RUNNING"          // the valid attempt is executing
	StatusRetryWait       Status = "RETRY_WAIT"       // waiting for a durable retry time
	StatusCancelRequested Status = "CANCEL_REQUESTED" // cancellation won, being delivered
	StatusSucceeded       Status = "SUCCEEDED"        // terminal
	StatusCanceled        Status = "CANCELED"         // terminal
	StatusDeadLettered    Status = "DEAD_LETTERED"    // terminal
)

// AllStatuses lists every status the database CHECK constraint permits. It is
// the single Go-side source of truth for that set.
func AllStatuses() []Status {
	return []Status{
		StatusPending, StatusQueued, StatusLeased, StatusRunning, StatusRetryWait,
		StatusCancelRequested, StatusSucceeded, StatusCanceled, StatusDeadLettered,
	}
}

// Valid reports whether s is a recognized status.
func (s Status) Valid() bool {
	for _, known := range AllStatuses() {
		if s == known {
			return true
		}
	}
	return false
}

// Terminal reports whether s can never transition again. A terminal job never
// returns to a non-terminal state (invariant 2).
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusCanceled, StatusDeadLettered:
		return true
	default:
		return false
	}
}

func (s Status) String() string { return string(s) }

// Executing reports the states in which an attempt of this job may still be
// running. It is deliberately a closed list rather than a "not terminal" test:
// CANCEL_REQUESTED is non-terminal but must stop start, success, failure, and
// renewal from committing.
func (s Status) Executing() bool {
	return s == StatusLeased || s == StatusRunning
}

// Job is a durable job record.
//
// M4 makes the whole V1 state machine reachable: delayed submission produces
// PENDING, retry produces RETRY_WAIT, cancellation produces CANCEL_REQUESTED
// and CANCELED, and permanent or exhausted failure produces DEAD_LETTERED.
type Job struct {
	ID                   uuid.UUID       `json:"id"`
	Scope                string          `json:"-"` // never returned to a client
	Queue                string          `json:"queue"`
	Type                 string          `json:"job_type"`
	Payload              json.RawMessage `json:"payload"`
	Status               Status          `json:"status"`
	Priority             int             `json:"priority"`
	MaxAttempts          int             `json:"max_attempts"`
	TimeoutSeconds       int             `json:"timeout_seconds"`
	RequiredCapabilities []string        `json:"required_capabilities"`

	// ScheduledAt is the requested earliest execution instant, canonicalized to
	// UTC. Nil means the job was submitted for immediate execution.
	ScheduledAt *time.Time `json:"scheduled_at"`
	// AvailableAt is the authoritative eligibility time PostgreSQL orders and
	// filters claims by. It starts at the schedule (or at submission time) and
	// is moved forward by retry backoff.
	AvailableAt time.Time `json:"available_at"`
	// CancelRequestedAt is the PostgreSQL instant cancellation won.
	CancelRequestedAt *time.Time `json:"cancel_requested_at"`
	// ReplayedFromJobID names the terminal job this one replaces. Replay never
	// resurrects a terminal job; it creates a new one linked back to it.
	ReplayedFromJobID *uuid.UUID `json:"replayed_from_job_id"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
