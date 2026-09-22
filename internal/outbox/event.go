// Package outbox implements the transactional outbox: broker notifications are
// written in the same PostgreSQL transaction as the state change they describe,
// then published by a separate process.
//
// See docs/adr/0004-transactional-outbox.md.
package outbox

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Event types and their schema versions. A change to the shape of an event's
// data member must bump its version so consumers can tell the shapes apart.
//
// The ENVELOPE is additive-only and is deliberately not versioned by this
// constant. That distinction was not written down before M6B added the trace
// member, and a wire contract between two processes that can run different
// versions during a rolling restart needs it stated rather than assumed:
//
//   - A new OPTIONAL envelope member may be added without a version bump. An
//     older consumer ignores an unknown JSON field, so it keeps working, and a
//     newer consumer reading an older envelope sees the member absent, which is
//     the same thing it sees when the member is legitimately empty.
//   - A change to the DATA member -- adding a required field, removing one,
//     changing a meaning -- bumps WorkAvailableSchemaVersion, because
//     decodeWorkNotification refuses a version it does not recognize and that
//     refusal is the whole point.
//
// M6B's Trace member is optional and additive, so it did not bump the version.
const (
	EventWorkAvailable         = "work.available"
	WorkAvailableSchemaVersion = 1
)

// Event is one row of the outbox.
type Event struct {
	ID            uuid.UUID
	Type          string
	SchemaVersion int
	// Data is the envelope's data member: identifiers and routing hints only.
	Data json.RawMessage
	// Trace is the persisted W3C trace context of the transaction that wrote
	// this event, or nil. See docs/adr/0004-transactional-outbox.md for why
	// the publisher cannot recover it any other way.
	Trace     *TraceContext
	Attempts  int
	CreatedAt time.Time
}

// WorkAvailableData tells a consumer that a queue may have claimable work.
//
// JobID is a hint for tracing and logging only. It is explicitly NOT
// authoritative: a worker must never execute it directly. Work is obtained by
// asking the control plane to claim, which is what enforces priority,
// capability matching, capacity, and single execution.
//
// The authoritative job payload is deliberately absent. Putting it here would
// duplicate authoritative state into a lossy, duplicating channel and leak job
// contents to anyone who can read the broker.
type WorkAvailableData struct {
	Queue string `json:"queue"`
	JobID string `json:"job_id"`
}

// TraceContext is the W3C trace context of the transaction that wrote an
// event, carried to whichever process eventually consumes it.
//
// It is metadata about the causal chain and never about the work itself: a
// consumer that ignores it entirely still behaves correctly, which is exactly
// why it is safe to put on a lossy, duplicating channel when the authoritative
// job payload is not.
type TraceContext struct {
	Traceparent string `json:"traceparent"`
	Tracestate  string `json:"tracestate,omitempty"`
}

// Envelope is the wire format published to the broker.
type Envelope struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	SchemaVersion int       `json:"schema_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	// Trace is absent when the event was written with no active span --
	// tracing disabled, an event predating M6B, or one of the
	// server-initiated notifications that is not a continuation of a client
	// request. A consumer treats its absence as "start a new root span".
	Trace *TraceContext   `json:"trace,omitempty"`
	Data  json.RawMessage `json:"data"`
}

// Envelope renders the event as its published wire form.
func (e Event) Envelope() Envelope {
	return Envelope{
		EventID:       e.ID.String(),
		EventType:     e.Type,
		SchemaVersion: e.SchemaVersion,
		OccurredAt:    e.CreatedAt.UTC(),
		Trace:         e.Trace,
		Data:          e.Data,
	}
}

// Body serializes the event for publication.
func (e Event) Body() ([]byte, error) {
	b, err := json.Marshal(e.Envelope())
	if err != nil {
		return nil, fmt.Errorf("marshal envelope for event %s: %w", e.ID, err)
	}
	return b, nil
}
