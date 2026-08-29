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
	Data      json.RawMessage
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

// Envelope is the wire format published to the broker.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Data          json.RawMessage `json:"data"`
}

// Envelope renders the event as its published wire form.
func (e Event) Envelope() Envelope {
	return Envelope{
		EventID:       e.ID.String(),
		EventType:     e.Type,
		SchemaVersion: e.SchemaVersion,
		OccurredAt:    e.CreatedAt.UTC(),
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
