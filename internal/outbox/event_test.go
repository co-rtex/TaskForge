package outbox

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEventBody_HasVersionedEnvelope(t *testing.T) {
	id := uuid.New()
	created := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	e := Event{
		ID:            id,
		Type:          EventWorkAvailable,
		SchemaVersion: WorkAvailableSchemaVersion,
		Data:          json.RawMessage(`{"queue":"default","job_id":"11111111-1111-1111-1111-111111111111"}`),
		CreatedAt:     created,
	}

	body, err := e.Body()
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, id.String(), got["event_id"])
	require.Equal(t, "work.available", got["event_type"])
	require.EqualValues(t, 1, got["schema_version"])
	require.Contains(t, got, "occurred_at")
	require.Contains(t, got, "data")
}

// The broker is a notification channel, not a store. A notification carries
// identifiers and routing hints only: putting the authoritative job payload on
// it would duplicate authoritative state into a lossy, duplicating channel and
// leak job contents to anything that can read the broker.
func TestEventBody_NeverCarriesTheAuthoritativeJobPayload(t *testing.T) {
	e := Event{
		ID:            uuid.New(),
		Type:          EventWorkAvailable,
		SchemaVersion: WorkAvailableSchemaVersion,
		CreatedAt:     time.Now(),
	}
	raw, err := json.Marshal(WorkAvailableData{Queue: "default", JobID: uuid.NewString()})
	require.NoError(t, err)
	e.Data = raw

	body, err := e.Body()
	require.NoError(t, err)
	require.NotContains(t, string(body), `"payload"`)

	var env Envelope
	require.NoError(t, json.Unmarshal(body, &env))

	var data map[string]any
	require.NoError(t, json.Unmarshal(env.Data, &data))
	require.ElementsMatch(t, []string{"queue", "job_id"}, keysOf(data),
		"work.available data must contain only routing hints")
}

func TestEnvelope_OccurredAtIsUTC(t *testing.T) {
	loc := time.FixedZone("UTC-5", -5*3600)
	e := Event{ID: uuid.New(), Type: EventWorkAvailable, SchemaVersion: 1,
		Data: json.RawMessage(`{}`), CreatedAt: time.Date(2026, 8, 29, 12, 0, 0, 0, loc)}
	require.Equal(t, time.UTC, e.Envelope().OccurredAt.Location())
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
