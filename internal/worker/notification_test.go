package worker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/outbox"
)

func notificationBody(t *testing.T, queueName string) []byte {
	t.Helper()
	data, err := json.Marshal(outbox.WorkAvailableData{
		Queue: queueName,
		// Deliberately not a UUID: the broker job id is a non-authoritative hint
		// and the decoder must not turn it into an execution target.
		JobID: "non-authoritative-hint",
	})
	require.NoError(t, err)
	event := outbox.Event{
		ID: uuid.New(), Type: outbox.EventWorkAvailable,
		SchemaVersion: outbox.WorkAvailableSchemaVersion,
		Data:          data, CreatedAt: time.Now(),
	}
	body, err := event.Body()
	require.NoError(t, err)
	return body
}

func TestDecodeWorkNotification_UsesEventIdentityAndQueueRouting(t *testing.T) {
	got, err := decodeWorkNotification(notificationBody(t, "default"))
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, got.EventID)
	require.Equal(t, "default", got.Queue)
}

func TestDecodeWorkNotification_RejectsMalformedAndUnknownVersions(t *testing.T) {
	_, err := decodeWorkNotification([]byte(`not-json`))
	require.Error(t, err)

	body := notificationBody(t, "default")
	var envelope outbox.Envelope
	require.NoError(t, json.Unmarshal(body, &envelope))
	envelope.SchemaVersion++
	body, err = json.Marshal(envelope)
	require.NoError(t, err)
	_, err = decodeWorkNotification(body)
	require.Error(t, err)
}
