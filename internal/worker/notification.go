package worker

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/outbox"
)

// workNotification contains only the routing information a worker may trust
// from the broker. The job-id hint is intentionally discarded.
type workNotification struct {
	EventID uuid.UUID
	Queue   string
}

func decodeWorkNotification(body []byte) (workNotification, error) {
	var envelope outbox.Envelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return workNotification{}, fmt.Errorf("decode notification envelope: %w", err)
	}
	if envelope.EventType != outbox.EventWorkAvailable ||
		envelope.SchemaVersion != outbox.WorkAvailableSchemaVersion {
		return workNotification{}, fmt.Errorf("unsupported notification type %q version %d",
			envelope.EventType, envelope.SchemaVersion)
	}
	eventID, err := uuid.Parse(envelope.EventID)
	if err != nil || eventID == uuid.Nil {
		return workNotification{}, fmt.Errorf("notification event id is not a UUID")
	}
	var data outbox.WorkAvailableData
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		return workNotification{}, fmt.Errorf("decode work notification data: %w", err)
	}
	if data.Queue == "" {
		return workNotification{}, fmt.Errorf("notification queue is empty")
	}
	return workNotification{EventID: eventID, Queue: data.Queue}, nil
}
