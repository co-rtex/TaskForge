package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/co-rtex/TaskForge/internal/jobs"
)

// Stable machine-readable error codes. Clients branch on these, not on messages,
// so they are part of the public contract and must not change meaning.
const (
	CodeValidationFailed    = "validation_failed"
	CodeMalformedJSON       = "malformed_json"
	CodeUnknownQueue        = "unknown_queue"
	CodeIdempotencyConflict = "idempotency_conflict"
	CodeNotFound            = "not_found"
	CodePayloadTooLarge     = "payload_too_large"
	CodeMethodNotAllowed    = "method_not_allowed"
	CodeInternal            = "internal_error"
	CodeServiceUnavailable  = "service_unavailable"
	CodeSessionConflict     = "worker_session_conflict"
	CodeSessionUnavailable  = "worker_session_unavailable"
	CodeClaimConflict       = "claim_conflict"
	CodeFenceRejected       = "fence_rejected"
	CodeLeaseExpired        = "lease_expired"
	CodeStateConflict       = "state_conflict"
	CodeRenewalConflict     = "renewal_conflict"
)

// ErrorBody is the single error shape every endpoint returns.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes one failure. Details is populated for validation errors.
type ErrorDetail struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Details   []jobs.FieldError `json:"details,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, log *slog.Logger, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status line is already sent, so this can only be logged.
		log.Error("write response body", slog.String("error", err.Error()))
	}
}

// writeError renders a sanitized error. Internal detail never reaches a client;
// the request id is the thread back to the server logs that hold it.
func writeError(w http.ResponseWriter, r *http.Request, log *slog.Logger, status int, code, message string, details []jobs.FieldError) {
	writeJSON(w, log, status, ErrorBody{Error: ErrorDetail{
		Code:      code,
		Message:   message,
		Details:   details,
		RequestID: RequestIDFrom(r.Context()),
	}})
}
