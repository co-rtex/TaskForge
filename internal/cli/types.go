package cli

// Request bodies this CLI constructs, mirroring the request schemas in
// api/openapi.yaml. There are no corresponding response types: every
// command passes the server's JSON response bytes straight through to
// stdout or stderr unmodified (see emit in run.go), so the server's own
// schema is the only contract that needs to stay in sync -- this CLI never
// re-derives or re-serializes a response.

// SubmitJobRequest mirrors api/openapi.yaml's SubmitJobRequest schema. The
// server owns validation (unknown queue, out-of-range fields, malformed
// timestamps); this type exists only for wire-format correctness.
type SubmitJobRequest struct {
	Queue                string         `json:"queue"`
	JobType              string         `json:"job_type"`
	Payload              map[string]any `json:"payload"`
	Priority             int            `json:"priority"`
	MaxAttempts          int            `json:"max_attempts"`
	TimeoutSeconds       int            `json:"timeout_seconds"`
	ScheduledAt          *string        `json:"scheduled_at,omitempty"`
	RequiredCapabilities []string       `json:"required_capabilities"`
}

// apiKeyCreateRequest mirrors api/openapi.yaml's ApiKeyCreateRequest
// schema, shared verbatim by /internal/v1/api-keys and
// /internal/v1/worker-keys (WorkerKeyCreateRequest has the identical
// shape).
type apiKeyCreateRequest struct {
	Scope string `json:"scope"`
	Name  string `json:"name"`
}
