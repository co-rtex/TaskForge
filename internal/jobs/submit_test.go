package jobs

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func intPtr(v int) *int       { return &v }
func strPtr(v string) *string { return &v }
func validReq() SubmitRequest {
	return SubmitRequest{Queue: "default", Type: "demo.echo", Payload: json.RawMessage(`{"a":1}`)}
}

func TestNormalize_AppliesDefaults(t *testing.T) {
	got, err := validReq().Normalize()
	require.NoError(t, err)
	require.Equal(t, DefaultPriority, got.Priority)
	require.Equal(t, DefaultMaxAttempts, got.MaxAttempts)
	require.Equal(t, DefaultTimeoutSeconds, got.TimeoutSeconds)
	require.Empty(t, got.RequiredCapabilities)
	require.Equal(t, `{"a":1}`, string(got.Payload))
}

// Priority 0 is a legal value and must not be replaced by the default. This is
// why the request field is a pointer.
func TestNormalize_ExplicitZeroPriorityIsKept(t *testing.T) {
	r := validReq()
	r.Priority = intPtr(0)
	got, err := r.Normalize()
	require.NoError(t, err)
	require.Equal(t, 0, got.Priority)
}

func TestNormalize_CapabilitiesAreASortedSet(t *testing.T) {
	r := validReq()
	r.RequiredCapabilities = []string{"gpu", "cpu", "gpu"}
	got, err := r.Normalize()
	require.NoError(t, err)
	require.Equal(t, []string{"cpu", "gpu"}, got.RequiredCapabilities)
}

func TestNormalize_RejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*SubmitRequest)
		wantField string
	}{
		{"missing queue", func(r *SubmitRequest) { r.Queue = "" }, "queue"},
		{"uppercase queue", func(r *SubmitRequest) { r.Queue = "Default" }, "queue"},
		{"queue too long", func(r *SubmitRequest) { r.Queue = strings.Repeat("a", 65) }, "queue"},
		{"missing job_type", func(r *SubmitRequest) { r.Type = "" }, "job_type"},
		{"bad job_type", func(r *SubmitRequest) { r.Type = "Demo Echo" }, "job_type"},
		{"missing payload", func(r *SubmitRequest) { r.Payload = nil }, "payload"},
		{"payload not object", func(r *SubmitRequest) { r.Payload = json.RawMessage(`[1,2]`) }, "payload"},
		{"payload not json", func(r *SubmitRequest) { r.Payload = json.RawMessage(`{`) }, "payload"},
		{"priority too low", func(r *SubmitRequest) { r.Priority = intPtr(-1) }, "priority"},
		{"priority too high", func(r *SubmitRequest) { r.Priority = intPtr(101) }, "priority"},
		{"max_attempts zero", func(r *SubmitRequest) { r.MaxAttempts = intPtr(0) }, "max_attempts"},
		{"max_attempts too high", func(r *SubmitRequest) { r.MaxAttempts = intPtr(101) }, "max_attempts"},
		{"timeout zero", func(r *SubmitRequest) { r.TimeoutSeconds = intPtr(0) }, "timeout_seconds"},
		{"timeout too high", func(r *SubmitRequest) { r.TimeoutSeconds = intPtr(86401) }, "timeout_seconds"},
		{"bad capability", func(r *SubmitRequest) { r.RequiredCapabilities = []string{"GPU!"} }, "required_capabilities[0]"},
		{"too many capabilities", func(r *SubmitRequest) {
			c := make([]string, MaxCapabilities+1)
			for i := range c {
				c[i] = "cpu"
			}
			r.RequiredCapabilities = c
		}, "required_capabilities"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := validReq()
			tc.mutate(&r)
			_, err := r.Normalize()

			var verr *ValidationError
			require.True(t, errors.As(err, &verr), "expected a *ValidationError, got %v", err)
			fields := make([]string, 0, len(verr.Fields))
			for _, f := range verr.Fields {
				fields = append(fields, f.Field)
			}
			require.Contains(t, fields, tc.wantField)
		})
	}
}

// Delayed execution needs the scheduler (milestone M4). Accepting the field and
// running the job immediately would be a lie, so it must be refused explicitly.
func TestNormalize_RejectsScheduledAtAsNotImplemented(t *testing.T) {
	r := validReq()
	r.ScheduledAt = strPtr("2030-01-01T00:00:00Z")
	_, err := r.Normalize()

	var verr *ValidationError
	require.True(t, errors.As(err, &verr))
	require.Len(t, verr.Fields, 1)
	require.Equal(t, "scheduled_at", verr.Fields[0].Field)
	require.Contains(t, verr.Fields[0].Message, "not implemented in this milestone")
}

func TestNormalize_ExplicitNullScheduledAtIsAccepted(t *testing.T) {
	var r SubmitRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"queue":"default","job_type":"demo.echo","payload":{"a":1},"scheduled_at":null
	}`), &r))
	_, err := r.Normalize()
	require.NoError(t, err)
}

func TestNormalize_ReportsEveryProblemAtOnce(t *testing.T) {
	r := SubmitRequest{Queue: "", Type: "", Payload: nil, Priority: intPtr(999)}
	_, err := r.Normalize()

	var verr *ValidationError
	require.True(t, errors.As(err, &verr))
	require.Len(t, verr.Fields, 4)
}

func TestValidateIdempotencyKey(t *testing.T) {
	require.NoError(t, ValidateIdempotencyKey("abc-123"))
	require.NoError(t, ValidateIdempotencyKey(strings.Repeat("k", MaxIdempotencyKeyLen)))

	for _, bad := range []string{"", "   ", strings.Repeat("k", MaxIdempotencyKeyLen+1), "has\nnewline", "tab\there"} {
		require.Error(t, ValidateIdempotencyKey(bad), "key %q should be rejected", bad)
	}
}
