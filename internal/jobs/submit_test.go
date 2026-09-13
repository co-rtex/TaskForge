package jobs

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

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

// Delayed execution is implemented in M4. The timestamp is canonicalized to UTC
// at the boundary so that persistence, the comparison against PostgreSQL time,
// and the idempotency fingerprint all see one representation of the instant.
func TestNormalize_AcceptsAndCanonicalizesScheduledAt(t *testing.T) {
	r := validReq()
	r.ScheduledAt = strPtr("2030-01-01T00:00:00Z")
	normalized, err := r.Normalize()
	require.NoError(t, err)
	require.NotNil(t, normalized.ScheduledAt)
	require.Equal(t, time.UTC, normalized.ScheduledAt.Location())
	require.True(t, normalized.ScheduledAt.Equal(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)))

	// The same instant written in another offset is the same request.
	offset := validReq()
	offset.ScheduledAt = strPtr("2029-12-31T19:00:00-05:00")
	other, err := offset.Normalize()
	require.NoError(t, err)
	require.True(t, other.ScheduledAt.Equal(*normalized.ScheduledAt))
	require.Equal(t, normalized.Fingerprint(), other.Fingerprint(),
		"equivalent offsets describing one instant must fingerprint identically")
}

func TestNormalize_RejectsAMalformedScheduledAt(t *testing.T) {
	for _, value := range []string{"2030-01-01", "not-a-time", "", "2030-13-01T00:00:00Z"} {
		r := validReq()
		r.ScheduledAt = strPtr(value)
		_, err := r.Normalize()

		var verr *ValidationError
		require.Truef(t, errors.As(err, &verr), "%q should be rejected", value)
		require.Len(t, verr.Fields, 1)
		require.Equal(t, "scheduled_at", verr.Fields[0].Field)
	}
}

// TestFingerprint_ImmediateSubmissionIsUnchangedByM4 is the upgrade-compatibility
// guard. An idempotency key recorded before this milestone must still replay its
// original job rather than answer 409, which means an immediate submission has to
// hash to exactly the byte stream M1 through M3 produced.
func TestFingerprint_ImmediateSubmissionIsUnchangedByM4(t *testing.T) {
	immediate, err := validReq().Normalize()
	require.NoError(t, err)

	// Recomputed here from the M1-M3 algorithm rather than pasted as a constant,
	// so this asserts that the byte stream is unchanged rather than that someone
	// remembered to update a hex literal.
	require.Equal(t, fingerprintBeforeM4(immediate), immediate.Fingerprint(),
		"an immediate submission's fingerprint must not change across the M4 upgrade")

	// An explicit null is the same request as an omitted field.
	var explicitNull SubmitRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"queue":"default","job_type":"demo.echo","payload":{"a":1},"scheduled_at":null
	}`), &explicitNull))
	nulled, err := explicitNull.Normalize()
	require.NoError(t, err)
	require.Equal(t, immediate.Fingerprint(), nulled.Fingerprint())

	// A delayed submission is a different request, and two different instants
	// are two different requests.
	delayed := validReq()
	delayed.ScheduledAt = strPtr("2030-01-01T00:00:00Z")
	first, err := delayed.Normalize()
	require.NoError(t, err)
	require.NotEqual(t, immediate.Fingerprint(), first.Fingerprint())

	delayed.ScheduledAt = strPtr("2030-01-01T00:00:01Z")
	second, err := delayed.Normalize()
	require.NoError(t, err)
	require.NotEqual(t, first.Fingerprint(), second.Fingerprint())
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

// fingerprintBeforeM4 is the M1-M3 fingerprint algorithm, reproduced exactly.
//
// It is deliberately a second implementation rather than a call into the
// production one: its whole job is to fail if the production byte stream for an
// immediate submission ever changes.
func fingerprintBeforeM4(n NormalizedRequest) string {
	h := sha256.New()
	write := func(b []byte) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(b)))
		h.Write(length[:])
		h.Write(b)
	}
	write([]byte("taskforge.fingerprint.v1"))
	write([]byte(n.Queue))
	write([]byte(n.Type))
	write(n.Payload)
	write([]byte(strconv.Itoa(n.Priority)))
	write([]byte(strconv.Itoa(n.MaxAttempts)))
	write([]byte(strconv.Itoa(n.TimeoutSeconds)))
	write([]byte(strconv.Itoa(len(n.RequiredCapabilities))))
	for _, c := range n.RequiredCapabilities {
		write([]byte(c))
	}
	return hex.EncodeToString(h.Sum(nil))
}
