package jobs

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func fingerprintOf(t *testing.T, r SubmitRequest) string {
	t.Helper()
	n, err := r.Normalize()
	require.NoError(t, err)
	return n.Fingerprint()
}

func TestFingerprint_IsStableAcrossEquivalentRequests(t *testing.T) {
	a := validReq()
	a.Payload = json.RawMessage(`{"b":2,"a":1}`)
	a.RequiredCapabilities = []string{"gpu", "cpu"}

	b := validReq()
	b.Payload = json.RawMessage(`{"a":1, "b":2}`)
	b.RequiredCapabilities = []string{"cpu", "gpu", "cpu"}

	require.Equal(t, fingerprintOf(t, a), fingerprintOf(t, b))
}

// An omitted field and its explicit default describe the same job, so they must
// produce the same fingerprint — otherwise a client that starts sending defaults
// explicitly would get spurious idempotency conflicts.
func TestFingerprint_DefaultsMatchExplicitValues(t *testing.T) {
	explicit := validReq()
	explicit.Priority = intPtr(DefaultPriority)
	explicit.MaxAttempts = intPtr(DefaultMaxAttempts)
	explicit.TimeoutSeconds = intPtr(DefaultTimeoutSeconds)

	require.Equal(t, fingerprintOf(t, validReq()), fingerprintOf(t, explicit))
}

func TestFingerprint_ChangesWithEveryDefiningField(t *testing.T) {
	base := fingerprintOf(t, validReq())

	mutations := map[string]func(*SubmitRequest){
		"queue":                 func(r *SubmitRequest) { r.Queue = "other" },
		"job_type":              func(r *SubmitRequest) { r.Type = "demo.sleep" },
		"payload":               func(r *SubmitRequest) { r.Payload = json.RawMessage(`{"a":2}`) },
		"priority":              func(r *SubmitRequest) { r.Priority = intPtr(51) },
		"max_attempts":          func(r *SubmitRequest) { r.MaxAttempts = intPtr(4) },
		"timeout_seconds":       func(r *SubmitRequest) { r.TimeoutSeconds = intPtr(301) },
		"required_capabilities": func(r *SubmitRequest) { r.RequiredCapabilities = []string{"cpu"} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			r := validReq()
			mutate(&r)
			require.NotEqual(t, base, fingerprintOf(t, r), "changing %s must change the fingerprint", name)
		})
	}
}

// Without length prefixing, queue "ab" + type "c" and queue "a" + type "bc"
// would hash identical byte streams.
func TestFingerprint_FieldsCannotBeRearranged(t *testing.T) {
	a := validReq()
	a.Queue = "ab"
	a.Type = "c"

	b := validReq()
	b.Queue = "a"
	b.Type = "bc"

	require.NotEqual(t, fingerprintOf(t, a), fingerprintOf(t, b))
}

func TestFingerprint_IsHexSHA256(t *testing.T) {
	fp := fingerprintOf(t, validReq())
	require.Len(t, fp, 64)
	require.Regexp(t, `^[0-9a-f]{64}$`, fp)
}

func TestFingerprint_IsDeterministicAcrossRuns(t *testing.T) {
	first := fingerprintOf(t, validReq())
	for i := 0; i < 50; i++ {
		require.Equal(t, first, fingerprintOf(t, validReq()))
	}
}
