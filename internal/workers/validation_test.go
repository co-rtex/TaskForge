package workers

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func validRegistration() Registration {
	return Registration{
		SessionID:         uuid.New(),
		Name:              "worker-1",
		Hostname:          "host.local",
		WorkerGroup:       "default",
		ConcurrencyLimit:  4,
		Capabilities:      []string{"gpu", "cpu", "cpu"},
		SupportedJobTypes: []string{"demo.echo", "demo.echo"},
	}
}

func TestNormalizeRegistration_CanonicalizesSets(t *testing.T) {
	got, err := NormalizeRegistration(validRegistration())
	require.NoError(t, err)
	require.Equal(t, []string{"cpu", "gpu"}, got.Capabilities)
	require.Equal(t, []string{"demo.echo"}, got.SupportedJobTypes)
}

func TestNormalizeRegistration_ReportsAllProblems(t *testing.T) {
	reg := validRegistration()
	reg.SessionID = uuid.Nil
	reg.Name = "Bad Name"
	reg.Hostname = ""
	reg.WorkerGroup = "BAD"
	reg.ConcurrencyLimit = 0
	reg.Capabilities = []string{"Bad Capability"}
	reg.SupportedJobTypes = nil

	_, err := NormalizeRegistration(reg)
	var validation *ValidationError
	require.ErrorAs(t, err, &validation)
	require.Len(t, validation.Fields, 7)
}

func TestNormalizeRegistration_ValidatesRawSetValuesBeforeCanonicalizing(t *testing.T) {
	for _, mutate := range []func(*Registration){
		func(reg *Registration) { reg.Capabilities = []string{"cpu", ""} },
		func(reg *Registration) { reg.SupportedJobTypes = []string{"demo.echo", " "} },
		func(reg *Registration) { reg.Capabilities = strings.Fields(strings.Repeat("cpu ", MaxCapabilities+1)) },
		func(reg *Registration) {
			reg.SupportedJobTypes = strings.Fields(strings.Repeat("demo.echo ", MaxSupportedJobTypes+1))
		},
	} {
		reg := validRegistration()
		mutate(&reg)
		_, err := NormalizeRegistration(reg)
		require.Error(t, err)
	}
}

func TestValidateClaimAndFence_RejectNilIdentifiers(t *testing.T) {
	require.Error(t, ValidateClaim(ClaimRequest{Queue: "default"}))
	require.Error(t, ValidateFence(Fence{}))
}

func TestClaimResult_SafeAcknowledgementIsDeliberatelyNarrow(t *testing.T) {
	require.True(t, (ClaimResult{Disposition: Claimed, Assignment: &Assignment{}}).SafeToAcknowledge())
	require.True(t, (ClaimResult{Disposition: QueueEmpty}).SafeToAcknowledge())
	require.True(t, (ClaimResult{Disposition: DuplicateNotification}).SafeToAcknowledge())
	for _, disposition := range []ClaimDisposition{NoEligibleJob, CapacityExhausted} {
		require.False(t, (ClaimResult{Disposition: disposition}).SafeToAcknowledge())
	}
	require.False(t, (ClaimResult{Disposition: Claimed}).SafeToAcknowledge())
	require.False(t, (ClaimResult{Disposition: QueueEmpty, Assignment: &Assignment{}}).SafeToAcknowledge())
}
