package jobs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusValid(t *testing.T) {
	for _, s := range AllStatuses() {
		require.True(t, s.Valid(), "%s should be valid", s)
	}
	for _, s := range []Status{"", "queued", "FAILED", "UNKNOWN"} {
		require.False(t, s.Valid(), "%q should be invalid", s)
	}
}

// There is deliberately no job-level FAILED status: one failed attempt is not a
// job outcome. This guards against it being reintroduced by accident.
func TestNoJobLevelFailedStatus(t *testing.T) {
	for _, s := range AllStatuses() {
		require.NotEqual(t, Status("FAILED"), s)
	}
}

func TestStatusTerminal(t *testing.T) {
	terminal := map[Status]bool{
		StatusSucceeded: true, StatusCanceled: true, StatusDeadLettered: true,
	}
	for _, s := range AllStatuses() {
		require.Equal(t, terminal[s], s.Terminal(), "terminality of %s", s)
	}
}
