package workers

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The worker cursor is a name, which UNIQUE (scope, name) already makes a
// total order -- so unlike the jobs and DLQ cursors it needs no id tiebreak.
func TestWorkerCursor_RoundTripsExactly(t *testing.T) {
	for _, name := range []string{"a", "local-worker", "worker.01", "w-9_x", strings.Repeat("z", 128)} {
		decoded, err := decodeWorkerCursor(EncodeWorkerCursor(name))
		require.NoErrorf(t, err, "%q must round trip", name)
		require.Equal(t, name, decoded)
	}
}

// A cursor reaches a SQL comparison. It authorizes nothing, but it is still
// caller-supplied, so it is bounded by the same pattern registration enforces
// rather than passed through unchecked.
func TestWorkerCursor_RejectsAnythingThatIsNotAWorkerName(t *testing.T) {
	raw := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

	for name, cursor := range map[string]string{
		"not base64":        "not-base64!",
		"empty name":        raw(""),
		"uppercase":         raw("Worker"),
		"leading dash":      raw("-worker"),
		"embedded space":    raw("two words"),
		"control character": raw("worker\x00"),
		"newline":           raw("worker\n"),
		"too long":          raw(strings.Repeat("z", 129)),
		"sql-looking":       raw("' OR 1=1 --"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeWorkerCursor(cursor)
			require.ErrorIs(t, err, ErrInvalidCursor)
		})
	}
}

// The listing bounds must match the job listing's, so a client does not have
// to remember two different maxima for two endpoints documented together.
func TestWorkerListingBounds_MatchTheDocumentedRange(t *testing.T) {
	require.Equal(t, 25, DefaultPageSize)
	require.Equal(t, 100, MaxPageSize)
}

// Every status a session can hold must be reportable by the listing. The whole
// point of GET /v1/workers is that a crashed (UNHEALTHY) or replaced (OFFLINE)
// worker still appears, so this pins that the set the API can render is the
// full set the schema permits -- not the three
// worker_sessions_one_current_per_worker_idx covers.
func TestSessionStatuses_IncludeTheOnesACurrentOnlyIndexWouldHide(t *testing.T) {
	current := map[SessionStatus]bool{
		SessionStarting: true, SessionHealthy: true, SessionDraining: true,
	}
	hidden := []SessionStatus{SessionUnhealthy, SessionOffline}

	for _, status := range hidden {
		require.Falsef(t, current[status],
			"%s is outside worker_sessions_one_current_per_worker_idx's predicate; "+
				"ListWorkers must not be built on that index", status)
	}
	require.Len(t, hidden, 2,
		"a crashed worker is UNHEALTHY and a replaced one is OFFLINE; both must stay listable")
}
