package jobs

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// A cursor must survive a round trip exactly. It carries a nanosecond-precision
// instant and a uuid, and losing either would silently move a page boundary.
func TestJobCursor_RoundTripsExactly(t *testing.T) {
	at := time.Date(2026, 9, 21, 12, 34, 56, 123456789, time.UTC)
	id := uuid.MustParse("3f2504e0-4f89-11d3-9a0c-0305e82c3301")

	decodedAt, decodedID, err := decodeJobCursor(EncodeJobCursor(at, id))
	require.NoError(t, err)
	require.True(t, at.Equal(decodedAt), "instant must survive: %s vs %s", at, decodedAt)
	require.Equal(t, id, decodedID)
}

// A non-UTC instant must encode as the same absolute time, so a client in one
// timezone and a server in another agree on the page boundary.
func TestJobCursor_NormalizesToUTC(t *testing.T) {
	zone := time.FixedZone("UTC+7", 7*3600)
	at := time.Date(2026, 9, 21, 19, 34, 56, 0, zone)
	id := uuid.New()

	decodedAt, _, err := decodeJobCursor(EncodeJobCursor(at, id))
	require.NoError(t, err)
	require.True(t, at.Equal(decodedAt))
	require.Equal(t, time.UTC, decodedAt.Location())
}

// Anything this endpoint did not issue is refused, rather than being coerced
// into some position the caller did not ask for.
func TestJobCursor_RejectsEverythingItDidNotIssue(t *testing.T) {
	for name, cursor := range map[string]string{
		"not base64":         "not-base64!",
		"base64 punctuation": "///",
		"no separator":       "bm90LWEtY3Vyc29y", // "not-a-cursor"
		"timestamp only":     "MjAyNi0wOS0yMQ",   // "2026-09-21"
		"bad timestamp":      "bm90LWEtdGltZXw=", // "not-a-time|"
		"bad uuid":           encodeRaw("2026-09-21T00:00:00Z|nope"),
		"empty halves":       encodeRaw("|"),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := decodeJobCursor(cursor)
			require.ErrorIs(t, err, ErrInvalidCursor)
		})
	}
}

// encodeRaw builds a syntactically valid base64url token whose DECODED
// contents are wrong, so the test exercises the parse failures rather than
// only the base64 one.
func encodeRaw(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

// boundPage is the single place a page size is clamped, so every listing
// endpoint agrees with the bound the document states for all of them.
func TestBoundPage_ClampsRatherThanRejects(t *testing.T) {
	require.Equal(t, DefaultPageSize, boundPage(0), "absent means default")
	require.Equal(t, DefaultPageSize, boundPage(-1), "negative means default")
	require.Equal(t, 1, boundPage(1))
	require.Equal(t, MaxPageSize, boundPage(MaxPageSize))
	require.Equal(t, MaxPageSize, boundPage(MaxPageSize+500), "oversized is clamped")
}

// The filter refuses a status this API does not know, and accepts every one it
// does. A status added to AllStatuses() is automatically covered.
func TestJobFilter_AcceptsEveryKnownStatusAndRefusesOthers(t *testing.T) {
	for _, status := range AllStatuses() {
		require.NoErrorf(t, JobFilter{Status: status}.Validate(),
			"%s is a real status and must be filterable", status)
	}
	for _, bad := range []Status{"succeeded", "NOPE", "RUNNING ", " RUNNING"} {
		require.ErrorIsf(t, JobFilter{Status: bad}.Validate(), ErrInvalidJobFilter,
			"%q is not a status", bad)
	}
}

// A queue that does not exist is deliberately NOT a filter error: it matches
// nothing. Only a name that could never be a queue at all is refused.
func TestJobFilter_RefusesOnlyImpossibleQueueNames(t *testing.T) {
	require.NoError(t, JobFilter{Queue: "default"}.Validate())
	require.NoError(t, JobFilter{Queue: "no-such-queue-exists"}.Validate(),
		"a nonexistent queue matches nothing; it is not an invalid filter")

	for _, bad := range []string{"UPPER", "-leading-dash", "has space", "has/slash"} {
		require.ErrorIsf(t, JobFilter{Queue: bad}.Validate(), ErrInvalidJobFilter,
			"%q cannot be a queue name", bad)
	}
}

// An empty filter lists everything and is not an error.
func TestJobFilter_ZeroValueIsValid(t *testing.T) {
	require.NoError(t, JobFilter{}.Validate())
}

// NonTerminalStatuses is derived from Status.Terminal() rather than written
// out, so a status added to the state machine cannot be silently omitted from
// queue depth. This asserts the derivation, not a copy of the list.
func TestNonTerminalStatuses_IsExactlyTheComplementOfTerminal(t *testing.T) {
	nonTerminal := NonTerminalStatuses()

	seen := make(map[Status]bool, len(nonTerminal))
	for _, status := range nonTerminal {
		require.Falsef(t, status.Terminal(), "%s is terminal and must not be counted as depth", status)
		seen[status] = true
	}
	for _, status := range AllStatuses() {
		if status.Terminal() {
			require.Falsef(t, seen[status], "%s is terminal but appears in the depth set", status)
			continue
		}
		require.Truef(t, seen[status], "%s is non-terminal and must appear in the depth set", status)
	}
	require.Len(t, nonTerminal, 6,
		"six non-terminal statuses; if this changes, jobs_scope_queue_depth_idx's "+
			"partial predicate in migration 0017 must change with it")
}
