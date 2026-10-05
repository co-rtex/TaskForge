package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var today = time.Date(2026, time.October, 5, 12, 0, 0, 0, time.UTC)

const oneValid = `
exceptions:
  - id: GO-2099-0001
    tool: govulncheck
    reason: The vulnerable function is reachable only from a developer script and never ships.
    accepted_by: Christian Cortez
    expires: 2026-12-31
`

func TestParseExceptions_AcceptsAWellFormedEntry(t *testing.T) {
	got, problems := parseExceptions([]byte(oneValid), today)

	require.Empty(t, problems)
	require.Len(t, got, 1)
	require.Equal(t, "GO-2099-0001", got[0].ID)
	require.Equal(t, toolGovulncheck, got[0].Tool)
}

func TestParseExceptions_AnEmptyListIsValid(t *testing.T) {
	for name, doc := range map[string]string{
		"empty list": "exceptions: []\n",
		"no key":     "{}\n",
		"comments":   "# nothing is excepted\nexceptions: []\n",
	} {
		got, problems := parseExceptions([]byte(doc), today)
		require.Emptyf(t, problems, name)
		require.Emptyf(t, got, name)
	}
}

func TestParseExceptions_ExpiryIsInclusiveOfTheLastDayAndFailsTheDayAfter(t *testing.T) {
	entry := func(date string) []byte {
		return []byte("exceptions:\n  - {id: X-1, tool: pip-audit, reason: r, accepted_by: a, expires: " + date + "}\n")
	}
	_, problems := parseExceptions(entry("2026-10-05"), today)
	require.Empty(t, problems, "it expires at the end of its last day")

	_, problems = parseExceptions(entry("2026-10-04"), today)
	require.Len(t, problems, 1)
	require.Contains(t, problems[0], "X-1")
	require.Contains(t, problems[0], "expired")
	require.Contains(t, problems[0], "2026-10-04")
}

func TestParseExceptions_EachMissingFieldIsNamed(t *testing.T) {
	for field, doc := range map[string]string{
		"id":          "{tool: pip-audit, reason: r, accepted_by: a, expires: 2026-12-31}",
		"tool":        "{id: X-1, reason: r, accepted_by: a, expires: 2026-12-31}",
		"reason":      "{id: X-1, tool: pip-audit, accepted_by: a, expires: 2026-12-31}",
		"accepted_by": "{id: X-1, tool: pip-audit, reason: r, expires: 2026-12-31}",
		"expires":     "{id: X-1, tool: pip-audit, reason: r, accepted_by: a}",
	} {
		_, problems := parseExceptions([]byte("exceptions:\n  - "+doc+"\n"), today)
		require.NotEmptyf(t, problems, "missing %s", field)
		require.Containsf(t, problems[0], field, "the problem must name the field")
	}
}

func TestParseExceptions_ABlankReasonIsNotAReason(t *testing.T) {
	for _, reason := range []string{`""`, `"   "`, `" \t "`} {
		_, problems := parseExceptions([]byte("exceptions:\n  - {id: X-1, tool: pip-audit, reason: "+reason+", accepted_by: a, expires: 2026-12-31}\n"), today)
		require.NotEmptyf(t, problems, "reason %s", reason)
		require.Contains(t, problems[0], "reason")
	}
}

func TestParseExceptions_AnUnknownToolIsRefused(t *testing.T) {
	_, problems := parseExceptions([]byte("exceptions:\n  - {id: X-1, tool: trivy, reason: r, accepted_by: a, expires: 2026-12-31}\n"), today)
	require.Len(t, problems, 1)
	require.Contains(t, problems[0], `unknown tool "trivy"`)
	for _, known := range []string{"govulncheck", "pip-audit", "npm-audit"} {
		_, problems := parseExceptions([]byte("exceptions:\n  - {id: X-1, tool: "+known+", reason: r, accepted_by: a, expires: 2026-12-31}\n"), today)
		require.Emptyf(t, problems, known)
	}
}

// gitleaks findings are accepted in .gitleaks.toml and nowhere else (ADR-0021).
// History is permanent, so a dated exception would only create renewal churn, and an
// exception that named the secret's value would put a revoked credential back in the
// tree. An entry for it here is not ignored: it fails the run, and says where to go.
func TestParseExceptions_AGitleaksEntryIsRefusedAndPointsToTheGitleaksConfig(t *testing.T) {
	doc := "exceptions:\n  - {id: 0123abc:f.go:generic-api-key:3, tool: gitleaks, reason: r, accepted_by: a, expires: 2026-12-31}\n"

	valid, problems := parseExceptions([]byte(doc), today)

	require.Empty(t, valid, "a refused entry excuses nothing")
	require.Len(t, problems, 1)
	require.Contains(t, problems[0], "0123abc:f.go:generic-api-key:3")
	require.Contains(t, problems[0], ".gitleaks.toml")
	require.Contains(t, problems[0], "ADR-0021")
}

func TestParseExceptions_TheListOfToolsThatMayBeExceptedExcludesGitleaksButTheRunnableToolsDoNot(t *testing.T) {
	require.NotContains(t, exceptableTools, toolGitleaks)
	require.Contains(t, knownTools, toolGitleaks, "gitleaks is still a tool the driver runs")
	for _, tool := range exceptableTools {
		require.Contains(t, knownTools, tool)
	}
}

func TestParseExceptions_AMalformedDateIsRefused(t *testing.T) {
	for _, date := range []string{"soon", "2026-13-01", "2026-12-31T00:00:00Z", "12/31/2026", "20261231"} {
		_, problems := parseExceptions([]byte("exceptions:\n  - {id: X-1, tool: pip-audit, reason: r, accepted_by: a, expires: \""+date+"\"}\n"), today)
		require.NotEmptyf(t, problems, date)
		require.Contains(t, problems[0], "expires")
	}
}

func TestParseExceptions_ADuplicateIsRefused(t *testing.T) {
	doc := "exceptions:\n" +
		"  - {id: X-1, tool: pip-audit, reason: r, accepted_by: a, expires: 2026-12-31}\n" +
		"  - {id: X-1, tool: pip-audit, reason: again, accepted_by: a, expires: 2026-12-31}\n"
	_, problems := parseExceptions([]byte(doc), today)
	require.NotEmpty(t, problems)
	require.Contains(t, problems[0], "duplicate")
}

func TestParseExceptions_AnUnknownKeyIsRefusedSoATypoCannotSilenceAnException(t *testing.T) {
	_, problems := parseExceptions([]byte("exceptions:\n  - {id: X-1, tool: pip-audit, reason: r, accepted_by: a, expires: 2026-12-31, expiry: 2099-01-01}\n"), today)
	require.NotEmpty(t, problems)
	for _, doc := range []string{"exception: []\n", "exceptions: {id: X}\n", "- id: X\n", "not yaml: [\n"} {
		_, problems := parseExceptions([]byte(doc), today)
		require.NotEmptyf(t, problems, doc)
	}
}

func TestParseExceptions_EveryProblemIsReportedNotJustTheFirst(t *testing.T) {
	doc := "exceptions:\n" +
		"  - {id: A-1, tool: pip-audit, reason: r, accepted_by: a, expires: 2020-01-01}\n" +
		"  - {id: B-1, tool: nope, reason: r, accepted_by: a, expires: 2026-12-31}\n" +
		"  - {id: C-1, tool: pip-audit, reason: '', accepted_by: a, expires: 2026-12-31}\n"
	_, problems := parseExceptions([]byte(doc), today)
	require.Len(t, problems, 3)
}

// The exceptions file in the repository is held to the same rules for its shape. Its
// DATES are not checked here: whether an entry has expired is a fact about today, and
// `make scan` (the scan job) fails on it. A unit test that failed on the day after an
// entry's expiry would turn the fast `checks` job red for the same reason, which is
// what keeping the scanners in a job of their own is meant to prevent (ADR-0021).
// Parsing against a date long before any entry's, so that expiry cannot fire, leaves
// exactly the well-formedness assertions.
func TestTheCommittedExceptionsFileIsWellFormed(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "security", "scan-exceptions.yaml"))
	require.NoError(t, err, "security/scan-exceptions.yaml must exist, even with no entries")

	_, problems := parseExceptions(raw, time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC))
	require.Empty(t, problems)
}

func TestExceptionMatching(t *testing.T) {
	list := []Exception{
		{ID: "GO-2099-0001", Tool: toolGovulncheck, Expires: "2026-12-31"},
		{ID: "CVE-2099-1111", Tool: toolGovulncheck, Expires: "2026-12-31"},
		{ID: "abc123:f.go:rule:1", Tool: toolGitleaks, Expires: "2026-12-31"},
	}
	byID := Finding{Tool: toolGovulncheck, ID: "GO-2099-0001"}
	byAlias := Finding{Tool: toolGovulncheck, ID: "GO-2099-0002", Aliases: []string{"CVE-2099-1111"}}
	otherTool := Finding{Tool: toolNpmAudit, ID: "GO-2099-0001"}
	unrelated := Finding{Tool: toolGovulncheck, ID: "GO-2099-9999"}

	_, ok := matchException(byID, list)
	require.True(t, ok)
	_, ok = matchException(byAlias, list)
	require.True(t, ok, "an exception may name any identifier the tool reports for the finding")
	_, ok = matchException(otherTool, list)
	require.False(t, ok, "an exception for one tool never excuses another's finding")
	_, ok = matchException(unrelated, list)
	require.False(t, ok)
}

// Defence in depth: parseExceptions refuses a gitleaks entry, but matching does not
// rely on that. Handed such an entry directly, a gitleaks finding still is not excused.
func TestExceptionMatching_AGitleaksFindingNeverMatchesAnException(t *testing.T) {
	finding := Finding{Tool: toolGitleaks, ID: "abc123:f.go:generic-api-key:1", Aliases: []string{"alias-1"}}

	_, ok := matchException(finding, []Exception{{ID: finding.ID, Tool: toolGitleaks, Expires: "2099-01-01"}})
	require.False(t, ok, "by id")
	_, ok = matchException(finding, []Exception{{ID: "alias-1", Tool: toolGitleaks, Expires: "2099-01-01"}})
	require.False(t, ok, "by alias")
}
