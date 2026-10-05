package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return raw
}

func ids(findings []Finding) []string {
	var out []string
	for _, f := range findings {
		out = append(out, f.ID)
	}
	return out
}

// --- govulncheck ----------------------------------------------------------------

// The reachable fixture is recorded output from a run against Go 1.25.0. It holds
// four findings with no called symbol (module and package level: imported, never
// called) and five with a call stack from the vulnerable symbol into this
// repository. Only the latter count.
func TestParseGovulncheck_OnlyFindingsWithACallStackAreReachable(t *testing.T) {
	got, err := parseGovulncheck(fixture(t, "govulncheck-reachable.json"))
	require.NoError(t, err)

	require.ElementsMatch(t, []string{"GO-2025-4010", "GO-2025-4012"}, ids(got.Findings), "one finding per vulnerability, not per call path")
	require.Contains(t, got.Note, "go1.25.0")

	byID := map[string]Finding{}
	for _, f := range got.Findings {
		byID[f.ID] = f
		require.Equal(t, toolGovulncheck, f.Tool)
	}
	require.Equal(t, []string{"CVE-2025-47912"}, byID["GO-2025-4010"].Aliases, "aliases are de-duplicated")
	require.Contains(t, byID["GO-2025-4010"].Location, "internal/config/config.go:401", "the shortest call path into the repository")
	require.Contains(t, byID["GO-2025-4010"].Location, "net/url")
	require.Contains(t, byID["GO-2025-4010"].Detail, "fixed in v1.25.2")
}

func TestParseGovulncheck_AnImportedButNeverCalledVulnerabilityIsNotAFinding(t *testing.T) {
	// Recorded from the Go 1.25.14 run: one module-level and one package-level
	// finding, neither with a called symbol.
	got, err := parseGovulncheck(fixture(t, "govulncheck-clean.json"))
	require.NoError(t, err)
	require.Empty(t, got.Findings)
	require.Contains(t, got.Note, "go1.25.14")
}

func TestParseGovulncheck_RefusesOutputThatIsNotGovulncheckOutput(t *testing.T) {
	for name, doc := range map[string]string{
		"empty":           "",
		"whitespace":      "  \n",
		"no config":       `{"osv":{"id":"GO-1"}}` + "\n",
		"not json":        "go: downloading golang.org/x/vuln v1.7.0\n",
		"truncated":       `{"config":{"go_version":"go1.25.14"}` + "\n" + `{"finding":{"osv":"GO-1","trace":[`,
		"error from tool": `{"config":{"go_version":"go1.25.14"}}` + "\n" + `{"error":"loading packages failed"}` + "\n",
	} {
		_, err := parseGovulncheck([]byte(doc))
		require.Errorf(t, err, "%s: silence or garbage must not read as a clean scan", name)
	}
}

func TestParseGovulncheck_AFindingWhoseTraceHasNoFrameIsNotReachableAndDoesNotPanic(t *testing.T) {
	doc := `{"config":{"go_version":"go1.25.14","db_last_modified":"2026-10-01T00:00:00Z"}}` + "\n" +
		`{"finding":{"osv":"GO-1","trace":[]}}` + "\n"
	got, err := parseGovulncheck([]byte(doc))
	require.NoError(t, err)
	require.Empty(t, got.Findings)
}

// --- gitleaks -------------------------------------------------------------------

func TestParseGitleaks_ReadsTheRecordedReportAndNamesWhereNotWhat(t *testing.T) {
	got, err := parseGitleaks(fixture(t, "gitleaks-report.json"))
	require.NoError(t, err)

	require.Len(t, got.Findings, 3)
	first := got.Findings[0]
	require.Equal(t, toolGitleaks, first.Tool)
	require.Equal(t, "c8da430d35ae111183ffac6f2614c8d689f8717a:dashboard/src/test/fixtures.ts:generic-api-key:20", first.ID,
		"the fingerprint names one finding at one place in history, so an exception is as narrow as the finding")
	require.Equal(t, "dashboard/src/test/fixtures.ts:20 (commit c8da430d)", first.Location)
	require.Contains(t, first.Detail, "generic-api-key")
}

func TestParseGitleaks_NeverCopiesASecretIntoWhatItPrints(t *testing.T) {
	const secret = "PLANTED-MARKER-NOT-A-REAL-SECRET-0123456789"
	doc := `[{"RuleID":"generic-api-key","Description":"Generic API Key","File":"a.go","StartLine":3,"Commit":"0123456789abcdef","Fingerprint":"0123456789abcdef:a.go:generic-api-key:3","Secret":"` + secret + `","Match":"key = ` + secret + `"}]`
	got, err := parseGitleaks([]byte(doc))
	require.NoError(t, err)
	require.Len(t, got.Findings, 1)
	f := got.Findings[0]
	for _, field := range append([]string{f.ID, f.Location, f.Detail}, f.Aliases...) {
		require.NotContains(t, field, secret)
	}
}

func TestParseGitleaks_AnEmptyReportIsCleanAndNoReportIsAnError(t *testing.T) {
	got, err := parseGitleaks(fixture(t, "gitleaks-clean.json"))
	require.NoError(t, err)
	require.Empty(t, got.Findings)

	for name, doc := range map[string]string{"empty": "", "not an array": `{"leaks":[]}`, "not json": "WRN leaks found: 3"} {
		_, err := parseGitleaks([]byte(doc))
		require.Errorf(t, err, "%s must not read as a clean scan", name)
	}
}

// --- pip-audit ------------------------------------------------------------------

func TestParsePipAudit_TheRecordedCleanRunHasSevenPackagesAndNoFindings(t *testing.T) {
	got, err := parsePipAudit(fixture(t, "pip-audit-clean.json"))
	require.NoError(t, err)
	require.Empty(t, got.Findings)
	require.Contains(t, got.Note, "7 packages")
}

func TestParsePipAudit_AVulnerableDependencyIsAFindingWithItsAliases(t *testing.T) {
	doc := `{"dependencies":[
	  {"name":"h11","version":"0.16.0","vulns":[]},
	  {"name":"httpx","version":"0.27.0","vulns":[
	    {"id":"PYSEC-2099-1","fix_versions":["0.27.1","0.28.0"],"aliases":["CVE-2099-0001","GHSA-aaaa-bbbb-cccc"],"description":"A made-up flaw."}]}
	],"fixes":[]}`
	got, err := parsePipAudit([]byte(doc))
	require.NoError(t, err)

	require.Len(t, got.Findings, 1)
	f := got.Findings[0]
	require.Equal(t, "PYSEC-2099-1", f.ID)
	require.Equal(t, []string{"CVE-2099-0001", "GHSA-aaaa-bbbb-cccc"}, f.Aliases)
	require.Equal(t, "httpx==0.27.0", f.Location)
	require.Contains(t, f.Detail, "fixed in 0.27.1, 0.28.0")
}

func TestParsePipAudit_APackageItCouldNotAuditIsNotAQuietPass(t *testing.T) {
	doc := `{"dependencies":[{"name":"internal-thing","skip_reason":"Dependency not found on PyPI and could not be audited"}],"fixes":[]}`
	got, err := parsePipAudit([]byte(doc))
	require.NoError(t, err)
	require.Len(t, got.Findings, 1)
	require.Equal(t, "pip-audit-skipped:internal-thing", got.Findings[0].ID)
}

func TestParsePipAudit_RefusesWhatIsNotAnAuditReport(t *testing.T) {
	for _, doc := range []string{"", "No known vulnerabilities found", `{"fixes":[]}`, `[]`} {
		_, err := parsePipAudit([]byte(doc))
		require.Errorf(t, err, "%q", doc)
	}
}

// --- npm audit ------------------------------------------------------------------

const npmAuditWithFindings = `{
  "auditReportVersion": 2,
  "vulnerabilities": {
    "vite": {"name":"vite","severity":"high","isDirect":false,"range":"<5.4.20","fixAvailable":true,
      "via":[{"source":1100001,"name":"vite","dependency":"vite","title":"Path traversal in the dev server","url":"https://github.com/advisories/GHSA-aaaa-1111-bbbb","severity":"high","range":"<5.4.20"}]},
    "lodash": {"name":"lodash","severity":"moderate","isDirect":true,"range":"<4.17.21","fixAvailable":true,
      "via":[{"source":1100002,"name":"lodash","dependency":"lodash","title":"Prototype pollution","url":"https://github.com/advisories/GHSA-cccc-2222-dddd","severity":"moderate","range":"<4.17.21"}]},
    "react-thing": {"name":"react-thing","severity":"high","isDirect":true,"range":"*","fixAvailable":false,
      "via":["vite"]},
    "parser": {"name":"parser","severity":"critical","isDirect":false,"range":"<2.0.0","fixAvailable":true,
      "via":[{"source":1100003,"name":"parser","dependency":"parser","title":"Remote code execution","url":"https://github.com/advisories/GHSA-eeee-3333-ffff","severity":"critical","range":"<2.0.0"}]}
  },
  "metadata": {"vulnerabilities":{"info":0,"low":0,"moderate":1,"high":2,"critical":1,"total":4},"dependencies":{"prod":4,"dev":148,"total":151}}
}`

func TestParseNpmAudit_HighAndCriticalAdvisoriesCountAndModerateOnesDoNot(t *testing.T) {
	got, err := parseNpmAudit([]byte(npmAuditWithFindings))
	require.NoError(t, err)

	require.ElementsMatch(t, []string{"GHSA-aaaa-1111-bbbb", "GHSA-eeee-3333-ffff"}, ids(got.Findings),
		"the moderate advisory is below the threshold, and a package that is only high because of another's advisory adds none of its own")
	for _, f := range got.Findings {
		require.Equal(t, toolNpmAudit, f.Tool)
	}
	require.Contains(t, got.Findings[0].Location+got.Findings[1].Location, "vite")
}

func TestParseNpmAudit_TheRecordedCleanRunHasNoFindings(t *testing.T) {
	got, err := parseNpmAudit(fixture(t, "npm-audit-clean.json"))
	require.NoError(t, err)
	require.Empty(t, got.Findings)
	require.Contains(t, got.Note, "4 production packages")
}

func TestParseNpmAudit_RefusesWhatIsNotAnAuditReport(t *testing.T) {
	for name, doc := range map[string]string{
		"empty":      "",
		"npm error":  `{"error":{"code":"ENOTFOUND","summary":"request to https://registry.npmjs.org failed","detail":"getaddrinfo ENOTFOUND"}}`,
		"no version": `{"vulnerabilities":{},"metadata":{}}`,
		"not json":   "npm ERR! audit endpoint returned an error",
	} {
		_, err := parseNpmAudit([]byte(doc))
		require.Errorf(t, err, "%s must not read as a clean audit", name)
	}
	_, err := parseNpmAudit([]byte(`{"error":{"code":"ENOTFOUND","summary":"offline"}}`))
	require.ErrorContains(t, err, "offline")
}
