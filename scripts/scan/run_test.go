package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fixtures returns sources for all four tools that report a clean scan.
func cleanSources(t *testing.T) map[string]source {
	t.Helper()
	return map[string]source{
		toolGovulncheck: staticSource(fixture(t, "govulncheck-clean.json")),
		toolGitleaks:    staticSource(fixture(t, "gitleaks-clean.json")),
		toolPipAudit:    staticSource(fixture(t, "pip-audit-clean.json")),
		toolNpmAudit:    staticSource(fixture(t, "npm-audit-clean.json")),
	}
}

func staticSource(data []byte) source {
	return func(context.Context) ([]byte, error) { return data, nil }
}

func writeExceptions(t *testing.T, doc string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scan-exceptions.yaml")
	require.NoError(t, os.WriteFile(path, []byte(doc), 0o600))
	return path
}

func runScan(t *testing.T, exceptions string, sources map[string]source, extra ...string) (int, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	args := append([]string{"-exceptions", exceptions}, extra...)
	code := run(args, &out, &errOut, today, sources)
	return code, out.String() + errOut.String()
}

const noExceptions = "exceptions: []\n"

func TestRun_AllToolsCleanPassesAndSaysWhatEachChecked(t *testing.T) {
	code, out := runScan(t, writeExceptions(t, noExceptions), cleanSources(t))

	require.Equal(t, exitPassed, code, out)
	for _, tool := range []string{toolGovulncheck, toolGitleaks, toolPipAudit, toolNpmAudit} {
		require.Regexpf(t, `(?m)^\s*ok\s+`+regexp.QuoteMeta(tool)+`\s`, out, "a line saying %s was clean", tool)
	}
	require.Contains(t, out, "go1.25.14")
	require.Contains(t, out, "RESULT: PASS")
}

// A finding with no exception blocks. This is the policy the owner chose: the
// scanners fail the build, they do not warn.
func TestRun_AReachableVulnerabilityWithNoExceptionFails(t *testing.T) {
	sources := cleanSources(t)
	sources[toolGovulncheck] = staticSource(fixture(t, "govulncheck-reachable.json"))

	code, out := runScan(t, writeExceptions(t, noExceptions), sources)

	require.Equal(t, exitFailed, code, out)
	require.Regexp(t, `(?m)^\s*FAIL\s+govulncheck\s+GO-2025-4010\s`, out)
	require.Regexp(t, `(?m)^\s*FAIL\s+govulncheck\s+GO-2025-4012\s`, out)
	require.Contains(t, out, "not excepted")
	require.Contains(t, out, "RESULT: FAIL")
	// One line per finding: tool, id, location, and whether it is excepted.
	require.Contains(t, out, "internal/config/config.go:401")
}

func TestRun_ADatedExceptionForEveryFindingLetsItPassAndSaysSoOnEachLine(t *testing.T) {
	sources := cleanSources(t)
	sources[toolGovulncheck] = staticSource(fixture(t, "govulncheck-reachable.json"))
	doc := `exceptions:
  - {id: GO-2025-4010, tool: govulncheck, reason: "Reachable only from config validation of a URL the operator wrote.", accepted_by: Christian Cortez, expires: 2026-12-31}
  - {id: CVE-2025-58186, tool: govulncheck, reason: "Matched by its CVE alias.", accepted_by: Christian Cortez, expires: 2026-12-31}
`
	code, out := runScan(t, writeExceptions(t, doc), sources)

	require.Equal(t, exitPassed, code, out)
	require.Regexp(t, `(?m)GO-2025-4010.*\bexcepted\b.*2026-12-31`, out)
	require.Regexp(t, `(?m)GO-2025-4012.*\bexcepted\b.*2026-12-31`, out)
	require.NotContains(t, out, "not excepted")
}

func TestRun_ExceptingOneOfTwoFindingsStillFails(t *testing.T) {
	sources := cleanSources(t)
	sources[toolGovulncheck] = staticSource(fixture(t, "govulncheck-reachable.json"))
	doc := "exceptions:\n  - {id: GO-2025-4010, tool: govulncheck, reason: r, accepted_by: a, expires: 2026-12-31}\n"

	code, out := runScan(t, writeExceptions(t, doc), sources)

	require.Equal(t, exitFailed, code, out)
	require.Regexp(t, `(?m)^\s*FAIL\s+govulncheck\s+GO-2025-4012\s`, out)
}

// The exceptions file is policy and is checked on every run, whatever the scanners
// found. An exception that has lapsed fails the build on its own.
func TestRun_AnExpiredExceptionFailsEvenWhenNothingWasFound(t *testing.T) {
	doc := "exceptions:\n  - {id: GO-2099-0001, tool: govulncheck, reason: r, accepted_by: a, expires: 2026-10-04}\n"

	code, out := runScan(t, writeExceptions(t, doc), cleanSources(t))

	require.Equal(t, exitFailed, code, out)
	require.Regexp(t, `(?m)^\s*FAIL\s+exceptions\s+GO-2099-0001\s.*expired`, out)
}

func TestRun_AnExpiredExceptionDoesNotExcuseTheFindingItNames(t *testing.T) {
	sources := cleanSources(t)
	sources[toolGovulncheck] = staticSource(fixture(t, "govulncheck-reachable.json"))
	doc := `exceptions:
  - {id: GO-2025-4010, tool: govulncheck, reason: r, accepted_by: a, expires: 2026-10-04}
  - {id: GO-2025-4012, tool: govulncheck, reason: r, accepted_by: a, expires: 2026-12-31}
`
	code, out := runScan(t, writeExceptions(t, doc), sources)

	require.Equal(t, exitFailed, code, out)
	require.Regexp(t, `(?m)^\s*FAIL\s+govulncheck\s+GO-2025-4010\s.*not excepted`, out)
	require.Regexp(t, `(?m)GO-2025-4012.*\bexcepted\b`, out)
}

func TestRun_AMalformedExceptionsFileFailsAndSaysWhy(t *testing.T) {
	code, out := runScan(t, writeExceptions(t, "exceptions:\n  - {id: X-1, tool: trivy, reason: r, accepted_by: a, expires: 2026-12-31}\n"), cleanSources(t))
	require.Equal(t, exitFailed, code, out)
	require.Contains(t, out, `unknown tool "trivy"`)

	code, out = runScan(t, filepath.Join(t.TempDir(), "missing.yaml"), cleanSources(t))
	require.Equal(t, exitFailed, code, out)
	require.Contains(t, out, "scan-exceptions")
}

// A scan that could not run is not a scan that found nothing.
func TestRun_AToolThatCannotRunFailsTheScan(t *testing.T) {
	sources := cleanSources(t)
	sources[toolGitleaks] = func(context.Context) ([]byte, error) { return nil, errors.New("docker: command not found") }

	code, out := runScan(t, writeExceptions(t, noExceptions), sources)

	require.Equal(t, exitFailed, code, out)
	require.Regexp(t, `(?m)^\s*FAIL\s+gitleaks\s+.*docker: command not found`, out)
	require.Regexp(t, `(?m)^\s*ok\s+govulncheck\s`, out, "the other tools still run and report")
}

func TestRun_OutputThatIsNotAReportFailsTheScan(t *testing.T) {
	sources := cleanSources(t)
	sources[toolPipAudit] = staticSource([]byte("No known vulnerabilities found"))

	code, out := runScan(t, writeExceptions(t, noExceptions), sources)

	require.Equal(t, exitFailed, code, out)
	require.Regexp(t, `(?m)^\s*FAIL\s+pip-audit\s`, out)
}

func TestRun_NamedToolsRunAloneAndAnUnknownNameIsAUsageError(t *testing.T) {
	sources := map[string]source{toolGitleaks: staticSource(fixture(t, "gitleaks-clean.json"))}
	code, out := runScan(t, writeExceptions(t, noExceptions), sources, "gitleaks")
	require.Equal(t, exitPassed, code, out)
	require.NotContains(t, out, "govulncheck")

	code, out = runScan(t, writeExceptions(t, noExceptions), sources, "trivy")
	require.Equal(t, exitUsage, code, out)
	require.Contains(t, out, "usage")
}

// The recorded-report flags exist so a finding can be fed to the real driver and
// the real exceptions logic without running a tool: the form the mutation checks
// use, and the form a reviewer can use to see the scan fail.
func TestRun_ARecordedReportCanBeSuppliedInPlaceOfRunningTheTool(t *testing.T) {
	report := filepath.Join("testdata", "govulncheck-reachable.json")

	code, out := runScan(t, writeExceptions(t, noExceptions), map[string]source{}, "-govulncheck-report", report, "govulncheck")

	require.Equal(t, exitFailed, code, out)
	require.Regexp(t, `(?m)^\s*FAIL\s+govulncheck\s+GO-2025-4010\s`, out)
}

func TestRun_AnExceptionThatMatchesNothingIsNotedAndDoesNotFail(t *testing.T) {
	doc := "exceptions:\n  - {id: GO-2099-0001, tool: govulncheck, reason: r, accepted_by: a, expires: 2026-12-31}\n"
	code, out := runScan(t, writeExceptions(t, doc), cleanSources(t))

	require.Equal(t, exitPassed, code, out)
	require.Contains(t, out, "GO-2099-0001")
	require.Contains(t, out, "matched no finding")
}

func TestRun_EveryLineOfOutputForAFindingNamesToolIdLocationAndStatus(t *testing.T) {
	sources := cleanSources(t)
	sources[toolGitleaks] = staticSource(fixture(t, "gitleaks-report.json"))

	_, out := runScan(t, writeExceptions(t, noExceptions), sources)

	var finding string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "dashboard/src/test/fixtures.ts") {
			finding = line
		}
	}
	require.NotEmpty(t, finding)
	for _, part := range []string{"gitleaks", "c8da430d35ae111183ffac6f2614c8d689f8717a", "fixtures.ts:20", "not excepted"} {
		require.Contains(t, finding, part)
	}
}

// --- pins -----------------------------------------------------------------------

func TestEveryToolIsPinnedExactly(t *testing.T) {
	require.Regexp(t, `^v\d+\.\d+\.\d+$`, govulncheckVersion, "govulncheck is run at an exact tag, never @latest")
	require.Regexp(t, `^\d+\.\d+\.\d+$`, pipAuditVersion, "pip-audit is installed with ==")
	require.Regexp(t, `^ghcr\.io/gitleaks/gitleaks:v\d+\.\d+\.\d+@sha256:[0-9a-f]{64}$`, gitleaksImage,
		"gitleaks runs from its container image, by version and digest; not gitleaks-action, which needs a licence for organisations")
}
