package verification

// These checks read the files `make scan` depends on: the Makefile, the gitleaks
// configuration, the exceptions file and the dashboard Dockerfile. The scan driver's
// own behaviour (parsing, exceptions, exit codes) is tested in scripts/scan. What is
// held here is that the pieces the driver trusts are shaped the way
// docs/adr/0021-container-images-and-supply-chain-scanning.md says.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	gitleaksConfigPath  = ".gitleaks.toml"
	exceptionsFilePath  = "security/scan-exceptions.yaml"
	dashDockerfilePath  = "dashboard/Dockerfile"
	scanDriverDirectory = "scripts/scan"
)

func TestMakefile_ScanRunsTheDriverAndHasAHelpLine(t *testing.T) {
	text := readRepoFile(t, makefilePath)

	require.Regexp(t, `(?m)^scan:.*##`, text, "the Makefile needs a scan target with a ## help line")

	recipe := regexp.MustCompile(`(?m)^scan:[^\n]*\n((?:\t[^\n]*\n)+)`).FindStringSubmatch(text)
	require.NotNil(t, recipe, "scan needs a recipe")
	require.Contains(t, recipe[1], "./scripts/scan", "make scan runs the driver, which applies the exceptions file; running a tool bare would skip it")

	phony := regexp.MustCompile(`(?s)\.PHONY:(.*?)\n\n`).FindStringSubmatch(text)
	require.NotNil(t, phony)
	require.Contains(t, strings.Fields(strings.ReplaceAll(phony[1], "\\", " ")), "scan")
}

func TestExceptionsFile_ExistsAtTheOnePathTheDriverAndCIUse(t *testing.T) {
	_, err := os.Stat(filepath.Join(repoRoot, exceptionsFilePath))
	require.NoError(t, err, "there is one exceptions file, and it is committed even when it is empty")

	driver := readRepoFile(t, filepath.Join(scanDriverDirectory, "main.go"))
	require.Contains(t, driver, `"`+exceptionsFilePath+`"`, "the driver's default is that file")
}

// An allowlist entry that is wider than its fixture hides the next real secret that
// lands in the same file or matches the same rule. Each entry must therefore name a
// rule, a path and a value, require all three, and say which fixture it covers.
func TestGitleaksConfig_EveryAllowlistEntryIsScopedToOneFixtureAndExplained(t *testing.T) {
	text := readRepoFile(t, gitleaksConfigPath)

	require.Regexp(t, `(?m)^\[extend\]\s*\nuseDefault\s*=\s*true`, text, "the default rule set stays on")
	require.NotRegexp(t, `(?m)^\[allowlist\]`, text, "a global allowlist is not scoped to a rule; use [[allowlists]] with targetRules")

	blocks := strings.Split(text, "[[allowlists]]")
	require.Greater(t, len(blocks), 1, "the known fixtures are allowlisted here")
	for i, block := range blocks[1:] {
		// The comment that explains the entry is the text above its header, which is
		// the tail of the previous block.
		comment := blocks[i]
		lines := strings.Split(strings.TrimRight(comment, "\n"), "\n")
		var explanation []string
		for j := len(lines) - 1; j >= 0 && strings.HasPrefix(strings.TrimSpace(lines[j]), "#"); j-- {
			explanation = append([]string{lines[j]}, explanation...)
		}
		require.Regexpf(t, `(?i)fixture`, strings.Join(explanation, "\n"), "allowlist entry %d needs a comment naming the fixture it covers", i+1)

		require.Regexpf(t, `(?m)^condition\s*=\s*"AND"`, block, "entry %d must require the path AND the value, not either", i+1)
		require.Regexpf(t, `(?m)^targetRules\s*=\s*\["[a-z0-9-]+"\]`, block, "entry %d must name the rule it covers", i+1)
		require.Regexpf(t, `(?m)^paths\s*=\s*\['''\^[^']+\$'''\]`, block, "entry %d must be anchored to one file path", i+1)
		require.Regexpf(t, `(?m)^regexes\s*=\s*\['''[^']+'''\]`, block, "entry %d must name the value it covers", i+1)
	}
}

func TestDashboardDockerfile_NodeIsPinnedOnceAndTheAuditRunsInIt(t *testing.T) {
	stages := parseDockerfile(t, readRepoFile(t, dashDockerfilePath))

	var nodeBases []string
	for _, base := range externalBases(stages) {
		if strings.HasPrefix(base, "node:") {
			nodeBases = append(nodeBases, base)
		}
	}
	require.Len(t, nodeBases, 1, "Node is pinned in one FROM; every other stage builds on it")
	require.Regexp(t, `^node:\d+\.\d+\.\d+-alpine@sha256:[0-9a-f]{64}$`, nodeBases[0])

	audit, ok := stageNamed(stages, "audit")
	require.True(t, ok, "the audit runs in a stage of the pinned Node build")
	require.Equal(t, "deps", audit.base, "the audit builds on the pinned stage, not on a second Node image")
	require.Contains(t, strings.Join(audit.instructions, "\n"), "npm audit --omit=dev", "production dependencies only")

	report, ok := stageNamed(stages, "audit-report")
	require.True(t, ok, "the report is exported from a scratch stage")
	require.Equal(t, "scratch", report.base)

	// And nothing else in the repository names a Node image: the root Dockerfile and
	// the scan driver reach Node only through this one.
	for _, rel := range []string{dockerfilePath} {
		require.NotRegexp(t, `(?m)^FROM\s+node:`, readRepoFile(t, rel), "%s must not pin Node", rel)
	}
	entries, err := os.ReadDir(filepath.Join(repoRoot, scanDriverDirectory))
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), "_test.go") || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		require.NotRegexpf(t, `node:\d`, readRepoFile(t, filepath.Join(scanDriverDirectory, e.Name())), "%s must not pin Node", e.Name())
	}
}
