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
