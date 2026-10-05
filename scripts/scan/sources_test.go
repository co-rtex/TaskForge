package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// pip-audit's whole tree is installed from the hash-locked file and from nothing
// else. The arguments are built by a function so a test can read them; grepping the
// source for the words would pass for a call that was never made.
func TestPipAuditInstallArgs_InstallFromTheLockWithRequireHashesAndNoDeps(t *testing.T) {
	args := pipAuditInstallArgs("/work/tool/bin/python", "/repo/security/pip-audit.requirements.txt")

	require.Equal(t, "/work/tool/bin/python", args[0], "pip runs under the tool venv's own interpreter")
	require.Equal(t, []string{"-m", "pip", "install"}, args[1:4])
	require.Contains(t, args, "--require-hashes", "pip refuses any file whose hash is not in the lock")
	require.Contains(t, args, "--no-deps", "the lock is the whole tree; pip must not resolve anything beside it")
	// Without this, a wheel whose hash does not match is silently skipped and pip falls
	// back to the source distribution, whose hash still does, and builds it with build
	// dependencies the lock does not cover. A tampered wheel must be a loud failure.
	require.Contains(t, args, "--only-binary=:all:", "a wheel that fails its hash must fail the install, not fall back to building from source")
	require.Contains(t, args, "-r")
	require.Equal(t, "/repo/security/pip-audit.requirements.txt", args[len(args)-1])
	require.Equal(t, 1, countFlag(args, "-r"))
	for _, a := range args {
		require.NotRegexp(t, `^pip-audit(==|$|>|<|~)`, a, "no requirement is named on the command line: that would be an unlocked install")
		require.NotContains(t, a, "pip-audit==")
	}
}

func countFlag(args []string, flag string) int {
	n := 0
	for _, a := range args {
		if a == flag {
			n++
		}
	}
	return n
}

// The version of pip-audit lives in one place: the lock. There is no second constant
// in the driver to drift from it.
func TestThePipAuditVersionLivesInTheLockfileAlone(t *testing.T) {
	require.Equal(t, "security/pip-audit.requirements.txt", defaultPipAuditLock)

	raw, err := os.ReadFile(filepath.Join("..", "..", defaultPipAuditLock))
	require.NoError(t, err)
	pins := regexp.MustCompile(`(?m)^pip-audit==(\d+\.\d+\.\d+)\b`).FindAllStringSubmatch(string(raw), -1)
	require.Len(t, pins, 1, "pip-audit is pinned exactly once in its own lock")

	// And the driver's source does not restate it.
	for _, name := range []string{"sources.go", "main.go"} {
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		require.NotContains(t, string(src), "pip-audit=="+pins[0][1], "%s restates the version", name)
		require.NotRegexp(t, `"pip-audit==`, string(src), "%s installs by version", name)
	}
}

// A shallow clone holds part of the history, and a scan of part of it is not a scan of
// the history. The owner's scope for gitleaks is the full history.
func TestShallowProblem(t *testing.T) {
	require.NotEmpty(t, shallowProblem("true\n"))
	require.Contains(t, shallowProblem("true\n"), "shallow clone: gitleaks would scan partial history; fetch full history")

	require.Empty(t, shallowProblem("false\n"))
	require.Empty(t, shallowProblem("false"))

	// git older than 2.15 does not know the option and echoes it back; anything that is
	// not exactly true or false is a failure to find out, and failing to find out refuses.
	for _, out := range []string{"", "--is-shallow-repository\n", "maybe\n", "true false\n"} {
		require.NotEmptyf(t, shallowProblem(out), "%q must refuse", out)
		require.Containsf(t, strings.ToLower(shallowProblem(out)), "could not tell", "%q", out)
	}
}
