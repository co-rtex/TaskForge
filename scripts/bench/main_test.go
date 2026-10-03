package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// gitIn runs git inside dir with an identity of its own, so the test depends on
// nothing in the developer's configuration.
func gitIn(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=bench-test", "-c", "user.email=bench-test@example.invalid"}, args...)...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}

// TestRun_ARecordedRunOnADirtyTreeIsRefusedBeforeAnythingStarts proves the guard
// is wired into the recorded path and not only correct in isolation: it runs the
// real entry point in a throwaway repository. Nothing is started, so it needs no
// infrastructure.
func TestRun_ARecordedRunOnADirtyTreeIsRefusedBeforeAnythingStarts(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	gitIn(t, dir, "init", "-q")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tracked.txt"), []byte("x"), 0o644))
	gitIn(t, dir, "add", "tracked.txt")
	gitIn(t, dir, "commit", "-q", "-m", "initial")

	// Dirty: an untracked file is a difference the commit does not contain.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stray.txt"), []byte("y"), 0o644))
	var stdout, stderr bytes.Buffer
	code := run([]string{"throughput", "faults", "--record"}, &stdout, &stderr)

	require.Equal(t, exitFailed, code)
	require.Contains(t, stderr.String(), "clean working tree")
	require.Contains(t, stderr.String(), "stray.txt", "it says what is dirty")
	require.Empty(t, stdout.String(), "nothing was started: no broker queue, no services")

	// The control: remove the stray file and the refusal is no longer about the
	// tree. This repository has no bin/, so it is refused for that instead, which
	// is the next guard and proves this run got past the first one.
	require.NoError(t, os.Remove(filepath.Join(dir, "stray.txt")))
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"throughput", "faults", "--record"}, &stdout, &stderr)

	require.Equal(t, exitFailed, code)
	require.NotContains(t, stderr.String(), "clean working tree")
	require.Contains(t, stderr.String(), "bin/")
	require.Empty(t, stdout.String())
}

func TestRun_ARecordedInvocationThatChangesADefinitionIsRefusedAsAUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"throughput", "faults", "--record", "--workers", "3"}, &stdout, &stderr)

	require.Equal(t, exitUsage, code)
	require.Contains(t, stderr.String(), "workers")
	require.Empty(t, stdout.String())
}

func TestRun_UsageErrorsPrintTheUsageAndStartNothing(t *testing.T) {
	for _, args := range [][]string{nil, {"saturate"}, {"throughput", "--bogus"}} {
		var stdout, stderr bytes.Buffer
		require.Equal(t, exitUsage, run(args, &stdout, &stderr), "%v", args)
		require.Contains(t, stderr.String(), "usage:")
		require.Empty(t, stdout.String())
	}
}
