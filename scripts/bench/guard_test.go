package main

import (
	"errors"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

const wantSHA = "99044203187854f9cffaf76658a094223061ab77"

// git returns a canned answer for `git status --porcelain`, and records that it
// was asked it, so a guard that never looked cannot pass.
func git(output string, err error, asked *[]string) GitFunc {
	return func(args ...string) (string, error) {
		if asked != nil {
			*asked = append(*asked, args[0]+" "+args[1])
		}
		return output, err
	}
}

func TestRequireCleanTree_AcceptsATreeWithNothingToReport(t *testing.T) {
	var asked []string
	require.NoError(t, RequireCleanTree(git("", nil, &asked)))
	require.Equal(t, []string{"status --porcelain"}, asked, "it must actually ask git, in the form that includes untracked files")
}

func TestRequireCleanTree_AWhitespaceOnlyAnswerIsStillClean(t *testing.T) {
	require.NoError(t, RequireCleanTree(git("\n", nil, nil)))
}

// The guard refuses, and it names what is dirty so the cause is not a guess.
func TestRequireCleanTree_RefusesAModifiedFile(t *testing.T) {
	err := RequireCleanTree(git(" M scripts/bench/main.go\n", nil, nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "scripts/bench/main.go")
}

func TestRequireCleanTree_RefusesAnUntrackedFileAndStagedWorkToo(t *testing.T) {
	err := RequireCleanTree(git("?? scratch.txt\nM  docs/x.md\n", nil, nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "scratch.txt")
	require.Contains(t, err.Error(), "docs/x.md")
}

func TestRequireCleanTree_AFailureToAskIsARefusalNotAPass(t *testing.T) {
	err := RequireCleanTree(git("", errors.New("not a git repository"), nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a git repository")
}

func info(settings map[string]string) *debug.BuildInfo {
	b := &debug.BuildInfo{}
	for k, v := range settings {
		b.Settings = append(b.Settings, debug.BuildSetting{Key: k, Value: v})
	}
	return b
}

func readerOf(infos map[string]*debug.BuildInfo) BuildInfoFunc {
	return func(path string) (*debug.BuildInfo, error) {
		if b, ok := infos[path]; ok {
			return b, nil
		}
		return nil, errors.New("no such file")
	}
}

func TestRequireBuiltFrom_AcceptsBinariesStampedWithTheRevisionAndAnUnmodifiedTree(t *testing.T) {
	good := info(map[string]string{"vcs.revision": wantSHA, "vcs.modified": "false"})
	read := readerOf(map[string]*debug.BuildInfo{"/bin/a": good, "/bin/b": good})

	require.NoError(t, RequireBuiltFrom("/bin", []string{"a", "b"}, wantSHA, read))
}

func TestRequireBuiltFrom_RefusesABinaryBuiltFromAnotherRevision(t *testing.T) {
	good := info(map[string]string{"vcs.revision": wantSHA, "vcs.modified": "false"})
	stale := info(map[string]string{"vcs.revision": "1111111111111111111111111111111111111111", "vcs.modified": "false"})
	read := readerOf(map[string]*debug.BuildInfo{"/bin/a": good, "/bin/b": stale})

	err := RequireBuiltFrom("/bin", []string{"a", "b"}, wantSHA, read)
	require.Error(t, err)
	require.Contains(t, err.Error(), "b")
	require.Contains(t, err.Error(), "make build")
}

func TestRequireBuiltFrom_RefusesABinaryBuiltFromAModifiedTree(t *testing.T) {
	dirty := info(map[string]string{"vcs.revision": wantSHA, "vcs.modified": "true"})
	err := RequireBuiltFrom("/bin", []string{"a"}, wantSHA, readerOf(map[string]*debug.BuildInfo{"/bin/a": dirty}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "modified")
}

func TestRequireBuiltFrom_RefusesABinaryThatCarriesNoRevisionAtAll(t *testing.T) {
	// A binary built with -buildvcs=false, or outside a repository, says nothing:
	// that is not evidence it came from this revision.
	err := RequireBuiltFrom("/bin", []string{"a"}, wantSHA, readerOf(map[string]*debug.BuildInfo{"/bin/a": info(nil)}))
	require.Error(t, err)
}

func TestRequireBuiltFrom_RefusesAMissingBinary(t *testing.T) {
	err := RequireBuiltFrom("/bin", []string{"a"}, wantSHA, readerOf(nil))
	require.Error(t, err)
}

// A stack that is already running on this machine competes for the work of the
// stack being measured. The outbox publisher and the scheduler claim events from
// one shared table whatever their scope, so a stray one publishes another run's
// notifications to a queue nobody reads. A smoke that was crashed once left
// exactly that behind, and the next runs failed in ways that looked like a slow
// system and were not.
func lister(lines ...string) ProcessLister {
	return func() ([]string, error) { return lines, nil }
}

func TestRequireQuietHost_AcceptsAMachineWithNoServiceProcesses(t *testing.T) {
	require.NoError(t, RequireQuietHost(lister(
		"  101 /sbin/launchd",
		"  202 /usr/local/bin/postgres -D /var/lib/postgresql/data",
		"  303 /usr/local/bin/docker compose up -d",
	)))
	require.NoError(t, RequireQuietHost(lister()), "an empty list is a quiet machine")
}

func TestRequireQuietHost_ARunningServiceBinaryIsRefusedAndNamed(t *testing.T) {
	err := RequireQuietHost(lister(
		"  101 /sbin/launchd",
		"36827 /Users/me/TaskForge/bin/taskforge-outbox",
	))

	require.Error(t, err)
	require.Contains(t, err.Error(), "36827")
	require.Contains(t, err.Error(), "taskforge-outbox")
	require.Contains(t, err.Error(), "stop", "it says what to do about it")
}

func TestRequireQuietHost_EveryServiceBinaryCounts(t *testing.T) {
	for _, name := range []string{"taskforge-api", "taskforge-outbox", "taskforge-scheduler", "taskforge-reconciler", "taskforge-worker"} {
		err := RequireQuietHost(lister("  900 /opt/x/bin/" + name + " --flag"))
		require.Errorf(t, err, "%s must be refused", name)
	}
}

func TestRequireQuietHost_ListsEveryStrayNotJustTheFirst(t *testing.T) {
	err := RequireQuietHost(lister(
		"  11 /a/bin/taskforge-api",
		"  12 /a/bin/taskforge-outbox",
		"  13 /a/bin/taskforge-worker",
	))
	for _, pid := range []string{"11", "12", "13"} {
		require.Contains(t, err.Error(), pid)
	}
}

// Text that merely mentions a service is not a service: an editor open on its
// source, the go tool building it, a grep for it, the CLI.
func TestRequireQuietHost_OnlyTheExecutableCounts(t *testing.T) {
	require.NoError(t, RequireQuietHost(lister(
		"  21 vim cmd/taskforge-worker/main.go",
		"  22 go build -o bin/ ./cmd/taskforge-worker",
		"  23 grep taskforge-outbox /tmp/log",
		"  24 /Users/me/TaskForge/bin/taskforge-cli jobs list",
	)))
}

func TestRequireQuietHost_ARunFromGoRunIsStillAServiceByItsBinaryName(t *testing.T) {
	require.Error(t, RequireQuietHost(lister("  31 /var/folders/xx/T/go-build123/b001/exe/taskforge-scheduler")))
}

func TestRequireQuietHost_AFailureToListIsARefusalNotAPass(t *testing.T) {
	err := RequireQuietHost(func() ([]string, error) { return nil, errors.New("ps: operation not permitted") })
	require.Error(t, err)
	require.Contains(t, err.Error(), "operation not permitted")
}
