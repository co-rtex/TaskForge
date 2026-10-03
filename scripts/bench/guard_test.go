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
