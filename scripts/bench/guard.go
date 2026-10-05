package main

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
)

// GitFunc runs git with the arguments and returns its standard output. It is a
// parameter so a test can answer for it.
type GitFunc func(args ...string) (string, error)

// RequireCleanTree refuses unless `git status --porcelain` reports nothing. A
// recorded run names the commit it measured; if the tree differed from that
// commit, the name would be a lie. Untracked files count too: a stray file is a
// difference the commit does not contain.
//
// If git cannot be asked, that is a refusal and not a pass.
func RequireCleanTree(git GitFunc) error {
	out, err := git("status", "--porcelain")
	if err != nil {
		return fmt.Errorf("could not check that the working tree is clean: %w", err)
	}
	if strings.TrimSpace(out) == "" {
		return nil
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	const shown = 20
	listed := lines[:min(len(lines), shown)]
	more := ""
	if len(lines) > shown {
		more = fmt.Sprintf("\n  ... and %d more", len(lines)-shown)
	}
	return fmt.Errorf("a recorded run needs a clean working tree, and `git status --porcelain` reports:\n  %s%s\ncommit or remove it first",
		strings.Join(listed, "\n  "), more)
}

// BuildInfoFunc reads the build information stamped into an executable.
type BuildInfoFunc func(path string) (*debug.BuildInfo, error)

// RequireBuiltFrom refuses unless every named binary in binDir was built from
// revision want, in a tree that had no modifications. `go build` stamps both
// into the executable, so this is what makes "the SHA this record measured" a
// fact about the binaries that ran and not only about the checkout: a stale
// ./bin left over from another commit would otherwise be measured under this
// commit's name.
//
// A binary with no revision at all (built outside a repository, or with
// -buildvcs=false) is a refusal too: it says nothing about where it came from.
func RequireBuiltFrom(binDir string, binaries []string, want string, read BuildInfoFunc) error {
	for _, name := range binaries {
		path := filepath.Join(binDir, name)
		info, err := read(path)
		if err != nil {
			return fmt.Errorf("could not read the build information of %s: %w; run `make build`", path, err)
		}
		settings := map[string]string{}
		for _, s := range info.Settings {
			settings[s.Key] = s.Value
		}
		switch {
		case settings["vcs.revision"] == "":
			return fmt.Errorf("%s carries no vcs.revision, so it cannot be shown to be this commit; run `make build`", path)
		case settings["vcs.revision"] != want:
			return fmt.Errorf("%s was built from %s, not from %s; run `make build`", path, settings["vcs.revision"], want)
		case settings["vcs.modified"] == "true":
			return fmt.Errorf("%s was built from a modified tree; run `make build` on a clean one", path)
		}
	}
	return nil
}

// ProcessLister returns one line per running process, "<pid> <command line>", as
// `ps -axo pid=,command=` prints them. It is a parameter so a test can answer.
type ProcessLister func() ([]string, error)

func listProcesses() ([]string, error) {
	out, err := exec.Command("ps", "-axo", "pid=,command=").Output()
	if err != nil {
		return nil, err
	}
	return strings.Split(strings.TrimRight(string(out), "\n"), "\n"), nil
}

// serviceBinaries are the long-running TaskForge processes. Any of them already
// running on this machine takes part in the work of the stack being measured: the
// outbox publisher, the scheduler and the reconciler claim rows from tables that
// every scope shares, so a stray one publishes, promotes and re-notifies for a run
// that is not its own. taskforge-cli is a client and is not here.
var serviceBinaries = []string{
	"taskforge-api", "taskforge-outbox", "taskforge-scheduler", "taskforge-reconciler", "taskforge-worker",
}

// RequireQuietHost refuses to start while another TaskForge service is running on
// this machine, and names each by pid so it can be stopped. It looks at the
// executable (the first word of the command) and not at the text, so an editor
// open on a source file, `go build`, or a grep for the name is not mistaken for a
// service. If the list cannot be read, that is a refusal and not a pass.
//
// It sees this machine only. A stack on another host pointed at the same database
// is not visible to it, and the record says so.
//
// This exists because it happened: a smoke run crashed without running its
// cleanup, its outbox and scheduler stayed up, and five later runs and eight
// integration tests failed in ways that looked like a slow system.
func RequireQuietHost(list ProcessLister) error {
	lines, err := list()
	if err != nil {
		return fmt.Errorf("could not check that no other TaskForge stack is running: %w", err)
	}
	var strays []string
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		executable := filepath.Base(fields[1])
		for _, name := range serviceBinaries {
			if executable == name {
				strays = append(strays, fmt.Sprintf("%s %s", fields[0], strings.Join(fields[1:], " ")))
			}
		}
	}
	if len(strays) == 0 {
		return nil
	}
	return fmt.Errorf("another TaskForge stack is running on this machine and would compete for the same work, so no measurement here could be trusted:\n  %s\nstop it first (for a crashed run's leftovers: pkill -f 'bin/taskforge-')",
		strings.Join(strays, "\n  "))
}
