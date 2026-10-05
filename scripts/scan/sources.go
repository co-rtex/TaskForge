package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

// The pinned versions of everything the scan runs. Each is exact: a floating
// version would make the same commit pass one day and fail the next for a reason
// that is not in the commit. TestEveryToolIsPinnedExactly holds the shapes.
//
// Node, which npm audit runs inside, is pinned in dashboard/Dockerfile and nowhere
// else.
const (
	// govulncheck v1.8.0 needs Go 1.26; v1.7.0 is the newest that builds with the
	// Go in go.mod. Raise this when go.mod's Go does.
	govulncheckVersion = "v1.7.0"

	// gitleaks runs from its published container image, pinned by version and by
	// multi-platform index digest. It is deliberately not gitleaks-action, which
	// needs a licence for organisations.
	gitleaksImage = "ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f"
)

// Paths, relative to the repository root the command is run from.
const (
	goModFile      = "go.mod"
	gitleaksConfig = ".gitleaks.toml"
	sdkDir         = "sdk/python"
	dashDockerfile = "dashboard/Dockerfile"
)

func liveSources() map[string]source {
	return map[string]source{
		toolGovulncheck: runGovulncheck,
		toolGitleaks:    runGitleaks,
		toolPipAudit:    runPipAudit,
		toolNpmAudit:    runNpmAudit,
	}
}

// exec runs a command and returns its standard output, with its standard error and
// exit status folded into the error so that a failure says what the tool said.
type execResult struct {
	stdout   []byte
	exitCode int
	err      error
}

func execute(ctx context.Context, dir string, env []string, name string, args ...string) execResult {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if env != nil {
		cmd.Env = append(os.Environ(), env...)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	res := execResult{stdout: stdout.Bytes()}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			res.exitCode = exit.ExitCode()
		} else {
			res.exitCode = -1
		}
		res.err = fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, truncate(stderr.String(), 600))
	}
	return res
}

var goDirective = regexp.MustCompile(`(?m)^go\s+(\d+\.\d+(?:\.\d+)?)\s*$`)

// runGovulncheck runs govulncheck over every package, as JSON.
//
// It runs under the exact Go toolchain go.mod declares. govulncheck reports
// standard-library vulnerabilities against whichever Go runs it, so on a machine
// with a newer Go the scan would pass for a toolchain the images are not built
// with; GOTOOLCHAIN pins it to the one the Dockerfile and CI use. With -json it
// exits 0 whether or not it found anything, so any other exit is the tool failing.
func runGovulncheck(ctx context.Context, _ sourceOptions) ([]byte, error) {
	mod, err := os.ReadFile(goModFile)
	if err != nil {
		return nil, err
	}
	m := goDirective.FindSubmatch(mod)
	if m == nil {
		return nil, fmt.Errorf("%s has no go directive", goModFile)
	}
	res := execute(ctx, "", []string{"GOTOOLCHAIN=go" + string(m[1])},
		"go", "run", "golang.org/x/vuln/cmd/govulncheck@"+govulncheckVersion, "-json", "./...")
	return res.stdout, res.err
}

// runGitleaks scans the full git history of every ref, not only the checked-out
// tree: a secret committed and then deleted is still in the history, and still
// leaked.
//
// The scan runs in the pinned container with the repository's .git directory
// mounted read-only. In a linked worktree .git is a file pointing elsewhere, so it
// mounts git's common directory, which is the real one in either case. The report
// goes to a mounted file, not to /dev/stdout, where the container loses it;
// --redact keeps the secrets out of the report, and --exit-code 0 makes a finding a
// report entry and not a process failure, so that a non-zero exit means gitleaks
// itself failed.
func runGitleaks(ctx context.Context, _ sourceOptions) ([]byte, error) {
	// A shallow clone holds part of the history, and a scan of part of it is not a
	// scan of the history. This is checked first, before anything is mounted.
	shallow := execute(ctx, "", nil, "git", "rev-parse", "--is-shallow-repository")
	if shallow.err != nil {
		return nil, shallow.err
	}
	if problem := shallowProblem(string(shallow.stdout)); problem != "" {
		return nil, errors.New(problem)
	}

	config, err := filepath.Abs(gitleaksConfig)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(config); err != nil {
		return nil, fmt.Errorf("%s is missing: %w", gitleaksConfig, err)
	}
	gitDir := execute(ctx, "", nil, "git", "rev-parse", "--path-format=absolute", "--git-common-dir")
	if gitDir.err != nil {
		return nil, gitDir.err
	}
	out, err := os.MkdirTemp("", "scan-gitleaks-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(out)

	res := execute(ctx, "", nil, "docker", "run", "--rm",
		"--volume", strings.TrimSpace(string(gitDir.stdout))+":/repo/.git:ro",
		"--volume", config+":/config/gitleaks.toml:ro",
		"--volume", out+":/out",
		"--workdir", "/repo",
		// Git refuses a repository owned by another user; the mount is read-only,
		// and the scan is the only thing that reads it.
		"--env", "GIT_CONFIG_COUNT=1", "--env", "GIT_CONFIG_KEY_0=safe.directory", "--env", "GIT_CONFIG_VALUE_0=*",
		gitleaksImage, "git", "/repo",
		"--config", "/config/gitleaks.toml",
		"--log-opts=--all",
		"--report-format", "json", "--report-path", "/out/report.json",
		"--redact", "--no-banner", "--exit-code", "0")
	if res.err != nil {
		return nil, res.err
	}
	return os.ReadFile(filepath.Join(out, "report.json"))
}

// pipAuditInstallArgs is the argument list that installs the scanner into its own
// virtual environment: from the committed hash-locked file, with --require-hashes so
// that pip refuses any file whose hash is not in the lock, --no-deps so that it
// resolves nothing beside the lock, and --only-binary=:all: so that a wheel failing its
// hash is a failure. Without that last one, pip silently skips a wheel whose hash does
// not match and falls back to the source distribution (whose hash is also in the lock),
// then builds it with build dependencies the lock does not cover: a tampered wheel
// would become a quiet downgrade instead of an error. No requirement is named on the
// command line. It is a function so that a test can read the arguments the driver uses.
func pipAuditInstallArgs(toolPython, lockFile string) []string {
	return []string{toolPython, "-m", "pip", "install", "--quiet", "--disable-pip-version-check",
		"--require-hashes", "--only-binary=:all:", "--no-deps", "-r", lockFile}
}

// shallowProblem judges the answer of `git rev-parse --is-shallow-repository`. Anything
// but exactly "false" refuses: "true" because the history is partial, and anything else
// (git older than 2.15 echoes the option back) because failing to find out cannot be
// allowed to read as "not shallow".
func shallowProblem(output string) string {
	switch strings.TrimSpace(output) {
	case "false":
		return ""
	case "true":
		return "shallow clone: gitleaks would scan partial history; fetch full history"
	}
	return fmt.Sprintf("could not tell whether this is a shallow clone (git said %q); gitleaks needs the full history", strings.TrimSpace(output))
}

// runPipAudit audits the Python SDK's runtime dependency tree.
//
// It installs the SDK into a clean virtual environment, so the tree audited is the
// one a user gets, resolved at the moment of the scan, and not the development
// tools beside it. The SDK has no lockfile; this resolution is the closest thing to
// what `pip install` would deliver. That tree is deliberately unlocked: it is what is
// being examined. The SCANNER is the opposite: pip-audit and its whole dependency tree
// are installed from a committed file in which every package is pinned and every
// distribution file is hash-locked, so the check that looks for a compromised
// dependency does not itself run whatever PyPI serves that day. The project itself is
// left out of the audited list (it is not on an index), and pip-audit is told not to
// resolve again.
//
// pip-audit exits 1 when it finds something, with the report on standard output, so
// exit 1 with output is handed to the parser, which decides.
func runPipAudit(ctx context.Context, opts sourceOptions) ([]byte, error) {
	sdk, err := filepath.Abs(sdkDir)
	if err != nil {
		return nil, err
	}
	lock, err := filepath.Abs(firstNonEmpty(opts.pipAuditLock, defaultPipAuditLock))
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(lock); err != nil {
		return nil, fmt.Errorf("the hash-locked pip-audit requirements are missing: %w", err)
	}
	work, err := os.MkdirTemp("", "scan-pip-audit-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(work)

	sdkPython := filepath.Join(work, "sdk", "bin", "python")
	toolDir := filepath.Join(work, "tool")
	toolPython := filepath.Join(toolDir, "bin", "python")
	for _, step := range [][]string{
		{"python3", "-m", "venv", filepath.Join(work, "sdk")},
		{sdkPython, "-m", "pip", "install", "--quiet", "--disable-pip-version-check", sdk},
		{"python3", "-m", "venv", toolDir},
		pipAuditInstallArgs(toolPython, lock),
	} {
		if res := execute(ctx, "", nil, step[0], step[1:]...); res.err != nil {
			return nil, res.err
		}
	}

	freeze := execute(ctx, "", nil, sdkPython, "-m", "pip", "freeze", "--disable-pip-version-check")
	if freeze.err != nil {
		return nil, freeze.err
	}
	var requirements []string
	for _, line := range strings.Split(string(freeze.stdout), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.Contains(line, " @ ") {
			requirements = append(requirements, line)
		}
	}
	reqFile := filepath.Join(work, "requirements.txt")
	if err := os.WriteFile(reqFile, []byte(strings.Join(requirements, "\n")+"\n"), 0o600); err != nil {
		return nil, err
	}

	res := execute(ctx, "", nil, filepath.Join(toolDir, "bin", "pip-audit"),
		"-r", reqFile, "--no-deps", "--disable-pip", "--progress-spinner", "off", "--format", "json")
	if res.err != nil && !(res.exitCode == 1 && len(res.stdout) > 0) {
		return nil, res.err
	}
	return res.stdout, nil
}

// runNpmAudit audits the dashboard's production dependencies in the pinned Node
// container: it builds the audit-report stage of dashboard/Dockerfile and reads the
// report out of it. --no-cache-filter makes the audit run every time; a cached
// layer would report the advisory database as it was when the layer was built.
func runNpmAudit(ctx context.Context, _ sourceOptions) ([]byte, error) {
	out, err := os.MkdirTemp("", "scan-npm-audit-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(out)

	res := execute(ctx, "", nil, "docker", "build", "--file", dashDockerfile,
		"--target", "audit-report", "--no-cache-filter", "audit", "--progress", "plain",
		"--output", "type=local,dest="+out, ".")
	if res.err != nil {
		return nil, res.err
	}
	return os.ReadFile(filepath.Join(out, "audit.json"))
}
