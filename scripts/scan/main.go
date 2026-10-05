// Command scan runs TaskForge's supply-chain scanners and applies the acceptances
// the repository records.
//
//	go run ./scripts/scan                   # make scan: all four tools
//	go run ./scripts/scan gitleaks          # one tool
//
// The tools are govulncheck (reachable vulnerabilities in the Go code),
// gitleaks (secrets in the full git history), pip-audit (the Python SDK's runtime
// dependency tree) and npm audit (the dashboard's production dependencies). Each
// is run at a pinned version and its machine-readable output is parsed.
//
// There are two acceptance mechanisms, with different meanings. A govulncheck,
// pip-audit or npm audit finding is accepted by a dated risk acceptance in
// security/scan-exceptions.yaml, and fails the run without a valid, unexpired one;
// a malformed or expired entry fails the run too. A gitleaks finding is accepted in
// .gitleaks.toml, which gitleaks itself reads, as a fake fixture or as a revoked
// secret pinned to its commit; it is never accepted in the exceptions file, and an
// entry for gitleaks there is an error.

// It exists because govulncheck has no way to say "this finding is accepted until
// that date", and because four tools with four exit-code conventions need one
// policy applied once. See docs/adr/0021-container-images-and-supply-chain-scanning.md.
//
// Like scripts/bench and scripts/imagesmoke it lives under scripts/ and not cmd/,
// so `make build` neither builds nor ships it.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"
)

// Exit codes. The first is the only one that means every scanner ran and found
// nothing that is not excepted.
const (
	exitPassed = 0
	exitFailed = 1
	exitUsage  = 2
)

// defaultExceptions is where the exceptions live, relative to the repository root
// the command is run from.
const defaultExceptions = "security/scan-exceptions.yaml"

// sourceOptions are the settings a live source reads from the command line.
type sourceOptions struct {
	// pipAuditLock is the hash-locked requirements file pip-audit is installed from.
	pipAuditLock string
}

// defaultPipAuditLock is where pip-audit's hash-locked tree is committed, relative to
// the repository root the command is run from. The version of pip-audit is the
// `pip-audit==` line in that file and nowhere else.
const defaultPipAuditLock = "security/pip-audit.requirements.txt"

// source produces one tool's raw machine-readable output, or says why it could not.
type source func(ctx context.Context, opts sourceOptions) ([]byte, error)

// parser turns a tool's raw output into findings, or says why it is not that
// tool's report.
type parser func([]byte) (scanResult, error)

var parsers = map[string]parser{
	toolGovulncheck: parseGovulncheck,
	toolGitleaks:    parseGitleaks,
	toolPipAudit:    parsePipAudit,
	toolNpmAudit:    parseNpmAudit,
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, time.Now(), liveSources())) }

const usageText = `usage: go run ./scripts/scan [flags] [tool ...]

tools: govulncheck gitleaks pip-audit npm-audit (default: all four)

flags:
  -exceptions path          the exceptions file (default security/scan-exceptions.yaml)
  -pip-audit-lock path      the hash-locked requirements pip-audit is installed from
                            (default security/pip-audit.requirements.txt)
  -govulncheck-report path  read recorded govulncheck -json output instead of running it
  -gitleaks-report path     read a recorded gitleaks JSON report instead of running it
  -pip-audit-report path    read a recorded pip-audit JSON report instead of running it
  -npm-audit-report path    read a recorded npm audit JSON report instead of running it
`

// run is the whole command, with its inputs injected: the arguments, the output
// streams, today's date, and where each tool's output comes from.
func run(args []string, stdout, stderr io.Writer, now time.Time, sources map[string]source) int {
	flags := flag.NewFlagSet("scan", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	exceptionsPath := flags.String("exceptions", defaultExceptions, "")
	pipAuditLock := flags.String("pip-audit-lock", defaultPipAuditLock, "")
	reports := map[string]*string{}
	for _, tool := range knownTools {
		reports[tool] = flags.String(tool+"-report", "", "")
	}
	if err := flags.Parse(args); err != nil {
		fmt.Fprintf(stderr, "scan: %v\n\n%s", err, usageText)
		return exitUsage
	}

	selected := flags.Args()
	if len(selected) == 0 {
		selected = knownTools
	}
	for _, tool := range selected {
		if !slices.Contains(knownTools, tool) {
			fmt.Fprintf(stderr, "scan: unknown tool %q\n\n%s", tool, usageText)
			return exitUsage
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	failed := false
	fail := func(tool, id, message string) {
		failed = true
		fmt.Fprintf(stdout, "  FAIL    %-12s %-22s %s\n", tool, id, message)
	}

	// The exceptions file is policy: it is checked on every run, whatever the tools
	// find, so a lapsed exception cannot sit unnoticed until the finding it covered
	// comes back.
	var exceptions []Exception
	raw, err := os.ReadFile(*exceptionsPath)
	if err != nil {
		fail("exceptions", "-", fmt.Sprintf("cannot read the scan-exceptions file %s: %v", *exceptionsPath, err))
	} else {
		valid, problems := parseExceptions(raw, now)
		exceptions = valid
		for _, problem := range problems {
			id, message, _ := strings.Cut(problem, " ")
			if strings.HasPrefix(problem, "the exceptions file") {
				id, message = "-", problem
			}
			fail("exceptions", id, message)
		}
	}

	matched := map[string]bool{}
	for _, tool := range knownTools {
		if !slices.Contains(selected, tool) {
			continue
		}
		data, err := obtain(ctx, tool, *reports[tool], sources, sourceOptions{pipAuditLock: *pipAuditLock})
		if err != nil {
			fail(tool, "-", "could not run: "+err.Error())
			continue
		}
		result, err := parsers[tool](data)
		if err != nil {
			fail(tool, "-", "unusable output: "+err.Error())
			continue
		}
		if len(result.Findings) == 0 {
			fmt.Fprintf(stdout, "  ok      %-12s no findings (%s)\n", tool, result.Note)
			continue
		}
		fmt.Fprintf(stdout, "  ....    %-12s %d finding(s) (%s)\n", tool, len(result.Findings), result.Note)
		for _, f := range result.Findings {
			line := fmt.Sprintf("%s  %s", f.Location, f.Detail)
			if e, ok := matchException(f, exceptions); ok {
				matched[exceptionKey(e)] = true
				fmt.Fprintf(stdout, "  waived  %-12s %-22s %s  -- excepted until %s by %s: %s\n",
					f.Tool, f.ID, line, e.Expires, e.AcceptedBy, e.Reason)
				continue
			}
			fail(f.Tool, f.ID, line+"  -- not excepted")
		}
	}

	for _, e := range exceptions {
		if slices.Contains(selected, e.Tool) && !matched[exceptionKey(e)] {
			fmt.Fprintf(stdout, "  note    exceptions   %s (%s) matched no finding in this run; remove it if the finding is gone\n", e.ID, e.Tool)
		}
	}

	if failed {
		fmt.Fprintln(stdout, "\nRESULT: FAIL")
		return exitFailed
	}
	fmt.Fprintln(stdout, "\nRESULT: PASS")
	return exitPassed
}

func exceptionKey(e Exception) string { return e.Tool + "\x00" + e.ID }

// obtain returns a tool's raw output: from a recorded report if one was named,
// otherwise from the tool's source.
func obtain(ctx context.Context, tool, reportPath string, sources map[string]source, opts sourceOptions) ([]byte, error) {
	if reportPath != "" {
		return os.ReadFile(reportPath)
	}
	src, ok := sources[tool]
	if !ok {
		return nil, fmt.Errorf("no way to run %s was provided", tool)
	}
	return src(ctx, opts)
}
