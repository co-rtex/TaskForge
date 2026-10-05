// Command bench is TaskForge's load generator and benchmark harness. It starts the
// real binaries from ./bin with scripts/internal/stack (the stack scripts/demo
// uses), offers load to them over HTTP, injects worker failures, and measures
// what happened by reading PostgreSQL.
//
//	go run ./scripts/bench throughput faults --record     # make bench
//	go run ./scripts/bench smoke                          # make bench-smoke
//
// The definitions, and why each was chosen, are in
// docs/adr/0020-benchmark-methodology.md. Briefly: every measured instant is
// PostgreSQL's clock; the headline run uses the shipped default timings; a miss
// is recorded as a miss next to the settings that caused it; and the recorded
// run is made once, on a clean tree, by hand. CI runs only the smoke, which
// records nothing.
//
// It lives under scripts/ and not cmd/, so `make build` neither builds nor ships
// it. It creates keys in a scope of its own, asserts and measures only that
// scope's jobs, and never deletes or truncates anything in the database.
package main

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// Exit codes. The first is the only one that means the run did what it was asked.
const (
	exitPassed      = 0
	exitFailed      = 1
	exitUsage       = 2
	exitInterrupted = 130 // the shell convention for a run ended by SIGINT
)

// recordDir is where a recorded run writes its two files, relative to the
// repository root the command is run from.
const recordDir = "docs/benchmarks"

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	o, err := parseArgs(args)
	if err != nil {
		fmt.Fprintf(stderr, "bench: %v\n\n%s", err, usageText)
		return exitUsage
	}
	if o.record {
		if err := o.validateRecordable(); err != nil {
			fmt.Fprintf(stderr, "bench: %v\n", err)
			return exitUsage
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if o.modes[0] == modeSmoke {
		if err := RequireQuietHost(listProcesses); err != nil {
			fmt.Fprintf(stderr, "bench: %v\n", err)
			return exitFailed
		}
		err := runSmoke(ctx, stdout)
		return finish(ctx, err, stderr)
	}

	// Provenance is checked before anything is started, so a recorded run that is
	// going to be refused is refused in a second and not after twenty minutes.
	prov := checkProvenance()
	if o.record {
		if prov.cleanErr != nil {
			fmt.Fprintf(stderr, "bench: %v\n", prov.cleanErr)
			return exitFailed
		}
		if prov.binariesErr != nil {
			fmt.Fprintf(stderr, "bench: %v\n", prov.binariesErr)
			return exitFailed
		}
	}

	// After the cheap, deterministic provenance checks, and before anything starts.
	if err := RequireQuietHost(listProcesses); err != nil {
		fmt.Fprintf(stderr, "bench: %v\n", err)
		return exitFailed
	}

	var th *ThroughputResult
	var f *FaultsResult
	var runErr error
	for _, mode := range []string{modeThroughput, modeFaults} {
		if !contains(o.modes, mode) || runErr != nil {
			continue
		}
		switch mode {
		case modeThroughput:
			res, err := runThroughput(ctx, o, stdout)
			th, runErr = &res, err
		case modeFaults:
			res, err := runFaults(ctx, o, stdout)
			f, runErr = &res, err
		}
	}
	if ctx.Err() != nil {
		return finish(ctx, runErr, stderr)
	}
	if runErr != nil {
		// A run that produced figures but is not valid is reported with them, and
		// is never recorded.
		report(stdout, o, prov, th, f, args, false)
		return finish(ctx, runErr, stderr)
	}

	if !report(stdout, o, prov, th, f, args, o.record) {
		return exitFailed
	}
	return exitPassed
}

func contains(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

func finish(ctx context.Context, err error, stderr io.Writer) int {
	switch {
	case ctx.Err() != nil:
		fmt.Fprintln(stderr, "bench: interrupted; the run did not complete")
		return exitInterrupted
	case err != nil:
		fmt.Fprintf(stderr, "bench: %v\n", err)
		return exitFailed
	default:
		return exitPassed
	}
}

// --- provenance -------------------------------------------------------------------

// provenance is what is known about the commit being measured.
type provenance struct {
	sha         string
	clean       bool
	cleanErr    error // non-nil if the tree is dirty or git could not be asked
	binariesOK  bool
	binariesErr error
}

func runGit(args ...string) (string, error) {
	out, err := exec.Command("git", args...).Output()
	return string(out), err
}

// checkProvenance asks git what commit this is and whether the tree is clean, and
// asks each binary in ./bin what commit it was built from. It never fails: what it
// could not establish is in the returned errors, and a recorded run refuses on any
// of them.
func checkProvenance() provenance {
	var p provenance
	out, err := runGit("rev-parse", "HEAD")
	if err != nil {
		p.cleanErr = fmt.Errorf("could not read the current commit: %w", err)
		p.binariesErr = p.cleanErr
		return p
	}
	p.sha = strings.TrimSpace(out)
	p.cleanErr = RequireCleanTree(runGit)
	p.clean = p.cleanErr == nil
	p.binariesErr = RequireBuiltFrom("bin", stack.RequiredBinaries(), p.sha, buildinfo.ReadFile)
	p.binariesOK = p.binariesErr == nil
	return p
}

// --- the report and the record ----------------------------------------------------

// report prints what the run found and, if record is set, writes it to
// docs/benchmarks. It returns false if the run could not be recorded as asked.
func report(out io.Writer, o options, prov provenance, th *ThroughputResult, f *FaultsResult, args []string, record bool) bool {
	timings, err := o.timings()
	if err != nil {
		fmt.Fprintf(out, "bench: %v\n", err)
		return false
	}
	env := captureEnvironment(runCommand, runtime.GOOS, serverVersion())
	rec := Record{
		SchemaVersion: 1, RecordedAt: time.Now().UTC(),
		Commit: prov.sha, TreeClean: prov.clean, BinariesChecked: prov.binariesOK,
		Command: "go run ./scripts/bench " + strings.Join(args, " "),
		Profile: o.profile, Seed: o.seed, Timing: timings.Env(), Environment: env,
		Throughput: th, Faults: f,
		Targets: evaluateTargets(th, f, timings.Env(), o.workers, o.concurrency),
	}

	if !record {
		fmt.Fprintf(out, "\n(not recorded: this run did not ask for --record, or it was not valid)\n\n%s\n", renderMarkdown(rec))
		return true
	}
	if (th != nil && !th.Valid) || (f != nil && !f.Valid) {
		fmt.Fprintf(out, "\nbench: not recording a run that is not valid.\n\n%s\n", renderMarkdown(rec))
		return false
	}

	short := rec.Commit
	if len(short) > 7 {
		short = short[:7]
	}
	mdPath, jsonPath := recordPaths(recordDir, rec.RecordedAt, short, o.profile)
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		fmt.Fprintf(out, "bench: encode the summary: %v\n", err)
		return false
	}
	if err := os.MkdirAll(recordDir, 0o755); err != nil {
		fmt.Fprintf(out, "bench: %v\n", err)
		return false
	}
	// O_EXCL: a record is never overwritten. Running again to get a better number
	// must not be able to replace the worse one.
	if err := writeNew(mdPath, []byte(renderMarkdown(rec))); err != nil {
		fmt.Fprintf(out, "bench: %v\n", err)
		return false
	}
	if err := writeNew(jsonPath, append(raw, '\n')); err != nil {
		fmt.Fprintf(out, "bench: %v\n", err)
		return false
	}
	fmt.Fprintf(out, "\n%s\n\nRecorded %s and %s\n", renderMarkdown(rec), mdPath, jsonPath)
	return true
}

func writeNew(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%s already exists; a record is never overwritten", path)
		}
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// serverVersion asks PostgreSQL for its version on a connection of this process's
// own, after the runs, for the record. If it cannot, the record says so.
func serverVersion() string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	in, err := stack.LoadInfra()
	if err != nil {
		return "unknown: " + err.Error()
	}
	pool, err := readdb.Open(ctx, in.DatabaseURL)
	if err != nil {
		return "unknown: " + err.Error()
	}
	defer pool.Close()
	version, err := readdb.ServerVersion(ctx, pool)
	if err != nil {
		return "unknown: " + err.Error()
	}
	return version
}
