// Command demo runs TaskForge's two demonstrations against the real binaries in
// ./bin: `success` (make demo) shows a job that succeeds, a job that retries
// until its budget is spent and is dead-lettered, and a job that is
// dead-lettered at once; `failure` (make demo-failure) kills one worker and
// freezes another, and shows the system recovering from both.
//
// It lives under scripts/ rather than cmd/ on purpose: `make build` compiles
// ./cmd/... and nothing else, so this program is neither built nor shipped with
// the product. It is a client of the product, in the way taskforge-cli is.
//
// What it does and does not touch:
//
//   - It starts its own api, outbox, scheduler, reconciler and workers on free
//     loopback ports, and its own broker queue, and stops every one of them on
//     every way out, including SIGINT and SIGTERM.
//   - It reads the infrastructure it is pointed at from the same TASKFORGE_*
//     variables the services read (and the same .env), falling back to the
//     compose defaults, so CI can point it at its own services.
//   - It creates keys in a scope unique to the run and asserts only on the jobs
//     it submitted. It never deletes or truncates anything in the database,
//     which may hold a developer's own data.
//
// It exits non-zero if any expectation fails, any wait times out, or anything
// it started could not be started.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

const (
	modeSuccess = "success"
	modeFailure = "failure"
)

// Exit codes. The first is the only one that means every expectation passed.
const (
	exitPassed      = 0
	exitFailed      = 1
	exitUsage       = 2
	exitInterrupted = 130 // the shell convention for a run ended by SIGINT
)

const usageText = `usage: go run ./scripts/demo <success|failure>

  success   one worker: a job that succeeds, a job that retries until its
            budget is spent and is dead-lettered, and a job that is
            dead-lettered at once                              (make demo)
  failure   a worker killed mid-job, and a worker frozen mid-job that wakes
            after its job has moved on                         (make demo-failure)
`

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 || (args[0] != modeSuccess && args[0] != modeFailure) {
		fmt.Fprint(stderr, usageText)
		return exitUsage
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	d, err := newDemo(stdout, args[0])
	if err != nil {
		fmt.Fprintf(stderr, "demo: %v\n", err)
		return exitFailed
	}
	return d.run(ctx)
}

// run executes the chosen demonstration and returns the process exit code.
func (d *demo) run(ctx context.Context) int {
	// Deferred as well as called below: a panic must not leave a worker behind.
	defer d.Cleanup()

	d.Say("TaskForge demo, %s mode. This run's scope is %q.", d.mode, d.Scope)
	d.Say("Process logs go to %s", d.LogDir)

	if err := d.Setup(ctx); err != nil {
		d.rep.add("the demo stack started", false, err.Error())
	} else if d.mode == modeSuccess {
		d.runSuccess(ctx)
	} else {
		d.runFailure(ctx)
	}

	// Stopped before the table is printed, so no process is still writing and
	// nothing the summary says depends on one that is still running.
	d.Cleanup()
	return d.finish(ctx)
}

// finish prints the expectation table and chooses the exit code.
func (d *demo) finish(ctx context.Context) int {
	failed := d.rep.print(d.Out, d.mode)
	d.Say("Process logs were left in %s", d.LogDir)
	switch {
	case ctx.Err() != nil:
		d.Say("Interrupted: the run did not complete.")
		return exitInterrupted
	case failed > 0 || d.rep.empty():
		return exitFailed
	default:
		return exitPassed
	}
}
