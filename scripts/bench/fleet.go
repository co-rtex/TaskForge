package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// workerHandle is one logical worker, which may be several processes over a run:
// killed, then started again under the same name.
type workerHandle struct {
	label string
	name  string

	mu   sync.Mutex
	proc *stack.Proc
	// down is set by the fault injector around a kill it made itself, so that the
	// watchdog does not mistake the injector's own work for a crash.
	down bool
}

func (w *workerHandle) setDown(v bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.down = v
}

func (w *workerHandle) expectedDown() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.down
}

func (w *workerHandle) current() *stack.Proc {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.proc
}

func (w *workerHandle) replace(p *stack.Proc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.proc = p
}

// fleet is a stack and its workers.
type fleet struct {
	st          *stack.Stack
	workers     []*workerHandle
	concurrency int
	// target is non-nil only when kills are aimed at attempts with time left (the
	// smoke). Nil is the recorded runs' selection.
	target *targeting
}

// startFleet starts the stack (api, outbox, scheduler, reconciler, a broker queue
// and a pair of keys in a scope of this run's own) and then the workers. Nothing
// it starts outlives close.
func startFleet(ctx context.Context, o options, out io.Writer) (*fleet, error) {
	return startFleetWith(ctx, o, out, nil)
}

// startFleetWith is startFleet with a hook that may change the stack's options
// before anything starts. Only the recovery probe passes one (to set the queue's
// visibility timeout or the workers' poll wait); nil is startFleet exactly.
func startFleetWith(ctx context.Context, o options, out io.Writer, adjust func(*stack.Options)) (*fleet, error) {
	timing, err := o.timings()
	if err != nil {
		return nil, err
	}
	stackOptions := stack.Options{Out: out, Prefix: "bench", Timing: timing}
	if adjust != nil {
		adjust(&stackOptions)
	}
	st, err := stack.New(stackOptions)
	if err != nil {
		return nil, err
	}
	f := &fleet{st: st, concurrency: o.concurrency, target: targetingFor(o)}
	if err := st.Setup(ctx); err != nil {
		st.Cleanup()
		return nil, err
	}
	st.Say("Profile %q. Starting %d workers of %d slots each.", o.profile, o.workers, o.concurrency)
	for i := 1; i <= o.workers; i++ {
		label := fmt.Sprintf("w%02d", i)
		proc, name, err := st.StartWorker(ctx, label, o.concurrency)
		if err != nil {
			st.Cleanup()
			return nil, fmt.Errorf("start worker %s: %w", label, err)
		}
		f.workers = append(f.workers, &workerHandle{label: label, name: name, proc: proc})
	}
	st.Say("All %d workers report ready (their sessions are registered).", o.workers)
	return f, nil
}

func (f *fleet) close() { f.st.Cleanup() }

// exitedOnTheirOwn lists workers whose process is not running. It is called when
// every kill has been followed by its restart, so a worker that is down then went
// down by itself, and a run that lost capacity it did not mean to lose says so.
func (f *fleet) exitedOnTheirOwn() []string {
	var out []string
	for _, w := range f.workers {
		if !w.current().Running() {
			out = append(out, fmt.Sprintf("worker %s is not running at the end of the run; it exited on its own or was never restarted", w.label))
		}
	}
	return out
}

// statusesAndTerminal reads the scope's job counts by status and how many of the
// jobs are not yet in a terminal state.
func statusesAndTerminal(ctx context.Context, q readdb.Querier, scope string) (map[string]int, int, error) {
	statuses, err := readdb.StatusCounts(ctx, q, scope)
	if err != nil {
		return nil, 0, err
	}
	nonTerminal := 0
	for status, n := range statuses {
		switch status {
		case "SUCCEEDED", "DEAD_LETTERED", "CANCELED":
		default:
			nonTerminal += n
		}
	}
	return statuses, nonTerminal, nil
}

// waitTerminal polls until every job of the scope is terminal or the deadline
// passes (on this process's monotonic clock; it bounds a wait and times nothing).
// It reports which ended it.
func waitTerminal(ctx context.Context, clk Clock, q readdb.Querier, scope string, deadline time.Time) (endedBy string, err error) {
	for {
		_, nonTerminal, err := statusesAndTerminal(ctx, q, scope)
		if err != nil {
			return "", err
		}
		if nonTerminal == 0 {
			return "all jobs terminal", nil
		}
		if !clk.Now().Before(deadline) {
			return "deadline", nil
		}
		if err := clk.SleepUntil(ctx, clk.Now().Add(500*time.Millisecond)); err != nil {
			return "", err
		}
	}
}

// clockDivergence is how far PostgreSQL's clock and this process's monotonic
// clock disagree about the time that passed between two readings of each. The
// two clocks have unrelated origins; only the elapsed time on each is compared.
func clockDivergence(pgStart, pgNow, monoStart, monoNow time.Time) time.Duration {
	d := pgNow.Sub(pgStart) - monoNow.Sub(monoStart)
	if d < 0 {
		d = -d
	}
	return d
}

// divergedTooFar reports whether a divergence is beyond what the run tolerates.
func divergedTooFar(d time.Duration) bool { return d > maxClockDivergence }

// watchInterval is how often the watchdog looks at the workers, and clockInterval
// how often it compares the clocks.
const (
	workerWatchInterval = 500 * time.Millisecond
	clockWatchInterval  = 5 * time.Second
)

// watch derives a context that is canceled, with the reason as its cause, as soon
// as the run can no longer be trusted: a worker the injector did not kill has
// exited, or PostgreSQL's clock and this machine's have drifted apart (a Docker
// VM whose clock was stepped, a host that slept). Either would make every figure
// the run goes on to produce wrong, and the second would go unnoticed until the
// end of a twenty minute run. Stopping at once says why, while the cause is
// still on the screen.
func (f *fleet) watch(parent context.Context) (context.Context, context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	go func() {
		pgStart, err := readdb.ClockNow(ctx, f.st.DB)
		monoStart := time.Now()
		if err != nil {
			return
		}
		workers := time.NewTicker(workerWatchInterval)
		defer workers.Stop()
		clocks := time.NewTicker(clockWatchInterval)
		defer clocks.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-workers.C:
				for _, w := range f.workers {
					if !w.current().Running() && !w.expectedDown() {
						cancel(fmt.Errorf("worker %s exited on its own; see %s. Its session may have been fenced, "+
							"which is what a stepped clock does", w.label, w.current().Log))
						return
					}
				}
			case <-clocks.C:
				pgNow, err := readdb.ClockNow(ctx, f.st.DB)
				if err != nil {
					continue
				}
				if d := clockDivergence(pgStart, pgNow, monoStart, time.Now()); divergedTooFar(d) {
					cancel(fmt.Errorf("PostgreSQL's clock and this machine's disagree by %s over %s: the host probably slept, "+
						"or the Docker VM's clock was stepped; every lease and heartbeat is judged on that clock, so the run "+
						"cannot be trusted", d.Round(time.Millisecond), time.Since(monoStart).Round(time.Second)))
					return
				}
			}
		}
	}()
	return ctx, cancel
}

// failureCause turns a canceled run context into the error that explains it. A
// cancellation that came from the caller (an interrupt) is not a cause of ours
// and is left alone.
func failureCause(ctx context.Context) error {
	cause := context.Cause(ctx)
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return nil
	}
	return cause
}
