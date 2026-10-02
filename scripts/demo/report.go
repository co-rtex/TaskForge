package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"
)

// expectation is one claim the demonstration makes about what the system did.
type expectation struct {
	name   string
	ok     bool
	detail string
}

// report collects every expectation a run evaluates and prints them at the end.
type report struct {
	mu    sync.Mutex
	items []expectation
}

func (r *report) add(name string, ok bool, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, expectation{name: name, ok: ok, detail: detail})
}

func (r *report) empty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items) == 0
}

// print writes the PASS/FAIL table and returns how many expectations failed.
func (r *report) print(out io.Writer, mode string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	width := 0
	for _, item := range r.items {
		width = max(width, len(item.name))
	}
	failed := 0
	target := "make demo"
	if mode == modeFailure {
		target = "make demo-failure"
	}
	fmt.Fprintf(out, "\n=== %s: expectations ===\n", target)
	for _, item := range r.items {
		verdict := "PASS"
		if !item.ok {
			verdict, failed = "FAIL", failed+1
		}
		fmt.Fprintf(out, "  %s  %-*s  %s\n", verdict, width, item.name, item.detail)
	}
	switch {
	case len(r.items) == 0:
		fmt.Fprintf(out, "=== RESULT: FAIL (nothing was checked) ===\n\n")
	case failed > 0:
		fmt.Fprintf(out, "=== RESULT: FAIL (%d of %d expectations failed) ===\n\n", failed, len(r.items))
	default:
		fmt.Fprintf(out, "=== RESULT: PASS (%d of %d expectations met) ===\n\n", len(r.items), len(r.items))
	}
	return failed
}

// expect records one expectation whose outcome the caller has already decided.
func (d *demo) expect(name string, ok bool, format string, args ...any) {
	d.rep.add(name, ok, fmt.Sprintf(format, args...))
}

// expectEqual records that got is what was expected. Each expected value is
// written at its call site, as a literal or a repository constant, so what a
// demonstration claims can be read there and changed there.
func expectEqual[T comparable](d *demo, name string, want, got T) {
	if want == got {
		d.rep.add(name, true, fmt.Sprintf("%v", got))
		return
	}
	d.rep.add(name, false, fmt.Sprintf("expected %v, got %v", want, got))
}

// expectJSONEqual records that two JSON documents are the same value, whatever
// their key order or spacing.
func expectJSONEqual(d *demo, name string, want, got []byte) {
	var w, g any
	if err := json.Unmarshal(want, &w); err != nil {
		d.rep.add(name, false, fmt.Sprintf("the expected document is not JSON: %v", err))
		return
	}
	if err := json.Unmarshal(got, &g); err != nil {
		d.rep.add(name, false, fmt.Sprintf("the document read back is not JSON: %v", err))
		return
	}
	if reflect.DeepEqual(w, g) {
		d.rep.add(name, true, strings.TrimSpace(string(got)))
		return
	}
	d.rep.add(name, false, fmt.Sprintf("expected %s, got %s", strings.TrimSpace(string(want)), strings.TrimSpace(string(got))))
}

// pollInterval is how often a wait looks again. The CLI answers in tens of
// milliseconds, so this is the granularity of every timeline printed.
const pollInterval = 200 * time.Millisecond

// waitFor polls cond until it reports done, the timeout passes, or the run is
// interrupted. Every wait in this program goes through here, so none is
// unbounded. cond returns whether it is satisfied and a short description of
// what it last observed, which a timeout reports.
//
// An error from cond is not fatal: the API may legitimately refuse a call while
// the system is mid-transition. It is remembered and the wait continues, and a
// timeout reports the last thing seen.
func (d *demo) waitFor(ctx context.Context, what string, timeout time.Duration, cond func(context.Context) (bool, string, error)) (observed string, ok bool) {
	deadline := time.Now().Add(timeout)
	for {
		done, seen, err := cond(ctx)
		if err != nil {
			seen = "error: " + err.Error()
		}
		observed = seen
		if err == nil && done {
			return observed, true
		}
		if ctx.Err() != nil {
			return "interrupted; last observed: " + observed, false
		}
		if time.Now().After(deadline) {
			return fmt.Sprintf("timed out after %s waiting for %s; last observed: %s", timeout, what, observed), false
		}
		select {
		case <-ctx.Done():
		case <-time.After(pollInterval):
		}
	}
}

// describeAttempts renders an attempt timeline on one line.
func describeAttempts(attempts []attemptView) string {
	parts := make([]string, 0, len(attempts))
	for _, a := range attempts {
		parts = append(parts, fmt.Sprintf("#%d %s on %s", a.AttemptNumber, a.Status, shortWorker(a.WorkerName)))
	}
	if len(parts) == 0 {
		return "no attempts"
	}
	return strings.Join(parts, ", ")
}

// shortWorker trims the run-unique part of a worker name for narration.
func shortWorker(name string) string {
	if i := strings.LastIndex(name, "-"); i >= 0 {
		return "worker-" + name[i+1:]
	}
	return name
}
