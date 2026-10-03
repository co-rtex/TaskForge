package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
)

// These tests need no infrastructure, so they run under `make test-unit`. What
// decides whether the stack can be trusted (hermetic environments, accepted
// timings, process supervision) is tested where it lives, in
// scripts/internal/stack. What is left here is the demonstration's own table,
// its bounded waits, and its command line.

func testDemo() *demo {
	return &demo{Stack: &stack.Stack{Out: &bytes.Buffer{}, Started: time.Now()}, rep: &report{}}
}

// --- reporting ------------------------------------------------------------------

func TestReport_PrintsPassAndFailAndCountsFailures(t *testing.T) {
	d := testDemo()
	expectEqual(d, "matches", "SUCCEEDED", "SUCCEEDED")
	expectEqual(d, "differs", "DEAD_LETTERED", "SUCCEEDED")
	expectEqual(d, "numbers", 1, 3)

	var out bytes.Buffer
	failed := d.rep.print(&out, modeSuccess)

	require.Equal(t, 2, failed)
	text := out.String()
	require.Contains(t, text, "PASS  matches")
	require.Contains(t, text, "FAIL  differs")
	require.Contains(t, text, "expected DEAD_LETTERED, got SUCCEEDED")
	require.Contains(t, text, "RESULT: FAIL (2 of 3 expectations failed)")
	require.Contains(t, text, "make demo:")
}

func TestReport_ARunThatCheckedNothingIsNotAPass(t *testing.T) {
	r := &report{}
	var out bytes.Buffer

	require.Equal(t, 0, r.print(&out, modeFailure))
	require.True(t, r.empty(), "main treats an empty report as a failure")
	require.Contains(t, out.String(), "RESULT: FAIL (nothing was checked)")
	require.Contains(t, out.String(), "make demo-failure:")
}

func TestReport_AllPassingIsAPass(t *testing.T) {
	d := testDemo()
	expectEqual(d, "a", 1, 1)
	d.expect("b", true, "fine")

	var out bytes.Buffer
	require.Equal(t, 0, d.rep.print(&out, modeSuccess))
	require.Contains(t, out.String(), "RESULT: PASS (2 of 2 expectations met)")
}

func TestExpectJSONEqual_IgnoresKeyOrderAndSpacingButNotValues(t *testing.T) {
	d := testDemo()
	expectJSONEqual(d, "reordered", []byte(`{"a":1,"b":"x"}`), []byte(`{ "b": "x", "a": 1 }`))
	expectJSONEqual(d, "different value", []byte(`{"a":1}`), []byte(`{"a":2}`))
	expectJSONEqual(d, "not json", []byte(`{"a":1}`), []byte(`nope`))

	require.True(t, d.rep.items[0].ok)
	require.False(t, d.rep.items[1].ok)
	require.False(t, d.rep.items[2].ok)
}

func TestWaitFor_IsBoundedAndReportsWhatItLastSaw(t *testing.T) {
	d := testDemo()

	calls := 0
	observed, ok := d.waitFor(t.Context(), "a thing that never happens", 400*time.Millisecond,
		func(context.Context) (bool, string, error) {
			calls++
			return false, fmt.Sprintf("call %d", calls), nil
		})

	require.False(t, ok)
	require.Contains(t, observed, "timed out after 400ms waiting for a thing that never happens")
	require.Contains(t, observed, "last observed: call")
	require.Greater(t, calls, 1)
}

func TestWaitFor_ReturnsAsSoonAsTheConditionHolds(t *testing.T) {
	d := testDemo()
	calls := 0
	observed, ok := d.waitFor(t.Context(), "three calls", 30*time.Second,
		func(context.Context) (bool, string, error) {
			calls++
			return calls == 3, "seen", nil
		})
	require.True(t, ok)
	require.Equal(t, "seen", observed)
	require.Equal(t, 3, calls)
}

func TestWaitFor_AnErrorMidTransitionDoesNotEndTheWait(t *testing.T) {
	d := testDemo()
	attempts := 0
	observed, ok := d.waitFor(t.Context(), "recovery", 30*time.Second,
		func(context.Context) (bool, string, error) {
			attempts++
			if attempts < 3 {
				return false, "", fmt.Errorf("busy")
			}
			return true, "recovered", nil
		})
	require.True(t, ok, "an error mid-transition must not end the wait")
	require.Equal(t, "recovered", observed)
}

func TestWaitFor_ATimeoutReportsTheLastErrorItSaw(t *testing.T) {
	d := testDemo()
	observed, ok := d.waitFor(t.Context(), "an answer", 300*time.Millisecond,
		func(context.Context) (bool, string, error) { return false, "", fmt.Errorf("connection refused") })
	require.False(t, ok)
	require.Contains(t, observed, "error: connection refused")
}

func TestWaitFor_StopsWhenInterrupted(t *testing.T) {
	d := testDemo()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	started := time.Now()
	observed, ok := d.waitFor(ctx, "never", 30*time.Second,
		func(context.Context) (bool, string, error) { return false, "x", nil })

	require.False(t, ok)
	require.Contains(t, observed, "interrupted")
	require.Less(t, time.Since(started), 5*time.Second)
}

// --- the binary refuses what it should ------------------------------------------

func TestRun_RejectsAMissingOrUnknownMode(t *testing.T) {
	for _, args := range [][]string{nil, {"other"}, {"success", "extra"}, {""}} {
		var stdout, stderr bytes.Buffer
		require.Equal(t, exitUsage, run(args, &stdout, &stderr), "%v", args)
		require.Contains(t, stderr.String(), "usage:")
		require.Empty(t, stdout.String())
	}
}

func TestNewDemo_NamesTheMissingBinaryAndTheCommandThatBuildsIt(t *testing.T) {
	// A working directory with no bin/ at all.
	t.Chdir(t.TempDir())
	t.Setenv("TASKFORGE_DATABASE_URL", "postgres://database.invalid:5442/taskforge")

	_, err := newDemo(&bytes.Buffer{}, modeSuccess)

	require.ErrorContains(t, err, "make build")
}
