package main

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/config"
)

// These tests need no infrastructure, so they run under `make test-unit`. They
// cover the parts of the demo that decide whether it can be trusted: that the
// processes it starts are pinned to the infrastructure it was pointed at, that
// the timings it chooses are ones the services accept, that it never leaves a
// process behind, and that its table says what happened.

func testInfra() infra {
	return infra{
		databaseURL:    "postgres://database.invalid:5442/taskforge",
		brokerEndpoint: "http://broker.invalid:9324", brokerRegion: "eu-test-1",
		brokerAccessKeyID: "broker-key", brokerSecretAccessKey: "broker-secret",
		resultsEndpoint: "http://results.invalid:4566", resultsBucket: "bucket-test",
		resultsRegion: "eu-test-2", resultsAccessKeyID: "results-key", resultsSecretAccessKey: "results-secret",
	}
}

func testDemo(profile timings) *demo {
	return &demo{
		infra: testInfra(), timing: profile, rep: &report{},
		runID: "123", scope: "demo-123", queueName: "taskforge-demo-123",
		apiAddr: "127.0.0.1:1001", outboxAddr: "127.0.0.1:1002",
		schedulerAddr: "127.0.0.1:1003", reconcilerAddr: "127.0.0.1:1004",
		apiURL: "http://127.0.0.1:1001", workerKey: "test-worker-key",
	}
}

// binaryEnvs builds, for every binary the demo starts, the exact environment it
// would be started with.
func binaryEnvs(t *testing.T, d *demo) map[string]map[string]string {
	t.Helper()
	envs := map[string]map[string]string{}
	for _, binary := range []string{"taskforge-api", "taskforge-outbox", "taskforge-scheduler", "taskforge-reconciler"} {
		env, err := d.env(binary, nil)
		require.NoError(t, err, binary)
		envs[binary] = env
	}
	worker, err := d.env("taskforge-worker", d.workerEnv("demo-123-a", "127.0.0.1:1005", 1))
	require.NoError(t, err)
	envs["taskforge-worker"] = worker
	return envs
}

func TestEnv_PinsEveryEndpointEachBinaryDialsToTheConfiguredInfrastructure(t *testing.T) {
	d := testDemo(failureTimings())
	want := map[string]string{
		"TASKFORGE_DATABASE_URL":     d.infra.databaseURL,
		"TASKFORGE_BROKER_ENDPOINT":  d.infra.brokerEndpoint,
		"TASKFORGE_RESULTS_ENDPOINT": d.infra.resultsEndpoint,
		"TASKFORGE_WORKER_API_URL":   d.apiURL,
	}

	for binary, env := range binaryEnvs(t, d) {
		endpoints, known := dialed[binary]
		require.Truef(t, known, "%s must be in the dialed table", binary)
		require.NotEmpty(t, endpoints)
		for _, name := range endpoints {
			require.Equalf(t, want[name], env[name], "%s must reach %s where the demo was pointed, not at a default", binary, name)
		}
	}

	// And the run's own broker queue, never the shared default one.
	for binary, env := range binaryEnvs(t, d) {
		require.Equalf(t, "taskforge-demo-123", env["TASKFORGE_BROKER_QUEUE_NAME"], binary)
	}
}

func TestEnv_RefusesToStartABinaryWithAnEndpointUnset(t *testing.T) {
	for name, mutate := range map[string]func(*infra){
		"database":     func(i *infra) { i.databaseURL = "" },
		"broker":       func(i *infra) { i.brokerEndpoint = "" },
		"object store": func(i *infra) { i.resultsEndpoint = "" },
	} {
		t.Run(name, func(t *testing.T) {
			d := testDemo(failureTimings())
			mutate(&d.infra)
			failures := 0
			for _, binary := range []string{"taskforge-api", "taskforge-outbox", "taskforge-scheduler", "taskforge-reconciler", "taskforge-worker"} {
				if _, err := d.env(binary, d.workerEnv("w", "127.0.0.1:1", 1)); err != nil {
					require.Contains(t, err.Error(), "loopback default")
					failures++
				}
			}
			require.Positive(t, failures, "a binary that dials %s must not be startable without it", name)
		})
	}

	_, err := testDemo(failureTimings()).env("taskforge-unknown", nil)
	require.ErrorContains(t, err, "not in the dialed table")
}

// loopbackDefaults returns every environment variable internal/config gives a
// loopback default, found by parsing config.go rather than from a list kept
// here, so a variable added to the configuration later is noticed.
func loopbackDefaults(t *testing.T) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "..", "internal", "config", "config.go"), nil, 0)
	require.NoError(t, err)

	literal := func(expr ast.Expr) (string, bool) {
		basic, ok := expr.(*ast.BasicLit)
		if !ok || basic.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(basic.Value)
		return value, err == nil
	}
	found := map[string]string{}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		if fn, ok := call.Fun.(*ast.Ident); !ok || fn.Name != "env" {
			return true
		}
		name, nameOK := literal(call.Args[0])
		value, valueOK := literal(call.Args[1])
		if nameOK && valueOK && (strings.Contains(value, "127.0.0.1") ||
			strings.Contains(value, "localhost") || strings.Contains(value, "[::1]")) {
			found[name] = value
		}
		return true
	})
	return found
}

// TestEnv_PinsEveryVariableThatDefaultsToLoopback is the hermeticity proof. A
// child that inherited nothing takes any variable it is not given from its
// default, and for an address that default is this machine's own loopback. That
// is correct on a laptop and wrong the moment the demo is pointed at services
// elsewhere, which is exactly what CI does.
func TestEnv_PinsEveryVariableThatDefaultsToLoopback(t *testing.T) {
	defaults := loopbackDefaults(t)
	for _, name := range []string{
		"TASKFORGE_DATABASE_URL", "TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_RESULTS_ENDPOINT", "TASKFORGE_WORKER_API_URL",
	} {
		require.Contains(t, defaults, name, "the scan must find the variables it exists to find")
	}

	pinned := map[string]bool{}
	for _, env := range binaryEnvs(t, testDemo(failureTimings())) {
		for name := range env {
			pinned[name] = true
		}
	}
	// Variables with a loopback default that no process the demo starts reads.
	notSpawned := map[string]string{}
	for name := range defaults {
		if pinned[name] {
			continue
		}
		reason, exempt := notSpawned[name]
		require.Truef(t, exempt, "%s defaults to a loopback address and no process the demo starts is given it", name)
		require.NotEmpty(t, reason)
	}
}

// clearTaskforgeEnv blanks every TASKFORGE_ variable the developer's shell
// carries, so a config load below sees only what the test sets. The services
// treat a blank value as absent.
func clearTaskforgeEnv(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		if name, _, _ := strings.Cut(entry, "="); strings.HasPrefix(name, "TASKFORGE_") {
			t.Setenv(name, "")
		}
	}
}

// TestTimings_AreValuesTheServicesAcceptAndActuallyLoad runs each profile
// through the services' own loaders. Two things are checked: that validation
// passes, which is the relationships (stale >= 3x heartbeat, 3x renew <= lease,
// request timeout <= both), and that the loaded value is the chosen one. The
// second matters because a duration the loader cannot parse is silently replaced
// by its default, which would pass validation while running something else.
func TestTimings_AreValuesTheServicesAcceptAndActuallyLoad(t *testing.T) {
	for name, profile := range map[string]timings{
		"success": successTimings(),
		"failure": failureTimings(),
	} {
		t.Run(name, func(t *testing.T) {
			clearTaskforgeEnv(t)
			d := testDemo(profile)
			for key, value := range d.baseEnv() {
				t.Setenv(key, value)
			}
			for key, value := range d.workerEnv("demo-123-a", "127.0.0.1:1005", 1) {
				t.Setenv(key, value)
			}

			shared, err := config.Load()
			require.NoError(t, err)
			worker, err := config.LoadWorker()
			require.NoError(t, err)
			require.NoError(t, config.ValidateWorkerTimings(shared, worker))

			require.Equal(t, profile.lease, shared.LeaseDuration)
			require.Equal(t, profile.heartbeat, shared.HeartbeatInterval)
			require.Equal(t, profile.stale, shared.SessionStaleAfter)
			require.Equal(t, profile.renew, shared.LeaseRenewInterval)
			require.Equal(t, profile.workerRequest, worker.RequestTimeout)
			require.Equal(t, profile.workerPollWait, worker.PollWait)
			require.Equal(t, profile.outboxPoll, shared.OutboxPollInterval)
			require.Equal(t, profile.outboxClaim, shared.OutboxClaimTimeout)
			require.Equal(t, profile.pollInterval, shared.ReconcilerPollInterval)
			require.Equal(t, profile.pollInterval, shared.SchedulerPollInterval)
			require.Equal(t, profile.renotifyAfter, shared.SchedulerRenotifyAfter)
			require.Equal(t, profile.retryBase, shared.JobRetryBase)
			require.Equal(t, profile.retryMax, shared.JobRetryMax)
			require.Equal(t, profile.retryMultiplier, shared.JobRetryMultiplier)
			require.Equal(t, profile.retryJitter, shared.JobRetryJitter)
			require.Equal(t, "taskforge-demo-123", shared.BrokerQueueName)
		})
	}
}

// The failure profile is the one the demonstration's story depends on: a lease
// and a staleness window short enough to see lapse. Pinned so a "harmless"
// loosening, which would still validate, cannot quietly make the demonstration
// take minutes.
func TestTimings_TheFailureProfileIsShortEnoughToWatch(t *testing.T) {
	f := failureTimings()
	require.LessOrEqual(t, f.lease, 5*time.Second)
	require.LessOrEqual(t, f.heartbeat, time.Second)
	require.LessOrEqual(t, f.stale, 3*time.Second)
	require.LessOrEqual(t, f.renew, 1500*time.Millisecond)
}

// --- process supervision -------------------------------------------------------

// scriptBinary writes an executable shell script named like a TaskForge binary,
// so the supervision code is exercised against a real process.
func scriptBinary(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755))
}

// shellEnv is the whole environment a supervised test script gets: only enough
// of a PATH for the shell to find sleep and env.
var shellEnv = map[string]string{"PATH": "/usr/bin:/bin"}

func supervisionDemo(t *testing.T, scripts map[string]string) *demo {
	t.Helper()
	dir := t.TempDir()
	for name, body := range scripts {
		scriptBinary(t, dir, name, body)
	}
	return &demo{binDir: dir, logDir: dir, workDir: dir, rep: &report{}, out: &bytes.Buffer{}, started: time.Now()}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func TestProc_RunsInItsOwnGroupAndStopTakesItsDescendantsWithIt(t *testing.T) {
	// The script starts a grandchild and writes its pid, so the test can prove the
	// whole group is stopped, not only the process the demo launched.
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	d := supervisionDemo(t, map[string]string{
		"taskforge-worker": fmt.Sprintf("sleep 300 &\necho $! > %s\nwait", pidFile),
	})

	p, err := d.start("worker-x", "taskforge-worker", "127.0.0.1:1", kindWorker, shellEnv)
	require.NoError(t, err)
	require.True(t, p.running())

	pgid, err := syscall.Getpgid(p.pid)
	require.NoError(t, err)
	require.Equal(t, p.pid, pgid, "a child leads its own process group, so a signal to it cannot reach this program")
	require.NotEqual(t, syscall.Getpgrp(), pgid)

	var grandchild int
	require.Eventually(t, func() bool {
		raw, err := os.ReadFile(pidFile)
		if err != nil {
			return false
		}
		grandchild, err = strconv.Atoi(strings.TrimSpace(string(raw)))
		return err == nil
	}, 5*time.Second, 20*time.Millisecond)
	require.True(t, alive(grandchild))

	p.stop()

	require.False(t, p.running(), "stop waits for the process")
	require.Eventually(t, func() bool { return !alive(grandchild) }, 5*time.Second, 20*time.Millisecond,
		"the group is killed, so nothing the child started outlives it")
}

func TestProc_StopEndsAFrozenProcessToo(t *testing.T) {
	// A frozen process cannot act on SIGTERM, so stop continues it first. If it did
	// not, a run interrupted while a worker is frozen would leave that worker behind.
	d := supervisionDemo(t, map[string]string{"taskforge-worker": "exec sleep 300"})
	p, err := d.start("worker-x", "taskforge-worker", "127.0.0.1:1", kindWorker, shellEnv)
	require.NoError(t, err)

	require.NoError(t, p.signal(syscall.SIGSTOP))
	started := time.Now()
	p.stop()

	require.False(t, p.running())
	require.Less(t, time.Since(started), stopGrace, "SIGTERM must have worked, not the SIGKILL fallback")
}

func TestProc_StopFallsBackToKillForAProcessThatIgnoresTerm(t *testing.T) {
	previous := stopGrace
	stopGrace = 500 * time.Millisecond
	defer func() { stopGrace = previous }()

	// The script says when its trap is installed. Signalling before that would
	// kill it with the TERM this test is trying to have it ignore.
	ready := filepath.Join(t.TempDir(), "ready")
	d := supervisionDemo(t, map[string]string{
		"taskforge-worker": fmt.Sprintf("trap '' TERM\n: > %s\nwhile true; do sleep 1; done", ready),
	})
	p, err := d.start("worker-x", "taskforge-worker", "127.0.0.1:1", kindWorker, shellEnv)
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 5*time.Second, 10*time.Millisecond)

	p.stop()

	require.False(t, p.running())
	require.True(t, p.diedFromSignal(syscall.SIGKILL), "waitErr=%v", p.waitErr)
}

func TestProc_StartGivesTheChildOnlyTheEnvironmentItIsHanded(t *testing.T) {
	t.Setenv("TASKFORGE_LEAK_CHECK", "inherited")
	out := filepath.Join(t.TempDir(), "env.txt")
	// Written whole, then renamed into place, so the test never reads half of it.
	d := supervisionDemo(t, map[string]string{
		"taskforge-worker": fmt.Sprintf("env > %s.tmp && mv %s.tmp %s\nexec sleep 300", out, out, out),
	})

	p, err := d.start("worker-x", "taskforge-worker", "127.0.0.1:1", kindWorker,
		map[string]string{"PATH": "/usr/bin:/bin", "TASKFORGE_PINNED": "yes"})
	require.NoError(t, err)
	defer p.stop()

	require.Eventually(t, func() bool { _, err := os.Stat(out); return err == nil }, 5*time.Second, 20*time.Millisecond)
	raw, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Contains(t, string(raw), "TASKFORGE_PINNED=yes")
	require.NotContains(t, string(raw), "TASKFORGE_LEAK_CHECK", "the developer's environment must not reach a child")
}

func TestProc_DiedFromSignalDistinguishesAKillFromAnExit(t *testing.T) {
	d := supervisionDemo(t, map[string]string{"taskforge-worker": "exit 3", "taskforge-api": "exec sleep 300"})

	exits, err := d.start("exits", "taskforge-worker", "127.0.0.1:1", kindWorker, shellEnv)
	require.NoError(t, err)
	require.True(t, exits.waitExit(5*time.Second))
	require.False(t, exits.diedFromSignal(syscall.SIGKILL), "an exit is not a kill")

	killed, err := d.start("killed", "taskforge-api", "127.0.0.1:2", kindService, shellEnv)
	require.NoError(t, err)
	require.NoError(t, killed.signal(syscall.SIGKILL))
	require.True(t, killed.waitExit(5*time.Second))
	require.True(t, killed.diedFromSignal(syscall.SIGKILL))
	require.False(t, killed.diedFromSignal(syscall.SIGTERM))
}

func TestProc_WaitReadyReportsAProcessThatExitedInsteadOfWaitingOut(t *testing.T) {
	d := supervisionDemo(t, map[string]string{"taskforge-api": "exit 1"})
	p, err := d.start("api", "taskforge-api", "127.0.0.1:1", kindService, shellEnv)
	require.NoError(t, err)

	started := time.Now()
	err = p.waitReady(t.Context(), 30*time.Second)

	require.ErrorContains(t, err, "exited before it was ready")
	require.Less(t, time.Since(started), 10*time.Second)
}

func TestDemo_StopProcsOnlyTouchesTheSelectedKind(t *testing.T) {
	d := supervisionDemo(t, map[string]string{"taskforge-worker": "exec sleep 300", "taskforge-api": "exec sleep 300"})
	worker, err := d.start("worker", "taskforge-worker", "127.0.0.1:1", kindWorker, shellEnv)
	require.NoError(t, err)
	service, err := d.start("api", "taskforge-api", "127.0.0.1:2", kindService, shellEnv)
	require.NoError(t, err)
	defer service.stop()

	d.stopProcs(func(p *proc) bool { return p.kind == kindWorker })

	require.False(t, worker.running())
	require.True(t, service.running(), "services outlive the workers so the run's keys can be revoked first")
}

// --- reporting ------------------------------------------------------------------

func TestReport_PrintsPassAndFailAndCountsFailures(t *testing.T) {
	d := testDemo(successTimings())
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
	d := testDemo(successTimings())
	expectEqual(d, "a", 1, 1)
	d.expect("b", true, "fine")

	var out bytes.Buffer
	require.Equal(t, 0, d.rep.print(&out, modeSuccess))
	require.Contains(t, out.String(), "RESULT: PASS (2 of 2 expectations met)")
}

func TestExpectJSONEqual_IgnoresKeyOrderAndSpacingButNotValues(t *testing.T) {
	d := testDemo(successTimings())
	expectJSONEqual(d, "reordered", []byte(`{"a":1,"b":"x"}`), []byte(`{ "b": "x", "a": 1 }`))
	expectJSONEqual(d, "different value", []byte(`{"a":1}`), []byte(`{"a":2}`))
	expectJSONEqual(d, "not json", []byte(`{"a":1}`), []byte(`nope`))

	require.True(t, d.rep.items[0].ok)
	require.False(t, d.rep.items[1].ok)
	require.False(t, d.rep.items[2].ok)
}

func TestWaitFor_IsBoundedAndReportsWhatItLastSaw(t *testing.T) {
	d := testDemo(successTimings())
	d.out = &bytes.Buffer{}

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
	d := testDemo(successTimings())
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
	d := testDemo(successTimings())
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
	d := testDemo(successTimings())
	observed, ok := d.waitFor(t.Context(), "an answer", 300*time.Millisecond,
		func(context.Context) (bool, string, error) { return false, "", fmt.Errorf("connection refused") })
	require.False(t, ok)
	require.Contains(t, observed, "error: connection refused")
}

func TestWaitFor_StopsWhenInterrupted(t *testing.T) {
	d := testDemo(successTimings())
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
