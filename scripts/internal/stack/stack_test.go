package stack

import (
	"bytes"
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
// cover the parts of the stack that decide whether it can be trusted: that the
// processes it starts are pinned to the infrastructure it was pointed at, that
// the timings it chooses are ones the services accept, and that it never leaves
// a process behind.

func testInfra() Infra {
	return Infra{
		DatabaseURL:    "postgres://database.invalid:5442/taskforge",
		BrokerEndpoint: "http://broker.invalid:9324", BrokerRegion: "eu-test-1",
		BrokerAccessKeyID: "broker-key", BrokerSecretAccessKey: "broker-secret",
		ResultsEndpoint: "http://results.invalid:4566", ResultsBucket: "bucket-test",
		ResultsRegion: "eu-test-2", ResultsAccessKeyID: "results-key", ResultsSecretAccessKey: "results-secret",
	}
}

func testStack(profile Timings) *Stack {
	return &Stack{
		Infra: testInfra(), Timing: profile, Prefix: "demo",
		RunID: "123", Scope: "demo-123", QueueName: "taskforge-demo-123",
		APIAddr: "127.0.0.1:1001", OutboxAddr: "127.0.0.1:1002",
		SchedulerAddr: "127.0.0.1:1003", ReconcilerAddr: "127.0.0.1:1004",
		APIURL: "http://127.0.0.1:1001", WorkerKey: "test-worker-key",
	}
}

// binaryEnvs builds, for every binary a stack starts, the exact environment it
// would be started with.
func binaryEnvs(t *testing.T, d *Stack) map[string]map[string]string {
	t.Helper()
	envs := map[string]map[string]string{}
	for _, binary := range []string{"taskforge-api", "taskforge-outbox", "taskforge-scheduler", "taskforge-reconciler"} {
		env, err := d.Env(binary, nil)
		require.NoError(t, err, binary)
		envs[binary] = env
	}
	worker, err := d.Env("taskforge-worker", d.WorkerEnv("demo-123-a", "127.0.0.1:1005", 1))
	require.NoError(t, err)
	envs["taskforge-worker"] = worker
	return envs
}

func TestEnv_PinsEveryEndpointEachBinaryDialsToTheConfiguredInfrastructure(t *testing.T) {
	d := testStack(FailureTimings())
	want := map[string]string{
		"TASKFORGE_DATABASE_URL":     d.Infra.DatabaseURL,
		"TASKFORGE_BROKER_ENDPOINT":  d.Infra.BrokerEndpoint,
		"TASKFORGE_RESULTS_ENDPOINT": d.Infra.ResultsEndpoint,
		"TASKFORGE_WORKER_API_URL":   d.APIURL,
	}

	for binary, env := range binaryEnvs(t, d) {
		endpoints, known := dialed[binary]
		require.Truef(t, known, "%s must be in the dialed table", binary)
		require.NotEmpty(t, endpoints)
		for _, name := range endpoints {
			require.Equalf(t, want[name], env[name], "%s must reach %s where the stack was pointed, not at a default", binary, name)
		}
	}

	// And the run's own broker queue, never the shared default one.
	for binary, env := range binaryEnvs(t, d) {
		require.Equalf(t, "taskforge-demo-123", env["TASKFORGE_BROKER_QUEUE_NAME"], binary)
	}
}

func TestEnv_RefusesToStartABinaryWithAnEndpointUnset(t *testing.T) {
	for name, mutate := range map[string]func(*Infra){
		"database":     func(i *Infra) { i.DatabaseURL = "" },
		"broker":       func(i *Infra) { i.BrokerEndpoint = "" },
		"object store": func(i *Infra) { i.ResultsEndpoint = "" },
	} {
		t.Run(name, func(t *testing.T) {
			d := testStack(FailureTimings())
			mutate(&d.Infra)
			failures := 0
			for _, binary := range []string{"taskforge-api", "taskforge-outbox", "taskforge-scheduler", "taskforge-reconciler", "taskforge-worker"} {
				if _, err := d.Env(binary, d.WorkerEnv("w", "127.0.0.1:1", 1)); err != nil {
					require.Contains(t, err.Error(), "loopback default")
					failures++
				}
			}
			require.Positive(t, failures, "a binary that dials %s must not be startable without it", name)
		})
	}

	_, err := testStack(FailureTimings()).Env("taskforge-unknown", nil)
	require.ErrorContains(t, err, "not in the dialed table")
}

// loopbackDefaults returns every environment variable internal/config gives a
// loopback default, found by parsing config.go rather than from a list kept
// here, so a variable added to the configuration later is noticed.
func loopbackDefaults(t *testing.T) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "..", "..", "internal", "config", "config.go"), nil, 0)
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
// is correct on a laptop and wrong the moment the stack is pointed at services
// elsewhere, which is exactly what CI does.
func TestEnv_PinsEveryVariableThatDefaultsToLoopback(t *testing.T) {
	defaults := loopbackDefaults(t)
	for _, name := range []string{
		"TASKFORGE_DATABASE_URL", "TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_RESULTS_ENDPOINT", "TASKFORGE_WORKER_API_URL",
	} {
		require.Contains(t, defaults, name, "the scan must find the variables it exists to find")
	}

	pinned := map[string]bool{}
	for _, env := range binaryEnvs(t, testStack(FailureTimings())) {
		for name := range env {
			pinned[name] = true
		}
	}
	// Variables with a loopback default that no process a stack starts reads.
	notSpawned := map[string]string{}
	for name := range defaults {
		if pinned[name] {
			continue
		}
		reason, exempt := notSpawned[name]
		require.Truef(t, exempt, "%s defaults to a loopback address and no process a stack starts is given it", name)
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
	for name, profile := range map[string]Timings{
		"success": SuccessTimings(),
		"failure": FailureTimings(),
		"shipped": ShippedTimings(),
		"tuned":   TunedTimings(),
	} {
		t.Run(name, func(t *testing.T) {
			clearTaskforgeEnv(t)
			d := testStack(profile)
			for key, value := range d.BaseEnv() {
				t.Setenv(key, value)
			}
			for key, value := range d.WorkerEnv("demo-123-a", "127.0.0.1:1005", 1) {
				t.Setenv(key, value)
			}

			shared, err := config.Load()
			require.NoError(t, err)
			worker, err := config.LoadWorker()
			require.NoError(t, err)
			require.NoError(t, config.ValidateWorkerTimings(shared, worker))

			require.Equal(t, profile.Lease, shared.LeaseDuration)
			require.Equal(t, profile.Heartbeat, shared.HeartbeatInterval)
			require.Equal(t, profile.Stale, shared.SessionStaleAfter)
			require.Equal(t, profile.Renew, shared.LeaseRenewInterval)
			require.Equal(t, profile.WorkerRequest, worker.RequestTimeout)
			require.Equal(t, profile.WorkerPollWait, worker.PollWait)
			require.Equal(t, profile.OutboxPoll, shared.OutboxPollInterval)
			require.Equal(t, profile.OutboxClaim, shared.OutboxClaimTimeout)
			require.Equal(t, profile.PollInterval, shared.ReconcilerPollInterval)
			require.Equal(t, profile.PollInterval, shared.SchedulerPollInterval)
			require.Equal(t, profile.RenotifyAfter, shared.SchedulerRenotifyAfter)
			require.Equal(t, profile.RetryBase, shared.JobRetryBase)
			require.Equal(t, profile.RetryMax, shared.JobRetryMax)
			require.Equal(t, profile.RetryMultiplier, shared.JobRetryMultiplier)
			require.Equal(t, profile.RetryJitter, shared.JobRetryJitter)
			require.Equal(t, "taskforge-demo-123", shared.BrokerQueueName)
		})
	}
}

// The failure profile is the one the demonstration's story depends on: a lease
// and a staleness window short enough to see lapse. Pinned so a "harmless"
// loosening, which would still validate, cannot quietly make the demonstration
// take minutes.
func TestTimings_TheFailureProfileIsShortEnoughToWatch(t *testing.T) {
	f := FailureTimings()
	require.LessOrEqual(t, f.Lease, 5*time.Second)
	require.LessOrEqual(t, f.Heartbeat, time.Second)
	require.LessOrEqual(t, f.Stale, 3*time.Second)
	require.LessOrEqual(t, f.Renew, 1500*time.Millisecond)
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

func supervisionStack(t *testing.T, scripts map[string]string) *Stack {
	t.Helper()
	dir := t.TempDir()
	for name, body := range scripts {
		scriptBinary(t, dir, name, body)
	}
	return &Stack{BinDir: dir, LogDir: dir, WorkDir: dir, Out: &bytes.Buffer{}, Started: time.Now()}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func TestProc_RunsInItsOwnGroupAndStopTakesItsDescendantsWithIt(t *testing.T) {
	// The script starts a grandchild and writes its pid, so the test can prove the
	// whole group is stopped, not only the process the demo launched.
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	d := supervisionStack(t, map[string]string{
		"taskforge-worker": fmt.Sprintf("sleep 300 &\necho $! > %s\nwait", pidFile),
	})

	p, err := d.Start("worker-x", "taskforge-worker", "127.0.0.1:1", KindWorker, shellEnv)
	require.NoError(t, err)
	require.True(t, p.Running())

	pgid, err := syscall.Getpgid(p.PID)
	require.NoError(t, err)
	require.Equal(t, p.PID, pgid, "a child leads its own process group, so a signal to it cannot reach this program")
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

	p.Stop()

	require.False(t, p.Running(), "stop waits for the process")
	require.Eventually(t, func() bool { return !alive(grandchild) }, 5*time.Second, 20*time.Millisecond,
		"the group is killed, so nothing the child started outlives it")
}

func TestProc_StopEndsAFrozenProcessToo(t *testing.T) {
	// A frozen process cannot act on SIGTERM, so stop continues it first. If it did
	// not, a run interrupted while a worker is frozen would leave that worker behind.
	d := supervisionStack(t, map[string]string{"taskforge-worker": "exec sleep 300"})
	p, err := d.Start("worker-x", "taskforge-worker", "127.0.0.1:1", KindWorker, shellEnv)
	require.NoError(t, err)

	require.NoError(t, p.Signal(syscall.SIGSTOP))
	started := time.Now()
	p.Stop()

	require.False(t, p.Running())
	require.Less(t, time.Since(started), stopGrace, "SIGTERM must have worked, not the SIGKILL fallback")
}

func TestProc_StopFallsBackToKillForAProcessThatIgnoresTerm(t *testing.T) {
	previous := stopGrace
	stopGrace = 500 * time.Millisecond
	defer func() { stopGrace = previous }()

	// The script says when its trap is installed. Signalling before that would
	// kill it with the TERM this test is trying to have it ignore.
	ready := filepath.Join(t.TempDir(), "ready")
	d := supervisionStack(t, map[string]string{
		"taskforge-worker": fmt.Sprintf("trap '' TERM\n: > %s\nwhile true; do sleep 1; done", ready),
	})
	p, err := d.Start("worker-x", "taskforge-worker", "127.0.0.1:1", KindWorker, shellEnv)
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 5*time.Second, 10*time.Millisecond)

	p.Stop()

	require.False(t, p.Running())
	require.True(t, p.DiedFromSignal(syscall.SIGKILL), "waitErr=%v", p.waitErr)
}

func TestProc_StartGivesTheChildOnlyTheEnvironmentItIsHanded(t *testing.T) {
	t.Setenv("TASKFORGE_LEAK_CHECK", "inherited")
	out := filepath.Join(t.TempDir(), "env.txt")
	// Written whole, then renamed into place, so the test never reads half of it.
	d := supervisionStack(t, map[string]string{
		"taskforge-worker": fmt.Sprintf("env > %s.tmp && mv %s.tmp %s\nexec sleep 300", out, out, out),
	})

	p, err := d.Start("worker-x", "taskforge-worker", "127.0.0.1:1", KindWorker,
		map[string]string{"PATH": "/usr/bin:/bin", "TASKFORGE_PINNED": "yes"})
	require.NoError(t, err)
	defer p.Stop()

	require.Eventually(t, func() bool { _, err := os.Stat(out); return err == nil }, 5*time.Second, 20*time.Millisecond)
	raw, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Contains(t, string(raw), "TASKFORGE_PINNED=yes")
	require.NotContains(t, string(raw), "TASKFORGE_LEAK_CHECK", "the developer's environment must not reach a child")
}

func TestProc_DiedFromSignalDistinguishesAKillFromAnExit(t *testing.T) {
	d := supervisionStack(t, map[string]string{"taskforge-worker": "exit 3", "taskforge-api": "exec sleep 300"})

	exits, err := d.Start("exits", "taskforge-worker", "127.0.0.1:1", KindWorker, shellEnv)
	require.NoError(t, err)
	require.True(t, exits.WaitExit(5*time.Second))
	require.False(t, exits.DiedFromSignal(syscall.SIGKILL), "an exit is not a kill")

	killed, err := d.Start("killed", "taskforge-api", "127.0.0.1:2", KindService, shellEnv)
	require.NoError(t, err)
	require.NoError(t, killed.Signal(syscall.SIGKILL))
	require.True(t, killed.WaitExit(5*time.Second))
	require.True(t, killed.DiedFromSignal(syscall.SIGKILL))
	require.False(t, killed.DiedFromSignal(syscall.SIGTERM))
}

func TestProc_WaitReadyReportsAProcessThatExitedInsteadOfWaitingOut(t *testing.T) {
	d := supervisionStack(t, map[string]string{"taskforge-api": "exit 1"})
	p, err := d.Start("api", "taskforge-api", "127.0.0.1:1", KindService, shellEnv)
	require.NoError(t, err)

	started := time.Now()
	err = p.WaitReady(t.Context(), 30*time.Second)

	require.ErrorContains(t, err, "exited before it was ready")
	require.Less(t, time.Since(started), 10*time.Second)
}

func TestStack_StopProcsOnlyTouchesTheSelectedKind(t *testing.T) {
	d := supervisionStack(t, map[string]string{"taskforge-worker": "exec sleep 300", "taskforge-api": "exec sleep 300"})
	worker, err := d.Start("worker", "taskforge-worker", "127.0.0.1:1", KindWorker, shellEnv)
	require.NoError(t, err)
	service, err := d.Start("api", "taskforge-api", "127.0.0.1:2", KindService, shellEnv)
	require.NoError(t, err)
	defer service.Stop()

	d.StopProcs(func(p *Proc) bool { return p.Kind == KindWorker })

	require.False(t, worker.Running())
	require.True(t, service.Running(), "services outlive the workers so the run's keys can be revoked first")
}

// TestTimings_ShippedIsExactlyWhatTheServicesDefaultTo is what makes the
// benchmark's headline run mean "shipped defaults". The profile spells every
// value out, so that the record can list each one; this test fails the moment
// internal/config's default for any of them changes without the profile, which
// would otherwise leave a record claiming defaults that are no longer the
// defaults.
func TestTimings_ShippedIsExactlyWhatTheServicesDefaultTo(t *testing.T) {
	clearTaskforgeEnv(t)
	// The one setting with no default, and not a timing: the worker refuses to
	// load without a credential, whatever else is left at its default.
	t.Setenv("TASKFORGE_WORKER_API_KEY", "test-worker-key")
	shared, err := config.Load()
	require.NoError(t, err)
	worker, err := config.LoadWorker()
	require.NoError(t, err)

	shipped := ShippedTimings()
	require.Equal(t, shared.LeaseDuration, shipped.Lease)
	require.Equal(t, shared.HeartbeatInterval, shipped.Heartbeat)
	require.Equal(t, shared.SessionStaleAfter, shipped.Stale)
	require.Equal(t, shared.LeaseRenewInterval, shipped.Renew)
	require.Equal(t, worker.RequestTimeout, shipped.WorkerRequest)
	require.Equal(t, worker.PollWait, shipped.WorkerPollWait)
	require.Equal(t, worker.ShutdownTimeout, shipped.WorkerShutdown)
	require.Equal(t, shared.APIRequestTimeout, shipped.APIRequest)
	require.Equal(t, shared.OutboxPollInterval, shipped.OutboxPoll)
	require.Equal(t, shared.OutboxClaimTimeout, shipped.OutboxClaim)
	require.Equal(t, shared.SchedulerRenotifyAfter, shipped.RenotifyAfter)
	require.Equal(t, shared.JobRetryBase, shipped.RetryBase)
	require.Equal(t, shared.JobRetryMax, shipped.RetryMax)
	require.Equal(t, shared.JobRetryMultiplier, shipped.RetryMultiplier)
	require.Equal(t, shared.JobRetryJitter, shipped.RetryJitter)

	// One interval serves two services, so both defaults must be that one value.
	require.Equal(t, shared.ReconcilerPollInterval, shipped.PollInterval)
	require.Equal(t, shared.SchedulerPollInterval, shipped.PollInterval)
}

// TestTimings_TunedOnlyChangesLeaseLivenessAndScanIntervals pins what "tuned"
// means, so a labelled second run cannot quietly become something else: nothing
// outside the lease and liveness windows and the scan intervals differs from the
// shipped profile.
func TestTimings_TunedOnlyChangesLeaseLivenessAndScanIntervals(t *testing.T) {
	shipped, tuned := ShippedTimings(), TunedTimings()

	require.Less(t, tuned.Lease, shipped.Lease)
	require.Less(t, tuned.Stale, shipped.Stale)
	require.Less(t, tuned.PollInterval, shipped.PollInterval)

	// Everything else is the shipped value.
	same := tuned
	same.Lease, same.Heartbeat, same.Stale, same.Renew, same.WorkerRequest =
		shipped.Lease, shipped.Heartbeat, shipped.Stale, shipped.Renew, shipped.WorkerRequest
	same.PollInterval, same.OutboxPoll, same.OutboxClaim, same.RenotifyAfter =
		shipped.PollInterval, shipped.OutboxPoll, shipped.OutboxClaim, shipped.RenotifyAfter
	require.Equal(t, shipped, same)
}

// TestTimings_EnvIsEveryTimingVariableAServiceOrWorkerIsHanded is what the
// benchmark record lists as "every TASKFORGE_* timing value in effect". It must
// be the very values the processes are started with, not a second rendering.
func TestTimings_EnvIsEveryTimingVariableAServiceOrWorkerIsHanded(t *testing.T) {
	d := testStack(TunedTimings())
	handed := d.BaseEnv()
	for name, value := range d.WorkerEnv("demo-123-a", "127.0.0.1:1005", 1) {
		handed[name] = value
	}

	listed := d.Timing.Env()
	require.NotEmpty(t, listed)
	for name, value := range listed {
		require.Equalf(t, handed[name], value, "%s as listed must equal what a process is handed", name)
	}
	for _, name := range []string{
		"TASKFORGE_LEASE_DURATION", "TASKFORGE_HEARTBEAT_INTERVAL", "TASKFORGE_SESSION_STALE_AFTER",
		"TASKFORGE_LEASE_RENEW_INTERVAL", "TASKFORGE_RECONCILER_POLL_INTERVAL",
		"TASKFORGE_SCHEDULER_POLL_INTERVAL", "TASKFORGE_WORKER_REQUEST_TIMEOUT",
	} {
		require.Contains(t, listed, name)
	}
	// Addresses, credentials and the queue name are not timings.
	require.NotContains(t, listed, "TASKFORGE_DATABASE_URL")
	require.NotContains(t, listed, "TASKFORGE_WORKER_API_KEY")
}

func TestStack_WorkerNameIsRunUniqueAndStableAcrossARestart(t *testing.T) {
	d := testStack(ShippedTimings())
	require.Equal(t, "demo-123-w07", d.WorkerName("w07"))
	require.Equal(t, d.WorkerName("w07"), d.WorkerName("w07"), "a restart must be able to ask for the same name again")
	require.NotEqual(t, d.WorkerName("w07"), d.WorkerName("w08"))
}
