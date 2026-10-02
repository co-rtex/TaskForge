//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/workers"
)

// Test-only timings. Deliberately short so a real process crash and its recovery
// fit inside a test, while still satisfying every relationship the configuration
// validates: stale >= 3x heartbeat, 3x renewal <= lease, and a transport timeout
// that cannot outlive either safety window.
const (
	crashHeartbeatInterval = "500ms"
	crashStaleAfter        = "2s"
	crashLeaseDuration     = "6s"
	crashRenewInterval     = "2s"
	crashRequestTimeout    = "1s"
)

// syncBuffer collects a child process's output. The race detector runs this
// suite, and a plain bytes.Buffer written by a pipe goroutine and read by the
// test would be a data race.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// service is one real TaskForge binary running as its own operating-system
// process.
type service struct {
	name string
	cmd  *exec.Cmd
	out  *syncBuffer
	addr string
	// waited guards Wait, which may be called by both the killer and cleanup.
	waited sync.Once
	err    error
}

// freePort asks the kernel for an unused loopback port and releases it. The
// child binds it a moment later; nothing else in this suite binds ephemeral
// ports, so the window is not contended.
func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

// buildBinaries compiles the real commands under test into a temporary
// directory. Nothing here stubs the worker: the process that gets killed below
// is the same binary `make build` produces.
func buildBinaries(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	build := exec.Command("go", "build", "-o", dir, "./cmd/...")
	build.Dir = filepath.Join("..", "..")
	output, err := build.CombinedOutput()
	require.NoErrorf(t, err, "building the real binaries failed:\n%s", output)
	return dir
}

// startService launches one real binary with an explicit environment. The
// developer's own environment is deliberately not inherited, and the working
// directory is empty so no .env can influence the child.
func startService(t *testing.T, binDir, name, addr string, env map[string]string) *service {
	t.Helper()
	svc := &service{name: name, addr: addr, out: &syncBuffer{}}
	svc.cmd = exec.Command(filepath.Join(binDir, name))
	svc.cmd.Dir = t.TempDir()
	svc.cmd.Stdout = svc.out
	svc.cmd.Stderr = svc.out
	for key, value := range env {
		svc.cmd.Env = append(svc.cmd.Env, key+"="+value)
	}
	requirePinnedEndpoints(t, name, svc.cmd.Env)
	require.NoError(t, svc.cmd.Start(), "starting %s", name)
	t.Cleanup(func() { svc.stop() })
	return svc
}

// serviceEndpoints names, for each real binary, the environment variables that
// carry the address of infrastructure it dials. It is read off the binary's own
// cmd/<name>/main.go: which of cfg.DatabaseURL, cfg.BrokerEndpoint,
// cfg.ResultsEndpoint, and the worker's API base URL that binary actually uses.
var serviceEndpoints = map[string][]string{
	"taskforge-api":        {"TASKFORGE_DATABASE_URL", "TASKFORGE_RESULTS_ENDPOINT"},
	"taskforge-outbox":     {"TASKFORGE_DATABASE_URL", "TASKFORGE_BROKER_ENDPOINT"},
	"taskforge-reconciler": {"TASKFORGE_DATABASE_URL"},
	"taskforge-scheduler":  {"TASKFORGE_DATABASE_URL"},
	"taskforge-worker": {
		"TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_RESULTS_ENDPOINT", "TASKFORGE_WORKER_API_URL",
	},
}

// suiteEndpoint is where the rest of the suite reaches each infrastructure
// service. A spawned binary must be pointed at the same place.
var suiteEndpoint = map[string]func() string{
	"TASKFORGE_DATABASE_URL":    dsn,
	"TASKFORGE_BROKER_ENDPOINT": brokerEndpoint,
	// The object store has its own suite-wide setting, exactly as the broker does.
	"TASKFORGE_RESULTS_ENDPOINT": resultsEndpoint,
}

// requirePinnedEndpoints asserts on the environment a child is about to be
// started with -- the exact slice handed to the operating system -- that every
// endpoint the binary dials is set, and set to where this suite reaches that
// service. An endpoint left out would silently take its loopback default.
func requirePinnedEndpoints(t *testing.T, binary string, childEnv []string) {
	t.Helper()
	endpoints, known := serviceEndpoints[binary]
	require.Truef(t, known,
		"%s is not in serviceEndpoints: read cmd/%s/main.go and list the endpoints it dials", binary, binary)

	got := make(map[string]string, len(childEnv))
	for _, entry := range childEnv {
		name, value, _ := strings.Cut(entry, "=")
		got[name] = value
	}
	for _, name := range endpoints {
		value := got[name]
		require.NotEmptyf(t, value, "%s would take %s from its loopback default", binary, name)
		if suite, ok := suiteEndpoint[name]; ok {
			require.Equalf(t, suite(), value, "%s is pointed somewhere other than where the suite reaches it", name)
		}
	}
}

// stop ends a service politely, then waits. Cleanup calls it for every service,
// including one that was already killed.
func (s *service) stop() {
	if s.cmd.Process == nil {
		return
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	s.wait()
}

func (s *service) wait() error {
	s.waited.Do(func() { s.err = s.cmd.Wait() })
	return s.err
}

// waitReady blocks until the child answers its own readiness probe.
func (s *service) waitReady(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	eventually(t, 60*time.Second, s.name+" reports ready", func() bool {
		response, err := client.Get("http://" + s.addr + "/readyz")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	})
}

// succeedStaller forwards everything to the real API but holds POST .../succeed
// requests until the caller gives up.
//
// It exists so the crash below happens while durable state says RUNNING. When
// this was written the only handler in taskforge-worker was demo.echo, which
// returns immediately, and a slow production handler purely to widen a test
// window was surface this project refused to grow. demo.sleep has since been
// added for M7B's demonstrations (ADR-0019), but this test still stalls one
// control-plane call instead: the worker holds an active lease on a RUNNING
// attempt, the real state a crashed worker leaves, and nothing else changes.
type succeedStaller struct {
	server  *httptest.Server
	stalled atomic.Int32
	// done releases handlers that are still holding a request when the test ends.
	// A SIGKILLed client never closes its socket, so the server-side request
	// context is not cancelled and httptest.Server.Close would block on that
	// handler forever.
	done      chan struct{}
	closeOnce sync.Once
}

func newSucceedStaller(t *testing.T, target string) *succeedStaller {
	t.Helper()
	parsed, err := url.Parse(target)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(parsed)
	proxy.ErrorLog = nil

	staller := &succeedStaller{done: make(chan struct{})}
	staller.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/succeed") {
			staller.stalled.Add(1)
			// Hold until the worker's own request timeout disconnects it, or until
			// the test tears the staller down. The second case is not optional: the
			// worker is about to be SIGKILLed, and a killed process leaves its
			// socket dangling rather than closing it, so the request context alone
			// would never fire.
			select {
			case <-r.Context().Done():
			case <-staller.done:
			}
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(staller.release)
	return staller
}

// release unblocks every held request and shuts the staller down.
func (s *succeedStaller) release() {
	s.closeOnce.Do(func() { close(s.done) })
	s.server.Close()
}

func (s *succeedStaller) URL() string { return s.server.URL }

// createIsolatedBrokerQueue gives one test a broker queue of its own.
//
// Most tests in this package share one queue and drain it in reset(). That is
// fine when every delivery is either acknowledged or drained before the test
// ends. It is not fine for a test that leaves messages IN FLIGHT: a worker
// SIGKILLed while long-polling, or one that legitimately declines to
// acknowledge a notification for work another worker already took, leaves a
// message invisible for the queue's visibility timeout. reset() cannot drain
// what it cannot see, so the message reappears during some later test and looks
// like that test's own doing.
//
// Rather than make every later test tolerate that, these tests consume from a
// queue nobody else touches.
func createIsolatedBrokerQueue(t *testing.T, prefix string) string {
	t.Helper()
	name := prefix + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	response, err := http.PostForm(brokerEndpoint()+"/", url.Values{
		"Action": {"CreateQueue"}, "QueueName": {name}, "Version": {"2012-11-05"},
	})
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8*1024))
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, response.StatusCode, "CreateQueue failed: %s", body)

	queueURL := queueURLPattern.FindStringSubmatch(string(body))
	require.Lenf(t, queueURL, 2, "CreateQueue returned no queue url: %s", body)

	t.Cleanup(func() {
		deleted, err := http.PostForm(brokerEndpoint()+"/", url.Values{
			"Action": {"DeleteQueue"}, "QueueUrl": {queueURL[1]}, "Version": {"2012-11-05"},
		})
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(deleted.Body, 8*1024))
			_ = deleted.Body.Close()
		}
	})
	return name
}

var queueURLPattern = regexp.MustCompile(`<QueueUrl>([^<]+)</QueueUrl>`)

// crashSharedEnvironment is every variable the crash test hands to every real
// binary it spawns. The children do not inherit the developer's environment (see
// startService), so anything not named here is whatever the binary's own
// default is -- and for an endpoint, the default is the loopback of the machine
// running the child.
//
// That is why each infrastructure endpoint is pinned explicitly, to the value
// the rest of the suite is itself using. A run whose PostgreSQL, broker, or
// object store is not on the test process's own loopback -- the suite inside a
// bridged container against infrastructure on the host, say -- would otherwise
// have a spawned binary quietly dial 127.0.0.1 for the one it forgot.
// TestWorkerProcessCrash_EnvironmentPinsEveryLoopbackDefault finds the
// variables with such a default from internal/config rather than from a list
// kept here by hand.
func crashSharedEnvironment(brokerQueue, apiAddr string, outboxPort, reconcilerPort int) map[string]string {
	return map[string]string{
		"TASKFORGE_DATABASE_URL":             dsn(),
		"TASKFORGE_BROKER_ENDPOINT":          brokerEndpoint(),
		"TASKFORGE_BROKER_QUEUE_NAME":        brokerQueue,
		"TASKFORGE_BROKER_REGION":            "us-east-1",
		"TASKFORGE_BROKER_ACCESS_KEY_ID":     "local",
		"TASKFORGE_BROKER_SECRET_ACCESS_KEY": "local",
		// The object store: taskforge-api serves results from it and
		// taskforge-worker uploads to it and probes it for readiness, each through
		// its own client built from this setting.
		"TASKFORGE_RESULTS_ENDPOINT":         resultsEndpoint(),
		"TASKFORGE_LEASE_DURATION":           crashLeaseDuration,
		"TASKFORGE_HEARTBEAT_INTERVAL":       crashHeartbeatInterval,
		"TASKFORGE_SESSION_STALE_AFTER":      crashStaleAfter,
		"TASKFORGE_LEASE_RENEW_INTERVAL":     crashRenewInterval,
		"TASKFORGE_API_ADDR":                 apiAddr,
		"TASKFORGE_API_REQUEST_TIMEOUT":      "5s",
		"TASKFORGE_OUTBOX_ADDR":              fmt.Sprintf("127.0.0.1:%d", outboxPort),
		"TASKFORGE_OUTBOX_POLL_INTERVAL":     "200ms",
		"TASKFORGE_OUTBOX_CLAIM_TIMEOUT":     "1s",
		"TASKFORGE_RECONCILER_ADDR":          fmt.Sprintf("127.0.0.1:%d", reconcilerPort),
		"TASKFORGE_RECONCILER_POLL_INTERVAL": "200ms",
		"TASKFORGE_LOG_LEVEL":                "info",
	}
}

// crashWorkerEnvironment is what a worker adds to the shared environment.
func crashWorkerEnvironment(name, addr, apiURL string) map[string]string {
	return map[string]string{
		"TASKFORGE_WORKER_NAME":             name,
		"TASKFORGE_WORKER_ADDR":             addr,
		"TASKFORGE_WORKER_API_URL":          apiURL,
		"TASKFORGE_WORKER_API_KEY":          currentWorkerKey(),
		"TASKFORGE_WORKER_QUEUE":            "default",
		"TASKFORGE_WORKER_GROUP":            "default",
		"TASKFORGE_WORKER_CONCURRENCY":      "1",
		"TASKFORGE_WORKER_CAPABILITIES":     "cpu",
		"TASKFORGE_WORKER_POLL_WAIT":        "1s",
		"TASKFORGE_WORKER_REQUEST_TIMEOUT":  crashRequestTimeout,
		"TASKFORGE_WORKER_SHUTDOWN_TIMEOUT": "2s",
	}
}

// TestWorkerProcessCrash_SigkillRecoversThroughTheRealBinaries is the roadmap's
// literal acceptance story, at the process boundary.
//
// Every participant is a real binary in its own process: taskforge-api,
// taskforge-outbox, two taskforge-worker processes, and taskforge-reconciler.
// Worker A is terminated with SIGKILL while durable state says its attempt is
// RUNNING — no in-process adapter, no cooperative shutdown, no chance for it to
// clean up after itself. Recovery then has to travel the whole real path:
// PostgreSQL expiry -> reconciler -> outbox -> ElasticMQ -> worker B.
func TestWorkerProcessCrash_SigkillRecoversThroughTheRealBinaries(t *testing.T) {
	reset(t)
	binDir := buildBinaries(t)
	brokerQueue := createIsolatedBrokerQueue(t, "taskforge-crash-")

	apiPort, outboxPort := freePort(t), freePort(t)
	reconcilerPort := freePort(t)
	workerAPort, workerBPort := freePort(t), freePort(t)
	apiAddr := fmt.Sprintf("127.0.0.1:%d", apiPort)

	shared := crashSharedEnvironment(brokerQueue, apiAddr, outboxPort, reconcilerPort)
	env := func(extra map[string]string) map[string]string {
		merged := make(map[string]string, len(shared)+len(extra))
		for key, value := range shared {
			merged[key] = value
		}
		for key, value := range extra {
			merged[key] = value
		}
		return merged
	}

	api := startService(t, binDir, "taskforge-api", apiAddr, env(nil))
	api.waitReady(t)
	outbox := startService(t, binDir, "taskforge-outbox",
		fmt.Sprintf("127.0.0.1:%d", outboxPort), env(nil))
	outbox.waitReady(t)

	// Worker A reaches the API through the staller; worker B goes straight to it.
	staller := newSucceedStaller(t, "http://"+apiAddr)

	response, submitted := submit(t, "http://"+apiAddr, "process-crash",
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"survive a kill"},"max_attempts":2}`)
	require.Equal(t, http.StatusCreated, response.StatusCode)
	jobID, err := uuid.Parse(submitted.ID)
	require.NoError(t, err)
	require.Equal(t, 2, submitted.MaxAttempts)

	workerEnv := func(name, addr, apiURL string) map[string]string {
		return env(crashWorkerEnvironment(name, addr, apiURL))
	}
	workerAAddr := fmt.Sprintf("127.0.0.1:%d", workerAPort)
	workerA := startService(t, binDir, "taskforge-worker", workerAAddr,
		workerEnv("crash-worker-a", workerAAddr, staller.URL()))
	workerA.waitReady(t)

	// Durable state, not a local flag, decides when the crash is "mid-job".
	var fence workers.Fence
	eventually(t, 60*time.Second, "worker A's attempt is durably RUNNING", func() bool {
		return testPool.QueryRow(context.Background(), `
			SELECT j.id, a.id, l.id, a.worker_id, a.worker_session_id
			FROM jobs j
			JOIN job_attempts a ON a.job_id = j.id
			JOIN leases l ON l.attempt_id = a.id
			WHERE j.id = $1 AND j.status = 'RUNNING'
			  AND a.status = 'RUNNING' AND l.status = 'ACTIVE'`, jobID,
		).Scan(&fence.JobID, &fence.AttemptID, &fence.LeaseID,
			&fence.WorkerID, &fence.SessionID) == nil
	})
	require.Equal(t, 1, countActiveLeases(t))
	sessionA := fence.SessionID
	require.Equal(t, "HEALTHY", sessionStatus(t, sessionA))

	// --- the crash ----------------------------------------------------------
	require.NoError(t, workerA.cmd.Process.Signal(syscall.SIGKILL))
	waitErr := workerA.wait()

	var exitErr *exec.ExitError
	require.ErrorAs(t, waitErr, &exitErr, "worker A must have died from the signal, not exited")
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.True(t, status.Signaled(), "worker A must have been signalled")
	require.Equal(t, syscall.SIGKILL, status.Signal(),
		"the crash must be SIGKILL: an uncatchable kill with no chance to clean up")

	// Nothing else may be holding worker A's slot open.
	require.Equal(t, 1, countActiveLeases(t),
		"the dead process's lease is still reserved immediately after the kill")

	// --- recovery, by the real reconciler ------------------------------------
	reconciler := startService(t, binDir, "taskforge-reconciler",
		fmt.Sprintf("127.0.0.1:%d", reconcilerPort), env(nil))
	reconciler.waitReady(t)

	// Heartbeats stopped because the process is gone, so PostgreSQL receipt time
	// is what notices. Nothing in the test tells the control plane A is dead.
	eventually(t, 60*time.Second, "worker A's session is detected stale", func() bool {
		return sessionStatus(t, sessionA) == "UNHEALTHY"
	})

	eventually(t, 60*time.Second, "the expired lease is reconciled", func() bool {
		state := readState(t, fence)
		return state.lease == "EXPIRED" && state.attempt == "ABANDONED" && state.job == "QUEUED"
	})
	require.Equal(t, 0, countActiveLeases(t), "abandonment must return the reserved capacity")

	// --- replacement ---------------------------------------------------------
	// Worker B is a different logical worker and talks to the API directly, so
	// its outcome report is not stalled. It can only learn about this job from
	// the recovery notification the reconciler wrote and the real outbox
	// publisher delivered to real ElasticMQ.
	workerBAddr := fmt.Sprintf("127.0.0.1:%d", workerBPort)
	workerB := startService(t, binDir, "taskforge-worker", workerBAddr,
		workerEnv("crash-worker-b", workerBAddr, "http://"+apiAddr))
	workerB.waitReady(t)

	eventually(t, 90*time.Second, "the recovered job reaches SUCCEEDED", func() bool {
		var jobStatus string
		return testPool.QueryRow(context.Background(),
			`SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&jobStatus) == nil &&
			jobStatus == "SUCCEEDED"
	})

	require.Equal(t, []string{"ABANDONED", "SUCCEEDED"}, attemptHistory(t, jobID))
	require.Equal(t, []string{"EXPIRED", "COMPLETED"}, leaseHistory(t, jobID))
	require.Equal(t, 0, countActiveLeases(t))

	// The two attempts belong to two different process sessions, which is the
	// whole point: authority moved, it was not inherited.
	var attemptSessions []uuid.UUID
	rows, err := testPool.Query(context.Background(),
		`SELECT worker_session_id FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number`, jobID)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		attemptSessions = append(attemptSessions, id)
	}
	require.NoError(t, rows.Err())
	require.Len(t, attemptSessions, 2)
	require.Equal(t, sessionA, attemptSessions[0])
	require.NotEqual(t, sessionA, attemptSessions[1])

	// The staller really did hold worker A's outcome report, so the crash
	// happened mid-job rather than after a quietly completed one.
	require.Positive(t, staller.stalled.Load())

	// Worker A cannot come back. Its session is fenced and its lease is closed.
	control := workers.NewStore(testPool, workers.StoreConfig{
		LeaseDuration: 6 * time.Second,
		RetryPolicy:   integrationRetryPolicy(),
	})
	_, err = control.Heartbeat(context.Background(), testScope,
		workers.HeartbeatRequest{WorkerID: fence.WorkerID, SessionID: sessionA})
	require.ErrorIs(t, err, workers.ErrSessionUnavailable)
	_, err = control.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
	require.ErrorIs(t, err, workers.ErrFenceRejected)
	require.ErrorIs(t, control.Succeed(context.Background(), testScope, fence, nil),
		workers.ErrFenceRejected)

	// Every surviving service stopped cleanly and logged no error.
	for _, svc := range []*service{workerB, reconciler, outbox, api} {
		svc.stop()
		require.NotContains(t, svc.out.String(), `"level":"ERROR"`,
			"%s logged an error:\n%s", svc.name, svc.out.String())
	}
}

// loopbackDefaults returns every environment variable internal/config gives a
// loopback default, found by parsing config.go rather than by a list kept here.
// A variable added to the configuration later is therefore noticed.
func loopbackDefaults(t *testing.T) map[string]string {
	t.Helper()
	path := filepath.Join("..", "..", "internal", "config", "config.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
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

// TestWorkerProcessCrash_EnvironmentPinsEveryLoopbackDefault is the hermeticity
// proof for TestWorkerProcessCrash_SigkillRecoversThroughTheRealBinaries.
//
// The crash test spawns real binaries with a hand-built environment. Any
// endpoint it leaves out silently becomes the binary's loopback default, which
// is correct on a developer's machine and wrong the moment the suite runs
// somewhere its infrastructure is not on its own loopback. That is how
// TASKFORGE_RESULTS_ENDPOINT was missed, and a hand-kept list of endpoints
// would miss the next one the same way.
//
// So this derives the list from internal/config and requires every variable
// with a loopback default to be either pinned by the environment the crash test
// builds, or named below with the reason it is not.
func TestWorkerProcessCrash_EnvironmentPinsEveryLoopbackDefault(t *testing.T) {
	defaults := loopbackDefaults(t)
	for _, endpoint := range []string{
		"TASKFORGE_DATABASE_URL", "TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_RESULTS_ENDPOINT",
		"TASKFORGE_WORKER_API_URL",
	} {
		require.Contains(t, defaults, endpoint, "the scan must find the variables it exists to find")
	}

	spawned := crashSharedEnvironment("queue", "127.0.0.1:1", 2, 3)
	maps.Copy(spawned, crashWorkerEnvironment("worker", "127.0.0.1:4", "http://127.0.0.1:5"))

	// Variables with a loopback default that no binary this test starts reads.
	notSpawned := map[string]string{
		"TASKFORGE_SCHEDULER_ADDR": "only taskforge-scheduler binds it, and this test starts none",
	}
	for name := range defaults {
		if _, pinned := spawned[name]; pinned {
			continue
		}
		reason, exempt := notSpawned[name]
		require.Truef(t, exempt,
			"%s defaults to a loopback address and the crash test does not pin it: a spawned binary "+
				"would use the default wherever the suite's infrastructure actually is", name)
		require.NotEmpty(t, reason)
	}
	for name := range notSpawned {
		require.NotContainsf(t, spawned, name, "%s is pinned now; drop it from notSpawned", name)
	}

	// What is pinned comes from the suite's own configuration. Overriding that
	// configuration must move every endpoint with it, which a default that merely
	// happens to equal today's value could not do.
	t.Setenv("TASKFORGE_TEST_DATABASE_URL", "postgres://database.invalid:5442/taskforge")
	t.Setenv("TASKFORGE_BROKER_ENDPOINT", "http://broker.invalid:9324")
	t.Setenv("TASKFORGE_RESULTS_ENDPOINT", "http://results.invalid:4566")
	moved := crashSharedEnvironment("queue", "127.0.0.1:1", 2, 3)
	require.Equal(t, "postgres://database.invalid:5442/taskforge", moved["TASKFORGE_DATABASE_URL"])
	require.Equal(t, "http://broker.invalid:9324", moved["TASKFORGE_BROKER_ENDPOINT"])
	require.Equal(t, "http://results.invalid:4566", moved["TASKFORGE_RESULTS_ENDPOINT"])
}
