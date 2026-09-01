//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"path/filepath"
	"regexp"
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
	require.NoError(t, svc.cmd.Start(), "starting %s", name)
	t.Cleanup(func() { svc.stop() })
	return svc
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
// It exists so the crash below happens while durable state says RUNNING. The
// only trusted handler compiled into taskforge-worker is demo.echo, which
// returns immediately, and adding a slow production handler purely to widen a
// test window is exactly the kind of production surface this project refuses to
// grow. Stalling one control-plane call instead leaves the worker holding an
// active lease on a RUNNING attempt — the real state a crashed worker leaves —
// without touching the worker, the handler registry, or the control plane.
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

// createIsolatedBrokerQueue gives this test a broker queue of its own.
//
// Every other test in this package shares one queue and drains it in reset().
// That is fine for tests whose consumers live and die inside the test, but this
// one runs real worker processes: a worker that is SIGKILLed or SIGTERMed while
// long-polling leaves a message in flight, and reset() cannot drain what is not
// visible. Rather than make every later test tolerate that, this test consumes
// from a queue nobody else touches.
func createIsolatedBrokerQueue(t *testing.T) string {
	t.Helper()
	name := "taskforge-crash-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

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
	brokerQueue := createIsolatedBrokerQueue(t)

	apiPort, outboxPort := freePort(t), freePort(t)
	reconcilerPort := freePort(t)
	workerAPort, workerBPort := freePort(t), freePort(t)
	apiAddr := fmt.Sprintf("127.0.0.1:%d", apiPort)

	shared := map[string]string{
		"TASKFORGE_DATABASE_URL":             dsn(),
		"TASKFORGE_DEV_SCOPE":                testScope,
		"TASKFORGE_BROKER_ENDPOINT":          brokerEndpoint(),
		"TASKFORGE_BROKER_QUEUE_NAME":        brokerQueue,
		"TASKFORGE_BROKER_REGION":            "us-east-1",
		"TASKFORGE_BROKER_ACCESS_KEY_ID":     "local",
		"TASKFORGE_BROKER_SECRET_ACCESS_KEY": "local",
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
		return env(map[string]string{
			"TASKFORGE_WORKER_NAME":             name,
			"TASKFORGE_WORKER_ADDR":             addr,
			"TASKFORGE_WORKER_API_URL":          apiURL,
			"TASKFORGE_WORKER_QUEUE":            "default",
			"TASKFORGE_WORKER_GROUP":            "default",
			"TASKFORGE_WORKER_CONCURRENCY":      "1",
			"TASKFORGE_WORKER_CAPABILITIES":     "cpu",
			"TASKFORGE_WORKER_POLL_WAIT":        "1s",
			"TASKFORGE_WORKER_REQUEST_TIMEOUT":  crashRequestTimeout,
			"TASKFORGE_WORKER_SHUTDOWN_TIMEOUT": "2s",
		})
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
	require.ErrorIs(t, control.Succeed(context.Background(), testScope, fence),
		workers.ErrFenceRejected)

	// Every surviving service stopped cleanly and logged no error.
	for _, svc := range []*service{workerB, reconciler, outbox, api} {
		svc.stop()
		require.NotContains(t, svc.out.String(), `"level":"ERROR"`,
			"%s logged an error:\n%s", svc.name, svc.out.String())
	}
}
