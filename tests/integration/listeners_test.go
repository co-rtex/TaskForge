//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/auth"
	"github.com/co-rtex/TaskForge/internal/config"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/hostallow"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/results"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workerauth"
)

// The tests in this file run the worker-control routes on the CONTROL listener
// (docs/adr/0026-listeners-by-surface.md) the way M8C2's deployment will: bound
// to an address that is not loopback, reached by a worker whose API URL names
// that address and whose configuration opted in to a remote API, with the
// address on the listener's Host allowlist. Nothing in them uses the admin
// listener's handler except where a test compares against it.

// nonLoopbackIPv4 returns an IPv4 address of this machine that is not loopback,
// from an interface that is up. A test that needs one fails rather than skips
// when there is none: a skipped run would read as a pass.
func nonLoopbackIPv4(t *testing.T) string {
	t.Helper()
	interfaces, err := net.Interfaces()
	require.NoError(t, err)
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			if ip := ipNet.IP.To4(); ip != nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
				return ip.String()
			}
		}
	}
	t.Fatal("this machine has no non-loopback IPv4 address to bind a control listener to")
	return ""
}

// listenerServer is one api.Server and the control and public handlers it
// builds, each served on its own non-loopback socket, plus the admin handler on
// loopback for comparison.
type listenerServer struct {
	controlURL, publicURL, adminURL string
	controlHost                     string
}

func startListenerServer(t *testing.T) *listenerServer {
	t.Helper()
	ip := nonLoopbackIPv4(t)
	hosts, err := hostallow.Parse([]string{ip})
	require.NoError(t, err)

	server := api.NewServer(
		jobs.NewStore(testPool),
		api.Config{MaxRequestBytes: 256 * 1024},
		discardLogger(),
		api.ReadinessCheck{Name: "postgres", Check: func(ctx context.Context) error { return database.Ping(ctx, testPool) }},
	).WithWorkerControl(workerControlForAPI()).
		WithWorkerReads(workerControlForAPI()).
		WithAuth(auth.NewStore(testPool)).
		WithWorkerAuth(workerauth.NewStore(testPool)).
		WithResults(results.NewStore(testPool), testObjects).
		WithControlHosts(hosts)

	serve := func(addr string, listener api.Listener) string {
		l, err := net.Listen("tcp", addr)
		require.NoError(t, err)
		httpServer := &http.Server{Handler: server.ListenerHandler(listener), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := httpServer.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("the %s listener stopped: %v", listener, err)
			}
		}()
		t.Cleanup(func() { _ = httpServer.Close() })
		return "http://" + l.Addr().String()
	}
	s := &listenerServer{
		controlURL: serve(net.JoinHostPort(ip, "0"), api.ListenerControl),
		publicURL:  serve(net.JoinHostPort(ip, "0"), api.ListenerPublic),
		adminURL:   serve("127.0.0.1:0", api.ListenerAdmin),
	}
	s.controlHost = strings.TrimPrefix(s.controlURL, "http://")
	t.Logf("control listener %s, public listener %s, admin listener %s", s.controlURL, s.publicURL, s.adminURL)
	return s
}

// TestControlListener_AWorkerOnANonLoopbackAddressRunsAJob proves a worker can
// register, claim, start and succeed through the control listener alone, bound
// to a non-loopback address, with a job submitted through the public listener
// on another. The worker's URL is accepted by the real worker configuration
// only because it opted in (Q5), and the listener really is control: it refuses
// a loopback Host, and it serves no /v1 route.
func TestControlListener_AWorkerOnANonLoopbackAddressRunsAJob(t *testing.T) {
	reset(t)
	s := startListenerServer(t)

	// The URL is one a worker in another container would be given. The real
	// configuration refuses it without the opt-in and accepts it with one.
	t.Setenv("TASKFORGE_WORKER_API_KEY", "placeholder")
	t.Setenv("TASKFORGE_WORKER_API_URL", s.controlURL)
	_, err := config.LoadWorker()
	require.ErrorContains(t, err, "TASKFORGE_WORKER_API_URL must use a loopback host")
	t.Setenv("TASKFORGE_WORKER_ALLOW_REMOTE_API", "true")
	workerConfig, err := config.LoadWorker()
	require.NoError(t, err)

	const scope = "tenant-on-the-control-listener"
	apiKey := mintKey(t, scope, "control-listener-submitter")
	workerKey := mintWorkerKey(t, scope, "control-listener-worker")

	request, err := http.NewRequest(http.MethodPost, s.publicURL+"/v1/jobs", strings.NewReader(
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"across listeners"}}`))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "m8c1-control-listener")
	response, err := clientWithKey(apiKey.Raw).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusCreated, response.StatusCode)
	var job api.JobResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&job))
	jobID := uuid.MustParse(job.ID)

	broker := newBrokerForQueue(t, createIsolatedBrokerQueue(t, "taskforge-m8c1-"))
	ctx, cancel := context.WithCancel(context.Background())
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		publisher := newPublisher(t, broker)
		for ctx.Err() == nil {
			_, _ = publisher.RunOnce(ctx)
			if !sleepCtx(ctx, 25*time.Millisecond) {
				return
			}
		}
	}()

	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	control := workerruntime.NewClient(workerConfig.APIBaseURL, &http.Client{Timeout: 10 * time.Second}, workerKey.Raw)
	registration := workerRegistration("control-listener-worker", 1, []string{"cpu"}, nil)
	runner := workerruntime.NewRunner(control, broker, registry, nil, workerruntime.RunnerConfig{
		Registration: registration,
		Queue:        "default", PollWait: time.Second,
		RetryAttempts: 3, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond, SessionStaleAfter: 3 * time.Second,
		RenewInterval: time.Second, ShutdownTimeout: 2 * time.Second,
	}, discardLogger())
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-publisherDone
		select {
		case <-runnerDone:
		case <-time.After(10 * time.Second):
			t.Error("the worker did not stop after cancellation")
		}
	})

	awaitJobStatus(t, jobID, "SUCCEEDED", 30*time.Second)

	// Durable state: one attempt, started and succeeded, by the session this
	// worker registered through the control listener under its own worker key.
	var attempts int
	var attemptStatus string
	var startedAt *time.Time
	var sessionID, keyID uuid.UUID
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT count(*) OVER (), a.status, a.started_at, ws.id, ws.worker_key_id
		FROM job_attempts a JOIN worker_sessions ws ON ws.id = a.worker_session_id
		WHERE a.job_id = $1`, jobID).Scan(&attempts, &attemptStatus, &startedAt, &sessionID, &keyID))
	require.Equal(t, 1, attempts)
	require.Equal(t, "SUCCEEDED", attemptStatus)
	require.NotNil(t, startedAt, "the attempt was started through the control listener")
	require.Equal(t, registration.SessionID, sessionID)
	require.Equal(t, workerKey.Key.ID, keyID)

	// And the listener the worker used is the control listener: the same claim
	// addressed to loopback is refused by its Host rule, and /v1 is not there.
	refused, err := http.NewRequest(http.MethodPost, s.controlURL+"/internal/v1/claims", strings.NewReader(`{}`))
	require.NoError(t, err)
	refused.Host = "127.0.0.1"
	refusedResponse, err := unauthenticatedClient().Do(refused)
	require.NoError(t, err)
	defer refusedResponse.Body.Close()
	require.Equal(t, http.StatusForbidden, refusedResponse.StatusCode)
	var body api.ErrorBody
	require.NoError(t, json.NewDecoder(refusedResponse.Body).Decode(&body))
	require.Equal(t, api.CodeOriginRefused, body.Error.Code)

	v1, err := clientWithKey(apiKey.Raw).Get(s.controlURL + "/v1/jobs/" + jobID.String())
	require.NoError(t, err)
	defer v1.Body.Close()
	require.Equal(t, http.StatusNotFound, v1.StatusCode, "the control listener serves no /v1 route")
}

// TestControlListener_RevokingAWorkerKeyCutsOffItsSessionAsOnAdmin proves
// ADR-0014's second tier is unchanged on the control listener: a session
// registered there under a worker key heartbeats until the key is revoked, and
// its very next call is refused, exactly as the same sequence is on the admin
// listener of the same server.
func TestControlListener_RevokingAWorkerKeyCutsOffItsSessionAsOnAdmin(t *testing.T) {
	reset(t)
	s := startListenerServer(t)

	for name, base := range map[string]string{"control": s.controlURL, "admin": s.adminURL} {
		t.Run(name, func(t *testing.T) {
			workerKey := mintWorkerKey(t, "tenant-revoked-on-"+name, "revoked-on-"+name)
			sessionID := uuid.New()
			body := `{"worker_name":"revoke-` + name + `","hostname":"revoke.local","worker_group":"default",
				"concurrency_limit":1,"capabilities":["cpu"],"supported_job_types":["demo.echo"]}`
			register, err := http.NewRequest(http.MethodPut,
				base+"/internal/v1/worker-sessions/"+sessionID.String(), strings.NewReader(body))
			require.NoError(t, err)
			register.Header.Set("Content-Type", "application/json")
			registered, err := clientWithKey(workerKey.Raw).Do(register)
			require.NoError(t, err)
			defer registered.Body.Close()
			require.Equal(t, http.StatusOK, registered.StatusCode)
			var session api.WorkerSessionResponse
			require.NoError(t, json.NewDecoder(registered.Body).Decode(&session))

			heartbeat := func() int {
				request, err := http.NewRequest(http.MethodPost,
					base+"/internal/v1/worker-sessions/"+sessionID.String()+"/heartbeat",
					strings.NewReader(`{"worker_id":"`+session.WorkerID+`"}`))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				response, err := unauthenticatedClient().Do(request)
				require.NoError(t, err)
				defer response.Body.Close()
				return response.StatusCode
			}
			require.Equal(t, http.StatusOK, heartbeat(), "the session heartbeats before revocation")

			_, err = workerAuthStore().Revoke(context.Background(), workerKey.Key.ID)
			require.NoError(t, err)
			require.Equal(t, http.StatusUnauthorized, heartbeat(), "the next call is refused once the key is revoked")
			require.Equal(t, http.StatusUnauthorized, heartbeat(), "and stays refused")

			var status string
			require.NoError(t, testPool.QueryRow(context.Background(),
				`SELECT status FROM worker_sessions WHERE id = $1`, sessionID).Scan(&status))
			require.Equal(t, "HEALTHY", status, "revocation refuses the next call; it does not expire the session")
		})
	}
}
