//go:build integration

// Package integration exercises TaskForge against real PostgreSQL and a real
// SQS-compatible broker.
//
// Mocks are deliberately absent here. Migrations, transaction boundaries,
// locking, idempotency under concurrency, and broker delivery cannot be proven
// by a fake — see AGENTS.md section 7.
//
// These tests share one database and one broker queue, so they must not call
// t.Parallel(): each one resets shared state before it runs.
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/auth"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/objectstore"
	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/queue/sqsbroker"
	"github.com/co-rtex/TaskForge/internal/workerauth"
)

const (
	defaultDSN             = "postgres://taskforge:taskforge@127.0.0.1:5442/taskforge?sslmode=disable"
	defaultBrokerEndpoint  = "http://127.0.0.1:9324"
	defaultBrokerQueueName = "taskforge-work-available"
	defaultResultsEndpoint = "http://127.0.0.1:4566"
	testResultsBucket      = "taskforge-results"
)

var testPool *pgxpool.Pool

// testObjects is a shared client against the real object store, exactly like
// testPool is shared against real PostgreSQL. Tests that want their own
// independent client use newObjectStore instead.
var testObjects *objectstore.Client

func dsn() string {
	if v := os.Getenv("TASKFORGE_TEST_DATABASE_URL"); v != "" {
		return v
	}
	return defaultDSN
}

func brokerEndpoint() string {
	if v := os.Getenv("TASKFORGE_BROKER_ENDPOINT"); v != "" {
		return v
	}
	return defaultBrokerEndpoint
}

func resultsEndpoint() string {
	if v := os.Getenv("TASKFORGE_RESULTS_ENDPOINT"); v != "" {
		return v
	}
	return defaultResultsEndpoint
}

// TestMain fails loudly rather than skipping when infrastructure is missing. A
// silently skipped integration suite is indistinguishable from a passing one.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := database.Connect(ctx, dsn())
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration tests need PostgreSQL at %s\nrun `make up` first\ncause: %v\n", dsn(), err)
		os.Exit(1)
	}
	testPool = pool

	if _, err := database.Migrate(ctx, dsn(), discardLogger()); err != nil {
		fmt.Fprintf(os.Stderr, "could not migrate the test database: %v\n", err)
		os.Exit(1)
	}

	if _, err := sqsbroker.New(ctx, brokerOptions()); err != nil {
		fmt.Fprintf(os.Stderr, "integration tests need an SQS-compatible broker at %s\nrun `make up` first\ncause: %v\n",
			brokerEndpoint(), err)
		os.Exit(1)
	}

	objects, err := objectstore.New(ctx, objectStoreOptions())
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not configure the results object-store client: %v\n", err)
		os.Exit(1)
	}
	if err := objects.EnsureBucket(ctx, testResultsBucket); err != nil {
		fmt.Fprintf(os.Stderr, "integration tests need an S3-compatible object store at %s\nrun `make up` first\ncause: %v\n",
			resultsEndpoint(), err)
		os.Exit(1)
	}
	testObjects = objects

	// The public API authenticates from M5A onward, so this suite needs a real
	// credential before any test runs.
	if err := mintSuiteAPIKey(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "could not mint the suite's api key: %v\n", err)
		os.Exit(1)
	}
	// Worker registration authenticates from M5B onward, and this suite's
	// e2eStack helpers register real workers under this key by default.
	if err := mintSuiteWorkerKey(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "could not mint the suite's worker key: %v\n", err)
		os.Exit(1)
	}
	http.DefaultClient.Transport = authorizingTransport{}
	http.DefaultTransport = authorizingTransport{}

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

func brokerOptions() sqsbroker.Options {
	return sqsbroker.Options{
		Endpoint:        brokerEndpoint(),
		Region:          "us-east-1",
		QueueName:       defaultBrokerQueueName,
		AccessKeyID:     "local",
		SecretAccessKey: "local",
	}
}

func objectStoreOptions() objectstore.Options {
	return objectstore.Options{
		Endpoint:        resultsEndpoint(),
		Region:          "us-east-1",
		AccessKeyID:     "local",
		SecretAccessKey: "local",
	}
}

// newObjectStore builds an independent object-store client, for a test that
// wants its own rather than the shared testObjects.
func newObjectStore(t *testing.T) *objectstore.Client {
	t.Helper()
	client, err := objectstore.New(context.Background(), objectStoreOptions())
	require.NoError(t, err)
	return client
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// --- public-API credentials -----------------------------------------------
//
// Every public route authenticates from M5A onward. These tests are about the
// job lifecycle, not about authentication, so the credential is presented by a
// transport rather than threaded through the forty-odd call sites that would
// otherwise each have to say something about M5A that none of them is about.
//
// What the credential must actually DO is proven directly, and negatively, in
// api_keys_test.go: every public route refuses an unauthenticated request, a
// revoked key stops working immediately, and a key for one scope cannot see
// another scope's job. A transport that quietly authorized everything would be
// a problem only if those tests did not exist.

var (
	suiteKeyMu  sync.RWMutex
	suiteAPIKey string
)

// mintSuiteAPIKey creates the credential this suite presents, for testScope.
func mintSuiteAPIKey(ctx context.Context) error {
	created, err := auth.NewStore(testPool).Create(ctx, testScope, "integration-suite")
	if err != nil {
		return err
	}
	suiteKeyMu.Lock()
	defer suiteKeyMu.Unlock()
	suiteAPIKey = created.Raw
	return nil
}

// currentAPIKey returns the credential the suite is presenting right now. It
// changes whenever reset truncates api_keys and mints a new one.
func currentAPIKey() string {
	suiteKeyMu.RLock()
	defer suiteKeyMu.RUnlock()
	return suiteAPIKey
}

// --- worker-key credentials -------------------------------------------------
//
// Since M5B, PUT /internal/v1/worker-sessions/{id} authenticates with a worker
// key -- see docs/adr/0014-worker-control-authentication.md. e2eStack's default
// worker (registered by (*e2eStack).startWorker) presents this suite-wide key,
// minted for testScope, so the ordinary case needs no test to think about
// worker keys at all. Tests about the credential itself, or about a worker
// registered for a DIFFERENT scope, mint their own with mintWorkerKey.

var (
	suiteWorkerKeyMu sync.RWMutex
	suiteWorkerKey   string
)

// mintSuiteWorkerKey creates the worker credential this suite's default
// worker presents, for testScope.
func mintSuiteWorkerKey(ctx context.Context) error {
	created, err := workerauth.NewStore(testPool).Create(ctx, testScope, "integration-suite-worker")
	if err != nil {
		return err
	}
	suiteWorkerKeyMu.Lock()
	defer suiteWorkerKeyMu.Unlock()
	suiteWorkerKey = created.Raw
	return nil
}

// currentWorkerKey returns the worker credential the suite's default worker is
// presenting right now. It changes whenever reset truncates worker_keys and
// mints a new one.
func currentWorkerKey() string {
	suiteWorkerKeyMu.RLock()
	defer suiteWorkerKeyMu.RUnlock()
	return suiteWorkerKey
}

// authorizingTransport presents the suite's credential on public requests that
// do not already carry one, and the suite's worker key on the one internal
// route that authenticates.
//
// It never overwrites an Authorization header a test set itself, which is what
// lets a test present a second scope's key, a revoked key, or no key at all and
// observe the real answer. Every /internal/v1 route other than worker
// registration is left alone: the rest of that surface is deliberately
// unauthenticated, and a transport that credentialed it would hide a
// regression that started requiring one.
type authorizingTransport struct{}

func (authorizingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	switch {
	case strings.HasPrefix(request.URL.Path, "/v1/") && request.Header.Get("Authorization") == "":
		// Cloned rather than mutated: a RoundTripper must not modify the request
		// it is given.
		request = request.Clone(request.Context())
		request.Header.Set("Authorization", "Bearer "+currentAPIKey())
	case isWorkerRegistrationRequest(request) && request.Header.Get("Authorization") == "":
		request = request.Clone(request.Context())
		request.Header.Set("Authorization", "Bearer "+currentWorkerKey())
	}
	return realTransport.RoundTrip(request)
}

// isWorkerRegistrationRequest identifies PUT /internal/v1/worker-sessions/{id}
// specifically, not its /heartbeat sub-route: registration is the one
// worker-control call that authenticates.
func isWorkerRegistrationRequest(r *http.Request) bool {
	return r.Method == http.MethodPut &&
		strings.HasPrefix(r.URL.Path, "/internal/v1/worker-sessions/") &&
		!strings.HasSuffix(r.URL.Path, "/heartbeat")
}

// realTransport is the transport underneath, captured at package initialization
// and therefore before TestMain replaces http.DefaultTransport. Dispatching
// through this variable rather than through http.DefaultTransport is what stops
// authorizingTransport from wrapping itself once it is installed there.
var realTransport = http.DefaultTransport

// unauthenticatedClient issues requests with no credential at all, bypassing
// authorizingTransport. It is what a test uses to prove a route actually
// refuses an anonymous caller.
func unauthenticatedClient() *http.Client {
	return &http.Client{Transport: realTransport, Timeout: 10 * time.Second}
}

// clientWithKey issues requests presenting one specific credential.
func clientWithKey(raw string) *http.Client {
	return &http.Client{Transport: fixedKeyTransport{raw: raw}, Timeout: 10 * time.Second}
}

type fixedKeyTransport struct{ raw string }

func (t fixedKeyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set("Authorization", "Bearer "+t.raw)
	return realTransport.RoundTrip(request)
}

// newBroker connects to the real broker, optionally through a custom endpoint.
func newBroker(t *testing.T, endpoint string) *sqsbroker.Broker {
	t.Helper()
	opts := brokerOptions()
	if endpoint != "" {
		opts.Endpoint = endpoint
	}
	b, err := sqsbroker.New(context.Background(), opts)
	require.NoError(t, err)
	return b
}

// newBrokerForQueue connects to the real broker on a named queue, for tests
// that own a queue nobody else touches.
func newBrokerForQueue(t *testing.T, queueName string) *sqsbroker.Broker {
	t.Helper()
	opts := brokerOptions()
	opts.QueueName = queueName
	b, err := sqsbroker.New(context.Background(), opts)
	require.NoError(t, err)
	return b
}

// reset clears all mutable state so each test starts from a known point.
//
// queues is preserved and re-seeded because it is reference data, not test
// output. The broker queue is drained too: a message left behind by an earlier
// test would be indistinguishable from one this test caused.
func reset(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	_, err := testPool.Exec(ctx, `
		TRUNCATE dlq_replays, dlq_entries, leases, job_attempts, results,
		         worker_sessions, workers, idempotency_records,
		         outbox_events, jobs, queues, api_keys, worker_keys CASCADE`)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, `
		INSERT INTO queues (name, worker_group, max_concurrency)
		VALUES ('default', 'default', 100)`)
	require.NoError(t, err)

	// api_keys and worker_keys were just truncated, so the credentials the
	// suite was presenting no longer exist. Minting here rather than lazily
	// keeps every test that calls reset holding live keys for testScope.
	require.NoError(t, mintSuiteAPIKey(ctx))
	require.NoError(t, mintSuiteWorkerKey(ctx))

	drainBroker(t, newBroker(t, ""))
}

// drainBroker removes every message currently on the queue.
func drainBroker(t *testing.T, b queue.Receiver) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 200; i++ {
		msgs, err := b.Receive(ctx, 10, 0)
		require.NoError(t, err)
		if len(msgs) == 0 {
			return
		}
		for _, m := range msgs {
			require.NoError(t, b.Delete(ctx, m.ReceiptHandle))
		}
	}
	t.Fatal("broker queue did not drain within 200 receive batches")
}

// receiveAll collects messages until none arrive for a short quiet period.
//
// It is not enough to receive once: SQS-style receive returns an arbitrary
// subset, so a single empty batch does not mean the queue is empty.
func receiveAll(t *testing.T, b queue.Receiver, quiet time.Duration) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var bodies [][]byte
	lastMessage := time.Now()
	for time.Since(lastMessage) < quiet {
		msgs, err := b.Receive(ctx, 10, 0)
		require.NoError(t, err)
		if len(msgs) == 0 {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		for _, m := range msgs {
			bodies = append(bodies, m.Body)
			require.NoError(t, b.Delete(ctx, m.ReceiptHandle))
		}
		lastMessage = time.Now()
	}
	return bodies
}

// eventually polls until cond holds or the deadline passes.
//
// Integration tests poll with a deadline instead of sleeping for a guessed
// duration: a fixed sleep is either flaky or slow, and usually both.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition never became true within %s: %s", timeout, what)
}

// flakyProxy is a controllable network fault injected in front of the real
// broker.
//
// Stopping the container would also work, but it makes the suite depend on the
// Docker CLI and leaves the broker stopped if a test dies mid-run. Failing at
// the network layer exercises the same code path — the real AWS SDK client, real
// error handling, real retry and recovery — deterministically.
type flakyProxy struct {
	server *httptest.Server
	down   atomic.Bool
}

func newFlakyProxy(t *testing.T, target string) *flakyProxy {
	t.Helper()
	u, err := url.Parse(target)
	require.NoError(t, err)

	rp := httputil.NewSingleHostReverseProxy(u)
	rp.ErrorLog = nil

	p := &flakyProxy{}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.down.Load() {
			http.Error(w, "broker unavailable", http.StatusServiceUnavailable)
			return
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(p.server.Close)
	return p
}

func (p *flakyProxy) URL() string { return p.server.URL }
func (p *flakyProxy) Stop()       { p.down.Store(true) }
func (p *flakyProxy) Start()      { p.down.Store(false) }

// --- direct database assertions -------------------------------------------

func countRows(t *testing.T, table string) int {
	t.Helper()
	var n int
	require.NoError(t, testPool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n))
	return n
}

func countPendingOutbox(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE status = 'PENDING'`).Scan(&n))
	return n
}

func countPublishedOutbox(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE status = 'PUBLISHED'`).Scan(&n))
	return n
}
