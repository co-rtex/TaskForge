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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/queue/sqsbroker"
)

const (
	defaultDSN             = "postgres://taskforge:taskforge@127.0.0.1:5442/taskforge?sslmode=disable"
	defaultBrokerEndpoint  = "http://127.0.0.1:9324"
	defaultBrokerQueueName = "taskforge-work-available"
)

var testPool *pgxpool.Pool

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

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
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

// reset clears all mutable state so each test starts from a known point.
//
// queues is preserved and re-seeded because it is reference data, not test
// output. The broker queue is drained too: a message left behind by an earlier
// test would be indistinguishable from one this test caused.
func reset(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	_, err := testPool.Exec(ctx, `
		TRUNCATE dlq_replays, dlq_entries, leases, job_attempts,
		         worker_sessions, workers, idempotency_records,
		         outbox_events, jobs, queues CASCADE`)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, `
		INSERT INTO queues (name, worker_group, max_concurrency)
		VALUES ('default', 'default', 100)`)
	require.NoError(t, err)

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
