//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/results"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// awaitSucceeded polls durable job status, exactly like the other E2E tests
// in this package -- never a sleep for a guessed duration.
func awaitSucceeded(t *testing.T, jobID string) {
	t.Helper()
	eventually(t, 15*time.Second, "the submitted job succeeds", func() bool {
		var status string
		return testPool.QueryRow(context.Background(),
			`SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&status) == nil && status == "SUCCEEDED"
	})
}

type storedResult struct {
	location    string
	inlineBody  *string
	bucket      *string
	key         *string
	sizeBytes   int64
	checksum    *string
	contentType string
}

func readStoredResult(t *testing.T, jobID string) storedResult {
	t.Helper()
	var row storedResult
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT location, inline_body::text, object_bucket, object_key,
		       size_bytes, checksum_sha256, content_type
		FROM results WHERE job_id = $1`, jobID,
	).Scan(&row.location, &row.inlineBody, &row.bucket, &row.key,
		&row.sizeBytes, &row.checksum, &row.contentType))
	return row
}

func fetchResult(t *testing.T, apiURL string, jobID string) (int, []byte) {
	t.Helper()
	resp, err := http.DefaultClient.Get(apiURL + "/v1/jobs/" + jobID + "/result")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

// testLogWriter routes a worker's structured logs into t.Log, so a failure
// in this specific worker -- as opposed to the many other things a shared
// test binary logs -- is visible in this test's own output rather than
// silently discarded.
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// startResultWorker registers one real worker session, with an explicit
// result threshold and a real object-store client, and returns a stop
// function. It mirrors TestWorker_EndToEndAndDuplicateBrokerDeliveryCreateOneAttempt's
// setup exactly, extended with the two result-specific RunnerConfig fields.
func startResultWorker(t *testing.T, apiURL string, broker queue.Broker, threshold int) {
	t.Helper()
	control := workerruntime.NewClient(apiURL, &http.Client{Timeout: 10 * time.Second}, currentWorkerKey())
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	runner := workerruntime.NewRunner(control, broker, registry, newObjectStore(t), workerruntime.RunnerConfig{
		Registration: workers.Registration{
			SessionID: uuid.New(), Name: "results-e2e-worker-" + uuid.New().String(), Hostname: "e2e.local",
			WorkerGroup: "default", ConcurrencyLimit: 2, Capabilities: []string{"cpu"},
		},
		Queue: "default", PollWait: time.Second,
		RetryAttempts: 3, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
		ResultInlineThresholdBytes: threshold, ResultsBucket: testResultsBucket,
	}, slog.New(slog.NewJSONHandler(testLogWriter{t}, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not stop after cancellation")
		}
	})
}

// TestResults_SmallResultRoundTripsInline is one side of ROADMAP.md's M5C
// acceptance criterion, end to end through the real HTTP API and a real
// worker process: small and large results round-trip, and the boundary is
// tested on both sides.
func TestResults_SmallResultRoundTripsInline(t *testing.T) {
	reset(t)
	server := newAPI(t)
	// An isolated queue, not the shared one: a real worker polls it for the
	// whole test, and reset's drain only removes what is visible at that
	// instant -- a message left invisible by an unrelated test elsewhere in
	// this package would starve this one for the rest of its own queue's
	// defaultVisibilityTimeout, exactly the failure mode
	// createIsolatedBrokerQueue exists to rule out for lifecycle_e2e_test.go.
	broker := newBrokerForQueue(t, createIsolatedBrokerQueue(t, "taskforge-results-e2e-"))

	payload := `{"message":"hello small result"}`
	resp, submitted := submit(t, server.URL, "results-small",
		fmt.Sprintf(`{"queue":"default","job_type":"demo.echo","payload":%s}`, payload))
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	stats, err := newPublisher(t, broker).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Published)

	// A generous threshold: this payload must stay inline.
	startResultWorker(t, server.URL, broker, 1024)
	awaitSucceeded(t, submitted.ID)

	stored := readStoredResult(t, submitted.ID)
	require.Equal(t, "inline", stored.location)
	require.Nil(t, stored.bucket)
	require.Nil(t, stored.key)
	require.Nil(t, stored.checksum)
	require.NotNil(t, stored.inlineBody)
	require.JSONEq(t, payload, *stored.inlineBody)
	require.Equal(t, "application/json", stored.contentType)

	status, body := fetchResult(t, server.URL, submitted.ID)
	require.Equal(t, http.StatusOK, status)
	require.JSONEq(t, payload, string(body))
}

// TestResults_LargeResultRoundTripsThroughTheObjectStore is the other side of
// the same boundary. DemoEcho returns an exact copy of the submitted
// payload, so padding the payload past a small, test-chosen threshold is
// what pushes this attempt's result into the object store -- the same
// Classify decision production traffic makes at the real 64KiB default.
func TestResults_LargeResultRoundTripsThroughTheObjectStore(t *testing.T) {
	reset(t)
	server := newAPI(t)
	// See TestResults_SmallResultRoundTripsInline's comment on why this is an
	// isolated queue rather than the shared one.
	broker := newBrokerForQueue(t, createIsolatedBrokerQueue(t, "taskforge-results-e2e-"))

	padding := strings.Repeat("x", 300)
	payload := fmt.Sprintf(`{"padding":%q}`, padding)
	require.Greater(t, len(payload), 100, "the payload must actually exceed the threshold under test")

	resp, submitted := submit(t, server.URL, "results-large",
		fmt.Sprintf(`{"queue":"default","job_type":"demo.echo","payload":%s}`, payload))
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	stats, err := newPublisher(t, broker).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Published)

	startResultWorker(t, server.URL, broker, 100)
	awaitSucceeded(t, submitted.ID)

	stored := readStoredResult(t, submitted.ID)
	require.Equal(t, "object", stored.location)
	require.Nil(t, stored.inlineBody)
	require.NotNil(t, stored.bucket)
	require.Equal(t, testResultsBucket, *stored.bucket)
	require.NotNil(t, stored.key)
	require.True(t, strings.HasPrefix(*stored.key, fmt.Sprintf("results/%s/%s/", testScope, submitted.ID)),
		"the object key must be results/<scope>/<job_id>/<attempt_id>, got %q", *stored.key)
	require.NotNil(t, stored.checksum)
	require.Len(t, *stored.checksum, 64)

	// The object genuinely exists in the object store under the recorded key,
	// not just a row that claims it does -- and the recorded size and
	// checksum describe exactly those bytes, not an assumption about how the
	// submission pipeline formatted the payload before DemoEcho echoed it.
	fromStore, err := testObjects.Get(context.Background(), *stored.bucket, *stored.key)
	require.NoError(t, err)
	require.JSONEq(t, payload, string(fromStore))
	require.Equal(t, int64(len(fromStore)), stored.sizeBytes)
	require.Equal(t, results.ChecksumSHA256(fromStore), *stored.checksum)

	status, body := fetchResult(t, server.URL, submitted.ID)
	require.Equal(t, http.StatusOK, status)
	require.JSONEq(t, payload, string(body),
		"the retrieval endpoint must serve the exact bytes regardless of where they are stored")
}

// TestResults_NoResultRecordedAnswersNotFound covers a job that never
// produces a result at all -- DemoEcho always returns one, so this drives
// the case through the HTTP contract directly rather than through a real
// handler that happens not to.
func TestResults_NoResultRecordedAnswersNotFound(t *testing.T) {
	reset(t)
	server := newAPI(t)

	status, body := fetchResult(t, server.URL, uuid.New().String())
	require.Equal(t, http.StatusNotFound, status)
	var decoded struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	require.Equal(t, "not_found", decoded.Error.Code)
}
