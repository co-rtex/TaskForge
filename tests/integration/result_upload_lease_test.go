//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// slowObjectStore delays every upload before handing it to the real object
// store, the way a throttled or distant S3 does.
type slowObjectStore struct {
	delay time.Duration
	next  workerruntime.ObjectStore
}

func (s slowObjectStore) Put(ctx context.Context, bucket, key string, body []byte, contentType string) error {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return s.next.Put(ctx, bucket, key, body, contentType)
}

// TestResults_AnUploadLongerThanTheLeaseStillSucceedsOnTheFirstAttempt runs the
// real control plane, reconciler and object store with a 4 s lease, and a
// result upload that takes one and a half lease windows. The worker must keep
// renewing while it uploads: if it stopped when the handler returned, the lease
// would lapse mid-upload, the reconciler would abandon the attempt, and the
// success the handler already produced would be refused.
func TestResults_AnUploadLongerThanTheLeaseStillSucceedsOnTheFirstAttempt(t *testing.T) {
	large := `{"blob":"` + strings.Repeat("x", 256) + `"}`
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.HandlerFunc(
		func(context.Context, workerruntime.Execution) (json.RawMessage, error) {
			return json.RawMessage(large), nil
		})))
	stack := startE2EStack(t, registry, time.Hour)

	runner := workerruntime.NewRunner(stack.control, stack.broker, registry,
		slowObjectStore{delay: e2eLease * 3 / 2, next: testObjects}, workerruntime.RunnerConfig{
			Registration: workers.Registration{
				SessionID: uuid.New(), Name: "slow-upload-worker", Hostname: "slow-upload.local",
				WorkerGroup: "default", ConcurrencyLimit: 1, Capabilities: []string{"cpu"},
			},
			Queue: "default", PollWait: time.Second,
			RetryAttempts: 3, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
			HeartbeatInterval: 50 * time.Millisecond, SessionStaleAfter: 3 * time.Second,
			RenewInterval:   time.Second,
			ShutdownTimeout: 2 * time.Second,
			// Well under the result's size, so it goes to the object store.
			ResultInlineThresholdBytes: 64, ResultsBucket: testResultsBucket,
		}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	stack.background.Add(1)
	go func() {
		defer stack.background.Done()
		done <- runner.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop after cancellation")
		}
	})

	job := stack.submit(t, "slow-upload-"+uuid.NewString(),
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"hi"},"timeout_seconds":60}`)
	jobID := uuid.MustParse(job.ID)
	awaitJobStatus(t, jobID, "SUCCEEDED", 30*time.Second)

	rows, err := testPool.Query(context.Background(), `
		SELECT a.attempt_number, a.status, l.status, l.renewal_version
		FROM job_attempts a JOIN leases l ON l.attempt_id = a.id
		WHERE a.job_id = $1 ORDER BY a.attempt_number`, jobID)
	require.NoError(t, err)
	defer rows.Close()
	type attempt struct {
		number         int
		status, lease  string
		renewalVersion int
	}
	var attempts []attempt
	for rows.Next() {
		var a attempt
		require.NoError(t, rows.Scan(&a.number, &a.status, &a.lease, &a.renewalVersion))
		attempts = append(attempts, a)
	}
	require.NoError(t, rows.Err())
	require.Len(t, attempts, 1, "the upload must not cost the attempt its lease: %+v", attempts)
	require.Equal(t, "SUCCEEDED", attempts[0].status)
	require.Equal(t, "COMPLETED", attempts[0].lease)
	require.GreaterOrEqual(t, attempts[0].renewalVersion, 2,
		"the lease outlived its first window only because it was renewed during the upload")

	var location string
	var size int64
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT location, size_bytes FROM results WHERE job_id = $1`, jobID).Scan(&location, &size))
	require.Equal(t, "object", location)
	require.Equal(t, int64(len(large)), size)
}
