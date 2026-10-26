//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// startScopedWorker runs a real DB-less worker that authenticates with key, so
// its session belongs to that key's scope rather than to testScope.
func (s *e2eStack) startScopedWorker(t *testing.T, key, name string, capabilities []string) {
	t.Helper()
	control := workerruntime.NewClient(s.baseURL, &http.Client{Timeout: 10 * time.Second}, key)
	runner := workerruntime.NewRunner(control, s.broker, s.registry, nil, workerruntime.RunnerConfig{
		Registration: workers.Registration{
			SessionID: uuid.New(), Name: name, Hostname: name + ".local",
			WorkerGroup: "default", ConcurrencyLimit: 2, Capabilities: capabilities,
		},
		Queue: "default", PollWait: time.Second,
		RetryAttempts: 3, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond, SessionStaleAfter: 3 * time.Second,
		RenewInterval:   time.Second,
		ShutdownTimeout: 2 * time.Second,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s.background.Add(1)
	go func() {
		defer s.background.Done()
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
}

var queueCountPattern = regexp.MustCompile(
	`<Name>(ApproximateNumberOfMessages(?:NotVisible)?)</Name>\s*<Value>(\d+)</Value>`)

// liveBrokerMessages is every message the broker still holds on queueURL,
// visible or in flight. It reads the broker's own counters rather than
// receiving, because receiving would change what it measures.
func liveBrokerMessages(t *testing.T, queueURL string) int {
	t.Helper()
	response, err := http.PostForm(brokerEndpoint()+"/", url.Values{
		"Action": {"GetQueueAttributes"}, "QueueUrl": {queueURL},
		"AttributeName.1": {"All"}, "Version": {"2012-11-05"},
	})
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, response.StatusCode, "GetQueueAttributes failed: %s", body)

	total, seen := 0, 0
	for _, match := range queueCountPattern.FindAllStringSubmatch(string(body), -1) {
		n, err := strconv.Atoi(match[2])
		require.NoError(t, err)
		total += n
		seen++
	}
	require.Equalf(t, 2, seen, "GetQueueAttributes did not report both message counts: %s", body)
	return total
}

// TestOpenDefect_AnotherScopesWorkerDeletesTheOnlyNotification pins an open,
// unresolved defect recorded in docs/CURRENT_STATE.md ("Open issue, unresolved:
// notification reachability across scopes and for unclaimable jobs"). What it
// asserts is the WRONG behavior, observed today; it is not a requirement. When
// the defect is fixed, this test must be inverted, not deleted.
//
// Every scope's notifications go to one broker queue, but a claim answers
// QUEUE_EMPTY for the claiming worker's own scope. A worker of another scope
// therefore deletes a notification it could never use, and the job it named sits
// QUEUED, beside a worker of its own scope, with nothing left to wake that worker
// until bounded re-notification (60 s by default) creates another.
func TestOpenDefect_AnotherScopesWorkerDeletesTheOnlyNotification(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	// Long enough that bounded re-notification cannot repair anything inside
	// this test: what is measured is what the claim path does on its own.
	stack := startE2EStack(t, registry, time.Hour)

	other := mintWorkerKey(t, "integration-other-scope", "other-scope-worker")
	stack.startScopedWorker(t, other.Raw, "other-scope-worker", []string{"cpu"})

	job := stack.submit(t, "cross-scope-"+uuid.NewString(),
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"hi"}}`)
	jobID := uuid.MustParse(job.ID)

	// The other scope's worker is the only consumer, so a published notification
	// that is gone from the broker is one it received and deleted.
	queueURL := stack.broker.(interface{ QueueURL() string }).QueueURL()
	eventually(t, 10*time.Second, "the job's notification is published and then deleted", func() bool {
		return countPublishedOutbox(t) == 1 && liveBrokerMessages(t, queueURL) == 0
	})

	stack.startWorker(t, "same-scope-worker", 2)
	eventually(t, 10*time.Second, "the job's own scope has a healthy worker session", func() bool {
		var healthy int
		require.NoError(t, testPool.QueryRow(context.Background(), `
			SELECT count(*) FROM worker_sessions s JOIN workers w ON w.id = s.worker_id
			WHERE w.name = 'same-scope-worker' AND s.scope = $1 AND s.status = 'HEALTHY'`,
			testScope).Scan(&healthy))
		return healthy == 1
	})
	// Absence cannot be polled for. A worker with a notification to receive
	// claims within milliseconds; two seconds without a claim is not a race.
	time.Sleep(2 * time.Second)

	var status string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&status))
	const defect = "open defect, pinned as observed (not correct behavior): " +
		"the job's only notification was deleted by another scope's worker"
	require.Equal(t, "QUEUED", status, defect)
	require.Equal(t, 0, liveBrokerMessages(t, queueURL), defect)
	require.Equal(t, 0, countPendingOutbox(t), defect)
	require.Equal(t, 0, countRows(t, "job_attempts"), defect)
}

// TestOpenDefect_AnUnclaimableJobsNotificationsAccumulate pins the second half
// of the same open defect. It too asserts the wrong behavior observed today, and
// must likewise be inverted when it is fixed.
//
// NO_ELIGIBLE_JOB is never acknowledged, so the notification of a job no running
// worker can take stays live and is redelivered every visibility timeout. Bounded
// re-notification sees only that the job is still QUEUED with no PENDING event,
// because the live notification was published, and adds another every interval.
// Nothing deletes any of them while the job waits.
func TestOpenDefect_AnUnclaimableJobsNotificationsAccumulate(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	stack := startE2EStack(t, registry, 300*time.Millisecond)
	// The worker runs and is healthy, but has no "gpu".
	stack.startWorker(t, "cpu-only-worker", 2)

	job := stack.submit(t, "unclaimable-"+uuid.NewString(),
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"hi"},"required_capabilities":["gpu"]}`)
	jobID := uuid.MustParse(job.ID)
	queueURL := stack.broker.(interface{ QueueURL() string }).QueueURL()

	samples := []int{}
	for i := 0; i < 4; i++ {
		time.Sleep(1500 * time.Millisecond)
		samples = append(samples, liveBrokerMessages(t, queueURL))
	}
	t.Logf("live broker messages for one unclaimable job, every 1.5 s: %v; its outbox events: %d",
		samples, len(eventsForJob(t, jobID)))

	var status string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&status))
	require.Equal(t, "QUEUED", status)
	require.Equal(t, 0, countRows(t, "job_attempts"))
	require.Greater(t, samples[0], 1,
		"open defect, pinned as observed (not correct behavior): re-notification adds to a notification that is still live")
	require.Greater(t, samples[len(samples)-1], samples[0],
		"open defect, pinned as observed (not correct behavior): the live set keeps growing while the job waits")
}
