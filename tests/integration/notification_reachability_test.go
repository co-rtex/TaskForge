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
	runner := workerruntime.NewRunner(control, s.router, s.registry, nil, workerruntime.RunnerConfig{
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

// TestReachability_AnotherScopesWorkerCannotTakeTheOnlyNotification is the
// inversion of the audit's first pinned defect (formerly
// TestOpenDefect_AnotherScopesWorkerDeletesTheOnlyNotification, which asserted
// that a scope-B worker deleted scope A's only notification and left A's job
// QUEUED, with nothing live, beside a healthy scope-A worker).
//
// Under ADR-0029 each (scope, queue) address has a broker queue of its own, so
// a scope-B worker never receives scope A's notification. The test keeps the
// original arrangement -- the other scope's worker running first, alone, with
// re-notification an hour away so nothing can repair a deletion -- and asserts
// the corrected guarantee: the notification is still live on A's queue when
// A's worker arrives, and that worker claims and completes the job from it.
func TestReachability_AnotherScopesWorkerCannotTakeTheOnlyNotification(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	// Long enough that bounded re-notification cannot repair anything inside
	// this test: what is measured is what routing and the claim path do alone.
	stack := startE2EStack(t, registry, time.Hour)

	const otherScope = "integration-other-scope"
	other := mintWorkerKey(t, otherScope, "other-scope-worker")
	stack.startScopedWorker(t, other.Raw, "other-scope-worker", []string{"cpu"})
	eventually(t, 10*time.Second, "the other scope's worker has a healthy session", func() bool {
		return healthySessions(t, "other-scope-worker", otherScope) == 1
	})

	job := stack.submit(t, "cross-scope-"+uuid.NewString(),
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"hi"}}`)
	jobID := uuid.MustParse(job.ID)

	ownQueue := stack.router.address(t, testScope, "default").QueueURL()
	otherQueue := stack.router.address(t, otherScope, "default").QueueURL()
	require.NotEqual(t, ownQueue, otherQueue, "two scopes must not share a broker queue")
	eventually(t, 10*time.Second, "the job's notification is published to its own scope's queue", func() bool {
		return countPublishedOutbox(t) == 1 && liveBrokerMessages(t, ownQueue) == 1
	})
	// Absence cannot be polled for. The other scope's worker long-polls its own
	// queue continuously; two seconds in which the original defect deleted the
	// message within milliseconds is not a race.
	time.Sleep(2 * time.Second)
	require.Equal(t, 1, liveBrokerMessages(t, ownQueue),
		"another scope's worker must not be able to consume this scope's notification")
	require.Zero(t, liveBrokerMessages(t, otherQueue))
	require.Equal(t, "QUEUED", jobStatus(t, jobID))
	require.Zero(t, countRows(t, "job_attempts"))

	stack.startWorker(t, "same-scope-worker", 2)
	eventually(t, 10*time.Second, "the job's own scope's worker completes it from the original notification", func() bool {
		return jobStatus(t, jobID) == "SUCCEEDED"
	})
	require.Len(t, eventsForJob(t, jobID), 1,
		"discovered from the one original notification, with no re-notification needed")
	require.Equal(t, 1, countRows(t, "job_attempts"))
	var otherAttempts int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM job_attempts WHERE scope = $1`, otherScope).Scan(&otherAttempts))
	require.Zero(t, otherAttempts)
	eventually(t, 10*time.Second, "the consumed notification is acknowledged", func() bool {
		return liveBrokerMessages(t, ownQueue) == 0
	})
}

// TestReachability_AnUnclaimableJobsNotificationsStayBounded is the inversion of
// the audit's second pinned defect (formerly
// TestOpenDefect_AnUnclaimableJobsNotificationsAccumulate, which recorded live
// broker messages [5 10 15 19] -- 19 outbox events -- in six seconds for one
// gpu job beside a cpu-only worker, with re-notification every 300 ms).
//
// The same arrangement now holds exactly one live message and one event: the
// cpu worker declines the job's notification and leaves it in the broker, and
// re-notification defers the job because no session could claim it. Bounded
// must not mean undiscoverable, so the test then starts a gpu worker and
// requires the job to complete.
func TestReachability_AnUnclaimableJobsNotificationsStayBounded(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	stack := startE2EStack(t, registry, 300*time.Millisecond)
	// The worker runs and is healthy, but has no "gpu".
	stack.startWorker(t, "cpu-only-worker", 2)

	job := stack.submit(t, "unclaimable-"+uuid.NewString(),
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"hi"},"required_capabilities":["gpu"]}`)
	jobID := uuid.MustParse(job.ID)
	queueURL := stack.ownQueue(t).QueueURL()

	eventually(t, 10*time.Second, "the job is deferred by re-notification as unclaimable", func() bool {
		return renotifyCheckedAt(t, jobID) != nil
	})
	// Twenty re-notification intervals: the original defect added a message
	// per interval.
	samples := []int{}
	for i := 0; i < 4; i++ {
		time.Sleep(1500 * time.Millisecond)
		samples = append(samples, liveBrokerMessages(t, queueURL))
	}
	t.Logf("live broker messages for one unclaimable job, every 1.5 s: %v; its outbox events: %d",
		samples, len(eventsForJob(t, jobID)))
	require.Equal(t, []int{1, 1, 1, 1}, samples,
		"one unclaimable job must hold exactly its one notification live, not one more per interval")
	require.Len(t, eventsForJob(t, jobID), 1)
	require.Equal(t, "QUEUED", jobStatus(t, jobID))
	require.Zero(t, countRows(t, "job_attempts"))

	// A worker that can take it joins. Its session opens the gate, and it
	// claims either the held original or the replacement the next scan writes.
	stack.startScopedWorker(t, currentWorkerKey(), "gpu-worker", []string{"cpu", "gpu"})
	eventually(t, 20*time.Second, "the job completes once a worker that can claim it exists", func() bool {
		return jobStatus(t, jobID) == "SUCCEEDED"
	})
	var gpuAttempts int
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM job_attempts a JOIN workers w ON w.id = a.worker_id
		WHERE a.job_id = $1 AND w.name = 'gpu-worker'`, jobID).Scan(&gpuAttempts))
	require.Equal(t, 1, gpuAttempts)
}

func renotifyCheckedAt(t *testing.T, jobID uuid.UUID) *time.Time {
	t.Helper()
	var at *time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT renotify_checked_at FROM jobs WHERE id = $1`, jobID).Scan(&at))
	return at
}

func healthySessions(t *testing.T, workerName, scope string) int {
	t.Helper()
	var healthy int
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM worker_sessions s JOIN workers w ON w.id = s.worker_id
		WHERE w.name = $1 AND s.scope = $2 AND s.status = 'HEALTHY'`,
		workerName, scope).Scan(&healthy))
	return healthy
}
