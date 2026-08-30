//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/jobs"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

const integrationLeaseDuration = 2 * time.Minute

func controlStore() *workers.Store {
	return workers.NewStore(testPool, integrationLeaseDuration)
}

func workerRegistration(name string, concurrency int, capabilities, jobTypes []string) workers.Registration {
	return workers.Registration{
		SessionID: uuid.New(), Name: name, Hostname: name + ".local",
		WorkerGroup: "default", ConcurrencyLimit: concurrency,
		Capabilities: capabilities, SupportedJobTypes: jobTypes,
	}
}

func registerWorker(t *testing.T, store *workers.Store, registration workers.Registration) workers.Session {
	t.Helper()
	session, err := store.Register(context.Background(), testScope, registration)
	require.NoError(t, err)
	return session
}

func createJob(t *testing.T, key, jobType string, priority int, capabilities []string) uuid.UUID {
	return createJobInQueue(t, key, "default", jobType, priority, capabilities)
}

func createJobInQueue(t *testing.T, key, queueName, jobType string, priority int, capabilities []string) uuid.UUID {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"key": key})
	require.NoError(t, err)
	maxAttempts, timeout := 3, 300
	request := jobs.SubmitRequest{
		Queue: queueName, Type: jobType, Payload: payload, Priority: &priority,
		MaxAttempts: &maxAttempts, TimeoutSeconds: &timeout,
		RequiredCapabilities: capabilities,
	}
	normalized, err := request.Normalize()
	require.NoError(t, err)
	result, err := jobs.NewStore(testPool).Submit(context.Background(), testScope, key, normalized)
	require.NoError(t, err)
	return result.Job.ID
}

func claimRequest(session workers.Session, queueName string) workers.ClaimRequest {
	return workers.ClaimRequest{
		WorkerID: session.WorkerID, SessionID: session.ID,
		ClaimRequestID: uuid.New(), Queue: queueName,
	}
}

func assignmentFence(assignment *workers.Assignment) workers.Fence {
	return workers.Fence{
		JobID: assignment.JobID, AttemptID: assignment.AttemptID,
		LeaseID: assignment.LeaseID, WorkerID: assignment.WorkerID,
		SessionID: assignment.SessionID,
	}
}

func TestWorkerRegistration_IsIdempotentAndReplacementFencesTheOldBoot(t *testing.T) {
	reset(t)
	store := controlStore()
	registration := workerRegistration("stable-worker", 1, []string{"cpu"}, []string{"demo.echo"})

	first := registerWorker(t, store, registration)
	replayed := registerWorker(t, store, registration)
	require.Equal(t, first, replayed)
	require.Equal(t, 1, countRows(t, "workers"))
	require.Equal(t, 1, countRows(t, "worker_sessions"))

	createJob(t, "replacement-one", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(first, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)

	replacementRegistration := registration
	replacementRegistration.SessionID = uuid.New()
	replacement := registerWorker(t, store, replacementRegistration)
	require.Equal(t, first.WorkerID, replacement.WorkerID)
	require.NotEqual(t, first.ID, replacement.ID)

	var oldStatus string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM worker_sessions WHERE id = $1`, first.ID).Scan(&oldStatus))
	require.Equal(t, "OFFLINE", oldStatus)

	_, err = store.Claim(context.Background(), testScope, claimRequest(first, "default"))
	require.ErrorIs(t, err, workers.ErrSessionUnavailable)
	oldFence := assignmentFence(claim.Assignment)
	require.ErrorIs(t, store.Start(context.Background(), testScope, oldFence), workers.ErrFenceRejected)
	require.ErrorIs(t, store.Succeed(context.Background(), testScope, oldFence), workers.ErrFenceRejected)

	var jobStatus, attemptStatus, leaseStatus string
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT j.status, a.status, l.status
		FROM jobs j
		JOIN job_attempts a ON a.job_id = j.id
		JOIN leases l ON l.attempt_id = a.id
		WHERE j.id = $1`, oldFence.JobID).Scan(&jobStatus, &attemptStatus, &leaseStatus))
	require.Equal(t, "LEASED", jobStatus)
	require.Equal(t, "LEASED", attemptStatus)
	require.Equal(t, "ACTIVE", leaseStatus)

	// The old boot's lease is not transferred or forgotten. It continues to
	// consume this logical worker's one slot until M3 reconciliation.
	createJob(t, "replacement-two", "demo.echo", 50, nil)
	blocked, err := store.Claim(context.Background(), testScope, claimRequest(replacement, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.CapacityExhausted, blocked.Disposition)
}

func TestClaim_RequestReplayReturnsTheSameCommittedAssignment(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store, workerRegistration("replay-worker", 2, nil, []string{"demo.echo"}))
	createJob(t, "claim-replay-one", "demo.echo", 60, nil)
	createJob(t, "claim-replay-two", "demo.echo", 50, nil)

	request := claimRequest(session, "default")
	first, err := store.Claim(context.Background(), testScope, request)
	require.NoError(t, err)
	second, err := store.Claim(context.Background(), testScope, request)
	require.NoError(t, err)
	require.True(t, second.Replayed)
	require.Equal(t, first.Assignment.JobID, second.Assignment.JobID)
	require.Equal(t, first.Assignment.AttemptID, second.Assignment.AttemptID)
	require.Equal(t, first.Assignment.LeaseID, second.Assignment.LeaseID)
	require.Equal(t, first.Assignment.LeaseExpiresAt, second.Assignment.LeaseExpiresAt)
	require.LessOrEqual(t, second.Assignment.LeaseRemaining, first.Assignment.LeaseRemaining)
	require.Equal(t, 1, countRows(t, "job_attempts"))
	require.Equal(t, 1, countRows(t, "leases"))

	_, err = testPool.Exec(context.Background(),
		`INSERT INTO queues (name, worker_group, max_concurrency) VALUES ('other', 'default', 10)`)
	require.NoError(t, err)
	request.Queue = "other"
	_, err = store.Claim(context.Background(), testScope, request)
	require.ErrorIs(t, err, workers.ErrClaimConflict)
}

func TestClaim_DuplicateNotificationAcrossSessionsConsumesOnlyOneJob(t *testing.T) {
	reset(t)
	store := controlStore()
	firstSession := registerWorker(t, store,
		workerRegistration("duplicate-event-a", 1, nil, []string{"demo.echo"}))
	secondSession := registerWorker(t, store,
		workerRegistration("duplicate-event-b", 1, nil, []string{"demo.echo"}))
	createJob(t, "duplicate-event-job-a", "demo.echo", 60, nil)
	createJob(t, "duplicate-event-job-b", "demo.echo", 50, nil)

	eventID := uuid.New()
	requests := []workers.ClaimRequest{
		{WorkerID: firstSession.WorkerID, SessionID: firstSession.ID, ClaimRequestID: eventID, Queue: "default"},
		{WorkerID: secondSession.WorkerID, SessionID: secondSession.ID, ClaimRequestID: eventID, Queue: "default"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan workers.ClaimResult, len(requests))
	errs := make(chan error, len(requests))
	for _, request := range requests {
		request := request
		go func() {
			<-start
			result, err := store.Claim(ctx, testScope, request)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	var outcomes []workers.ClaimDisposition
	for range requests {
		select {
		case err := <-errs:
			t.Fatalf("duplicate notification claim failed: %v", err)
		case result := <-results:
			outcomes = append(outcomes, result.Disposition)
		case <-ctx.Done():
			t.Fatal("duplicate notification claims timed out")
		}
	}
	require.ElementsMatch(t,
		[]workers.ClaimDisposition{workers.Claimed, workers.DuplicateNotification}, outcomes)
	require.Equal(t, 1, countRows(t, "job_attempts"))
	require.Equal(t, 1, countRows(t, "leases"))
	require.Equal(t, 1, countActiveLeases(t))
	var queued int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM jobs WHERE status = 'QUEUED'`).Scan(&queued))
	require.Equal(t, 1, queued)
}

func TestClaim_ConcurrentCrossQueueRequestIDReuseReturnsStableConflict(t *testing.T) {
	reset(t)
	store := controlStore()
	_, err := testPool.Exec(context.Background(),
		`INSERT INTO queues (name, worker_group, max_concurrency) VALUES ('other', 'default', 10)`)
	require.NoError(t, err)
	firstSession := registerWorker(t, store,
		workerRegistration("cross-queue-event-a", 1, nil, []string{"demo.echo"}))
	secondSession := registerWorker(t, store,
		workerRegistration("cross-queue-event-b", 1, nil, []string{"demo.echo"}))
	createJob(t, "cross-queue-event-default", "demo.echo", 50, nil)
	createJobInQueue(t, "cross-queue-event-other", "other", "demo.echo", 50, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queueBlocker, err := testPool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = queueBlocker.Rollback(context.Background()) })
	_, err = queueBlocker.Exec(ctx, `
		SELECT name FROM queues
		WHERE name IN ('default', 'other')
		ORDER BY name
		FOR UPDATE`)
	require.NoError(t, err)

	claimID := uuid.New()
	requests := []workers.ClaimRequest{
		{WorkerID: firstSession.WorkerID, SessionID: firstSession.ID, ClaimRequestID: claimID, Queue: "default"},
		{WorkerID: secondSession.WorkerID, SessionID: secondSession.ID, ClaimRequestID: claimID, Queue: "other"},
	}
	type outcome struct {
		result workers.ClaimResult
		err    error
	}
	outcomes := make(chan outcome, len(requests))
	start := make(chan struct{})
	for _, request := range requests {
		request := request
		go func() {
			<-start
			result, err := store.Claim(ctx, testScope, request)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	waitForDatabaseWaiters(t, 2)
	require.NoError(t, queueBlocker.Commit(ctx))

	var claimed, conflicts int
	for range requests {
		select {
		case outcome := <-outcomes:
			if outcome.err != nil {
				require.ErrorIs(t, outcome.err, workers.ErrClaimConflict)
				conflicts++
				continue
			}
			require.Equal(t, workers.Claimed, outcome.result.Disposition)
			claimed++
		case <-ctx.Done():
			t.Fatal("cross-queue claims timed out")
		}
	}
	require.Equal(t, 1, claimed)
	require.Equal(t, 1, conflicts)
	require.Equal(t, 1, countRows(t, "job_attempts"))
	require.Equal(t, 1, countRows(t, "leases"))
	require.Equal(t, 1, countActiveLeases(t))
}

// Concurrent HTTP requests use independent HTTP and PostgreSQL connections.
// Sharing one connection would serialize before the transaction and prove
// nothing about the queue/session locks.
func TestClaim_ExactlyOneWorkerWinsAContestedJob(t *testing.T) {
	reset(t)
	server := newAPI(t)
	client := workerruntime.NewClient(server.URL, &http.Client{Timeout: 10 * time.Second})
	createJob(t, "contested", "demo.echo", 50, nil)

	const contenders = 24
	sessions := make([]workers.Session, contenders)
	for i := range sessions {
		registration := workerRegistration(fmt.Sprintf("contender-%02d", i), 1, nil, []string{"demo.echo"})
		var err error
		sessions[i], err = client.Register(context.Background(), registration)
		require.NoError(t, err)
	}

	start := make(chan struct{})
	results := make(chan workers.ClaimResult, contenders)
	errs := make(chan error, contenders)
	var group sync.WaitGroup
	for _, session := range sessions {
		session := session
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			result, err := client.Claim(context.Background(), claimRequest(session, "default"))
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)
	require.Empty(t, errs)

	winners := 0
	for result := range results {
		if result.Disposition == workers.Claimed {
			winners++
		}
	}
	require.Equal(t, 1, winners)
	require.Equal(t, 1, countRows(t, "job_attempts"))
	var activeLeases int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM leases WHERE status = 'ACTIVE'`).Scan(&activeLeases))
	require.Equal(t, 1, activeLeases)
}

func TestClaim_QueueAndWorkerCapacityHoldUnderContention(t *testing.T) {
	t.Run("queue capacity", func(t *testing.T) {
		reset(t)
		_, err := testPool.Exec(context.Background(),
			`UPDATE queues SET max_concurrency = 3 WHERE name = 'default'`)
		require.NoError(t, err)
		store := controlStore()
		const contenders = 16
		sessions := make([]workers.Session, contenders)
		for i := range sessions {
			createJob(t, fmt.Sprintf("queue-limit-%02d", i), "demo.echo", 50, nil)
			sessions[i] = registerWorker(t, store,
				workerRegistration(fmt.Sprintf("queue-worker-%02d", i), 1, nil, []string{"demo.echo"}))
		}
		claimed := concurrentClaims(t, store, sessions)
		require.Equal(t, 3, claimed)
		require.Equal(t, 3, countActiveLeases(t))
	})

	t.Run("logical worker capacity", func(t *testing.T) {
		reset(t)
		store := controlStore()
		const contenders = 16
		queues := make([]string, contenders)
		for i := 0; i < contenders; i++ {
			queues[i] = fmt.Sprintf("worker-capacity-%02d", i)
			_, err := testPool.Exec(context.Background(), `
				INSERT INTO queues (name, worker_group, max_concurrency)
				VALUES ($1, 'default', 100)`, queues[i])
			require.NoError(t, err)
			createJobInQueue(t, fmt.Sprintf("worker-limit-%02d", i), queues[i], "demo.echo", 50, nil)
		}
		session := registerWorker(t, store,
			workerRegistration("bounded-worker", 2, nil, []string{"demo.echo"}))
		sessions := make([]workers.Session, contenders)
		for i := range sessions {
			sessions[i] = session
		}
		claimed := concurrentClaimsOnQueues(t, store, sessions, queues)
		require.Equal(t, 2, claimed)
		require.Equal(t, 2, countActiveLeases(t))
		var queued int
		require.NoError(t, testPool.QueryRow(context.Background(),
			`SELECT count(*) FROM jobs WHERE status = 'QUEUED'`).Scan(&queued))
		require.Equal(t, contenders-2, queued)
	})
}

func concurrentClaims(t *testing.T, store *workers.Store, sessions []workers.Session) int {
	queues := make([]string, len(sessions))
	for i := range queues {
		queues[i] = "default"
	}
	return concurrentClaimsOnQueues(t, store, sessions, queues)
}

func concurrentClaimsOnQueues(t *testing.T, store *workers.Store, sessions []workers.Session, queues []string) int {
	t.Helper()
	require.Len(t, queues, len(sessions))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan workers.ClaimResult, len(sessions))
	errs := make(chan error, len(sessions))
	var group sync.WaitGroup
	for i, session := range sessions {
		session, queueName := session, queues[i]
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			result, err := store.Claim(ctx, testScope, claimRequest(session, queueName))
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("concurrent claims did not finish before their deadline")
	}
	close(results)
	close(errs)
	require.Empty(t, errs)
	claimed := 0
	for result := range results {
		if result.Disposition == workers.Claimed {
			claimed++
		}
	}
	return claimed
}

func countActiveLeases(t *testing.T) int {
	t.Helper()
	var count int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM leases WHERE status = 'ACTIVE'`).Scan(&count))
	return count
}

func TestClaim_UsesStrictDeterministicOrdering(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("ordering-worker", 10, nil, []string{"demo.echo"}))
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	ids := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		uuid.MustParse("00000000-0000-0000-0000-000000000004"),
		uuid.MustParse("00000000-0000-0000-0000-000000000005"),
	}
	insertOrderedJob(t, ids[0], 50, base, base)
	insertOrderedJob(t, ids[1], 50, base, base)
	insertOrderedJob(t, ids[2], 50, base, base.Add(time.Second))
	insertOrderedJob(t, ids[3], 50, base.Add(time.Second), base.Add(-time.Second))
	insertOrderedJob(t, ids[4], 100, base.Add(5*time.Second), base.Add(5*time.Second))

	want := []uuid.UUID{ids[4], ids[0], ids[1], ids[2], ids[3]}
	for i, expected := range want {
		result, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, result.Disposition, "claim %d", i)
		require.Equal(t, expected, result.Assignment.JobID, "claim %d", i)
	}
}

func insertOrderedJob(t *testing.T, id uuid.UUID, priority int, availableAt, createdAt time.Time) {
	t.Helper()
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO jobs (
			id, scope, queue, job_type, payload, status, priority,
			max_attempts, timeout_seconds, available_at, created_at, updated_at
		) VALUES ($1, $2, 'default', 'demo.echo', '{}'::jsonb, 'QUEUED', $3,
		          3, 300, $4, $5, $5)`, id, testScope, priority, availableAt, createdAt)
	require.NoError(t, err)
}

func TestClaim_FiltersCapabilitiesWorkerGroupAndTrustedHandlers(t *testing.T) {
	reset(t)
	store := controlStore()
	unsupported := createJob(t, "filter-unsupported", "demo.unknown", 100, nil)
	gpu := createJob(t, "filter-gpu", "demo.echo", 90, []string{"gpu"})
	cpu := createJob(t, "filter-cpu", "demo.echo", 10, []string{"cpu"})

	cpuSession := registerWorker(t, store,
		workerRegistration("cpu-worker", 2, []string{"cpu"}, []string{"demo.echo"}))
	result, err := store.Claim(context.Background(), testScope, claimRequest(cpuSession, "default"))
	require.NoError(t, err)
	require.Equal(t, cpu, result.Assignment.JobID,
		"higher-priority incompatible and unsupported jobs must be skipped")

	wrongGroupRegistration := workerRegistration("wrong-group", 1, []string{"gpu"}, []string{"demo.echo"})
	wrongGroupRegistration.WorkerGroup = "other"
	wrongGroup := registerWorker(t, store, wrongGroupRegistration)
	result, err = store.Claim(context.Background(), testScope, claimRequest(wrongGroup, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.NoEligibleJob, result.Disposition)
	require.False(t, result.SafeToAcknowledge())

	gpuSession := registerWorker(t, store,
		workerRegistration("gpu-worker", 1, []string{"gpu"}, []string{"demo.echo"}))
	result, err = store.Claim(context.Background(), testScope, claimRequest(gpuSession, "default"))
	require.NoError(t, err)
	require.Equal(t, gpu, result.Assignment.JobID)

	plainSession := registerWorker(t, store,
		workerRegistration("plain-worker", 1, nil, []string{"demo.echo"}))
	result, err = store.Claim(context.Background(), testScope, claimRequest(plainSession, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.NoEligibleJob, result.Disposition)
	var unsupportedStatus string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM jobs WHERE id = $1`, unsupported).Scan(&unsupportedStatus))
	require.Equal(t, "QUEUED", unsupportedStatus)
}

func TestFencedStartAndSuccessAreIdempotentAndReleaseCapacity(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("transition-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "transition-one", "demo.echo", 100, nil)
	secondJob := createJob(t, "transition-two", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)

	wrong := fence
	wrong.LeaseID = uuid.New()
	require.ErrorIs(t, store.Start(context.Background(), testScope, wrong), workers.ErrFenceRejected)
	require.NoError(t, store.Start(context.Background(), testScope, fence))
	var startedAt time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT started_at FROM job_attempts WHERE id = $1`, fence.AttemptID).Scan(&startedAt))
	require.NoError(t, store.Start(context.Background(), testScope, fence))
	var replayedStartedAt time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT started_at FROM job_attempts WHERE id = $1`, fence.AttemptID).Scan(&replayedStartedAt))
	require.Equal(t, startedAt, replayedStartedAt)

	wrong = fence
	wrong.SessionID = uuid.New()
	require.ErrorIs(t, store.Succeed(context.Background(), testScope, wrong), workers.ErrFenceRejected)
	require.NoError(t, store.Succeed(context.Background(), testScope, fence))
	var finishedAt time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT finished_at FROM job_attempts WHERE id = $1`, fence.AttemptID).Scan(&finishedAt))
	require.NoError(t, store.Succeed(context.Background(), testScope, fence))
	var replayedFinishedAt time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT finished_at FROM job_attempts WHERE id = $1`, fence.AttemptID).Scan(&replayedFinishedAt))
	require.Equal(t, finishedAt, replayedFinishedAt)
	require.False(t, finishedAt.Before(startedAt))
	require.Equal(t, 0, countActiveLeases(t))

	next, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, next.Disposition)
	require.Equal(t, secondJob, next.Assignment.JobID)
}

func TestExpiredLeaseCannotStart(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("expired-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "expired", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)

	_, err = testPool.Exec(context.Background(), `
		UPDATE leases
		SET acquired_at = now() - interval '2 minutes',
		    renewed_at = now() - interval '2 minutes',
		    expires_at = now() - interval '1 minute'
		WHERE id = $1`, fence.LeaseID)
	require.NoError(t, err)
	require.ErrorIs(t, store.Start(context.Background(), testScope, fence), workers.ErrLeaseExpired)
}

func TestClaim_LeaseWindowStartsAfterCapacityLockWait(t *testing.T) {
	reset(t)
	const leaseDuration = 750 * time.Millisecond
	store := workers.NewStore(testPool, leaseDuration)
	session := registerWorker(t, store,
		workerRegistration("clock-claim-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "clock-claim", "demo.echo", 50, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queueLock, err := testPool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
	_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
	require.NoError(t, err)

	var claimStartedAt time.Time
	require.NoError(t, testPool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&claimStartedAt))
	type claimOutcome struct {
		result workers.ClaimResult
		err    error
	}
	resultCh := make(chan claimOutcome, 1)
	go func() {
		result, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		resultCh <- claimOutcome{result: result, err: err}
	}()
	waitForDatabaseLock(t, "SELECT worker_group, max_concurrency")
	waitForServerTime(t, claimStartedAt.Add(leaseDuration+250*time.Millisecond))

	var releasedAt time.Time
	require.NoError(t, testPool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&releasedAt))
	require.NoError(t, queueLock.Commit(ctx))
	outcome := <-resultCh
	require.NoError(t, outcome.err)
	require.Equal(t, workers.Claimed, outcome.result.Disposition)

	var acquiredAt, expiresAt time.Time
	require.NoError(t, testPool.QueryRow(ctx, `
		SELECT acquired_at, expires_at FROM leases WHERE id = $1`,
		outcome.result.Assignment.LeaseID).Scan(&acquiredAt, &expiresAt))
	require.False(t, acquiredAt.Before(releasedAt),
		"lease acquisition must be sampled after the capacity lock is released")
	require.Equal(t, leaseDuration, expiresAt.Sub(acquiredAt))
	require.Equal(t, expiresAt, outcome.result.Assignment.LeaseExpiresAt)
}

func TestSucceed_WaitingAcrossExpiryIsRejectedWithoutMutation(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("clock-success-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "clock-success", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)
	require.NoError(t, store.Start(context.Background(), testScope, fence))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var expiresAt time.Time
	require.NoError(t, testPool.QueryRow(ctx, `
		UPDATE leases
		SET acquired_at = clock_timestamp() - interval '1 second',
		    renewed_at = clock_timestamp() - interval '1 second',
		    expires_at = clock_timestamp() + interval '750 milliseconds'
		WHERE id = $1
		RETURNING expires_at`, fence.LeaseID).Scan(&expiresAt))

	queueLock, err := testPool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
	_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
	require.NoError(t, err)

	resultCh := make(chan error, 1)
	go func() { resultCh <- store.Succeed(ctx, testScope, fence) }()
	waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")
	waitForServerTime(t, expiresAt.Add(50*time.Millisecond))
	require.NoError(t, queueLock.Commit(ctx))
	require.ErrorIs(t, <-resultCh, workers.ErrLeaseExpired)

	var jobStatus, attemptStatus, leaseStatus string
	var finishedAt, releasedAt *time.Time
	require.NoError(t, testPool.QueryRow(ctx, `
		SELECT j.status, a.status, l.status, a.finished_at, l.released_at
		FROM jobs j
		JOIN job_attempts a ON a.job_id = j.id
		JOIN leases l ON l.attempt_id = a.id
		WHERE j.id = $1`, fence.JobID).Scan(
		&jobStatus, &attemptStatus, &leaseStatus, &finishedAt, &releasedAt))
	require.Equal(t, "RUNNING", jobStatus)
	require.Equal(t, "RUNNING", attemptStatus)
	require.Equal(t, "ACTIVE", leaseStatus)
	require.Nil(t, finishedAt)
	require.Nil(t, releasedAt)
}

func TestWorkerControl_RequestTimeoutCancelsDatabaseLockWait(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("request-timeout-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "request-timeout", "demo.echo", 50, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queueLock, err := testPool.Begin(ctx)
	require.NoError(t, err)
	defer queueLock.Rollback(context.Background())
	_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
	require.NoError(t, err)

	handler := api.NewServer(jobs.NewStore(testPool), api.Config{
		MaxRequestBytes: 256 * 1024,
		RequestTimeout:  100 * time.Millisecond,
		DevScope:        testScope,
	}, discardLogger()).WithWorkerControl(store).Handler()
	server := httptest.NewServer(handler)
	defer server.Close()
	body := fmt.Sprintf(`{"worker_id":%q,"worker_session_id":%q,"claim_request_id":%q,"queue":"default"}`,
		session.WorkerID.String(), session.ID.String(), uuid.NewString())
	request, err := http.NewRequest(http.MethodPost, server.URL+"/internal/v1/claims", strings.NewReader(body))
	require.NoError(t, err)
	started := time.Now()
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()

	// A deadline burned on a contended authority-row lock is an availability
	// condition the caller may retry, not an internal fault.
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	var envelope api.ErrorBody
	require.NoError(t, json.NewDecoder(response.Body).Decode(&envelope))
	require.Equal(t, api.CodeServiceUnavailable, envelope.Error.Code)
	require.Equal(t, "service_unavailable", envelope.Error.Code)
	require.NotEmpty(t, envelope.Error.Message, "the structured envelope must carry a message")
	require.NotContains(t, envelope.Error.Message, "SELECT",
		"the sanitized envelope must never leak SQL")
	require.Less(t, time.Since(started), time.Second)

	// The worker client keeps treating 5xx as retryable.
	require.True(t, (&workerruntime.RemoteError{
		Status: response.StatusCode, Code: envelope.Error.Code,
	}).Retryable(), "a 503 must stay retryable for the worker client")

	// Nothing durable was mutated by the abandoned transaction.
	require.Equal(t, 0, countRows(t, "job_attempts"))
	require.Equal(t, 0, countRows(t, "leases"))
	var jobStatus string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM jobs`).Scan(&jobStatus))
	require.Equal(t, "QUEUED", jobStatus, "the contended job must remain claimable")
}

func waitForDatabaseLock(t *testing.T, queryFragment string) {
	t.Helper()
	eventually(t, 3*time.Second, "database operation waits on an authority-row lock", func() bool {
		var waiting bool
		err := testPool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%' || $1 || '%'
			)`, queryFragment).Scan(&waiting)
		return err == nil && waiting
	})
}

func waitForDatabaseWaiters(t *testing.T, want int) {
	t.Helper()
	eventually(t, 3*time.Second, "database operations wait on claim-id and queue locks", func() bool {
		var waiting int
		err := testPool.QueryRow(context.Background(), `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock'
			  AND (
				query LIKE '%pg_advisory_xact_lock%'
				OR query LIKE '%SELECT worker_group, max_concurrency%'
			  )`).Scan(&waiting)
		return err == nil && waiting >= want
	})
}

func waitForServerTime(t *testing.T, target time.Time) {
	t.Helper()
	eventually(t, 3*time.Second, "PostgreSQL clock crosses the target", func() bool {
		var reached bool
		err := testPool.QueryRow(context.Background(),
			`SELECT clock_timestamp() >= $1`, target).Scan(&reached)
		return err == nil && reached
	})
}

func TestClaim_RollsBackAttemptWhenLeaseInsertFails(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("rollback-worker", 1, nil, []string{"demo.echo"}))
	jobID := createJob(t, "claim-rollback", "demo.echo", 50, nil)

	_, _ = testPool.Exec(context.Background(),
		`DROP TRIGGER IF EXISTS taskforge_test_fail_lease_insert ON leases`)
	_, err := testPool.Exec(context.Background(), `
		CREATE OR REPLACE FUNCTION taskforge_test_fail_lease_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected lease failure'; END $$`)
	require.NoError(t, err)
	_, err = testPool.Exec(context.Background(), `
		CREATE TRIGGER taskforge_test_fail_lease_insert
		BEFORE INSERT ON leases FOR EACH ROW
		EXECUTE FUNCTION taskforge_test_fail_lease_insert()`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(),
			`DROP TRIGGER IF EXISTS taskforge_test_fail_lease_insert ON leases`)
		_, _ = testPool.Exec(context.Background(),
			`DROP FUNCTION IF EXISTS taskforge_test_fail_lease_insert()`)
	})

	_, err = store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.Error(t, err)
	require.Equal(t, 0, countRows(t, "job_attempts"))
	require.Equal(t, 0, countRows(t, "leases"))
	var status string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&status))
	require.Equal(t, "QUEUED", status)
}

// registerReplacement boots a new process session for the same logical worker,
// which fences the prior current session.
func registerReplacement(t *testing.T, store *workers.Store, previous workers.Registration) workers.Session {
	t.Helper()
	replacement := previous
	replacement.SessionID = uuid.New()
	return registerWorker(t, store, replacement)
}

func sessionStatus(t *testing.T, id uuid.UUID) string {
	t.Helper()
	var status string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM worker_sessions WHERE id = $1`, id).Scan(&status))
	return status
}

// TestRegisterReplacement_RacingClaimYieldsOnlyValidSerialOutcomes proves that a
// process boot replacing its predecessor is serialized against that
// predecessor's in-flight claim. Registration's fencing UPDATE and the claim's
// worker-session FOR UPDATE contend on the same row, so there is no
// check-then-update window: the claim either commits before replacement or is
// rejected afterwards, never both and never partially.
func TestRegisterReplacement_RacingClaimYieldsOnlyValidSerialOutcomes(t *testing.T) {
	t.Run("replacement first: the fenced claim commits nothing", func(t *testing.T) {
		reset(t)
		store := controlStore()
		registration := workerRegistration("race-claim-worker", 1, nil, []string{"demo.echo"})
		original := registerWorker(t, store, registration)
		jobID := createJob(t, "race-claim", "demo.echo", 50, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// Park the claim on the queue capacity lock, before it reaches the
		// worker-session row, so replacement is guaranteed to commit first.
		queueLock, err := testPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
		_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
		require.NoError(t, err)

		claimErr := make(chan error, 1)
		go func() {
			_, err := store.Claim(ctx, testScope, claimRequest(original, "default"))
			claimErr <- err
		}()
		waitForDatabaseLock(t, "SELECT worker_group, max_concurrency")

		replacement := registerReplacement(t, store, registration)
		require.Equal(t, original.WorkerID, replacement.WorkerID)
		require.NoError(t, queueLock.Commit(ctx))

		require.ErrorIs(t, <-claimErr, workers.ErrSessionUnavailable,
			"a claim that waited across replacement must be fenced, not honored")
		require.Equal(t, "OFFLINE", sessionStatus(t, original.ID))
		require.Equal(t, "HEALTHY", sessionStatus(t, replacement.ID))
		require.Equal(t, 0, countRows(t, "job_attempts"), "no attempt may survive a fenced claim")
		require.Equal(t, 0, countRows(t, "leases"), "no lease may survive a fenced claim")
		var status string
		require.NoError(t, testPool.QueryRow(context.Background(),
			`SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&status))
		require.Equal(t, "QUEUED", status, "the job must stay claimable by the replacement")
	})

	t.Run("unbarriered race resolves to exactly one valid serial outcome", func(t *testing.T) {
		const rounds = 12
		fenced, claimed := 0, 0
		for round := 0; round < rounds; round++ {
			reset(t)
			store := controlStore()
			registration := workerRegistration("race-claim-loop", 1, nil, []string{"demo.echo"})
			original := registerWorker(t, store, registration)
			createJob(t, fmt.Sprintf("race-claim-loop-%02d", round), "demo.echo", 50, nil)

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			replacementRegistration := registration
			replacementRegistration.SessionID = uuid.New()

			start := make(chan struct{})
			claimErr := make(chan error, 1)
			registerErr := make(chan error, 1)
			go func() {
				<-start
				_, err := store.Claim(ctx, testScope, claimRequest(original, "default"))
				claimErr <- err
			}()
			go func() {
				<-start
				_, err := store.Register(ctx, testScope, replacementRegistration)
				registerErr <- err
			}()
			close(start)

			require.NoError(t, <-registerErr, "replacement registration must always succeed")
			err := <-claimErr
			cancel()

			attempts, leases := countRows(t, "job_attempts"), countActiveLeases(t)
			switch {
			case err == nil:
				// The claim committed before replacement fenced the old session.
				claimed++
				require.Equal(t, 1, attempts, "a winning claim creates exactly one attempt")
				require.Equal(t, 1, leases, "a winning claim creates exactly one active lease")
			default:
				require.ErrorIs(t, err, workers.ErrSessionUnavailable,
					"the only valid loss is current-session fencing")
				fenced++
				require.Equal(t, 0, attempts, "a fenced claim leaves no attempt behind")
				require.Equal(t, 0, leases, "a fenced claim leaves no capacity reserved")
			}
			require.Equal(t, "OFFLINE", sessionStatus(t, original.ID))
			require.Equal(t, "HEALTHY", sessionStatus(t, replacementRegistration.SessionID))
			require.LessOrEqual(t, countRows(t, "leases"), 1, "no round may produce a second lease")
		}
		t.Logf("valid serial outcomes over %d rounds: claimed-before-replacement=%d fenced-after-replacement=%d",
			rounds, claimed, fenced)
	})
}

// TestRegisterReplacement_RacingSucceedYieldsOnlyValidSerialOutcomes proves the
// same serialization for an outcome report. A replaced process boot can never
// commit a stale success, and the rejection mutates nothing.
func TestRegisterReplacement_RacingSucceedYieldsOnlyValidSerialOutcomes(t *testing.T) {
	t.Run("replacement first: the fenced success mutates nothing", func(t *testing.T) {
		reset(t)
		store := controlStore()
		registration := workerRegistration("race-succeed-worker", 1, nil, []string{"demo.echo"})
		original := registerWorker(t, store, registration)
		createJob(t, "race-succeed", "demo.echo", 50, nil)
		claim, err := store.Claim(context.Background(), testScope, claimRequest(original, "default"))
		require.NoError(t, err)
		fence := assignmentFence(claim.Assignment)
		require.NoError(t, store.Start(context.Background(), testScope, fence))

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		queueLock, err := testPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = queueLock.Rollback(context.Background()) })
		_, err = queueLock.Exec(ctx, `SELECT name FROM queues WHERE name = 'default' FOR UPDATE`)
		require.NoError(t, err)

		succeedErr := make(chan error, 1)
		go func() { succeedErr <- store.Succeed(ctx, testScope, fence) }()
		waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")

		replacement := registerReplacement(t, store, registration)
		require.NoError(t, queueLock.Commit(ctx))

		require.ErrorIs(t, <-succeedErr, workers.ErrFenceRejected,
			"a replaced boot must never commit an outcome")
		require.Equal(t, "OFFLINE", sessionStatus(t, original.ID))
		require.Equal(t, "HEALTHY", sessionStatus(t, replacement.ID))

		var jobStatus, attemptStatus, leaseStatus string
		var finishedAt, releasedAt *time.Time
		require.NoError(t, testPool.QueryRow(context.Background(), `
			SELECT j.status, a.status, l.status, a.finished_at, l.released_at
			FROM jobs j
			JOIN job_attempts a ON a.job_id = j.id
			JOIN leases l ON l.attempt_id = a.id
			WHERE j.id = $1`, fence.JobID).Scan(
			&jobStatus, &attemptStatus, &leaseStatus, &finishedAt, &releasedAt))
		require.Equal(t, "RUNNING", jobStatus)
		require.Equal(t, "RUNNING", attemptStatus)
		require.Equal(t, "ACTIVE", leaseStatus)
		require.Nil(t, finishedAt, "a rejected success must not stamp a finish time")
		require.Nil(t, releasedAt, "a rejected success must not release the lease")
		require.Equal(t, 1, countRows(t, "job_attempts"))
		require.Equal(t, 1, countRows(t, "leases"))
	})

	t.Run("unbarriered race resolves to exactly one valid serial outcome", func(t *testing.T) {
		const rounds = 12
		committed, fenced := 0, 0
		for round := 0; round < rounds; round++ {
			reset(t)
			store := controlStore()
			registration := workerRegistration("race-succeed-loop", 1, nil, []string{"demo.echo"})
			original := registerWorker(t, store, registration)
			createJob(t, fmt.Sprintf("race-succeed-loop-%02d", round), "demo.echo", 50, nil)
			claim, err := store.Claim(context.Background(), testScope, claimRequest(original, "default"))
			require.NoError(t, err)
			fence := assignmentFence(claim.Assignment)
			require.NoError(t, store.Start(context.Background(), testScope, fence))

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			replacementRegistration := registration
			replacementRegistration.SessionID = uuid.New()

			start := make(chan struct{})
			succeedErr := make(chan error, 1)
			registerErr := make(chan error, 1)
			go func() {
				<-start
				succeedErr <- store.Succeed(ctx, testScope, fence)
			}()
			go func() {
				<-start
				_, err := store.Register(ctx, testScope, replacementRegistration)
				registerErr <- err
			}()
			close(start)

			require.NoError(t, <-registerErr, "replacement registration must always succeed")
			err = <-succeedErr
			cancel()

			var jobStatus, attemptStatus, leaseStatus string
			require.NoError(t, testPool.QueryRow(context.Background(), `
				SELECT j.status, a.status, l.status
				FROM jobs j
				JOIN job_attempts a ON a.job_id = j.id
				JOIN leases l ON l.attempt_id = a.id
				WHERE j.id = $1`, fence.JobID).Scan(&jobStatus, &attemptStatus, &leaseStatus))

			switch {
			case err == nil:
				committed++
				require.Equal(t, "SUCCEEDED", jobStatus)
				require.Equal(t, "SUCCEEDED", attemptStatus)
				require.Equal(t, "COMPLETED", leaseStatus)
				require.Equal(t, 0, countActiveLeases(t), "a committed success releases capacity")
			default:
				require.ErrorIs(t, err, workers.ErrFenceRejected,
					"the only valid loss is current-session fencing")
				fenced++
				require.Equal(t, "RUNNING", jobStatus, "a fenced success must not advance the job")
				require.Equal(t, "RUNNING", attemptStatus)
				require.Equal(t, "ACTIVE", leaseStatus)
				require.Equal(t, 1, countActiveLeases(t), "a fenced success must not release capacity")
			}
			require.Equal(t, 1, countRows(t, "job_attempts"), "no round may create a second attempt")
			require.Equal(t, 1, countRows(t, "leases"), "no round may create a second lease")
			require.Equal(t, "OFFLINE", sessionStatus(t, original.ID))
		}
		t.Logf("valid serial outcomes over %d rounds: committed-before-replacement=%d fenced-after-replacement=%d",
			rounds, committed, fenced)
	})
}
