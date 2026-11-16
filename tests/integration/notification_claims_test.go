//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/workers"
)

// These tests pin the claim's decisions relative to the NOTIFICATION it was
// woken by (ADR-0028), on the store directly so each disposition is
// deterministic. Routing makes a foreign notification unreachable in a correct
// deployment; the guard is what keeps a routing mistake from becoming the
// pre-ADR-0028 defect again.

const foreignScope = "integration-scope-b"

func eventFor(t *testing.T, jobID uuid.UUID) uuid.UUID {
	t.Helper()
	events := eventsForJob(t, jobID)
	require.NotEmpty(t, events)
	return events[len(events)-1].ID
}

func claimWith(t *testing.T, store *workers.Store, scope string, session workers.Session, eventID uuid.UUID) workers.ClaimResult {
	t.Helper()
	result, err := store.Claim(context.Background(), scope, workers.ClaimRequest{
		WorkerID: session.WorkerID, SessionID: session.ID, ClaimRequestID: eventID, Queue: "default",
	})
	require.NoError(t, err)
	return result
}

func leasesUnderClaimID(t *testing.T, eventID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM leases WHERE claim_request_id = $1`, eventID).Scan(&n))
	return n
}

// TestClaim_AForeignNotificationClaimsNothingAndIsNotAcknowledged covers both
// halves of the original cross-scope defect at the decision itself: a scope-B
// session presenting scope A's event must neither answer QUEUE_EMPTY (which
// deleted A's only wakeup) nor claim B's own due job under A's event id (which
// spent A's wakeup on B's work).
func TestClaim_AForeignNotificationClaimsNothingAndIsNotAcknowledged(t *testing.T) {
	reset(t)
	store := controlStore()
	jobA := createJob(t, "job-a", "demo.echo", 50, nil)
	eventA := eventFor(t, jobA)
	sessionB, err := store.Register(context.Background(), foreignScope,
		workerRegistration("worker-b", 2, nil, []string{"demo.echo"}))
	require.NoError(t, err)

	// Scope B has nothing due: the old answer was QUEUE_EMPTY, safe to delete.
	result := claimWith(t, store, foreignScope, sessionB, eventA)
	require.Equal(t, workers.ForeignNotification, result.Disposition)
	require.False(t, result.SafeToAcknowledge(), "another scope's notification must never be deleted")
	require.Nil(t, result.Assignment)

	// Scope B has its own due job: the old answer claimed it under A's id.
	jobB := createJobInScope(t, foreignScope, "job-b", "default", "demo.echo")
	result = claimWith(t, store, foreignScope, sessionB, eventA)
	require.Equal(t, workers.ForeignNotification, result.Disposition)
	require.Nil(t, result.Assignment, "B's work must not be claimed under A's notification")
	require.Zero(t, leasesUnderClaimID(t, eventA))
	require.Equal(t, "QUEUED", jobStatus(t, jobB))

	// Positive controls: each scope's own notification still works, and A's
	// event is still available to A, so nothing above consumed it.
	result = claimWith(t, store, foreignScope, sessionB, eventFor(t, jobB))
	require.Equal(t, workers.Claimed, result.Disposition)
	require.Equal(t, jobB, result.Assignment.JobID)
	sessionA := registerWorker(t, store, workerRegistration("worker-a", 1, nil, []string{"demo.echo"}))
	result = claimWith(t, store, testScope, sessionA, eventA)
	require.Equal(t, workers.Claimed, result.Disposition)
	require.Equal(t, jobA, result.Assignment.JobID)
}

// TestClaim_ConcurrentForeignPresentationsCannotStealTheWakeup races many
// scope-B sessions, each on its own connection, against scope A's one session,
// all presenting A's one event at once. Exactly A's claim may use it.
func TestClaim_ConcurrentForeignPresentationsCannotStealTheWakeup(t *testing.T) {
	reset(t)
	store := controlStore()
	jobA := createJob(t, "job-a", "demo.echo", 50, nil)
	eventA := eventFor(t, jobA)
	const foreignSessions = 8
	var sessionsB []workers.Session
	for i := 0; i < foreignSessions; i++ {
		createJobInScope(t, foreignScope, "job-b-"+uuid.NewString()[:8], "default", "demo.echo")
		session, err := store.Register(context.Background(), foreignScope,
			workerRegistration("worker-b-"+uuid.NewString()[:8], 1, nil, []string{"demo.echo"}))
		require.NoError(t, err)
		sessionsB = append(sessionsB, session)
	}
	sessionA := registerWorker(t, store, workerRegistration("worker-a", 1, nil, []string{"demo.echo"}))

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan workers.ClaimResult, foreignSessions+1)
	errs := make(chan error, foreignSessions+1)
	// Only the test goroutine may stop the test, so the racers hand back what
	// they got and every assertion runs after they finish.
	present := func(scope string, session workers.Session) {
		defer wg.Done()
		<-start
		result, err := store.Claim(context.Background(), scope, workers.ClaimRequest{
			WorkerID: session.WorkerID, SessionID: session.ID, ClaimRequestID: eventA, Queue: "default",
		})
		if err != nil {
			errs <- err
			return
		}
		results <- result
	}
	for _, session := range sessionsB {
		wg.Add(1)
		go present(foreignScope, session)
	}
	wg.Add(1)
	go present(testScope, sessionA)
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	claimed, foreign := 0, 0
	for result := range results {
		switch result.Disposition {
		case workers.Claimed:
			claimed++
			require.Equal(t, jobA, result.Assignment.JobID)
		case workers.ForeignNotification:
			foreign++
		default:
			t.Fatalf("unexpected disposition %s", result.Disposition)
		}
	}
	require.Equal(t, 1, claimed)
	require.Equal(t, foreignSessions, foreign)
	require.Equal(t, 1, leasesUnderClaimID(t, eventA))
	var foreignLeases int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM leases WHERE scope = $1`, foreignScope).Scan(&foreignLeases))
	require.Zero(t, foreignLeases)
}

// TestClaim_AStaleNotificationNobodyCanUseIsReleased walks the swap that used to
// leave a job nobody can claim holding other jobs' leftovers alive forever, and
// then shows the job is still discovered once someone can claim it.
func TestClaim_AStaleNotificationNobodyCanUseIsReleased(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	gpuJob := createJob(t, "gpu-job", "demo.echo", 50, []string{"gpu"})
	gpuEvent := eventFor(t, gpuJob)
	cpuA := registerWorker(t, store, workerRegistration("cpu-a", 1, []string{"cpu"}, []string{"demo.echo"}))
	cpuB := registerWorker(t, store, workerRegistration("cpu-b", 1, []string{"cpu"}, []string{"demo.echo"}))

	// The gpu job's own, current notification is held for a worker that can
	// take it: not safe to acknowledge.
	result := claimWith(t, store, testScope, cpuA, gpuEvent)
	require.Equal(t, workers.NoEligibleJob, result.Disposition)
	require.False(t, result.SafeToAcknowledge())

	// A cpu job arrives and cpu-a, redelivered the gpu job's notification,
	// claims the cpu job with it: claims are fungible within a scope.
	cpuJob := createJob(t, "cpu-job", "demo.echo", 50, nil)
	cpuEvent := eventFor(t, cpuJob)
	result = claimWith(t, store, testScope, cpuA, gpuEvent)
	require.Equal(t, workers.Claimed, result.Disposition)
	require.Equal(t, cpuJob, result.Assignment.JobID)

	// The cpu job's own notification is now a leftover: its job is LEASED.
	// Before ADR-0028 cpu-b answered NO_ELIGIBLE_JOB (the gpu job is due) and
	// it stayed live as long as the gpu job waited.
	result = claimWith(t, store, testScope, cpuB, cpuEvent)
	require.Equal(t, workers.StaleNotification, result.Disposition)
	require.True(t, result.SafeToAcknowledge())
	require.Nil(t, result.Assignment)
	require.Equal(t, "QUEUED", jobStatus(t, gpuJob))

	// The gpu job's own wakeup was spent on the cpu job, so it depends on
	// re-notification, which defers it while nobody could claim it...
	strandAll(t, gpuJob)
	stats, err := jobStore().RenotifyStrandedQueued(ctx, time.Minute, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Deferred)
	// ...and repairs it once a session could.
	gpu := registerWorker(t, store, workerRegistration("gpu", 1, []string{"gpu"}, []string{"demo.echo"}))
	backdateNotification(t, gpuJob, 2*time.Minute)
	_, err = testPool.Exec(ctx, `UPDATE jobs SET renotify_checked_at = clock_timestamp() - interval '2 minutes' WHERE id = $1`, gpuJob)
	require.NoError(t, err)
	stats, err = jobStore().RenotifyStrandedQueued(ctx, time.Minute, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Renotified)
	result = claimWith(t, store, testScope, gpu, eventFor(t, gpuJob))
	require.Equal(t, workers.Claimed, result.Disposition)
	require.Equal(t, gpuJob, result.Assignment.JobID)
}

// TestClaim_ARequeuedJobsOlderNotificationIsStale: a generation bump ends the
// earlier advertisement even though the job is QUEUED again, so the earlier
// event is released while the current one is held.
func TestClaim_ARequeuedJobsOlderNotificationIsStale(t *testing.T) {
	reset(t)
	store := controlStore()
	jobID := createJob(t, "requeued", "demo.echo", 50, []string{"gpu"})
	older := eventFor(t, jobID)
	// Open generation 2 the way a requeue does: one more event, one more
	// generation, in one statement pair.
	_, err := testPool.Exec(context.Background(),
		`UPDATE jobs SET notification_generation = 2, last_notification_at = clock_timestamp() WHERE id = $1`, jobID)
	require.NoError(t, err)
	current := uuid.New()
	_, err = testPool.Exec(context.Background(), `
		INSERT INTO outbox_events (id, event_type, schema_version, payload, job_id, notification_generation)
		VALUES ($1, 'work.available', 1, '{"queue":"default"}', $2, 2)`, current, jobID)
	require.NoError(t, err)
	cpu := registerWorker(t, store, workerRegistration("cpu", 1, []string{"cpu"}, []string{"demo.echo"}))

	require.Equal(t, workers.StaleNotification, claimWith(t, store, testScope, cpu, older).Disposition)
	require.Equal(t, workers.NoEligibleJob, claimWith(t, store, testScope, cpu, current).Disposition)
}

// TestClaim_AnUnattributedClaimKeepsItsDisposition: a claim identity that is
// not an outbox event carries no notification to decide relative to, so its
// answer is exactly the pre-ADR-0028 one.
func TestClaim_AnUnattributedClaimKeepsItsDisposition(t *testing.T) {
	reset(t)
	store := controlStore()
	createJob(t, "gpu-job", "demo.echo", 50, []string{"gpu"})
	cpu := registerWorker(t, store, workerRegistration("cpu", 1, []string{"cpu"}, []string{"demo.echo"}))
	require.Equal(t, workers.NoEligibleJob, claimWith(t, store, testScope, cpu, uuid.New()).Disposition)
}
