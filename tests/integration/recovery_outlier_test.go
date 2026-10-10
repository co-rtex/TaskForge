//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/workers"
	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// TestRecoveryOutlier_ARestartedWorkersCapacityHoldsANotificationAndTheRecoveryClaimsAnOlderJob
// pins the mechanism M8D3 found behind the headline benchmark's 50.04 s recovery,
// step by step through the real store and with no clock involved. Every step is
// documented, intended behavior; what the test holds is that they combine as
// docs/CURRENT_STATE.md's M8D3 section says.
//
//  1. A killed worker's restarted boot is still charged for the dead boot's
//     active lease (ADR-0006), so at its limit a claim is CAPACITY_EXHAUSTED, and
//     that disposition is not safe to acknowledge (ADR-0003 step 5): the broker
//     keeps the message invisible until its visibility timeout lapses.
//  2. When the dead boot's lease is reconciled, the job is requeued with a fresh
//     notification, and a claim made with that notification takes the queue's
//     oldest eligible job (ADR-0003's ordering), which is the job whose own
//     notification was held in step 1, not the recovered job.
//  3. The recovered job is claimed only when the held notification comes back.
//
// In a running system step 3 waits for the visibility timeout, which is the S5
// excess the probe measured. Here it is a direct call, so the test is about the
// order of outcomes and not about how long anything takes.
func TestRecoveryOutlier_ARestartedWorkersCapacityHoldsANotificationAndTheRecoveryClaimsAnOlderJob(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()

	// A logical worker of two slots. Its first boot claims job X and is "killed":
	// nothing more is done with it, so X's lease stays ACTIVE.
	registration := workerRegistration("outlier-victim", 2, nil, []string{"demo.echo"})
	deadBoot := registerWorker(t, store, registration)
	x := claimedAndRunning(t, store, deadBoot, "outlier-x")

	// The restart: a new boot of the same logical worker. It claims job A, and now
	// holds one lease of its own plus the dead boot's, which is its limit.
	restarted := registerReplacement(t, store, registration)
	createJob(t, "outlier-a", "demo.echo", 50, nil)
	a, err := store.Claim(ctx, testScope, claimRequest(restarted, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, a.Disposition)

	// Job B's notification reaches the restarted boot, which has a free local slot
	// but no logical capacity: CAPACITY_EXHAUSTED, and not safe to acknowledge.
	jobB := createJob(t, "outlier-b", "demo.echo", 50, nil)
	eventB := firstWorkAvailableEvent(t, jobB)
	held, err := store.Claim(ctx, testScope, workers.ClaimRequest{
		WorkerID: restarted.WorkerID, SessionID: restarted.ID, ClaimRequestID: eventB, Queue: "default",
	})
	require.NoError(t, err)
	require.Equal(t, workers.CapacityExhausted, held.Disposition,
		"the dead boot's active lease still counts against the logical worker")
	require.False(t, held.SafeToAcknowledge(),
		"so the worker leaves B's message unacknowledged, and the broker holds it for its visibility timeout")

	// The dead boot's lease lapses and is reconciled: X is QUEUED again with a fresh
	// recovery notification. The expiry is set to the present instant, after X's
	// submission, so the recovery event is the only one in the lease's window.
	_, err = testPool.Exec(ctx, `UPDATE leases SET expires_at = clock_timestamp() WHERE id = $1`, x.LeaseID)
	require.NoError(t, err)
	stats, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.RequeuedJobs)
	eventX := latestWorkAvailableEvent(t, x.JobID)
	require.NotEqual(t, firstWorkAvailableEvent(t, x.JobID), eventX, "a fresh event, not X's submission event")

	// Another worker receives X's recovery notification. Its claim takes the oldest
	// eligible job, B, whose own message is the one being held.
	other := registerWorker(t, store, workerRegistration("outlier-other", 4, nil, []string{"demo.echo"}))
	recoveryClaim, err := store.Claim(ctx, testScope, workers.ClaimRequest{
		WorkerID: other.WorkerID, SessionID: other.ID, ClaimRequestID: eventX, Queue: "default",
	})
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, recoveryClaim.Disposition)
	require.Equal(t, jobB, recoveryClaim.Assignment.JobID,
		"the recovery notification claims B, which became eligible before X was requeued")
	require.Equal(t, "QUEUED", jobStatus(t, x.JobID), "X is still waiting, with no live notification")

	// B's held message comes back after its visibility timeout, and claims X.
	late, err := store.Claim(ctx, testScope, workers.ClaimRequest{
		WorkerID: other.WorkerID, SessionID: other.ID, ClaimRequestID: eventB, Queue: "default",
	})
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, late.Disposition)
	require.Equal(t, x.JobID, late.Assignment.JobID)

	// And the probe's timeline query tells exactly this story.
	hops, err := readdb.RecoveryTimeline(ctx, testPool, testScope, deadBoot.ID)
	require.NoError(t, err)
	require.Len(t, hops, 1)
	require.Equal(t, eventX, *hops[0].EventID)
	require.Equal(t, jobB, *hops[0].EventClaimedJobID, "the recovery event's own claim took another job")
	require.Equal(t, eventB, *hops[0].ReplacementEventID, "the replacement was claimed by B's held notification")
	require.Equal(t, other.ID, *hops[0].ReplacementSessionID)
}

func workAvailableEvents(t *testing.T, jobID uuid.UUID) []uuid.UUID {
	t.Helper()
	rows, err := testPool.Query(context.Background(), `
		SELECT id FROM outbox_events
		WHERE job_id = $1 AND event_type = 'work.available'
		ORDER BY created_at, id`, jobID)
	require.NoError(t, err)
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, ids)
	return ids
}

func firstWorkAvailableEvent(t *testing.T, jobID uuid.UUID) uuid.UUID {
	t.Helper()
	return workAvailableEvents(t, jobID)[0]
}

func latestWorkAvailableEvent(t *testing.T, jobID uuid.UUID) uuid.UUID {
	t.Helper()
	ids := workAvailableEvents(t, jobID)
	return ids[len(ids)-1]
}

func jobStatus(t *testing.T, jobID uuid.UUID) string {
	t.Helper()
	var status string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM jobs WHERE id = $1`, jobID).Scan(&status))
	return status
}
