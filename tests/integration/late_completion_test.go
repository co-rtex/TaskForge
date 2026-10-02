//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/results"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// requireControlRejection asserts that a worker-control call was refused with
// the given stable error code, as the real worker client would see it.
func requireControlRejection(t *testing.T, err error, code string) {
	t.Helper()
	var remote *workerruntime.RemoteError
	require.ErrorAs(t, err, &remote, "the call must fail at the control plane, not in transit")
	require.Equal(t, http.StatusConflict, remote.Status)
	require.Equal(t, code, remote.Code)
}

// TestLateCompletion_AfterReassignmentIsRejectedAndOnlyTheReplacementCommits is
// scenario S3 end to end: the real HTTP control surface and the worker's own
// client, a real reconciliation pass, and a second worker session taking over.
//
// The sequence is the one that makes a zombie dangerous. Worker A's attempt 1
// loses its lease. Reconciliation abandons the attempt and requeues the job,
// worker B claims it as attempt 2, and only then does A -- whose session is
// still healthy, so no session fence applies -- report a success and a
// failure for attempt 1. Both must be refused while attempt 2 is still running,
// which is what distinguishes "rejected because it was stale" from "rejected
// because the job had already finished".
//
// Time is PostgreSQL's throughout (AGENTS.md section 6): the lease is aged with
// the database clock, and every decision is made by the control plane against
// its own post-lock sample.
func TestLateCompletion_AfterReassignmentIsRejectedAndOnlyTheReplacementCommits(t *testing.T) {
	reset(t)
	ctx := context.Background()
	server := newAPI(t)
	client := workerruntime.NewClient(server.URL, &http.Client{Timeout: 10 * time.Second}, currentWorkerKey())
	store := controlStore()

	sessionA, err := client.Register(ctx,
		workerRegistration("late-completion-a", 1, nil, []string{"demo.echo"}))
	require.NoError(t, err)
	sessionB, err := client.Register(ctx,
		workerRegistration("late-completion-b", 1, nil, []string{"demo.echo"}))
	require.NoError(t, err)
	jobID := createJobWithBudget(t, "late-completion", 3, 300)

	// --- attempt 1: worker A claims and starts -------------------------------
	claimA, err := client.Claim(ctx, claimRequest(sessionA, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claimA.Disposition)
	require.Equal(t, 1, claimA.Assignment.AttemptNumber)
	fenceA := assignmentFence(claimA.Assignment)
	_, err = client.Start(ctx, fenceA)
	require.NoError(t, err)
	require.Equal(t, jobState{job: "RUNNING", attempt: "RUNNING", lease: "ACTIVE"}, readState(t, fenceA))

	// --- the lease expires, and reconciliation abandons attempt 1 ------------
	expireLease(t, fenceA.LeaseID)
	pass, err := newReconciler(t, store, time.Minute).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, pass.ExpiredLeases)
	require.Equal(t, 1, pass.RequeuedJobs)
	require.Equal(t, jobState{job: "QUEUED", attempt: "ABANDONED", lease: "EXPIRED"}, readState(t, fenceA))
	require.Equal(t, "HEALTHY", sessionStatus(t, sessionA.ID),
		"worker A's process is alive and heartbeating: only lease state can refuse its late calls")

	// --- attempt 2: a different session claims and starts --------------------
	claimB, err := client.Claim(ctx, claimRequest(sessionB, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claimB.Disposition)
	require.Equal(t, jobID, claimB.Assignment.JobID)
	require.Equal(t, 2, claimB.Assignment.AttemptNumber)
	fenceB := assignmentFence(claimB.Assignment)
	require.NotEqual(t, fenceA.SessionID, fenceB.SessionID)
	require.NotEqual(t, fenceA.LeaseID, fenceB.LeaseID)
	_, err = client.Start(ctx, fenceB)
	require.NoError(t, err)
	require.Equal(t, jobState{job: "RUNNING", attempt: "RUNNING", lease: "ACTIVE"}, readState(t, fenceB))

	beforeLate := durableSnapshot(t, jobID)

	// --- attempt 1's late success, carrying a result that must never land ----
	staleBody := json.RawMessage(`{"from":"attempt-1"}`)
	err = client.Succeed(ctx, fenceA, &workers.ResultRef{
		Location: results.LocationInline, Inline: staleBody, SizeBytes: int64(len(staleBody)),
	})
	// The reason is pinned, not just the refusal: the lease is the specific
	// cause, and a different code would mean a different defense answered.
	requireControlRejection(t, err, api.CodeLeaseExpired)

	// --- attempt 1's late failure --------------------------------------------
	lateFailure := failureReport(fenceA, lifecycle.ClassRetryable, "late_failure", "reported after reassignment")
	_, err = client.Fail(ctx, lateFailure)
	requireControlRejection(t, err, api.CodeLeaseExpired)

	require.Equal(t, beforeLate, durableSnapshot(t, jobID),
		"two refused reports must leave every durable row byte-for-byte as it was")
	var retained int
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT count(*) FROM job_attempts WHERE outcome_request_id = $1`,
		lateFailure.OutcomeRequestID).Scan(&retained))
	require.Zero(t, retained, "a refused failure must not retain its outcome identity anywhere")
	require.Equal(t, jobState{job: "RUNNING", attempt: "RUNNING", lease: "ACTIVE"}, readState(t, fenceB),
		"attempt 2 is untouched and still the only live authority")

	// --- attempt 2 completes -------------------------------------------------
	winningBody := json.RawMessage(`{"from":"attempt-2"}`)
	require.NoError(t, client.Succeed(ctx, fenceB, &workers.ResultRef{
		Location: results.LocationInline, Inline: winningBody, SizeBytes: int64(len(winningBody)),
	}))

	// --- final durable state -------------------------------------------------
	require.Equal(t, "SUCCEEDED", readJob(t, jobID).status)
	require.Equal(t, []string{"ABANDONED", "SUCCEEDED"}, attemptHistory(t, jobID))
	require.Equal(t, []string{"EXPIRED", "COMPLETED"}, leaseHistory(t, jobID))
	require.Equal(t, 0, countActiveLeases(t))
	require.Empty(t, dlqRows(t, jobID), "a refused failure must not dead-letter or schedule a retry")

	// Exactly one result reference exists, and it is attempt 2's.
	require.Equal(t, 1, countRows(t, "results"))
	var resultAttempt uuid.UUID
	var resultBody string
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT attempt_id, inline_body::text FROM results WHERE job_id = $1`, jobID,
	).Scan(&resultAttempt, &resultBody))
	require.Equal(t, fenceB.AttemptID, resultAttempt)
	require.JSONEq(t, string(winningBody), resultBody)

	// The rows that record the accepted outcome carry attempt 2's fence values,
	// and the abandoned attempt still carries attempt 1's.
	var attemptID, workerID, sessionID, leaseID, leaseAttempt uuid.UUID
	require.NoError(t, testPool.QueryRow(ctx, `
		SELECT a.id, a.worker_id, a.worker_session_id, l.id, l.attempt_id
		FROM job_attempts a JOIN leases l ON l.attempt_id = a.id
		WHERE a.job_id = $1 AND a.status = 'SUCCEEDED' AND l.status = 'COMPLETED'`, jobID,
	).Scan(&attemptID, &workerID, &sessionID, &leaseID, &leaseAttempt))
	require.Equal(t, fenceB.AttemptID, attemptID)
	require.Equal(t, fenceB.WorkerID, workerID)
	require.Equal(t, fenceB.SessionID, sessionID)
	require.Equal(t, fenceB.LeaseID, leaseID)
	require.Equal(t, fenceB.AttemptID, leaseAttempt)

	abandoned := readAttemptOutcome(t, fenceA.AttemptID)
	require.Equal(t, "ABANDONED", abandoned.status)
	require.Nil(t, abandoned.outcomeID, "the abandoned attempt never acquired an outcome identity")
	var abandonedSession uuid.UUID
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT worker_session_id FROM job_attempts WHERE id = $1`, fenceA.AttemptID,
	).Scan(&abandonedSession))
	require.Equal(t, fenceA.SessionID, abandonedSession)
}
