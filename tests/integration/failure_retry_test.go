//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/workers"
)

func failureReport(fence workers.Fence, class lifecycle.FailureClass, code, message string) workers.FailureReport {
	return workers.FailureReport{
		Fence: fence, OutcomeRequestID: uuid.New(),
		Class: class, ErrorCode: code, ErrorMessage: message,
	}
}

// attemptOutcomeRow is what an operator and a replay both read back.
type attemptOutcomeRow struct {
	status       string
	failureClass *string
	errorCode    *string
	errorMessage *string
	retryDelayMs *int64
	retryAt      *time.Time
	outcomeID    *uuid.UUID
	timeoutAt    *time.Time
	finishedAt   *time.Time
}

func readAttemptOutcome(t *testing.T, attemptID uuid.UUID) attemptOutcomeRow {
	t.Helper()
	var row attemptOutcomeRow
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT status, failure_class, error_code, error_message,
		       retry_delay_ms, retry_at, outcome_request_id, timeout_at, finished_at
		FROM job_attempts WHERE id = $1`, attemptID,
	).Scan(&row.status, &row.failureClass, &row.errorCode, &row.errorMessage,
		&row.retryDelayMs, &row.retryAt, &row.outcomeID, &row.timeoutAt, &row.finishedAt))
	return row
}

type jobRow struct {
	status      string
	availableAt time.Time
	generation  int
}

func readJob(t *testing.T, jobID uuid.UUID) jobRow {
	t.Helper()
	var row jobRow
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT status, available_at, notification_generation FROM jobs WHERE id = $1`, jobID,
	).Scan(&row.status, &row.availableAt, &row.generation))
	return row
}

func dlqRows(t *testing.T, jobID uuid.UUID) []string {
	t.Helper()
	rows, err := testPool.Query(context.Background(),
		`SELECT reason FROM dlq_entries WHERE job_id = $1`, jobID)
	require.NoError(t, err)
	defer rows.Close()
	var reasons []string
	for rows.Next() {
		var reason string
		require.NoError(t, rows.Scan(&reason))
		reasons = append(reasons, reason)
	}
	require.NoError(t, rows.Err())
	return reasons
}

// TestFail_RetryableFailureEntersRetryWaitWithAPersistedDecision is the
// happy path of the retry story, and it asserts the two things a replay later
// depends on: the delay and the instant it produced are both persisted, and
// nothing is notified yet.
func TestFail_RetryableFailureEntersRetryWaitWithAPersistedDecision(t *testing.T) {
	reset(t)
	ctx := context.Background()
	// A seeded source makes the drawn delay a fact rather than a range.
	store := controlStoreWithJitter(lifecycle.NewSeededJitter(20260901))
	session := registerWorker(t, store,
		workerRegistration("retry-worker", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "retry-wait")
	outboxBefore := pendingOutboxIDs(t)

	before := serverNow(t)
	result, err := store.Fail(ctx, testScope,
		failureReport(fence, lifecycle.ClassRetryable, "upstream_5xx", "upstream returned 502"))
	require.NoError(t, err)
	require.Equal(t, "RETRY_WAIT", result.JobStatus)
	require.Equal(t, workers.AttemptFailed, result.AttemptStatus)
	require.NotNil(t, result.RetryAt)
	require.NotNil(t, result.RetryDelay)
	require.False(t, result.Replayed)

	attempt := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "FAILED", attempt.status)
	require.Equal(t, "RETRYABLE", *attempt.failureClass)
	require.Equal(t, "upstream_5xx", *attempt.errorCode)
	require.Equal(t, "upstream returned 502", *attempt.errorMessage)
	require.NotNil(t, attempt.finishedAt)
	require.NotNil(t, attempt.retryDelayMs)
	require.NotNil(t, attempt.retryAt)

	// The first attempt of a job waits the base delay, within the jitter band.
	require.GreaterOrEqual(t, *attempt.retryDelayMs, int64(800))
	require.LessOrEqual(t, *attempt.retryDelayMs, int64(1200))

	job := readJob(t, fence.JobID)
	require.Equal(t, "RETRY_WAIT", job.status)
	require.True(t, job.availableAt.Equal(*attempt.retryAt),
		"the job's eligibility must be exactly the persisted retry instant")
	require.True(t, attempt.retryAt.After(before),
		"retry_at is derived from PostgreSQL time sampled after the authority locks")

	state := readState(t, fence)
	require.Equal(t, "RELEASED", state.lease, "a reported failure hands authority back")
	require.Equal(t, 0, countActiveLeases(t), "capacity is released by the lease leaving ACTIVE")

	// A RETRY_WAIT job is durable and scheduled; advertising it now would wake a
	// worker for work it cannot claim until available_at passes.
	require.Empty(t, newPendingOutbox(t, outboxBefore),
		"no notification may exist before the scheduler promotes the job")
	require.Empty(t, dlqRows(t, fence.JobID))
	require.Equal(t, 1, job.generation, "RETRY_WAIT opens no new notification generation")
}

// TestFail_BackoffGrowsExponentiallyAcrossAttempts walks a job through its whole
// budget and checks that each attempt waits longer than the last.
func TestFail_BackoffGrowsExponentiallyAcrossAttempts(t *testing.T) {
	reset(t)
	ctx := context.Background()
	// No jitter: this is about growth, and a band would make the comparison
	// between consecutive attempts weaker than it needs to be.
	store := controlStoreWithJitter(nil)
	session := registerWorker(t, store,
		workerRegistration("backoff-worker", 1, nil, []string{"demo.echo"}))
	jobID := createJobWithBudget(t, "backoff", 6, 300)

	var delays []int64
	for attempt := 1; attempt <= 5; attempt++ {
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equalf(t, workers.Claimed, claim.Disposition, "attempt %d could not be claimed", attempt)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)

		_, err = store.Fail(ctx, testScope,
			failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
		require.NoError(t, err)

		row := readAttemptOutcome(t, fence.AttemptID)
		require.NotNil(t, row.retryDelayMs)
		delays = append(delays, *row.retryDelayMs)

		// Make the job eligible again without waiting out a real backoff.
		_, err = testPool.Exec(ctx,
			`UPDATE jobs SET status = 'QUEUED', available_at = clock_timestamp() WHERE id = $1`, jobID)
		require.NoError(t, err)
	}

	// base 1s, multiplier 2, cap 1m.
	require.Equal(t, []int64{1000, 2000, 4000, 8000, 16000}, delays)
}

// TestFail_BackoffIsCappedAndCannotOverflow pushes the attempt number far past
// anything the schema allows. math.Pow(2, 99) is +Inf, and converting +Inf to a
// Duration is undefined, so the cap has to apply before the conversion.
func TestFail_BackoffIsCappedAndCannotOverflow(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStoreWithJitter(nil)
	session := registerWorker(t, store,
		workerRegistration("cap-worker", 1, nil, []string{"demo.echo"}))

	jobID := createJobWithBudget(t, "cap", 100, 300)
	// Fabricate the attempt history a long-failing job would have, so the next
	// real attempt is number 40 rather than number 1.
	seedPriorAttempts(t, jobID, session, 39)

	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	require.Equal(t, 40, claim.Assignment.AttemptNumber)
	fence := assignmentFence(claim.Assignment)
	startAttempt(t, store, fence)

	_, err = store.Fail(ctx, testScope,
		failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
	require.NoError(t, err)

	row := readAttemptOutcome(t, fence.AttemptID)
	require.NotNil(t, row.retryDelayMs)
	require.EqualValues(t, time.Minute.Milliseconds(), *row.retryDelayMs,
		"the delay must clamp to the configured maximum, not overflow")
	require.Positive(t, *row.retryDelayMs)
	require.True(t, row.retryAt.After(serverNow(t).Add(-time.Second)))
}

// TestFail_PermanentFailureDeadLettersWithBudgetRemaining proves the
// classification is load-bearing: a permanent failure does not burn the
// remaining attempts first.
func TestFail_PermanentFailureDeadLettersWithBudgetRemaining(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("permanent-worker", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "permanent")
	outboxBefore := pendingOutboxIDs(t)

	result, err := store.Fail(ctx, testScope,
		failureReport(fence, lifecycle.ClassPermanent, "invalid_payload", "the payload names no known account"))
	require.NoError(t, err)
	require.Equal(t, "DEAD_LETTERED", result.JobStatus)
	require.Equal(t, lifecycle.ReasonPermanentFailure, result.DeadLetterReason)
	require.Nil(t, result.RetryAt)

	require.Equal(t, "DEAD_LETTERED", readJob(t, fence.JobID).status)
	require.Equal(t, []string{"PERMANENT_FAILURE"}, dlqRows(t, fence.JobID))
	require.Equal(t, 1, countRows(t, "job_attempts"),
		"a permanent failure must not consume the remaining attempt budget")
	require.Equal(t, 0, countActiveLeases(t))
	require.Empty(t, newPendingOutbox(t, outboxBefore),
		"a dead-lettered job is terminal and must never be advertised again")

	attempt := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "FAILED", attempt.status,
		"the attempt keeps its truthful status; EXHAUSTED is a job-level reason, not an attempt status")
	require.Equal(t, "PERMANENT", *attempt.failureClass)
	require.Nil(t, attempt.retryAt, "a dead-lettered job has no retry to schedule")
}

// TestFail_ExhaustedBudgetDeadLettersExactlyOnce walks a job to the end of its
// budget and proves the final attempt keeps its own truthful status.
func TestFail_ExhaustedBudgetDeadLettersExactlyOnce(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("exhaust-worker", 1, nil, []string{"demo.echo"}))
	jobID := createJobWithBudget(t, "exhaust", 2, 300)

	for attempt := 1; attempt <= 2; attempt++ {
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, claim.Disposition)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)

		result, err := store.Fail(ctx, testScope,
			failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
		require.NoError(t, err)
		if attempt == 1 {
			require.Equal(t, "RETRY_WAIT", result.JobStatus)
			_, err = testPool.Exec(ctx,
				`UPDATE jobs SET status = 'QUEUED', available_at = clock_timestamp() WHERE id = $1`, jobID)
			require.NoError(t, err)
			continue
		}
		require.Equal(t, "DEAD_LETTERED", result.JobStatus)
		require.Equal(t, lifecycle.ReasonAttemptsExhausted, result.DeadLetterReason)
	}

	require.Equal(t, []string{"FAILED", "FAILED"}, attemptHistory(t, jobID))
	require.Equal(t, []string{"ATTEMPTS_EXHAUSTED"}, dlqRows(t, jobID))
	require.Equal(t, "DEAD_LETTERED", readJob(t, jobID).status)
}

// TestFail_AbandonmentAndFailureShareOneDLQTable is the boundary ADR-0009 drew.
// M3 reached DEAD_LETTERED through a completely different path; after M4 both
// paths must produce an entry in the same table through the same helper, or the
// DLQ would list only half the dead-lettered jobs.
func TestFail_AbandonmentAndFailureShareOneDLQTable(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("shared-dlq", 1, nil, []string{"demo.echo"}))

	// One job exhausts its budget through reported failures.
	failedJob := createJobWithBudget(t, "shared-dlq-failed", 1, 300)
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	failedFence := assignmentFence(claim.Assignment)
	startAttempt(t, store, failedFence)
	_, err = store.Fail(ctx, testScope,
		failureReport(failedFence, lifecycle.ClassRetryable, "transient", ""))
	require.NoError(t, err)

	// Another exhausts it through M3 abandonment, untouched by M4.
	abandonedJob := createJobWithBudget(t, "shared-dlq-abandoned", 1, 300)
	claim, err = store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	abandonedFence := assignmentFence(claim.Assignment)
	startAttempt(t, store, abandonedFence)
	expireLease(t, abandonedFence.LeaseID)
	stats, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.DeadLetteredJobs)

	require.Equal(t, []string{"ATTEMPTS_EXHAUSTED"}, dlqRows(t, failedJob))
	require.Equal(t, []string{"ATTEMPTS_EXHAUSTED"}, dlqRows(t, abandonedJob))
	require.Equal(t, 2, countRows(t, "dlq_entries"))

	// The attempts differ, truthfully, even though the job-level reason is the
	// same: one was judged and one was interrupted.
	require.Equal(t, []string{"FAILED"}, attemptHistory(t, failedJob))
	require.Equal(t, []string{"ABANDONED"}, attemptHistory(t, abandonedJob))
	abandoned := readAttemptOutcome(t, abandonedFence.AttemptID)
	require.Equal(t, "ABANDONED", *abandoned.failureClass)
	require.Nil(t, abandoned.outcomeID, "nobody requested the abandonment, so there is no identity to retain")
}

// TestFail_AbandonmentWithBudgetRemainingStaysImmediateRecovery pins ADR-0009's
// decision against the most likely M4 regression: quietly routing abandonment
// through the retry policy would give crash recovery a backoff it must not have.
func TestFail_AbandonmentWithBudgetRemainingStaysImmediateRecovery(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("adr9-worker", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "adr9")
	outboxBefore := pendingOutboxIDs(t)
	expireLease(t, fence.LeaseID)

	before := serverNow(t)
	stats, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.RequeuedJobs)
	require.Zero(t, stats.RetryWaitingJobs, "crash recovery is not retry")

	job := readJob(t, fence.JobID)
	require.Equal(t, "QUEUED", job.status, "not RETRY_WAIT: the work was interrupted, not judged")
	require.False(t, job.availableAt.After(serverNow(t)),
		"a recovered job is claimable immediately, with no backoff")
	require.True(t, !job.availableAt.Before(before))
	require.Equal(t, 2, job.generation, "becoming QUEUED again opens a new notification generation")

	row := readAttemptOutcome(t, fence.AttemptID)
	require.Equal(t, "ABANDONED", row.status)
	require.NotNil(t, row.retryAt, "the decision is recorded, with a zero delay")
	require.EqualValues(t, 0, *row.retryDelayMs)

	// Unlike RETRY_WAIT, immediate recovery IS advertised, because the job is
	// claimable now and nothing else will wake a replacement worker.
	added := newPendingOutbox(t, outboxBefore)
	require.Len(t, added, 1, "exactly one fresh recovery notification")
	var eventJob uuid.UUID
	var generation int
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT job_id, notification_generation FROM outbox_events WHERE id = $1`,
		added[0]).Scan(&eventJob, &generation))
	require.Equal(t, fence.JobID, eventJob)
	require.Equal(t, 2, generation, "the event advertises the CURRENT eligibility generation")
}

// TestFail_IsFencedByEveryIdentifier proves a failure report is refused unless
// every part of the fence is the current one.
func TestFail_IsFencedByEveryIdentifier(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("fenced-fail", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "fenced-fail")

	for name, mutate := range map[string]func(workers.Fence) workers.Fence{
		"wrong job":     func(f workers.Fence) workers.Fence { f.JobID = uuid.New(); return f },
		"wrong attempt": func(f workers.Fence) workers.Fence { f.AttemptID = uuid.New(); return f },
		"wrong lease":   func(f workers.Fence) workers.Fence { f.LeaseID = uuid.New(); return f },
		"wrong worker":  func(f workers.Fence) workers.Fence { f.WorkerID = uuid.New(); return f },
		"wrong session": func(f workers.Fence) workers.Fence { f.SessionID = uuid.New(); return f },
	} {
		t.Run(name, func(t *testing.T) {
			_, err := store.Fail(ctx, testScope,
				failureReport(mutate(fence), lifecycle.ClassRetryable, "transient", ""))
			require.ErrorIs(t, err, workers.ErrFenceRejected)
		})
	}

	require.Equal(t, "RUNNING", readAttemptOutcome(t, fence.AttemptID).status,
		"a rejected report must mutate nothing")
	require.Equal(t, "RUNNING", readJob(t, fence.JobID).status)
}

// TestFail_ExactReplayReturnsTheCommittedDecision is the ambiguity contract. A
// worker whose failure report committed but whose response was lost must be
// able to retry, and the retry must return the SAME decision — not consume
// budget again, not redraw jitter, not create a second dead-letter entry.
func TestFail_ExactReplayReturnsTheCommittedDecision(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStoreWithJitter(lifecycle.NewSeededJitter(7))
	session := registerWorker(t, store,
		workerRegistration("replay-fail", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "replay-fail")

	report := failureReport(fence, lifecycle.ClassRetryable, "upstream_5xx", "upstream returned 502")
	first, err := store.Fail(ctx, testScope, report)
	require.NoError(t, err)
	require.False(t, first.Replayed)
	committed := readAttemptOutcome(t, fence.AttemptID)

	for i := 0; i < 3; i++ {
		replay, err := store.Fail(ctx, testScope, report)
		require.NoError(t, err)
		require.True(t, replay.Replayed)
		require.Equal(t, first.JobStatus, replay.JobStatus)
		require.True(t, first.RetryAt.Equal(*replay.RetryAt),
			"a replay must return the committed instant, never a freshly computed one")
		require.Equal(t, *first.RetryDelay, *replay.RetryDelay)
	}

	require.Equal(t, committed, readAttemptOutcome(t, fence.AttemptID),
		"a replay must not move a single stored field")
	require.Equal(t, 1, countRows(t, "job_attempts"), "a replay must not consume budget again")
	require.Empty(t, dlqRows(t, fence.JobID))

	job := readJob(t, fence.JobID)
	require.Equal(t, "RETRY_WAIT", job.status)
	require.True(t, job.availableAt.Equal(*committed.retryAt),
		"a replay must not push the job's eligibility forward")
}

// TestFail_ReplayAtTheEndOfTheBudgetDoesNotDuplicateTheDLQEntry is the same
// contract at the point where getting it wrong is most expensive.
func TestFail_ReplayAtTheEndOfTheBudgetDoesNotDuplicateTheDLQEntry(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("replay-dlq", 1, nil, []string{"demo.echo"}))
	createJobWithBudget(t, "replay-dlq", 1, 300)

	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)
	startAttempt(t, store, fence)

	report := failureReport(fence, lifecycle.ClassPermanent, "invalid_payload", "")
	first, err := store.Fail(ctx, testScope, report)
	require.NoError(t, err)
	require.Equal(t, "DEAD_LETTERED", first.JobStatus)

	replay, err := store.Fail(ctx, testScope, report)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Equal(t, "DEAD_LETTERED", replay.JobStatus)
	require.Equal(t, lifecycle.ReasonPermanentFailure, replay.DeadLetterReason)

	require.Equal(t, []string{"PERMANENT_FAILURE"}, dlqRows(t, fence.JobID),
		"an ambiguous failure retry must never create a second dead-letter entry")
	require.Equal(t, 1, countRows(t, "dlq_entries"))
}

// TestFail_OutcomeIdentityIsRetainedForTheLifetimeOfHistory covers the two ways
// an identity can be misused, each of which must be a stable domain conflict
// rather than a leaked uniqueness error.
func TestFail_OutcomeIdentityIsRetainedForTheLifetimeOfHistory(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("identity-fail", 2, nil, []string{"demo.echo"}))

	firstJob := createJobWithBudget(t, "identity-one", 3, 300)
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	firstFence := assignmentFence(claim.Assignment)
	startAttempt(t, store, firstFence)

	report := failureReport(firstFence, lifecycle.ClassRetryable, "transient", "first")
	_, err = store.Fail(ctx, testScope, report)
	require.NoError(t, err)

	t.Run("replaying with a different body is a conflict", func(t *testing.T) {
		for _, changed := range []workers.FailureReport{
			{Fence: firstFence, OutcomeRequestID: report.OutcomeRequestID,
				Class: lifecycle.ClassPermanent, ErrorCode: "transient", ErrorMessage: "first"},
			{Fence: firstFence, OutcomeRequestID: report.OutcomeRequestID,
				Class: lifecycle.ClassRetryable, ErrorCode: "different_code", ErrorMessage: "first"},
			{Fence: firstFence, OutcomeRequestID: report.OutcomeRequestID,
				Class: lifecycle.ClassRetryable, ErrorCode: "transient", ErrorMessage: "second"},
		} {
			_, err := store.Fail(ctx, testScope, changed)
			require.ErrorIs(t, err, workers.ErrOutcomeConflict,
				"the caller is claiming the committed decision rested on inputs it did not")
		}
		require.Equal(t, "RETRYABLE", *readAttemptOutcome(t, firstFence.AttemptID).failureClass)
	})

	t.Run("reusing the identity for another attempt is a conflict", func(t *testing.T) {
		// A second, unrelated attempt on a different job.
		createJobWithBudget(t, "identity-two", 3, 300)
		second, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, second.Disposition)
		secondFence := assignmentFence(second.Assignment)
		require.NotEqual(t, firstFence.AttemptID, secondFence.AttemptID)
		startAttempt(t, store, secondFence)

		_, err = store.Fail(ctx, testScope, workers.FailureReport{
			Fence: secondFence, OutcomeRequestID: report.OutcomeRequestID,
			Class: lifecycle.ClassRetryable, ErrorCode: "transient", ErrorMessage: "first",
		})
		require.ErrorIs(t, err, workers.ErrOutcomeConflict)
		require.Equal(t, "RUNNING", readAttemptOutcome(t, secondFence.AttemptID).status,
			"the rejected report must mutate nothing")
	})

	// The identity is retained forever, unlike a renewal identity, which is
	// superseded by the next generation (ADR-0008's scope note).
	t.Run("the identity is still taken after the job is terminal", func(t *testing.T) {
		var owner uuid.UUID
		require.NoError(t, testPool.QueryRow(ctx,
			`SELECT id FROM job_attempts WHERE outcome_request_id = $1`,
			report.OutcomeRequestID).Scan(&owner))
		require.Equal(t, firstFence.AttemptID, owner)
		_ = firstJob
	})
}

// TestFail_ServerAuthoritativeClassesAreRejected proves a worker cannot declare
// an outcome only PostgreSQL may decide about it.
func TestFail_ServerAuthoritativeClassesAreRejected(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("class-guard", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "class-guard")

	for _, class := range []lifecycle.FailureClass{
		lifecycle.ClassTimedOut, lifecycle.ClassCanceled, lifecycle.ClassAbandoned, "MADE_UP",
	} {
		_, err := store.Fail(ctx, testScope, failureReport(fence, class, "transient", ""))
		var validation *workers.ValidationError
		require.ErrorAsf(t, err, &validation, "class %s must be rejected", class)
	}
	require.Equal(t, "RUNNING", readAttemptOutcome(t, fence.AttemptID).status)
}

// TestFail_BoundedErrorDetailIsEnforcedBeforeTheDatabase proves the Go
// validator refuses what the CHECK constraints would refuse, so a caller gets a
// field-level 422 rather than a leaked constraint violation.
func TestFail_BoundedErrorDetailIsEnforcedBeforeTheDatabase(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("bounds-guard", 1, nil, []string{"demo.echo"}))
	fence := claimedAndRunning(t, store, session, "bounds-guard")

	oversized := make([]byte, lifecycle.MaxErrorMessageBytes+1)
	for i := range oversized {
		oversized[i] = 'm'
	}
	for name, report := range map[string]workers.FailureReport{
		"empty code":       failureReport(fence, lifecycle.ClassRetryable, "", "ok"),
		"uppercase code":   failureReport(fence, lifecycle.ClassRetryable, "Transient", "ok"),
		"spaced code":      failureReport(fence, lifecycle.ClassRetryable, "transient failure", "ok"),
		"oversized code":   failureReport(fence, lifecycle.ClassRetryable, string(oversized), "ok"),
		"newline message":  failureReport(fence, lifecycle.ClassRetryable, "transient", "line\nbreak"),
		"control message":  failureReport(fence, lifecycle.ClassRetryable, "transient", "bell\x07"),
		"oversize message": failureReport(fence, lifecycle.ClassRetryable, "transient", string(oversized)),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := store.Fail(ctx, testScope, report)
			var validation *workers.ValidationError
			require.ErrorAs(t, err, &validation)
		})
	}
	require.Equal(t, "RUNNING", readAttemptOutcome(t, fence.AttemptID).status)
}

// TestFail_RequiresARunningAttemptUnderALiveLease covers the states from which
// a failure cannot be reported at all.
func TestFail_RequiresARunningAttemptUnderALiveLease(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()

	t.Run("an expired lease cannot report a failure", func(t *testing.T) {
		reset(t)
		session := registerWorker(t, store,
			workerRegistration("expired-fail", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "expired-fail")
		expireLease(t, fence.LeaseID)

		_, err := store.Fail(ctx, testScope,
			failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
		require.ErrorIs(t, err, workers.ErrLeaseExpired)
		require.Equal(t, "RUNNING", readAttemptOutcome(t, fence.AttemptID).status)
	})

	t.Run("a succeeded attempt cannot then fail", func(t *testing.T) {
		reset(t)
		session := registerWorker(t, store,
			workerRegistration("succeeded-fail", 1, nil, []string{"demo.echo"}))
		fence := claimedAndRunning(t, store, session, "succeeded-fail")
		require.NoError(t, store.Succeed(ctx, testScope, fence))

		_, err := store.Fail(ctx, testScope,
			failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
		require.ErrorIs(t, err, workers.ErrLeaseExpired,
			"a completed lease is no longer authority")
		require.Equal(t, "SUCCEEDED", readAttemptOutcome(t, fence.AttemptID).status)
	})

	t.Run("a stale session cannot report a failure", func(t *testing.T) {
		reset(t)
		registration := workerRegistration("stale-fail", 1, nil, []string{"demo.echo"})
		session := registerWorker(t, store, registration)
		fence := claimedAndRunning(t, store, session, "stale-fail")
		registerReplacement(t, store, registration)

		_, err := store.Fail(ctx, testScope,
			failureReport(fence, lifecycle.ClassRetryable, "transient", ""))
		require.ErrorIs(t, err, workers.ErrFenceRejected)
		require.Equal(t, "RUNNING", readAttemptOutcome(t, fence.AttemptID).status)
	})
}

// createJobWithBudget submits a job with an explicit attempt budget and timeout.
func createJobWithBudget(t *testing.T, key string, maxAttempts, timeoutSeconds int) uuid.UUID {
	t.Helper()
	return createJobWithOptions(t, key, "default", "demo.echo", 50, nil,
		maxAttempts, timeoutSeconds, nil)
}

// seedPriorAttempts fabricates finished attempt history so a test can reach a
// high attempt number without executing the intervening attempts.
//
// The rows are written directly because what is under test is the arithmetic
// applied to attempt NUMBER, not the path that produced the earlier attempts.
func seedPriorAttempts(t *testing.T, jobID uuid.UUID, session workers.Session, count int) {
	t.Helper()
	for i := 1; i <= count; i++ {
		_, err := testPool.Exec(context.Background(), `
			INSERT INTO job_attempts (
				id, job_id, scope, queue, attempt_number, worker_id, worker_session_id,
				status, started_at, finished_at, failure_class, error_code
			) VALUES (gen_random_uuid(), $1, $2, 'default', $3, $4, $5,
			          'FAILED', clock_timestamp(), clock_timestamp(), 'RETRYABLE', 'transient')`,
			jobID, testScope, i, session.WorkerID, session.ID)
		require.NoError(t, err)
	}
}
