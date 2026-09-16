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

// leaseOutcome describes what one committed terminal outcome left behind, so a
// replay can be checked against the exact durable state rather than against a
// remembered expectation.
type leaseOutcome struct {
	job     string
	attempt string
	lease   string
}

func readOutcomeState(t *testing.T, fence workers.Fence) leaseOutcome {
	t.Helper()
	var state leaseOutcome
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT j.status, a.status, l.status
		FROM jobs j
		JOIN job_attempts a ON a.job_id = j.id
		JOIN leases l ON l.attempt_id = a.id
		WHERE j.id = $1 AND a.id = $2 AND l.id = $3`,
		fence.JobID, fence.AttemptID, fence.LeaseID,
	).Scan(&state.job, &state.attempt, &state.lease))
	return state
}

// replaceSession registers a second boot of the same worker, which takes the
// previous session OFFLINE. Every fence issued to the old boot is then unusable
// for new authority.
func replaceSession(t *testing.T, store *workers.Store, registration workers.Registration) workers.Session {
	t.Helper()
	replacement := registration
	replacement.SessionID = uuid.New()
	session, err := store.Register(context.Background(), testScope, replacement)
	require.NoError(t, err)
	require.Equal(t, workers.SessionHealthy, session.Status)
	return session
}

// closeLease drives one lease past its window and lets reconciliation close it,
// which is what happens to a worker that reports its outcome after a network
// partition outlasted the lease.
func closeLease(t *testing.T, fence workers.Fence) {
	t.Helper()
	expireLease(t, fence.LeaseID)
}

// TestOutcomeReplay_CommittedHistoryIsRecognizedWithoutLiveAuthority is the
// distinction the whole retained-outcome-identity design rests on.
//
// Reporting an outcome for the first time is a mutation, and a mutation needs
// current authority: the session has to be HEALTHY, or a replaced boot could
// overwrite work its replacement is doing. Recognizing an outcome that is
// already committed is not a mutation. It reads immutable history and returns
// what is already there.
//
// Conflating the two is what made an ambiguous response unrecoverable in
// practice. The response is lost precisely when the network is bad; the worker
// then reconnects, and reconnecting is exactly what replaces its session or
// lapses its lease. A replay refused because the fence is no longer live is a
// replay refused in every case it was built for.
func TestOutcomeReplay_CommittedHistoryIsRecognizedWithoutLiveAuthority(t *testing.T) {
	ctx := context.Background()

	// invalidate is how the fence stopped being live. Both are ordinary: a
	// worker restart replaces the session, and a partition longer than the lease
	// window closes the lease.
	invalidations := map[string]func(t *testing.T, store *workers.Store, registration workers.Registration, fence workers.Fence){
		"after the session was replaced": func(t *testing.T, store *workers.Store, registration workers.Registration, _ workers.Fence) {
			replaceSession(t, store, registration)
		},
		"after the lease expired": func(t *testing.T, _ *workers.Store, _ workers.Registration, fence workers.Fence) {
			closeLease(t, fence)
		},
	}

	for name, invalidate := range invalidations {
		t.Run("success replays "+name, func(t *testing.T) {
			reset(t)
			store := controlStore()
			registration := workerRegistration("replay-success", 1, nil, []string{"demo.echo"})
			session := registerWorker(t, store, registration)
			createJob(t, "replay-success", "demo.echo", 50, nil)
			claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
			require.NoError(t, err)
			fence := assignmentFence(claim.Assignment)
			startAttempt(t, store, fence)

			require.NoError(t, store.Succeed(ctx, testScope, fence, nil))
			committed := readOutcomeState(t, fence)
			require.Equal(t, leaseOutcome{"SUCCEEDED", "SUCCEEDED", "COMPLETED"}, committed)

			invalidate(t, store, registration, fence)

			require.NoError(t, store.Succeed(ctx, testScope, fence, nil),
				"a committed success must still be recognized once its fence is no longer live")
			require.Equal(t, committed, readOutcomeState(t, fence),
				"a replay reads history; it must not write any part of it again")
			require.Equal(t, 1, countRows(t, "job_attempts"))
			require.Equal(t, 1, countRows(t, "leases"))
			require.Equal(t, 0, countActiveLeases(t))
		})

		t.Run("failure replays "+name, func(t *testing.T) {
			reset(t)
			store := controlStore()
			registration := workerRegistration("replay-failure", 1, nil, []string{"demo.echo"})
			session := registerWorker(t, store, registration)
			createJobWithOptions(t, "replay-failure", "default", "demo.echo", 50, nil, 3, 300, nil)
			claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
			require.NoError(t, err)
			fence := assignmentFence(claim.Assignment)
			startAttempt(t, store, fence)

			report := failureReport(fence, lifecycle.ClassRetryable, "upstream_5xx", "upstream returned 502")
			first, err := store.Fail(ctx, testScope, report)
			require.NoError(t, err)
			require.False(t, first.Replayed)
			require.NotNil(t, first.RetryAt)
			committed := readOutcomeState(t, fence)

			invalidate(t, store, registration, fence)

			replayed, err := store.Fail(ctx, testScope, report)
			require.NoError(t, err,
				"a committed failure must still be recognized once its fence is no longer live")
			require.True(t, replayed.Replayed)
			require.Equal(t, first.JobStatus, replayed.JobStatus)
			require.Equal(t, first.AttemptStatus, replayed.AttemptStatus)
			require.NotNil(t, replayed.RetryAt)
			// Read back from the attempt, never recomputed: recomputing would
			// draw fresh jitter and answer a different instant every time.
			require.WithinDuration(t, *first.RetryAt, *replayed.RetryAt, 0,
				"a replayed retry instant must be the one that committed")
			require.Equal(t, *first.RetryDelay, *replayed.RetryDelay)
			require.Equal(t, committed, readOutcomeState(t, fence))
			require.Equal(t, 1, countRows(t, "job_attempts"),
				"a replay must not consume another attempt")
		})

		t.Run("cancellation acknowledgment replays "+name, func(t *testing.T) {
			reset(t)
			store := controlStore()
			registration := workerRegistration("replay-cancel", 1, nil, []string{"demo.echo"})
			session := registerWorker(t, store, registration)
			createJob(t, "replay-cancel", "demo.echo", 50, nil)
			claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
			require.NoError(t, err)
			fence := assignmentFence(claim.Assignment)
			startAttempt(t, store, fence)

			_, err = jobStore().RequestCancel(ctx, testScope, fence.JobID)
			require.NoError(t, err)

			ack := cancelAck(fence)
			first, err := store.AcknowledgeCancellation(ctx, testScope, ack)
			require.NoError(t, err)
			require.False(t, first.Replayed)
			require.Equal(t, workers.AttemptCanceled, first.AttemptStatus)
			committed := readOutcomeState(t, fence)

			invalidate(t, store, registration, fence)

			replayed, err := store.AcknowledgeCancellation(ctx, testScope, ack)
			require.NoError(t, err,
				"a committed cancellation acknowledgment must still be recognized")
			require.True(t, replayed.Replayed)
			require.Equal(t, first.JobStatus, replayed.JobStatus)
			require.Equal(t, first.AttemptStatus, replayed.AttemptStatus)
			require.Equal(t, committed, readOutcomeState(t, fence))
			require.Equal(t, 1, countRows(t, "job_attempts"))
		})
	}
}

// TestOutcomeReplay_RecognitionIsExactAndNothingElse is what keeps the rule
// above from becoming "an expired fence can do whatever it likes".
//
// Recognition is not leniency. Every part of the stored fence, the retained
// outcome identity, and the reported body has to match before history is
// returned; anything else is a deterministic conflict, whether the session is
// healthy or not.
func TestOutcomeReplay_RecognitionIsExactAndNothingElse(t *testing.T) {
	ctx := context.Background()

	setup := func(t *testing.T) (*workers.Store, workers.Registration, workers.Fence, workers.FailureReport) {
		t.Helper()
		reset(t)
		store := controlStore()
		registration := workerRegistration("replay-exact", 1, nil, []string{"demo.echo"})
		session := registerWorker(t, store, registration)
		createJobWithOptions(t, "replay-exact", "default", "demo.echo", 50, nil, 3, 300, nil)
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)

		report := failureReport(fence, lifecycle.ClassRetryable, "upstream_5xx", "upstream returned 502")
		_, err = store.Fail(ctx, testScope, report)
		require.NoError(t, err)
		return store, registration, fence, report
	}

	t.Run("a changed body is a conflict, not a replay", func(t *testing.T) {
		store, registration, _, report := setup(t)
		replaceSession(t, store, registration)

		for name, mutate := range map[string]func(workers.FailureReport) workers.FailureReport{
			"different class": func(r workers.FailureReport) workers.FailureReport {
				r.Class = lifecycle.ClassPermanent
				return r
			},
			"different code": func(r workers.FailureReport) workers.FailureReport {
				r.ErrorCode = "upstream_4xx"
				return r
			},
			"different message": func(r workers.FailureReport) workers.FailureReport {
				r.ErrorMessage = "upstream returned 503"
				return r
			},
		} {
			_, err := store.Fail(ctx, testScope, mutate(report))
			require.ErrorIsf(t, err, workers.ErrOutcomeConflict,
				"%s must be a conflict: the same identity cannot describe two outcomes", name)
		}
	})

	t.Run("a foreign identity is a conflict, not a replay", func(t *testing.T) {
		store, registration, _, report := setup(t)
		replaceSession(t, store, registration)

		fresh := report
		fresh.OutcomeRequestID = uuid.New()
		_, err := store.Fail(ctx, testScope, fresh)
		require.ErrorIs(t, err, workers.ErrFenceRejected,
			"an identity this attempt never retained is a first-time outcome, which needs live authority")
	})

	t.Run("a different fence is a conflict, not a replay", func(t *testing.T) {
		store, registration, fence, report := setup(t)
		replaceSession(t, store, registration)

		for name, mutate := range map[string]func(workers.Fence) workers.Fence{
			"foreign job":     func(f workers.Fence) workers.Fence { f.JobID = uuid.New(); return f },
			"foreign attempt": func(f workers.Fence) workers.Fence { f.AttemptID = uuid.New(); return f },
			"foreign lease":   func(f workers.Fence) workers.Fence { f.LeaseID = uuid.New(); return f },
			"foreign worker":  func(f workers.Fence) workers.Fence { f.WorkerID = uuid.New(); return f },
			"foreign session": func(f workers.Fence) workers.Fence { f.SessionID = uuid.New(); return f },
		} {
			wrong := report
			wrong.Fence = mutate(fence)
			_, err := store.Fail(ctx, testScope, wrong)
			require.Errorf(t, err, "%s must never be recognized as this attempt's history", name)
			require.NotErrorIsf(t, err, workers.ErrOutcomeConflict,
				"%s is not an identity reuse; it names a fence this outcome never had", name)
		}
	})

	t.Run("a first-time outcome still requires live authority", func(t *testing.T) {
		reset(t)
		store := controlStore()
		registration := workerRegistration("replay-authority", 1, nil, []string{"demo.echo"})
		session := registerWorker(t, store, registration)
		createJobWithOptions(t, "replay-authority", "default", "demo.echo", 50, nil, 3, 300, nil)
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)

		// Nothing has committed for this attempt yet. Every terminal transition
		// is therefore a mutation, and a replaced boot has no authority for one.
		replaceSession(t, store, registration)

		require.ErrorIs(t, store.Succeed(ctx, testScope, fence, nil), workers.ErrFenceRejected)
		_, err = store.Fail(ctx, testScope,
			failureReport(fence, lifecycle.ClassRetryable, "upstream_5xx", "upstream returned 502"))
		require.ErrorIs(t, err, workers.ErrFenceRejected)
		_, err = store.AcknowledgeCancellation(ctx, testScope, cancelAck(fence))
		require.ErrorIs(t, err, workers.ErrFenceRejected)
		_, err = store.Start(ctx, testScope, fence)
		require.ErrorIs(t, err, workers.ErrFenceRejected)

		require.Equal(t, leaseOutcome{"RUNNING", "RUNNING", "ACTIVE"}, readOutcomeState(t, fence),
			"a refused first-time outcome must leave the attempt exactly as it was")
	})

	t.Run("one attempt's outcome identity cannot describe another attempt", func(t *testing.T) {
		store, registration, _, report := setup(t)

		// A second job, claimed by a live session, so authority is not what is
		// being tested here.
		createJobWithOptions(t, "replay-exact-second", "default", "demo.echo", 50, nil, 3, 300, nil)
		session := replaceSession(t, store, registration)
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		other := assignmentFence(claim.Assignment)
		startAttempt(t, store, other)

		reused := report
		reused.Fence = other
		_, err = store.Fail(ctx, testScope, reused)
		require.ErrorIs(t, err, workers.ErrOutcomeConflict,
			"an outcome identity is retained for the lifetime of history and belongs to one attempt")
	})
}

// makeRetryDue moves a RETRY_WAIT job's eligibility instant into the past so the
// real scheduler promotes it now.
//
// Only available_at is touched. The backoff itself is real and already committed;
// waiting it out would be a sleep, and AGENTS.md section 7 forbids those. Nothing
// about the attempt's recorded outcome is altered, which is what the assertions
// below are actually about.
func makeRetryDue(t *testing.T, jobID uuid.UUID) {
	t.Helper()
	tag, err := testPool.Exec(context.Background(),
		`UPDATE jobs SET available_at = clock_timestamp() - interval '1 second'
		 WHERE id = $1 AND status = 'RETRY_WAIT'`, jobID)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected(), "the job must be waiting on a retry")
}

// TestOutcomeReplay_AnswersTheDecisionThatCommittedNotWhatTheJobDidNext is the
// case the other replay tests stop short of.
//
// A retryable failure puts the job into RETRY_WAIT and leaves the attempt
// terminal. The job then moves on: the scheduler promotes it, a new attempt
// claims it, and that attempt succeeds. None of that changes the original
// attempt's outcome, and none of it may change what replaying that outcome
// answers.
//
// Reading the live job row would report SUCCEEDED — a value the Outcome contract
// does not even permit, and a direct contradiction of the promise that an
// ambiguous report returns the decision that committed. The decision is
// reconstructed from the attempt alone, which is immutable once terminal.
func TestOutcomeReplay_AnswersTheDecisionThatCommittedNotWhatTheJobDidNext(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	registration := workerRegistration("replay-advances", 1, nil, []string{"demo.echo"})
	session := registerWorker(t, store, registration)
	createJobWithOptions(t, "replay-advances", "default", "demo.echo", 50, nil, 3, 300, nil)

	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	first := assignmentFence(claim.Assignment)
	startAttempt(t, store, first)

	report := failureReport(first, lifecycle.ClassRetryable, "upstream_5xx", "upstream returned 502")
	committed, err := store.Fail(ctx, testScope, report)
	require.NoError(t, err)
	require.False(t, committed.Replayed)
	require.Equal(t, "RETRY_WAIT", committed.JobStatus)
	require.NotNil(t, committed.RetryAt)
	require.NotNil(t, committed.RetryDelay)

	// Drive the job all the way past the failure: due, promoted, claimed again,
	// and completed successfully.
	makeRetryDue(t, first.JobID)
	stats, err := jobStore().PromoteDueJobs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.PromotedJobs)

	claim, err = store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	second := assignmentFence(claim.Assignment)
	require.NotEqual(t, first.AttemptID, second.AttemptID)
	startAttempt(t, store, second)
	require.NoError(t, store.Succeed(ctx, testScope, second, nil))

	require.Equal(t, "SUCCEEDED", readState(t, second).job,
		"the job really has moved on, which is what makes the replay below meaningful")

	// The ambiguous first report, retried at last.
	replayed, err := store.Fail(ctx, testScope, report)
	require.NoError(t, err)

	require.Equal(t, "RETRY_WAIT", replayed.JobStatus,
		"a replay reports the decision that committed, not what the job did afterwards")
	require.Equal(t, committed.JobID, replayed.JobID)
	require.Equal(t, committed.AttemptStatus, replayed.AttemptStatus)
	require.Equal(t, committed.DeadLetterReason, replayed.DeadLetterReason)
	require.NotNil(t, replayed.RetryAt)
	require.WithinDuration(t, *committed.RetryAt, *replayed.RetryAt, 0)
	require.NotNil(t, replayed.RetryDelay)
	require.Equal(t, *committed.RetryDelay, *replayed.RetryDelay)

	// Field-by-field above, then the whole value: the only difference the
	// contract allows between a first response and its replay is the flag that
	// says which one it is.
	expected := committed
	expected.Replayed = true
	require.Equal(t, expected, replayed, "the complete replay response must be the committed one")

	// And the replay really was a read: the successful second attempt is intact.
	require.Equal(t, "SUCCEEDED", readState(t, second).job)
	require.Equal(t, 2, countRows(t, "job_attempts"))
}

// TestOutcomeReplay_ReconstructionIsUnambiguousForEveryTerminalDecision covers
// the other shapes a committed failure can take, so the reconstruction is
// pinned against every branch rather than only the one that motivated it.
//
// Each case advances the job past the decision first, because a decision that
// only replays correctly while the job sits still is not preserved at all.
func TestOutcomeReplay_ReconstructionIsUnambiguousForEveryTerminalDecision(t *testing.T) {
	ctx := context.Background()

	t.Run("exhausted retryable failure replays as DEAD_LETTERED", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("replay-exhausted", 1, nil, []string{"demo.echo"}))
		// One attempt of budget, so the first retryable failure exhausts it.
		createJobWithOptions(t, "replay-exhausted", "default", "demo.echo", 50, nil, 1, 300, nil)
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)

		report := failureReport(fence, lifecycle.ClassRetryable, "upstream_5xx", "still failing")
		committed, err := store.Fail(ctx, testScope, report)
		require.NoError(t, err)
		require.Equal(t, "DEAD_LETTERED", committed.JobStatus)
		require.Equal(t, lifecycle.ReasonAttemptsExhausted, committed.DeadLetterReason)
		require.Nil(t, committed.RetryAt)

		// A dead-lettered job stays dead-lettered, but a replay creates a linked
		// replacement — the original's history has company now.
		_, err = jobStore().Replay(ctx, testScope, fence.JobID, "replay-exhausted-key")
		require.NoError(t, err)

		replayed, err := store.Fail(ctx, testScope, report)
		require.NoError(t, err)
		expected := committed
		expected.Replayed = true
		require.Equal(t, expected, replayed)
	})

	t.Run("permanent failure replays with its own dead-letter reason", func(t *testing.T) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store,
			workerRegistration("replay-permanent", 1, nil, []string{"demo.echo"}))
		createJobWithOptions(t, "replay-permanent", "default", "demo.echo", 50, nil, 5, 300, nil)
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)

		report := failureReport(fence, lifecycle.ClassPermanent, "invalid_payload", "no such account")
		committed, err := store.Fail(ctx, testScope, report)
		require.NoError(t, err)
		require.Equal(t, "DEAD_LETTERED", committed.JobStatus)
		require.Equal(t, lifecycle.ReasonPermanentFailure, committed.DeadLetterReason,
			"a permanent failure did not exhaust the budget, and must not say it did")

		_, err = jobStore().Replay(ctx, testScope, fence.JobID, "replay-permanent-key")
		require.NoError(t, err)

		replayed, err := store.Fail(ctx, testScope, report)
		require.NoError(t, err)
		expected := committed
		expected.Replayed = true
		require.Equal(t, expected, replayed)
	})

	t.Run("cancellation acknowledgment replays as CANCELED", func(t *testing.T) {
		reset(t)
		store := controlStore()
		registration := workerRegistration("replay-cancel-status", 1, nil, []string{"demo.echo"})
		session := registerWorker(t, store, registration)
		createJob(t, "replay-cancel-status", "demo.echo", 50, nil)
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)

		_, err = jobStore().RequestCancel(ctx, testScope, fence.JobID)
		require.NoError(t, err)
		ack := cancelAck(fence)
		committed, err := store.AcknowledgeCancellation(ctx, testScope, ack)
		require.NoError(t, err)
		require.Equal(t, "CANCELED", committed.JobStatus)

		// Replacing the session is the closest a canceled job comes to moving
		// on: it is terminal, so nothing else can change under the replay.
		replaceSession(t, store, registration)
		replayed, err := store.AcknowledgeCancellation(ctx, testScope, ack)
		require.NoError(t, err)
		expected := committed
		expected.Replayed = true
		require.Equal(t, expected, replayed)
	})
}

// storeWithRetryPolicy builds a control store whose retry policy is the thing
// under test, with jitter disabled so the delay is exactly the policy's.
func storeWithRetryPolicy(policy lifecycle.RetryPolicy) *workers.Store {
	return storeWithRetryPolicyAndJitter(policy, nil)
}

func storeWithRetryPolicyAndJitter(policy lifecycle.RetryPolicy, jitter lifecycle.JitterSource) *workers.Store {
	return workers.NewStore(testPool, workers.StoreConfig{
		LeaseDuration: integrationLeaseDuration,
		RetryPolicy:   policy,
		Jitter:        jitter,
	})
}

// fixedJitter returns one sample forever, so a test can drive the policy to an
// exact delay instead of asserting a range around a random one.
type fixedJitter float64

func (f fixedJitter) Float64() float64 { return float64(f) }

// TestOutcomeReplay_SubMillisecondRetryDelaysReplayIdentically is the boundary a
// one-second policy cannot reach.
//
// job_attempts.retry_delay_ms stores whole milliseconds. A delay below one
// millisecond used to be decided as a delayed retry -- the transition branched
// on the unrounded duration and reported RETRY_WAIT -- and then persisted as 0,
// so the replay read the stored 0 back and reported QUEUED. One committed,
// immutable decision with two different answers, and the second one describing a
// transition the job never made. No later promotion or success could repair it,
// because the lossy record was already written.
//
// The delay is now quantized once, at the policy, and the transition is chosen
// from the same integer that gets stored, so there is no representation left for
// the two to disagree about.
func TestOutcomeReplay_SubMillisecondRetryDelaysReplayIdentically(t *testing.T) {
	ctx := context.Background()

	// Every maximum here is a whole millisecond, because Max is a strict upper
	// bound on a value stored in that unit and a policy that cannot express its
	// own bound is not a valid one. The BASE is what is under test, and it is
	// free to be smaller than the granularity it will be stored at.
	for name, policy := range map[string]lifecycle.RetryPolicy{
		"1ns base": {
			Base: time.Nanosecond, Max: time.Millisecond, Multiplier: 1, Jitter: 0,
		},
		"500 microsecond base": {
			Base: 500 * time.Microsecond, Max: time.Millisecond, Multiplier: 1, Jitter: 0,
		},
		"exactly 1ms": {
			Base: time.Millisecond, Max: time.Millisecond, Multiplier: 1, Jitter: 0,
		},
	} {
		t.Run(name, func(t *testing.T) {
			reset(t)
			require.NoError(t, policy.Validate(), "the policy under test must be a valid one")
			store := storeWithRetryPolicy(policy)
			session := registerWorker(t, store,
				workerRegistration("sub-ms-replay", 1, nil, []string{"demo.echo"}))
			createJobWithOptions(t, "sub-ms-replay", "default", "demo.echo", 50, nil, 3, 300, nil)

			claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
			require.NoError(t, err)
			first := assignmentFence(claim.Assignment)
			startAttempt(t, store, first)

			report := failureReport(first, lifecycle.ClassRetryable, "upstream_5xx", "upstream returned 502")
			committed, err := store.Fail(ctx, testScope, report)
			require.NoError(t, err)
			require.False(t, committed.Replayed)

			// A positive backoff stays a delayed retry. Turning it into an
			// immediate requeue would be a different behavior under load, not a
			// rounding detail.
			require.Equal(t, "RETRY_WAIT", committed.JobStatus)
			require.NotNil(t, committed.RetryAt)
			require.NotNil(t, committed.RetryDelay)

			// Drive the job past the failure entirely: promoted, claimed again,
			// and completed successfully.
			makeRetryDue(t, first.JobID)
			stats, err := jobStore().PromoteDueJobs(ctx, 10)
			require.NoError(t, err)
			require.Equal(t, 1, stats.PromotedJobs)

			claim, err = store.Claim(ctx, testScope, claimRequest(session, "default"))
			require.NoError(t, err)
			require.Equal(t, workers.Claimed, claim.Disposition)
			second := assignmentFence(claim.Assignment)
			require.NotEqual(t, first.AttemptID, second.AttemptID)
			startAttempt(t, store, second)
			require.NoError(t, store.Succeed(ctx, testScope, second, nil))
			require.Equal(t, "SUCCEEDED", readState(t, second).job)

			// The ambiguous first report, retried at last.
			replayed, err := store.Fail(ctx, testScope, report)
			require.NoError(t, err)

			// Asserted before anything about magnitude, because this is the
			// symptom: the decision reported RETRY_WAIT and its own replay used
			// to answer QUEUED, describing a transition the job never made.
			require.Equal(t, committed.JobStatus, replayed.JobStatus,
				"a decision and its own replay must not disagree about what happened")
			require.Equal(t, "RETRY_WAIT", replayed.JobStatus,
				"a sub-millisecond delay must not replay as an immediate requeue")

			expected := committed
			expected.Replayed = true
			require.Equal(t, expected, replayed,
				"the complete replay response must be the committed one, field for field")

			// And the decision really is one the attempt row can hold: a positive
			// policy rounds up to the shortest storable delay rather than down to
			// an immediate requeue, and the stored integer agrees with both
			// responses.
			require.Equal(t, time.Millisecond, *committed.RetryDelay,
				"a positive sub-millisecond policy is rounded up to the shortest storable delay")
			stored := readAttemptOutcome(t, first.AttemptID)
			require.NotNil(t, stored.retryDelayMs)
			require.Equal(t, committed.RetryDelay.Milliseconds(), *stored.retryDelayMs)
			require.Positive(t, *stored.retryDelayMs,
				"the persisted integer must agree that this was a delayed retry")
		})
	}
}

// TestOutcomeReplay_ZeroDelayRetryableFailureReplaysAsImmediateRetry covers the
// other side of the quantization boundary.
//
// Rounding a positive delay up must not also invent one where the calculation
// produced none. Full jitter at a sample of 0 gives factor 1 + 1*(2*0 - 1) = 0,
// so the delay really is zero however large the base is, and a zero delay is an
// immediate requeue rather than the shortest backoff — ADR-0009's recovery path
// and the shortest real retry have to stay distinguishable in attempt history.
//
// The existing zero-delay coverage is an ABANDONED attempt, which reconciliation
// produces and which carries no outcome identity, so it can never be replayed.
// This is a worker-reported retryable failure, which can.
func TestOutcomeReplay_ZeroDelayRetryableFailureReplaysAsImmediateRetry(t *testing.T) {
	reset(t)
	ctx := context.Background()

	// A large base, so a zero result can only come from the jitter reducing the
	// calculation to zero rather than from the base being small.
	policy := lifecycle.RetryPolicy{
		Base: time.Minute, Max: time.Hour, Multiplier: 2, Jitter: 1,
	}
	require.NoError(t, policy.Validate())
	require.Zero(t, policy.Delay(1, fixedJitter(0)),
		"the policy under test must actually compute a zero delay")

	store := storeWithRetryPolicyAndJitter(policy, fixedJitter(0))
	session := registerWorker(t, store,
		workerRegistration("zero-delay-replay", 1, nil, []string{"demo.echo"}))
	createJobWithOptions(t, "zero-delay-replay", "default", "demo.echo", 50, nil, 3, 300, nil)

	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	first := assignmentFence(claim.Assignment)
	startAttempt(t, store, first)

	report := failureReport(first, lifecycle.ClassRetryable, "upstream_5xx", "upstream returned 502")
	committed, err := store.Fail(ctx, testScope, report)
	require.NoError(t, err)
	require.False(t, committed.Replayed)

	// The immediate-retry state: claimable now, not waiting on a backoff.
	require.Equal(t, "QUEUED", committed.JobStatus,
		"a zero delay is an immediate requeue, not the shortest possible backoff")
	require.NotNil(t, committed.RetryDelay)
	require.Zero(t, *committed.RetryDelay)
	require.NotNil(t, committed.RetryAt)
	require.Equal(t, "QUEUED", readState(t, first).job)

	// The zero is recorded rather than left NULL, which is what keeps
	// "requeued immediately" and "no decision was made" distinguishable.
	stored := readAttemptOutcome(t, first.AttemptID)
	require.NotNil(t, stored.retryDelayMs)
	require.Zero(t, *stored.retryDelayMs)

	// No promotion needed — the job is already claimable. Let a second attempt
	// take it and succeed, so the job has moved on before the replay.
	claim, err = store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	second := assignmentFence(claim.Assignment)
	require.NotEqual(t, first.AttemptID, second.AttemptID)
	startAttempt(t, store, second)
	require.NoError(t, store.Succeed(ctx, testScope, second, nil))
	require.Equal(t, "SUCCEEDED", readState(t, second).job)

	replayed, err := store.Fail(ctx, testScope, report)
	require.NoError(t, err)

	require.Equal(t, committed.JobStatus, replayed.JobStatus,
		"a decision and its own replay must not disagree about what happened")
	require.Equal(t, "QUEUED", replayed.JobStatus,
		"a zero-delay retry must not replay as a delayed one")
	expected := committed
	expected.Replayed = true
	require.Equal(t, expected, replayed,
		"the complete replay response must be the committed one, field for field")
}
