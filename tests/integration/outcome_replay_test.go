//go:build integration

package integration

import (
	"context"
	"testing"

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

			require.NoError(t, store.Succeed(ctx, testScope, fence))
			committed := readOutcomeState(t, fence)
			require.Equal(t, leaseOutcome{"SUCCEEDED", "SUCCEEDED", "COMPLETED"}, committed)

			invalidate(t, store, registration, fence)

			require.NoError(t, store.Succeed(ctx, testScope, fence),
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

		require.ErrorIs(t, store.Succeed(ctx, testScope, fence), workers.ErrFenceRejected)
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
