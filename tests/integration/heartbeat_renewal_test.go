//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/workers"
)

func heartbeat(t *testing.T, store *workers.Store, session workers.Session) workers.HeartbeatResult {
	t.Helper()
	result, err := store.Heartbeat(context.Background(), testScope,
		workers.HeartbeatRequest{WorkerID: session.WorkerID, SessionID: session.ID})
	require.NoError(t, err)
	return result
}

func renewalRequest(fence workers.Fence, expected int) workers.RenewalRequest {
	return workers.RenewalRequest{
		Fence: fence, RenewalRequestID: uuid.New(), ExpectedVersion: expected,
	}
}

func leaseRow(t *testing.T, leaseID uuid.UUID) (status string, expiresAt, renewedAt time.Time, version int) {
	t.Helper()
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT status, expires_at, renewed_at, renewal_version
		FROM leases WHERE id = $1`, leaseID).Scan(&status, &expiresAt, &renewedAt, &version))
	return
}

func serverNow(t *testing.T) time.Time {
	t.Helper()
	var now time.Time
	require.NoError(t, testPool.QueryRow(context.Background(), `SELECT clock_timestamp()`).Scan(&now))
	return now
}

// TestHeartbeat_UsesPostgreSQLTimeAndIsMonotonic proves invariant 18 for
// heartbeats: the stored time comes from the database, never from a caller.
func TestHeartbeat_UsesPostgreSQLTimeAndIsMonotonic(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("heartbeat-worker", 1, nil, []string{"demo.echo"}))

	before := serverNow(t)
	first := heartbeat(t, store, session)
	after := serverNow(t)

	require.Equal(t, session.ID, first.SessionID)
	require.Equal(t, workers.SessionHealthy, first.Status)
	require.False(t, first.LastHeartbeatAt.Before(before),
		"the receipt time must be sampled by PostgreSQL during this call")
	require.False(t, first.LastHeartbeatAt.After(after))
	require.True(t, first.LastHeartbeatAt.After(session.RegisteredAt),
		"a heartbeat must advance past the registration time")

	// Repeating is harmless and monotonic: it may advance again, never backwards.
	second := heartbeat(t, store, session)
	require.False(t, second.LastHeartbeatAt.Before(first.LastHeartbeatAt))

	var stored time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT last_heartbeat_at FROM worker_sessions WHERE id = $1`, session.ID).Scan(&stored))
	require.True(t, second.LastHeartbeatAt.Equal(stored),
		"the response must report exactly what was committed")
}

// TestHeartbeat_IsSampledAfterTheAuthorityLockWait is the analogue of the M2
// claim-clock test. A heartbeat that waited on the session row must stamp the
// time it actually committed, not the time it started waiting.
func TestHeartbeat_IsSampledAfterTheAuthorityLockWait(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("heartbeat-clock-worker", 1, nil, []string{"demo.echo"}))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sessionLock, err := testPool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sessionLock.Rollback(context.Background()) })
	_, err = sessionLock.Exec(ctx,
		`SELECT status FROM worker_sessions WHERE id = $1 FOR UPDATE`, session.ID)
	require.NoError(t, err)

	type outcome struct {
		result workers.HeartbeatResult
		err    error
	}
	results := make(chan outcome, 1)
	go func() {
		result, err := store.Heartbeat(ctx, testScope,
			workers.HeartbeatRequest{WorkerID: session.WorkerID, SessionID: session.ID})
		results <- outcome{result, err}
	}()
	waitForDatabaseLock(t, "SELECT status FROM worker_sessions")
	waitForServerTime(t, serverNow(t).Add(250*time.Millisecond))

	releasedAt := serverNow(t)
	require.NoError(t, sessionLock.Commit(ctx))

	got := <-results
	require.NoError(t, got.err)
	require.False(t, got.result.LastHeartbeatAt.Before(releasedAt),
		"the receipt time must be sampled after the authority lock was granted")
}

// TestHeartbeat_CannotReviveAFencedSession is the rule that keeps a dead process
// dead. A late heartbeat from a replaced boot must be rejected without touching
// anything.
func TestHeartbeat_CannotReviveAFencedSession(t *testing.T) {
	reset(t)
	store := controlStore()
	registration := workerRegistration("fenced-heartbeat-worker", 1, nil, []string{"demo.echo"})
	original := registerWorker(t, store, registration)
	beforeReplacement := heartbeat(t, store, original)

	replacement := registerReplacement(t, store, registration)
	require.Equal(t, "OFFLINE", sessionStatus(t, original.ID))

	_, err := store.Heartbeat(context.Background(), testScope,
		workers.HeartbeatRequest{WorkerID: original.WorkerID, SessionID: original.ID})
	require.ErrorIs(t, err, workers.ErrSessionUnavailable)

	var status string
	var stored time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status, last_heartbeat_at FROM worker_sessions WHERE id = $1`,
		original.ID).Scan(&status, &stored))
	require.Equal(t, "OFFLINE", status, "a rejected heartbeat must not change the session status")
	require.True(t, stored.Equal(beforeReplacement.LastHeartbeatAt),
		"a rejected heartbeat must not advance the stored receipt time")

	// The replacement is unaffected and can still heartbeat.
	heartbeat(t, store, replacement)

	// An unknown session is equally unavailable, not an internal error.
	_, err = store.Heartbeat(context.Background(), testScope,
		workers.HeartbeatRequest{WorkerID: original.WorkerID, SessionID: uuid.New()})
	require.ErrorIs(t, err, workers.ErrSessionUnavailable)
}

// TestHeartbeat_RacingSessionReplacementHasOnlyValidSerialOutcomes arranges the
// contention deliberately rather than hoping the scheduler produces it. Both
// operations lock the same session row, so there is no check-then-update window.
func TestHeartbeat_RacingSessionReplacementHasOnlyValidSerialOutcomes(t *testing.T) {
	reset(t)
	store := controlStore()
	registration := workerRegistration("heartbeat-race-worker", 1, nil, []string{"demo.echo"})
	original := registerWorker(t, store, registration)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sessionLock, err := testPool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sessionLock.Rollback(context.Background()) })
	_, err = sessionLock.Exec(ctx,
		`SELECT status FROM worker_sessions WHERE id = $1 FOR UPDATE`, original.ID)
	require.NoError(t, err)

	// Park the heartbeat on the session row, then let replacement commit first.
	heartbeatErr := make(chan error, 1)
	go func() {
		_, err := store.Heartbeat(ctx, testScope,
			workers.HeartbeatRequest{WorkerID: original.WorkerID, SessionID: original.ID})
		heartbeatErr <- err
	}()
	waitForDatabaseLock(t, "SELECT status FROM worker_sessions")

	replacementErr := make(chan error, 1)
	go func() {
		replacement := registration
		replacement.SessionID = uuid.New()
		_, err := store.Register(ctx, testScope, replacement)
		replacementErr <- err
	}()
	waitForDatabaseLock(t, "UPDATE worker_sessions")

	require.NoError(t, sessionLock.Commit(ctx))

	// Whichever order PostgreSQL grants the lock in, both outcomes are valid and
	// neither leaves a revived session behind.
	hbErr, regErr := <-heartbeatErr, <-replacementErr
	require.NoError(t, regErr, "a replacement boot must always be able to register")
	if hbErr != nil {
		require.ErrorIs(t, hbErr, workers.ErrSessionUnavailable)
	}
	require.Equal(t, "OFFLINE", sessionStatus(t, original.ID),
		"the replaced boot must end OFFLINE regardless of who won")
	var current int
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM worker_sessions
		WHERE worker_id = $1 AND status IN ('STARTING', 'HEALTHY', 'DRAINING')`,
		original.WorkerID).Scan(&current))
	require.Equal(t, 1, current, "exactly one session may be current for a logical worker")
}

// A stale session loses every capability at once. This is the whole point of
// marking it non-current.
func TestStaleSession_CanDoNothingAtAll(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("stale-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "stale-one", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)
	require.NoError(t, store.Start(context.Background(), testScope, fence))

	// Age the heartbeat past the threshold, then let the reconciler fence it.
	_, err = testPool.Exec(context.Background(), `
		UPDATE worker_sessions
		SET registered_at = clock_timestamp() - interval '1 minute',
		    last_heartbeat_at = clock_timestamp() - interval '1 minute'
		WHERE id = $1`, session.ID)
	require.NoError(t, err)

	marked, err := store.MarkStaleSessions(context.Background(), 5*time.Second, 10)
	require.NoError(t, err)
	require.Equal(t, 1, marked)
	require.Equal(t, "UNHEALTHY", sessionStatus(t, session.ID),
		"a missed heartbeat is UNHEALTHY; OFFLINE stays reserved for replacement and shutdown")

	var endedAt *time.Time
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT ended_at FROM worker_sessions WHERE id = $1`, session.ID).Scan(&endedAt))
	require.NotNil(t, endedAt, "a status that ends authority must record when")

	_, err = store.Heartbeat(context.Background(), testScope,
		workers.HeartbeatRequest{WorkerID: session.WorkerID, SessionID: session.ID})
	require.ErrorIs(t, err, workers.ErrSessionUnavailable)

	_, err = store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.ErrorIs(t, err, workers.ErrSessionUnavailable)

	_, err = store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
	require.ErrorIs(t, err, workers.ErrFenceRejected)

	require.ErrorIs(t, store.Start(context.Background(), testScope, fence), workers.ErrFenceRejected)
	require.ErrorIs(t, store.Succeed(context.Background(), testScope, fence), workers.ErrFenceRejected)

	// Marking it again changes nothing.
	marked, err = store.MarkStaleSessions(context.Background(), 5*time.Second, 10)
	require.NoError(t, err)
	require.Zero(t, marked)
}

// A session that keeps heartbeating is never fenced, no matter how long it has
// been running.
func TestMarkStaleSessions_LeavesAHeartbeatingSessionAlone(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("healthy-worker", 1, nil, []string{"demo.echo"}))
	heartbeat(t, store, session)

	marked, err := store.MarkStaleSessions(context.Background(), 5*time.Second, 10)
	require.NoError(t, err)
	require.Zero(t, marked)
	require.Equal(t, "HEALTHY", sessionStatus(t, session.ID))
}

// TestRenewal_ExtendsTheWindowUnderTheCompleteFence is the happy path plus the
// five-part fence that guards it. Each variant changes exactly one identifier.
func TestRenewal_ExtendsTheWindowUnderTheCompleteFence(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renew-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "renew-fence", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)
	require.NoError(t, store.Start(context.Background(), testScope, fence))

	_, originalExpiry, _, originalVersion := leaseRow(t, fence.LeaseID)
	require.Equal(t, 0, originalVersion, "a claimed lease starts at generation 0")

	result, err := store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
	require.NoError(t, err)
	require.Equal(t, 1, result.RenewalVersion)
	require.False(t, result.Replayed)
	require.True(t, result.ExpiresAt.After(originalExpiry), "renewal must move the expiry forward")
	require.Positive(t, result.Remaining)

	status, expiresAt, renewedAt, version := leaseRow(t, fence.LeaseID)
	require.Equal(t, "ACTIVE", status)
	require.Equal(t, 1, version)
	require.True(t, expiresAt.Equal(result.ExpiresAt))
	require.True(t, expiresAt.After(renewedAt), "the timeline constraint must still hold")
	require.Equal(t, integrationLeaseDuration, expiresAt.Sub(renewedAt),
		"the renewed window is the server-owned lease duration")

	// Every identifier in the fence is load-bearing.
	for name, broken := range map[string]workers.Fence{
		"job id":     {JobID: uuid.New(), AttemptID: fence.AttemptID, LeaseID: fence.LeaseID, WorkerID: fence.WorkerID, SessionID: fence.SessionID},
		"attempt id": {JobID: fence.JobID, AttemptID: uuid.New(), LeaseID: fence.LeaseID, WorkerID: fence.WorkerID, SessionID: fence.SessionID},
		"lease id":   {JobID: fence.JobID, AttemptID: fence.AttemptID, LeaseID: uuid.New(), WorkerID: fence.WorkerID, SessionID: fence.SessionID},
		"worker id":  {JobID: fence.JobID, AttemptID: fence.AttemptID, LeaseID: fence.LeaseID, WorkerID: uuid.New(), SessionID: fence.SessionID},
		"session id": {JobID: fence.JobID, AttemptID: fence.AttemptID, LeaseID: fence.LeaseID, WorkerID: fence.WorkerID, SessionID: uuid.New()},
	} {
		t.Run("a wrong "+name+" is rejected", func(t *testing.T) {
			_, err := store.RenewLease(context.Background(), testScope, renewalRequest(broken, 1))
			require.ErrorIs(t, err, workers.ErrFenceRejected)
			_, _, _, unchanged := leaseRow(t, fence.LeaseID)
			require.Equal(t, 1, unchanged, "a rejected renewal must not advance the generation")
		})
	}
}

// TestRenewal_ExactReplayReturnsTheCommittedWindowWithoutExtendingItAgain is the
// property that makes an ambiguous renewal response safe to retry.
func TestRenewal_ExactReplayReturnsTheCommittedWindowWithoutExtendingItAgain(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renew-replay-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "renew-replay", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)

	request := renewalRequest(fence, 0)
	first, err := store.RenewLease(context.Background(), testScope, request)
	require.NoError(t, err)
	require.False(t, first.Replayed)
	_, committedExpiry, committedRenewal, _ := leaseRow(t, fence.LeaseID)

	replay, err := store.RenewLease(context.Background(), testScope, request)
	require.NoError(t, err)
	require.True(t, replay.Replayed, "an exact replay must be reported as such")
	require.Equal(t, first.RenewalVersion, replay.RenewalVersion)
	require.True(t, replay.ExpiresAt.Equal(first.ExpiresAt))
	require.LessOrEqual(t, replay.Remaining, first.Remaining,
		"a replay reports the window that remains, never the window that was granted")

	_, expiresAt, renewedAt, version := leaseRow(t, fence.LeaseID)
	require.Equal(t, 1, version, "a replay must not advance the generation")
	require.True(t, expiresAt.Equal(committedExpiry), "a replay must not move the expiry")
	require.True(t, renewedAt.Equal(committedRenewal), "a replay must not restamp the renewal time")
}

// TestRenewal_StaleAndCompetingGenerationsMutateNothing covers the two ways a
// generation can be wrong: an old request arriving late, and a second distinct
// request for a generation that was already consumed.
func TestRenewal_StaleAndCompetingGenerationsMutateNothing(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renew-generation-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "renew-generation", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)

	_, err = store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
	require.NoError(t, err)
	_, err = store.RenewLease(context.Background(), testScope, renewalRequest(fence, 1))
	require.NoError(t, err)
	_, expiryAtTwo, renewalAtTwo, _ := leaseRow(t, fence.LeaseID)

	t.Run("a delayed older generation is a stable conflict", func(t *testing.T) {
		_, err := store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
		require.ErrorIs(t, err, workers.ErrRenewalConflict)
	})
	t.Run("a competing request for the consumed generation is a stable conflict", func(t *testing.T) {
		_, err := store.RenewLease(context.Background(), testScope, renewalRequest(fence, 1))
		require.ErrorIs(t, err, workers.ErrRenewalConflict)
	})
	t.Run("a generation from the future is a stable conflict", func(t *testing.T) {
		_, err := store.RenewLease(context.Background(), testScope, renewalRequest(fence, 5))
		require.ErrorIs(t, err, workers.ErrRenewalConflict)
	})

	_, expiresAt, renewedAt, version := leaseRow(t, fence.LeaseID)
	require.Equal(t, 2, version)
	require.True(t, expiresAt.Equal(expiryAtTwo), "no rejected renewal may move the expiry")
	require.True(t, renewedAt.Equal(renewalAtTwo))
}

// Reusing one renewal identity against a different lease must be a deterministic
// domain conflict, not a leaked uniqueness error from the index that enforces it.
func TestRenewal_IdentityReuseAcrossLeasesIsADomainConflict(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renew-identity-worker", 2, nil, []string{"demo.echo"}))
	createJob(t, "renew-identity-one", "demo.echo", 50, nil)
	createJob(t, "renew-identity-two", "demo.echo", 50, nil)

	firstClaim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	secondClaim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	firstFence := assignmentFence(firstClaim.Assignment)
	secondFence := assignmentFence(secondClaim.Assignment)

	identity := uuid.New()
	_, err = store.RenewLease(context.Background(), testScope, workers.RenewalRequest{
		Fence: firstFence, RenewalRequestID: identity, ExpectedVersion: 0})
	require.NoError(t, err)

	_, err = store.RenewLease(context.Background(), testScope, workers.RenewalRequest{
		Fence: secondFence, RenewalRequestID: identity, ExpectedVersion: 0})
	require.ErrorIs(t, err, workers.ErrRenewalConflict)
	require.NotContains(t, err.Error(), "SQLSTATE",
		"the caller must get a domain conflict, never a leaked driver error")

	_, _, _, version := leaseRow(t, secondFence.LeaseID)
	require.Equal(t, 0, version, "the second lease must be untouched")
}

// TestRenewal_TwoConcurrentRenewalsForOneGenerationHaveExactlyOneWinner uses
// separate connections, because a shared one would serialize before the
// transaction and prove nothing about the row lock.
func TestRenewal_TwoConcurrentRenewalsForOneGenerationHaveExactlyOneWinner(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renew-contention-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "renew-contention", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)

	type outcome struct {
		result workers.RenewalResult
		err    error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			result, err := store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
			results <- outcome{result, err}
		}()
	}
	close(start)

	winners, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		got := <-results
		switch {
		case got.err == nil:
			winners++
			require.Equal(t, 1, got.result.RenewalVersion)
		default:
			require.ErrorIs(t, got.err, workers.ErrRenewalConflict)
			conflicts++
		}
	}
	require.Equal(t, 1, winners, "exactly one distinct renewal may consume a generation")
	require.Equal(t, 1, conflicts)

	_, _, _, version := leaseRow(t, fence.LeaseID)
	require.Equal(t, 1, version, "the lease may be extended only once per generation")
}

// TestRenewal_WaitingAcrossExpiryIsRejectedWithoutMutation is the reason renewal
// resamples PostgreSQL time after its locks instead of trusting transaction-start
// now(). It parks the renewal on the queue lock and lets the lease lapse under it.
func TestRenewal_WaitingAcrossExpiryIsRejectedWithoutMutation(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renew-expiry-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "renew-expiry", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)
	require.NoError(t, store.Start(context.Background(), testScope, fence))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	renewErr := make(chan error, 1)
	go func() {
		_, err := store.RenewLease(ctx, testScope, renewalRequest(fence, 0))
		renewErr <- err
	}()
	waitForDatabaseLock(t, "SELECT name FROM queues WHERE name")
	waitForServerTime(t, expiresAt.Add(50*time.Millisecond))
	require.NoError(t, queueLock.Commit(ctx))

	require.ErrorIs(t, <-renewErr, workers.ErrLeaseExpired)

	status, stillExpiresAt, _, version := leaseRow(t, fence.LeaseID)
	require.Equal(t, "ACTIVE", status, "a rejected renewal must not close the lease either")
	require.True(t, stillExpiresAt.Equal(expiresAt), "a rejected renewal must not move the expiry")
	require.Equal(t, 0, version)
}

// A lease that has already reached a terminal state is never resurrected by a
// renewal that arrives afterwards.
func TestRenewal_NeverResurrectsAClosedLease(t *testing.T) {
	reset(t)
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("renew-closed-worker", 1, nil, []string{"demo.echo"}))
	createJob(t, "renew-closed", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	fence := assignmentFence(claim.Assignment)
	require.NoError(t, store.Start(context.Background(), testScope, fence))
	require.NoError(t, store.Succeed(context.Background(), testScope, fence))

	_, err = store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
	require.ErrorIs(t, err, workers.ErrLeaseExpired)

	status, _, _, version := leaseRow(t, fence.LeaseID)
	require.Equal(t, "COMPLETED", status)
	require.Equal(t, 0, version)

	var jobStatus, attemptStatus string
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT j.status, a.status FROM jobs j JOIN job_attempts a ON a.job_id = j.id
		WHERE j.id = $1`, fence.JobID).Scan(&jobStatus, &attemptStatus))
	require.Equal(t, "SUCCEEDED", jobStatus, "terminal success is never overwritten")
	require.Equal(t, "SUCCEEDED", attemptStatus)
}
