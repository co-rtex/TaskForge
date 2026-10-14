//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/reconciler"
	"github.com/co-rtex/TaskForge/internal/workers"
	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// M8D4: the reconciler's precedence for a killed worker's attempts, pinned through
// the real store with no sleeps and no wall-clock race. Every instant a case
// depends on is placed by SQL relative to clock_timestamp(), and the reconciler is
// called directly.
//
// Existing tests already pin parts of these cases, and are not replaced:
// TestTimeout_ExpiredLeaseScanRecognizesAnAlreadyDueDeadline and
// TestTimeout_LeaseExpiryWithNoDueDeadlineIsStillAbandonment (timeout_test.go) for
// (a) and (b), TestTimeout_DueAttemptBecomesTimedOutAndEntersRetryWait for the
// timeout sweep with a live lease, TestReconcile_AbandonsAnAttemptThatNeverStarted
// (reconciliation_test.go) for (d), and TestCancel_TakesPrecedenceOverADueTimeout
// and TestCancel_ReconciliationFinalizesWhenNoWorkerAcknowledges
// (cancellation_test.go) for (g). None of them asserts all of what a kill's outcome
// decides at once: the attempt and job status, the failure class and code, whether
// the job backs off or is requeued at once, whether a work.available event exists,
// and whether the attempt consumed budget. The table below asserts all of them for
// each case, and proves consumption by running each case a second time with a
// budget of one.
//
// Concurrent reconcilers racing on one attempt are pinned by
// TestReconcile_ConcurrentReconcilersProduceOneRepair (reconciliation_test.go,
// whole RunOnce passes on separate connections) and
// TestTimeout_ConcurrentReconcilersProduceOneOutcome (timeout_test.go); they are
// not repeated here.

// placeAttempt rewrites a claimed attempt's timeline relative to clock_timestamp():
// created and (when started) started ten minutes ago, the lease acquired then and
// expiring at expiresIn from now, and the deadline at timeoutIn from now. It is
// what a worker killed ten minutes ago leaves behind, with the two instants that
// decide its outcome put where the case needs them.
func placeAttempt(t *testing.T, fence workers.Fence, started bool, expiresIn, timeoutIn time.Duration) {
	t.Helper()
	ctx := context.Background()
	if started {
		tag, err := testPool.Exec(ctx, `
			UPDATE job_attempts
			SET created_at = clock_timestamp() - interval '10 minutes',
			    started_at = clock_timestamp() - interval '10 minutes' + interval '12 milliseconds',
			    timeout_at = clock_timestamp() + make_interval(secs => $2::double precision)
			WHERE id = $1 AND status = 'RUNNING'`, fence.AttemptID, timeoutIn.Seconds())
		require.NoError(t, err)
		require.EqualValues(t, 1, tag.RowsAffected())
	} else {
		tag, err := testPool.Exec(ctx, `
			UPDATE job_attempts SET created_at = clock_timestamp() - interval '10 minutes'
			WHERE id = $1 AND status = 'LEASED' AND timeout_at IS NULL`, fence.AttemptID)
		require.NoError(t, err)
		require.EqualValues(t, 1, tag.RowsAffected())
	}
	// A lease that was renewed keeps a renewal instant after its acquisition: a
	// minute ago, so that it is still before any expiry a case places.
	tag, err := testPool.Exec(ctx, `
		UPDATE leases
		SET acquired_at = clock_timestamp() - interval '10 minutes',
		    renewed_at = CASE WHEN renewal_version > 0 THEN clock_timestamp() - interval '1 minute'
		                      ELSE clock_timestamp() - interval '10 minutes' END,
		    expires_at = clock_timestamp() + make_interval(secs => $2::double precision)
		WHERE id = $1 AND status = 'ACTIVE'`, fence.LeaseID, expiresIn.Seconds())
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

// staleSession makes a session's last heartbeat an hour old: a dead worker's.
func staleSession(t *testing.T, sessionID uuid.UUID) {
	t.Helper()
	tag, err := testPool.Exec(context.Background(), `
		UPDATE worker_sessions
		SET registered_at = clock_timestamp() - interval '2 hours',
		    last_heartbeat_at = clock_timestamp() - interval '1 hour'
		WHERE id = $1`, sessionID)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

// killOutcome is everything a precedence case asserts.
type killOutcome struct {
	attempt, job, lease string
	class, code         string
	// backoff is RETRY_WAIT with retry_at after the decision; immediate is a
	// requeue with a zero delay and retry_at equal to the decision instant.
	backoff, immediate bool
	event              bool   // a new pending work.available event for the job
	dlqReason          string // "" when the job is not dead-lettered
}

func requireKillOutcome(t *testing.T, fence workers.Fence, before map[uuid.UUID]struct{}, want killOutcome) {
	t.Helper()
	state := readState(t, fence)
	require.Equal(t, want.attempt, state.attempt, "attempt status")
	require.Equal(t, want.job, state.job, "job status")
	require.Equal(t, want.lease, state.lease, "lease status")

	row := readAttemptOutcome(t, fence.AttemptID)
	require.NotNil(t, row.failureClass)
	require.Equal(t, want.class, *row.failureClass, "failure class")
	require.NotNil(t, row.errorCode)
	require.Equal(t, want.code, *row.errorCode, "error code")
	require.NotNil(t, row.finishedAt)

	switch {
	case want.backoff:
		require.NotNil(t, row.retryAt)
		require.NotNil(t, row.retryDelayMs)
		require.Positive(t, *row.retryDelayMs, "a timeout backs off")
		require.True(t, row.retryAt.After(*row.finishedAt), "retry_at is after the decision")
		require.True(t, readJob(t, fence.JobID).availableAt.Equal(*row.retryAt), "the job waits until retry_at")
	case want.immediate:
		require.NotNil(t, row.retryAt)
		require.NotNil(t, row.retryDelayMs)
		require.Zero(t, *row.retryDelayMs, "an abandonment is requeued with no backoff (ADR-0009)")
		require.True(t, row.retryAt.Equal(*row.finishedAt))
	default:
		require.Nil(t, row.retryAt, "no retry was decided")
		require.Nil(t, row.retryDelayMs)
	}

	events := 0
	for _, id := range newPendingOutbox(t, before) {
		var jobID uuid.UUID
		var eventType string
		require.NoError(t, testPool.QueryRow(context.Background(),
			`SELECT job_id, event_type FROM outbox_events WHERE id = $1`, id).Scan(&jobID, &eventType))
		if jobID == fence.JobID && eventType == "work.available" {
			events++
		}
	}
	if want.event {
		require.Equal(t, 1, events, "exactly one recovery work.available event")
	} else {
		require.Zero(t, events, "no work.available event")
	}

	if want.dlqReason == "" {
		require.Empty(t, dlqRows(t, fence.JobID))
	} else {
		require.Equal(t, []string{want.dlqReason}, dlqRows(t, fence.JobID))
	}
}

// TestPrecedence_AKilledAttemptsOutcome is cases (a) to (d) and (g). Each runs
// twice: with budget left (max_attempts 3) it shows whether the job backs off or is
// requeued at once, and with a budget of one it shows whether the attempt consumed
// budget (it did if the job is then dead-lettered for ATTEMPTS_EXHAUSTED).
func TestPrecedence_AKilledAttemptsOutcome(t *testing.T) {
	type pass func(t *testing.T, store *workers.Store) reconciler.Result
	runOnce := func(t *testing.T, store *workers.Store) reconciler.Result {
		result, err := newReconciler(t, store, time.Minute).RunOnce(context.Background())
		require.NoError(t, err)
		return result
	}

	timedOut := func(lease string) func(exhausted bool) killOutcome {
		return func(exhausted bool) killOutcome {
			want := killOutcome{attempt: "TIMED_OUT", lease: lease, class: "TIMED_OUT", code: lifecycle.CodeTimeout}
			if exhausted {
				want.job, want.dlqReason = "DEAD_LETTERED", "ATTEMPTS_EXHAUSTED"
			} else {
				want.job, want.backoff = "RETRY_WAIT", true
			}
			return want
		}
	}
	abandoned := func(exhausted bool) killOutcome {
		want := killOutcome{attempt: "ABANDONED", lease: "EXPIRED", class: "ABANDONED", code: lifecycle.CodeAbandoned}
		if exhausted {
			want.job, want.dlqReason = "DEAD_LETTERED", "ATTEMPTS_EXHAUSTED"
		} else {
			want.job, want.immediate, want.event = "QUEUED", true, true
		}
		return want
	}

	cases := []struct {
		name string
		// arrange puts the claimed attempt where the case needs it. started says
		// whether the attempt is started before arrange runs.
		started bool
		arrange func(t *testing.T, store *workers.Store, session workers.Session, fence workers.Fence)
		run     pass
		// stats is what the pass reports: ExpiredLeases, TimedOutAttempts.
		expiredLeases, timedOutAttempts int
		want                            func(exhausted bool) killOutcome
	}{
		{
			// The reviewer's hypothesis for a never-renewed killed attempt with
			// timeout_seconds equal to the lease: the deadline (start + 30 s) falls a
			// claim-to-start latency after the expiry (claim + 30 s), so a pass that
			// comes after both makes it TIMED_OUT, on the expired-lease path.
			name: "(a) RUNNING, lease expired, deadline already due: TIMED_OUT on the expired-lease path", started: true,
			arrange: func(t *testing.T, _ *workers.Store, session workers.Session, fence workers.Fence) {
				placeAttempt(t, fence, true, -2*time.Second, -2*time.Second+12*time.Millisecond)
				staleSession(t, session.ID)
			},
			run: runOnce, expiredLeases: 1, timedOutAttempts: 1, want: timedOut("EXPIRED"),
		},
		{
			// The same attempt, reached by a pass inside the window: after the lease
			// expired, before the deadline.
			name: "(b) RUNNING, lease expired, deadline not yet due: ABANDONED and requeued at once", started: true,
			arrange: func(t *testing.T, _ *workers.Store, session workers.Session, fence workers.Fence) {
				placeAttempt(t, fence, true, -2*time.Second, time.Hour)
				staleSession(t, session.ID)
			},
			run: runOnce, expiredLeases: 1, timedOutAttempts: 0, want: abandoned,
		},
		{
			// A renewed lease outlives a deadline that comes first: the timeout sweep
			// records the outcome while the lease is still ACTIVE, and the
			// expired-lease path never sees it (ExpiredLeases 0, lease RELEASED). The
			// session is dead: RunOnce marks it stale first, and that changes nothing.
			name: "(c) RUNNING, lease renewed and live, deadline due, session stale: TIMED_OUT by the timeout sweep", started: true,
			arrange: func(t *testing.T, store *workers.Store, session workers.Session, fence workers.Fence) {
				_, err := store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
				require.NoError(t, err)
				placeAttempt(t, fence, true, time.Hour, -time.Second)
				staleSession(t, session.ID)
			},
			run: runOnce, expiredLeases: 0, timedOutAttempts: 1, want: timedOut("RELEASED"),
		},
		{
			name: "(d) LEASED, never started, lease expired: ABANDONED", started: false,
			arrange: func(t *testing.T, _ *workers.Store, session workers.Session, fence workers.Fence) {
				placeAttempt(t, fence, false, -2*time.Second, 0)
				staleSession(t, session.ID)
			},
			run: runOnce, expiredLeases: 1, timedOutAttempts: 0, want: abandoned,
		},
		{
			// Cancellation won first, so the lapse is neither a timeout nor an
			// abandonment, even with the deadline due: no budget, no retry, no event.
			name: "(g) CANCEL_REQUESTED, lease expired, deadline due: the cancellation is finalized", started: true,
			arrange: func(t *testing.T, _ *workers.Store, session workers.Session, fence workers.Fence) {
				_, err := jobStore().RequestCancel(context.Background(), testScope, fence.JobID)
				require.NoError(t, err)
				placeAttempt(t, fence, true, -2*time.Second, -time.Second)
				staleSession(t, session.ID)
			},
			run: runOnce, expiredLeases: 1, timedOutAttempts: 0,
			want: func(bool) killOutcome {
				return killOutcome{attempt: "CANCELED", job: "CANCELED", lease: "EXPIRED",
					class: "CANCELED", code: lifecycle.CodeCanceled}
			},
		},
	}

	for _, c := range cases {
		for _, budget := range []int{3, 1} {
			exhausted := budget == 1
			label := "budget left"
			if exhausted {
				label = "budget of one"
			}
			t.Run(c.name+"/"+label, func(t *testing.T) {
				reset(t)
				store := controlStoreWithJitter(lifecycle.NewSeededJitter(8))
				session := registerWorker(t, store,
					workerRegistration("precedence-"+uuid.NewString()[:8], 1, nil, []string{"demo.echo"}))
				createJobWithOptions(t, "precedence-"+uuid.NewString()[:8], "default", "demo.echo", 50, nil, budget, 30, nil)
				claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
				require.NoError(t, err)
				require.Equal(t, workers.Claimed, claim.Disposition)
				fence := assignmentFence(claim.Assignment)
				if c.started {
					startAttempt(t, store, fence)
				}
				c.arrange(t, store, session, fence)
				before := pendingOutboxIDs(t)

				result := c.run(t, store)
				require.Equal(t, c.expiredLeases, result.ExpiredLeases, "expired-lease repairs")
				require.Equal(t, c.timedOutAttempts, result.TimedOutAttempts, "timeouts recorded")
				requireKillOutcome(t, fence, before, c.want(exhausted))
				require.Equal(t, 0, countActiveLeases(t), "the capacity was released")
			})
		}
	}
}

// lapsingStore is the real store with a hook that runs once, as soon as the first
// of the two outcome sweeps of a pass returns, whichever that is. It is how a
// lease that lapses between the two sweeps of one pass is placed deterministically.
type lapsingStore struct {
	*workers.Store
	once  sync.Once
	lapse func()
	order []string
}

func (s *lapsingStore) ReconcileDueTimeouts(ctx context.Context, limit int) (workers.ReconcileStats, error) {
	stats, err := s.Store.ReconcileDueTimeouts(ctx, limit)
	s.order = append(s.order, "timeouts")
	s.once.Do(s.lapse)
	return stats, err
}

func (s *lapsingStore) ReconcileExpiredLeases(ctx context.Context, limit int) (workers.ReconcileStats, error) {
	stats, err := s.Store.ReconcileExpiredLeases(ctx, limit)
	s.order = append(s.order, "expired-leases")
	s.once.Do(s.lapse)
	return stats, err
}

// TestPrecedence_TheTimeoutSweepRunsFirstInAPass pins RunOnce's order of the two
// outcome sweeps. That order is observable only for an attempt whose lease lapses
// between them: each sweep revalidates under its locks, so for a lease whose state
// does not change during the pass both orders reach the same verdict. Here the
// deadline is due and the lease is live when the pass begins and lapses after the
// first sweep. With the timeout sweep first, the pass records TIMED_OUT and hands
// the lease back (RELEASED); with the order swapped, the expired-lease path finds a
// live lease, the timeout sweep then finds an expired one, and the pass records
// nothing at all: the attempt waits for the next pass, and then ends with its lease
// EXPIRED.
func TestPrecedence_TheTimeoutSweepRunsFirstInAPass(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store, workerRegistration("precedence-order", 1, nil, []string{"demo.echo"}))
	fence, _ := claimedRunningWithTimeout(t, store, session, "precedence-order", 3, 30)
	placeAttempt(t, fence, true, time.Hour, -time.Second)
	staleSession(t, session.ID)

	wrapped := &lapsingStore{Store: store, lapse: func() {
		tag, err := testPool.Exec(ctx, `
			UPDATE leases SET expires_at = clock_timestamp() - interval '1 second'
			WHERE id = $1 AND status = 'ACTIVE'`, fence.LeaseID)
		require.NoError(t, err)
		_ = tag // RowsAffected is 0 when the first sweep already released the lease
	}}
	result, err := reconciler.New(wrapped, reconciler.Config{
		StaleAfter: time.Minute, PollInterval: time.Second, BatchSize: 50,
	}, discardLogger()).RunOnce(ctx)
	require.NoError(t, err)

	require.Equal(t, []string{"timeouts", "expired-leases"}, wrapped.order, "the order RunOnce ran the sweeps in")
	require.Equal(t, 1, result.TimedOutAttempts, "one pass resolves it")
	require.Zero(t, result.ExpiredLeases)
	state := readState(t, fence)
	require.Equal(t, "TIMED_OUT", state.attempt)
	require.Equal(t, "RELEASED", state.lease, "found while the lease was live, so handed back rather than expired")
}

// frozenClockSchema holds the test's own clock_timestamp(). It shadows
// pg_catalog.clock_timestamp() only for connections whose search_path names it
// before pg_catalog, which is the frozen pool alone.
const frozenClockSchema = "m8d4_frozen_clock"

// frozenClockStore returns a store whose every clock_timestamp() returns the given
// instant, and nothing else changed: the same SQL, the same locks, real PostgreSQL.
//
// It is how case (e) is made exact. The reconciler's decision instant is a
// clock_timestamp() sampled after its row locks, which no test can otherwise land
// on exactly. PostgreSQL resolves an unqualified function through the search_path,
// and pg_catalog is searched first only when the path does not name it; a path of
// "m8d4_frozen_clock, pg_catalog, public" makes the schema's clock_timestamp() win.
// The schema is dropped when the test ends.
func frozenClockStore(t *testing.T, at time.Time) *workers.Store {
	t.Helper()
	ctx := context.Background()
	for _, ddl := range []string{
		`CREATE SCHEMA ` + frozenClockSchema,
		`CREATE TABLE ` + frozenClockSchema + `.frozen (at timestamptz NOT NULL)`,
		`CREATE FUNCTION ` + frozenClockSchema + `.clock_timestamp() RETURNS timestamptz
		    LANGUAGE sql VOLATILE AS 'SELECT at FROM ` + frozenClockSchema + `.frozen'`,
	} {
		_, err := testPool.Exec(ctx, ddl)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		_, err := testPool.Exec(context.Background(), `DROP SCHEMA `+frozenClockSchema+` CASCADE`)
		require.NoError(t, err)
	})
	_, err := testPool.Exec(ctx, `INSERT INTO `+frozenClockSchema+`.frozen (at) VALUES ($1)`, at)
	require.NoError(t, err)

	config, err := pgxpool.ParseConfig(dsn())
	require.NoError(t, err)
	config.ConnConfig.RuntimeParams["search_path"] = frozenClockSchema + ", pg_catalog, public"
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var seen time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&seen))
	require.True(t, seen.Equal(at), "the frozen pool's clock must be the frozen instant")
	return workers.NewStore(pool, workers.StoreConfig{
		LeaseDuration: integrationLeaseDuration,
		RetryPolicy:   integrationRetryPolicy(),
	})
}

// TestPrecedence_ADeadlineExactlyAtTheDecisionInstantIsDue is case (e): a
// timeout_at exactly equal to the reconciler's post-lock clock sample is due
// (fenceState.timedOut is !serverNow.Before(timeoutAt)), on both paths, and one
// microsecond later is not.
func TestPrecedence_ADeadlineExactlyAtTheDecisionInstantIsDue(t *testing.T) {
	// The frozen instant is a minute in the past on the real clock, so every
	// timestamp the claim and start wrote (real clock) can be placed before it.
	type arranged struct {
		fence  workers.Fence
		frozen time.Time
	}
	arrange := func(t *testing.T, leaseLive bool, deadlineAfter time.Duration) (arranged, *workers.Store) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store, workerRegistration("precedence-boundary", 1, nil, []string{"demo.echo"}))
		fence, _ := claimedRunningWithTimeout(t, store, session, "precedence-boundary", 3, 30)
		var frozen time.Time
		require.NoError(t, testPool.QueryRow(context.Background(),
			`SELECT date_trunc('milliseconds', clock_timestamp() - interval '1 minute')`).Scan(&frozen))
		expires := frozen // expired at the decision instant: the lease path owns it
		if leaseLive {
			expires = frozen.Add(time.Hour)
		}
		_, err := testPool.Exec(context.Background(), `
			UPDATE job_attempts SET created_at = $2::timestamptz - interval '10 seconds',
			       started_at = $2::timestamptz - interval '10 seconds',
			       timeout_at = $3
			WHERE id = $1`, fence.AttemptID, frozen, frozen.Add(deadlineAfter))
		require.NoError(t, err)
		_, err = testPool.Exec(context.Background(), `
			UPDATE leases SET acquired_at = $2::timestamptz - interval '10 seconds',
			       renewed_at = $2::timestamptz - interval '10 seconds',
			       expires_at = $3
			WHERE id = $1`, fence.LeaseID, frozen, expires)
		require.NoError(t, err)
		return arranged{fence: fence, frozen: frozen}, frozenClockStore(t, frozen)
	}

	t.Run("timeout sweep, lease live, timeout_at == serverNow: TIMED_OUT", func(t *testing.T) {
		a, frozenStore := arrange(t, true, 0)
		stats, err := frozenStore.ReconcileDueTimeouts(context.Background(), 10)
		require.NoError(t, err)
		require.Equal(t, 1, stats.TimedOutAttempts)
		row := readAttemptOutcome(t, a.fence.AttemptID)
		require.Equal(t, "TIMED_OUT", row.status)
		require.True(t, row.finishedAt.Equal(a.frozen), "decided at the frozen instant")
		require.True(t, row.timeoutAt.Equal(*row.finishedAt), "the deadline is exactly the decision instant")
		require.Equal(t, "RELEASED", readState(t, a.fence).lease)
	})
	t.Run("expired-lease path, timeout_at == serverNow: TIMED_OUT", func(t *testing.T) {
		a, frozenStore := arrange(t, false, 0)
		stats, err := frozenStore.ReconcileExpiredLeases(context.Background(), 10)
		require.NoError(t, err)
		require.Equal(t, 1, stats.TimedOutAttempts)
		require.Zero(t, stats.RequeuedJobs)
		row := readAttemptOutcome(t, a.fence.AttemptID)
		require.Equal(t, "TIMED_OUT", row.status)
		require.True(t, row.timeoutAt.Equal(*row.finishedAt))
		require.Equal(t, "EXPIRED", readState(t, a.fence).lease)
	})
	t.Run("expired-lease path, timeout_at one microsecond after serverNow: ABANDONED", func(t *testing.T) {
		a, frozenStore := arrange(t, false, time.Microsecond)
		stats, err := frozenStore.ReconcileExpiredLeases(context.Background(), 10)
		require.NoError(t, err)
		require.Zero(t, stats.TimedOutAttempts)
		require.Equal(t, 1, stats.RequeuedJobs)
		require.Equal(t, "ABANDONED", readAttemptOutcome(t, a.fence.AttemptID).status)
	})
	t.Run("timeout sweep, timeout_at one microsecond after serverNow: not due", func(t *testing.T) {
		a, frozenStore := arrange(t, true, time.Microsecond)
		stats, err := frozenStore.ReconcileDueTimeouts(context.Background(), 10)
		require.NoError(t, err)
		require.Zero(t, stats.TimedOutAttempts)
		require.Equal(t, "RUNNING", readAttemptOutcome(t, a.fence.AttemptID).status)
	})
}

// TestPrecedence_AThirdAttemptThatTimesOutDeadLetters is case (f): two attempts
// abandoned, the third killed while RUNNING and reached after its deadline. It
// consumes the last of max_attempts 3, so the job is dead-lettered, and
// KillOutcomes, read at that kill, reports exactly that attempt as
// DEAD_LETTERED_ON_TIMEOUT and not the two earlier ones.
func TestPrecedence_AThirdAttemptThatTimesOutDeadLetters(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store, workerRegistration("precedence-third", 1, nil, []string{"demo.echo"}))
	jobID := createJobWithOptions(t, "precedence-third", "default", "demo.echo", 50, nil, 3, 30, nil)

	claimAndStart := func() workers.Fence {
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, claim.Disposition)
		require.Equal(t, jobID, claim.Assignment.JobID)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)
		return fence
	}
	for i := 1; i <= 2; i++ {
		fence := claimAndStart()
		placeAttempt(t, fence, true, -2*time.Second, time.Hour)
		stats, err := store.ReconcileExpiredLeases(ctx, 10)
		require.NoError(t, err)
		require.Equal(t, 1, stats.RequeuedJobs, "attempt %d abandoned and requeued", i)
	}
	third := claimAndStart()
	placeAttempt(t, third, true, -2*time.Second, -2*time.Second+12*time.Millisecond)
	kill := serverNow(t)
	before := pendingOutboxIDs(t)

	stats, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.TimedOutAttempts)
	require.Equal(t, 1, stats.DeadLetteredJobs)
	requireKillOutcome(t, third, before, killOutcome{
		attempt: "TIMED_OUT", job: "DEAD_LETTERED", lease: "EXPIRED", class: "TIMED_OUT",
		code: lifecycle.CodeTimeout, dlqReason: "ATTEMPTS_EXHAUSTED",
	})
	require.Equal(t, []string{"ABANDONED", "ABANDONED", "TIMED_OUT"}, attemptHistory(t, jobID))

	outcomes, err := readdb.KillOutcomes(ctx, testPool, testScope, session.ID, kill)
	require.NoError(t, err)
	require.Len(t, outcomes, 1, "the two earlier attempts finished before the kill")
	o := outcomes[0]
	require.Equal(t, 3, o.AttemptNumber)
	require.Equal(t, readdb.HeldRunning, o.StateAtKill)
	require.Equal(t, readdb.OutcomeTimedOutDeadLetter, o.Outcome())
	require.Equal(t, "DEAD_LETTERED", o.JobStatus)
	require.NotNil(t, o.DLQReason)
	require.Equal(t, "ATTEMPTS_EXHAUSTED", *o.DLQReason)
	require.Nil(t, o.ReplacementClaimedAt, "nothing replaces a dead-lettered job")
	require.Nil(t, o.RetryAt, "no retry was decided")
}
