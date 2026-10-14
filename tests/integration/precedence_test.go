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
// M8D5 changed one rule (ADR-0025): on the expired-lease path a lease that lapsed
// strictly before its deadline is an abandonment even when the deadline is due by
// the time the reconciler arrives. Cases (a), (a'), (g'), (h), the tie in case
// (e), and the healthy-worker budget test pin it.
//
// Existing tests already pin parts of these cases, and are not replaced:
// TestTimeout_ExpiredLeaseScanRecognizesAnAlreadyDueDeadline and
// TestTimeout_LeaseExpiryWithNoDueDeadlineIsStillAbandonment (timeout_test.go) for
// (a') and (b), TestTimeout_DueAttemptBecomesTimedOutAndEntersRetryWait for the
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

// placed is where placeAttempt put a case's instants: every one of them relative
// to the single clock reading now.
type placed struct {
	now       time.Time
	expiresAt time.Time
	timeoutAt time.Time // zero for an attempt that never started
}

// placeAttempt rewrites a claimed attempt's timeline relative to ONE reading of
// clock_timestamp(): created and (when started) started ten minutes before it, the
// lease acquired then and expiring at expiresIn from it, and the deadline at
// timeoutIn from it. It is what a worker killed ten minutes ago leaves behind, with
// the two instants that decide its outcome put where the case needs them.
//
// Since M8D5 the ORDER of those two instants decides a lapsed lease's outcome
// (ADR-0025), so both are computed from the same reading and read back: no case
// may depend on the microseconds between two clock reads.
func placeAttempt(t *testing.T, fence workers.Fence, started bool, expiresIn, timeoutIn time.Duration) placed {
	t.Helper()
	ctx := context.Background()
	p := placed{now: serverNow(t)}
	p.expiresAt = p.now.Add(expiresIn)
	if started {
		p.timeoutAt = p.now.Add(timeoutIn)
		tag, err := testPool.Exec(ctx, `
			UPDATE job_attempts
			SET created_at = $2::timestamptz - interval '10 minutes',
			    started_at = $2::timestamptz - interval '10 minutes' + interval '12 milliseconds',
			    timeout_at = $3
			WHERE id = $1 AND status = 'RUNNING'`, fence.AttemptID, p.now, p.timeoutAt)
		require.NoError(t, err)
		require.EqualValues(t, 1, tag.RowsAffected())
	} else {
		tag, err := testPool.Exec(ctx, `
			UPDATE job_attempts SET created_at = $2::timestamptz - interval '10 minutes'
			WHERE id = $1 AND status = 'LEASED' AND timeout_at IS NULL`, fence.AttemptID, p.now)
		require.NoError(t, err)
		require.EqualValues(t, 1, tag.RowsAffected())
	}
	// A lease that was renewed keeps a renewal instant after its acquisition: a
	// minute before the reading, so that it is still before any expiry a case
	// places.
	tag, err := testPool.Exec(ctx, `
		UPDATE leases
		SET acquired_at = $2::timestamptz - interval '10 minutes',
		    renewed_at = CASE WHEN renewal_version > 0 THEN $2::timestamptz - interval '1 minute'
		                      ELSE $2::timestamptz - interval '10 minutes' END,
		    expires_at = $3
		WHERE id = $1 AND status = 'ACTIVE'`, fence.LeaseID, p.now, p.expiresAt)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())

	// Read back: the stored instants are exactly the placed ones, so their order is
	// the case's and not an accident of timing.
	var expiresAt time.Time
	var timeoutAt *time.Time
	require.NoError(t, testPool.QueryRow(ctx, `
		SELECT l.expires_at, a.timeout_at FROM leases l JOIN job_attempts a ON a.id = $2
		WHERE l.id = $1`, fence.LeaseID, fence.AttemptID).Scan(&expiresAt, &timeoutAt))
	require.True(t, expiresAt.Equal(p.expiresAt), "expires_at is where the case put it")
	if started {
		require.NotNil(t, timeoutAt)
		require.True(t, timeoutAt.Equal(p.timeoutAt), "timeout_at is where the case put it")
		require.Equal(t, timeoutIn-expiresIn, timeoutAt.Sub(expiresAt), "timeout_at - expires_at is exactly the case's")
	} else {
		require.Nil(t, timeoutAt)
	}
	return p
}

// placeDeadlineAndExpiry puts a RUNNING attempt's deadline and its lease's expiry
// at the given offsets from ONE reading of clock_timestamp(), both lease and
// attempt timelines moved with them, and returns that reading. Since M8D5 the
// order of the two decides a lapsed lease's outcome (ADR-0025), so a test that
// places both must not leave it to the microseconds between two clock reads, as
// calling expireAttemptDeadline and then expireLease did.
func placeDeadlineAndExpiry(t *testing.T, fence workers.Fence, timeoutIn, expiresIn time.Duration) time.Time {
	t.Helper()
	ctx := context.Background()
	now := serverNow(t)
	tag, err := testPool.Exec(ctx, `
		UPDATE job_attempts
		SET created_at = $2::timestamptz - interval '3 minutes',
		    started_at = $2::timestamptz - interval '2 minutes',
		    timeout_at = $3
		WHERE id = $1`, fence.AttemptID, now, now.Add(timeoutIn))
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
	tag, err = testPool.Exec(ctx, `
		UPDATE leases
		SET acquired_at = $2::timestamptz - interval '2 minutes',
		    renewed_at = $2::timestamptz - interval '2 minutes',
		    expires_at = $3
		WHERE id = $1`, fence.LeaseID, now, now.Add(expiresIn))
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())

	var timeoutAt, expiresAt time.Time
	require.NoError(t, testPool.QueryRow(ctx, `
		SELECT a.timeout_at, l.expires_at FROM job_attempts a JOIN leases l ON l.id = $2
		WHERE a.id = $1`, fence.AttemptID, fence.LeaseID).Scan(&timeoutAt, &expiresAt))
	require.Equal(t, timeoutIn-expiresIn, timeoutAt.Sub(expiresAt), "timeout_at - expires_at is exactly the test's")
	return now
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

// abandoned is ADR-0009's outcome: requeued at once with budget left, or
// dead-lettered for ATTEMPTS_EXHAUSTED when it was the last attempt.
func abandoned(exhausted bool) killOutcome {
	want := killOutcome{attempt: "ABANDONED", lease: "EXPIRED", class: "ABANDONED", code: lifecycle.CodeAbandoned}
	if exhausted {
		want.job, want.dlqReason = "DEAD_LETTERED", "ATTEMPTS_EXHAUSTED"
	} else {
		want.job, want.immediate, want.event = "QUEUED", true, true
	}
	return want
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

// TestPrecedence_AKilledAttemptsOutcome is cases (a) to (d), (g) and (h), with
// M8D5's (a') and (g') beside the cases they mirror. Each runs
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
	canceled := func(bool) killOutcome {
		return killOutcome{attempt: "CANCELED", job: "CANCELED", lease: "EXPIRED",
			class: "CANCELED", code: lifecycle.CodeCanceled}
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
			// T1. A killed RUNNING attempt never renewed, with timeout_seconds equal to
			// the lease: the expiry (claim + 30 s) falls a claim-to-start latency, here
			// 12 ms, before the deadline (start + 30 s). A pass that comes after both
			// finds the deadline due, and since M8D5 that no longer matters: the lease
			// lapsed strictly before the deadline, so it is an abandonment (ADR-0025).
			// M8D4 recorded TIMED_OUT here.
			name: "(a) RUNNING, lease expired 12 ms before a deadline that is now due: ABANDONED on the expired-lease path", started: true,
			arrange: func(t *testing.T, _ *workers.Store, session workers.Session, fence workers.Fence) {
				p := placeAttempt(t, fence, true, -2*time.Second, -2*time.Second+12*time.Millisecond)
				require.True(t, p.expiresAt.Before(p.timeoutAt) && !p.now.Before(p.timeoutAt), "lapsed first, both past")
				staleSession(t, session.ID)
			},
			run: runOnce, expiredLeases: 1, timedOutAttempts: 0, want: abandoned,
		},
		{
			// T2. The mirror image: the lease lapsed AFTER the deadline (a deadline
			// shorter than the lease, or a lease renewed past it), and the pass comes
			// late, after both. The lease outlived the budget, so it is a timeout, on
			// the expired-lease path, with backoff.
			name: "(a') RUNNING, lease expired 12 ms after a due deadline, late pass: TIMED_OUT on the expired-lease path", started: true,
			arrange: func(t *testing.T, _ *workers.Store, session workers.Session, fence workers.Fence) {
				p := placeAttempt(t, fence, true, -2*time.Second, -2*time.Second-12*time.Millisecond)
				require.True(t, p.expiresAt.After(p.timeoutAt), "the deadline came first")
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
			// T5. Cancellation won first, so the lapse is neither a timeout nor an
			// abandonment, even with the deadline due and the lease lapsed before it
			// (which on its own would now be an abandonment): no budget, no retry, no
			// event.
			name: "(g) CANCEL_REQUESTED, lease expired before a due deadline: the cancellation is finalized", started: true,
			arrange: func(t *testing.T, _ *workers.Store, session workers.Session, fence workers.Fence) {
				_, err := jobStore().RequestCancel(context.Background(), testScope, fence.JobID)
				require.NoError(t, err)
				p := placeAttempt(t, fence, true, -2*time.Second, -time.Second)
				require.True(t, p.expiresAt.Before(p.timeoutAt), "lapsed first")
				staleSession(t, session.ID)
			},
			run: runOnce, expiredLeases: 1, timedOutAttempts: 0, want: canceled,
		},
		{
			// The same with the lease lapsing after the deadline, which on its own
			// would be a timeout: cancellation still wins.
			name: "(g') CANCEL_REQUESTED, lease expired after a due deadline: the cancellation is finalized", started: true,
			arrange: func(t *testing.T, _ *workers.Store, session workers.Session, fence workers.Fence) {
				_, err := jobStore().RequestCancel(context.Background(), testScope, fence.JobID)
				require.NoError(t, err)
				p := placeAttempt(t, fence, true, -time.Second, -2*time.Second)
				require.True(t, p.expiresAt.After(p.timeoutAt), "the deadline came first")
				staleSession(t, session.ID)
			},
			run: runOnce, expiredLeases: 1, timedOutAttempts: 0, want: canceled,
		},
		{
			// T7. A healthy worker whose handler overran: it renewed its lease up to
			// the deadline (renewal refuses after it), so the lease expires 19 s after
			// the deadline, and the pass comes after both. The session is healthy and
			// stays so. The lease outlived the budget: TIMED_OUT, with backoff.
			name: "(h) RUNNING, renewed lease expired 19 s after a due deadline, session healthy: TIMED_OUT", started: true,
			arrange: func(t *testing.T, store *workers.Store, _ workers.Session, fence workers.Fence) {
				_, err := store.RenewLease(context.Background(), testScope, renewalRequest(fence, 0))
				require.NoError(t, err)
				p := placeAttempt(t, fence, true, -time.Second, -20*time.Second)
				require.Equal(t, 19*time.Second, p.expiresAt.Sub(p.timeoutAt))
			},
			run: runOnce, expiredLeases: 1, timedOutAttempts: 1, want: timedOut("EXPIRED"),
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
	p := placeAttempt(t, fence, true, time.Hour, -time.Second)
	staleSession(t, session.ID)

	// The lapse puts expires_at half a second after the deadline, both past, from
	// the same reading as the deadline: under either rule a lease that outlived the
	// deadline is a timeout, so what this test observes is the sweep order alone.
	lapsedAt := p.timeoutAt.Add(500 * time.Millisecond)
	require.True(t, lapsedAt.Before(p.now))
	wrapped := &lapsingStore{Store: store, lapse: func() {
		tag, err := testPool.Exec(ctx, `
			UPDATE leases SET expires_at = $2
			WHERE id = $1 AND status = 'ACTIVE'`, fence.LeaseID, lapsedAt)
		require.NoError(t, err)
		_ = tag // RowsAffected is 0 when the first sweep already released the lease
	}}
	result, err := reconciler.New(wrapped, reconciler.Config{
		StaleAfter: time.Minute, PollInterval: time.Second, BatchSize: 50,
	}, discardLogger()).RunOnce(ctx)
	require.NoError(t, err)

	state := readState(t, fence)
	require.Equal(t, "TIMED_OUT", state.attempt, "one pass resolves it")
	require.Equal(t, "RELEASED", state.lease, "found while the lease was live, so handed back rather than expired")
	require.Equal(t, 1, result.TimedOutAttempts)
	require.Zero(t, result.ExpiredLeases)
	require.Equal(t, []string{"timeouts", "expired-leases"}, wrapped.order, "the order RunOnce ran the sweeps in")
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
	// A run interrupted before its cleanup leaves the schema behind; dropping it
	// first keeps that from failing every later run.
	for _, ddl := range []string{
		`DROP SCHEMA IF EXISTS ` + frozenClockSchema + ` CASCADE`,
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
// microsecond later is not. Since M8D5 it also pins the tie between the two
// stored instants on the expired-lease path: expires_at == timeout_at is
// TIMED_OUT, one microsecond earlier is ABANDONED.
func TestPrecedence_ADeadlineExactlyAtTheDecisionInstantIsDue(t *testing.T) {
	// The frozen instant is a minute in the past on the real clock, so every
	// timestamp the claim and start wrote (real clock) can be placed before it.
	type arranged struct {
		fence  workers.Fence
		frozen time.Time
	}
	// arrange places expires_at and timeout_at at the given offsets from the frozen
	// instant, which is also the reconciler's post-lock sample.
	const leaseLive = time.Hour
	arrange := func(t *testing.T, expiresAfter, deadlineAfter time.Duration) (arranged, *workers.Store) {
		reset(t)
		store := controlStore()
		session := registerWorker(t, store, workerRegistration("precedence-boundary", 1, nil, []string{"demo.echo"}))
		fence, _ := claimedRunningWithTimeout(t, store, session, "precedence-boundary", 3, 30)
		var frozen time.Time
		require.NoError(t, testPool.QueryRow(context.Background(),
			`SELECT date_trunc('milliseconds', clock_timestamp() - interval '1 minute')`).Scan(&frozen))
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
			WHERE id = $1`, fence.LeaseID, frozen, frozen.Add(expiresAfter))
		require.NoError(t, err)
		return arranged{fence: fence, frozen: frozen}, frozenClockStore(t, frozen)
	}

	t.Run("timeout sweep, lease live, timeout_at == serverNow: TIMED_OUT", func(t *testing.T) {
		a, frozenStore := arrange(t, leaseLive, 0)
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
		a, frozenStore := arrange(t, 0, 0)
		// expires_at == timeout_at == serverNow: the lease is no longer usable, so the timeout
		// sweep, though the deadline is due, must leave the attempt to the lease
		// path (internal/workers/timeout.go requires a usable lease).
		swept, err := frozenStore.ReconcileDueTimeouts(context.Background(), 10)
		require.NoError(t, err)
		require.Zero(t, swept.TimedOutAttempts, "the timeout sweep skips an attempt whose lease expires at the decision instant")
		require.Equal(t, 1, swept.Skipped, "it was a candidate and was skipped under the locks")
		require.Equal(t, "RUNNING", readAttemptOutcome(t, a.fence.AttemptID).status)

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
		a, frozenStore := arrange(t, 0, time.Microsecond)
		stats, err := frozenStore.ReconcileExpiredLeases(context.Background(), 10)
		require.NoError(t, err)
		require.Zero(t, stats.TimedOutAttempts)
		require.Equal(t, 1, stats.RequeuedJobs)
		require.Equal(t, "ABANDONED", readAttemptOutcome(t, a.fence.AttemptID).status)
	})
	t.Run("timeout sweep, timeout_at one microsecond after serverNow: not due", func(t *testing.T) {
		a, frozenStore := arrange(t, leaseLive, time.Microsecond)
		stats, err := frozenStore.ReconcileDueTimeouts(context.Background(), 10)
		require.NoError(t, err)
		require.Zero(t, stats.TimedOutAttempts)
		require.Equal(t, "RUNNING", readAttemptOutcome(t, a.fence.AttemptID).status)
	})

	// T3, M8D5's tie rule (ADR-0025): on the expired-lease path with the deadline
	// due (a second before the decision instant), the outcome turns on expires_at
	// against timeout_at alone. One microsecond before is an abandonment; equal is
	// a timeout.
	expiredLeasePath := func(t *testing.T, expiresAfter time.Duration) (arranged, workers.ReconcileStats) {
		a, frozenStore := arrange(t, expiresAfter, -time.Second)
		stats, err := frozenStore.ReconcileExpiredLeases(context.Background(), 10)
		require.NoError(t, err)
		require.Equal(t, 1, stats.ExpiredLeases)
		row := readAttemptOutcome(t, a.fence.AttemptID)
		require.True(t, row.finishedAt.Equal(a.frozen), "decided at the frozen instant")
		require.True(t, row.timeoutAt.Before(*row.finishedAt), "the deadline is due")
		require.Equal(t, "EXPIRED", readState(t, a.fence).lease)
		return a, stats
	}
	t.Run("expired-lease path, deadline due, expires_at one microsecond before timeout_at: ABANDONED", func(t *testing.T) {
		a, stats := expiredLeasePath(t, -time.Second-time.Microsecond)
		require.Zero(t, stats.TimedOutAttempts)
		require.Equal(t, 1, stats.RequeuedJobs)
		row := readAttemptOutcome(t, a.fence.AttemptID)
		require.Equal(t, "ABANDONED", row.status)
		require.NotNil(t, row.retryDelayMs)
		require.Zero(t, *row.retryDelayMs, "requeued with no backoff")
	})
	t.Run("expired-lease path, deadline due, expires_at == timeout_at: TIMED_OUT", func(t *testing.T) {
		a, stats := expiredLeasePath(t, -time.Second)
		require.Equal(t, 1, stats.TimedOutAttempts)
		require.Zero(t, stats.RequeuedJobs)
		row := readAttemptOutcome(t, a.fence.AttemptID)
		require.Equal(t, "TIMED_OUT", row.status)
		require.NotNil(t, row.retryDelayMs)
		require.Positive(t, *row.retryDelayMs, "a timeout backs off")
	})
}

// TestPrecedence_AThirdAttemptThatTimesOutDeadLetters is case (f): two attempts
// abandoned, the third killed while RUNNING and reached after its deadline. It
// consumes the last of max_attempts 3, so the job is dead-lettered, and
// KillOutcomes, read at that kill, reports exactly that attempt as dead-lettered
// and not the two earlier ones.
//
// Since M8D5 the third attempt's lease decides which kind (ADR-0025). A lease
// renewed past the deadline lapses after it: DEAD_LETTERED_ON_TIMEOUT, the case's
// original intent. A lease never renewed lapses 12 ms before it:
// DEAD_LETTERED_ON_ABANDON, even though the deadline is due when the pass comes.
func TestPrecedence_AThirdAttemptThatTimesOutDeadLetters(t *testing.T) {
	cases := []struct {
		name                 string
		renew                bool
		expiresIn, timeoutIn time.Duration
		want                 killOutcome
		bucket               string
		timedOutAttempts     int
	}{
		{
			name: "renewed lease lapsed 1 s after the deadline: DEAD_LETTERED_ON_TIMEOUT", renew: true,
			expiresIn: -2 * time.Second, timeoutIn: -3 * time.Second,
			want: killOutcome{attempt: "TIMED_OUT", job: "DEAD_LETTERED", lease: "EXPIRED", class: "TIMED_OUT",
				code: lifecycle.CodeTimeout, dlqReason: "ATTEMPTS_EXHAUSTED"},
			bucket: readdb.OutcomeTimedOutDeadLetter, timedOutAttempts: 1,
		},
		{
			name:      "lease never renewed lapsed 12 ms before a due deadline: DEAD_LETTERED_ON_ABANDON",
			expiresIn: -2 * time.Second, timeoutIn: -2*time.Second + 12*time.Millisecond,
			want: abandoned(true), bucket: readdb.OutcomeAbandonedDeadLetter, timedOutAttempts: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
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
			if c.renew {
				_, err := store.RenewLease(ctx, testScope, renewalRequest(third, 0))
				require.NoError(t, err)
			}
			p := placeAttempt(t, third, true, c.expiresIn, c.timeoutIn)
			require.True(t, !p.now.Before(p.timeoutAt), "the deadline is due when the pass comes")
			kill := serverNow(t)
			before := pendingOutboxIDs(t)

			stats, err := store.ReconcileExpiredLeases(ctx, 10)
			require.NoError(t, err)
			require.Equal(t, c.timedOutAttempts, stats.TimedOutAttempts)
			require.Equal(t, 1, stats.DeadLetteredJobs)
			requireKillOutcome(t, third, before, c.want)
			require.Equal(t, []string{"ABANDONED", "ABANDONED", c.want.attempt}, attemptHistory(t, jobID))

			outcomes, err := readdb.KillOutcomes(ctx, testPool, testScope, session.ID, kill)
			require.NoError(t, err)
			require.Len(t, outcomes, 1, "the two earlier attempts finished before the kill")
			o := outcomes[0]
			require.Equal(t, 3, o.AttemptNumber)
			require.Equal(t, readdb.HeldRunning, o.StateAtKill)
			require.Equal(t, c.bucket, o.Outcome())
			require.Equal(t, "DEAD_LETTERED", o.JobStatus)
			require.NotNil(t, o.DLQReason)
			require.Equal(t, "ATTEMPTS_EXHAUSTED", *o.DLQReason)
			require.Nil(t, o.ReplacementClaimedAt, "nothing replaces a dead-lettered job")
			require.Nil(t, o.RetryAt, "no retry was decided")
		})
	}
}

// TestPrecedence_AHealthyWorkerThatKeepsLosingItsLeaseExhaustsItsBudget is T6,
// the risk ADR-0025 accepts. A worker whose session stays HEALTHY (it keeps
// heartbeating) but whose RUNNING attempt's lease lapses before a deadline that
// is due by the time the reconciler arrives is abandoned, not timed out, every
// round: requeued at once with no backoff, and claimed again. It cannot loop
// forever: abandonment consumes the attempt budget (ADR-0009), so after
// max_attempts rounds the job is dead-lettered, with exactly max_attempts
// attempts, the last of them ABANDONED.
func TestPrecedence_AHealthyWorkerThatKeepsLosingItsLeaseExhaustsItsBudget(t *testing.T) {
	const maxAttempts = 3
	reset(t)
	ctx := context.Background()
	store := controlStoreWithJitter(lifecycle.NewSeededJitter(8))
	session := registerWorker(t, store, workerRegistration("precedence-starved", 1, nil, []string{"demo.echo"}))
	jobID := createJobWithOptions(t, "precedence-starved", "default", "demo.echo", 50, nil, maxAttempts, 30, nil)

	for round := 1; round <= maxAttempts; round++ {
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, claim.Disposition, "round %d: claimed at once", round)
		require.Equal(t, jobID, claim.Assignment.JobID)
		require.Equal(t, round, claim.Assignment.AttemptNumber)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)
		p := placeAttempt(t, fence, true, -2*time.Second, -2*time.Second+12*time.Millisecond)
		require.True(t, p.expiresAt.Before(p.timeoutAt) && !p.now.Before(p.timeoutAt), "lapsed first, both past")
		before := pendingOutboxIDs(t)

		result, err := newReconciler(t, store, time.Minute).RunOnce(ctx)
		require.NoError(t, err)
		require.Zero(t, result.StaleSessions, "round %d: the session is healthy", round)
		require.Equal(t, 1, result.ExpiredLeases)
		require.Zero(t, result.TimedOutAttempts, "round %d: not a timeout", round)
		requireKillOutcome(t, fence, before, abandoned(round == maxAttempts))

		var status string
		require.NoError(t, testPool.QueryRow(ctx,
			`SELECT status FROM worker_sessions WHERE id = $1`, session.ID).Scan(&status))
		require.Equal(t, "HEALTHY", status, "round %d", round)
	}

	require.Equal(t, []string{"ABANDONED", "ABANDONED", "ABANDONED"}, attemptHistory(t, jobID), "exactly max_attempts attempts")
	require.Equal(t, "DEAD_LETTERED", readJob(t, jobID).status)
	require.Equal(t, []string{"ATTEMPTS_EXHAUSTED"}, dlqRows(t, jobID))
	claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.NotEqual(t, workers.Claimed, claim.Disposition, "nothing is left to claim")
}

// TestPrecedence_AnEarlierTimeoutIsNotTheAttemptThatDeadLettered is the review's B1
// fixture. Attempt 1 is killed RUNNING and times out (budget left: RETRY_WAIT);
// attempt 2, after the retry, is killed and abandoned, which exhausts max_attempts 2
// and dead-letters the job. Read at attempt 1's kill, attempt 1 is a plain timeout
// even though its job is now DEAD_LETTERED: the DLQ entry names attempt 2. Read at
// attempt 2's kill, attempt 2 is the one dead-lettered, on an abandonment.
func TestPrecedence_AnEarlierTimeoutIsNotTheAttemptThatDeadLettered(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := controlStore()
	session := registerWorker(t, store, workerRegistration("precedence-earlier-timeout", 1, nil, []string{"demo.echo"}))
	jobID := createJobWithOptions(t, "precedence-earlier-timeout", "default", "demo.echo", 50, nil, 2, 30, nil)
	claimAndStart := func() workers.Fence {
		claim, err := store.Claim(ctx, testScope, claimRequest(session, "default"))
		require.NoError(t, err)
		require.Equal(t, workers.Claimed, claim.Disposition)
		fence := assignmentFence(claim.Assignment)
		startAttempt(t, store, fence)
		return fence
	}

	// Attempt 1's lease was renewed past its deadline and lapsed a second after it,
	// so it is a timeout under ADR-0025 as it was before.
	first := claimAndStart()
	_, err := store.RenewLease(ctx, testScope, renewalRequest(first, 0))
	require.NoError(t, err)
	p := placeAttempt(t, first, true, -2*time.Second, -3*time.Second)
	require.True(t, p.expiresAt.After(p.timeoutAt))
	firstKill := serverNow(t)
	stats, err := store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.TimedOutAttempts)
	require.Equal(t, 1, stats.RetryWaitingJobs)

	makeRetryDue(t, jobID)
	promoted, err := jobStore().PromoteDueJobs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, promoted.PromotedJobs)
	second := claimAndStart()
	expireLease(t, second.LeaseID) // its deadline, start + 30 s, is not due: an abandonment
	secondKill := serverNow(t)
	stats, err = store.ReconcileExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, stats.DeadLetteredJobs)
	require.Equal(t, []string{"TIMED_OUT", "ABANDONED"}, attemptHistory(t, jobID))
	require.Equal(t, "DEAD_LETTERED", readJob(t, jobID).status)

	byNumber := func(kill time.Time, number int) readdb.KillOutcome {
		t.Helper()
		outcomes, err := readdb.KillOutcomes(ctx, testPool, testScope, session.ID, kill)
		require.NoError(t, err)
		for _, o := range outcomes {
			if o.AttemptNumber == number {
				return o
			}
		}
		require.FailNow(t, "attempt not reported", "attempt %d at kill %s", number, kill)
		return readdb.KillOutcome{}
	}

	earlier := byNumber(firstKill, 1)
	require.Equal(t, "DEAD_LETTERED", earlier.JobStatus, "the job as it is now")
	require.Nil(t, earlier.DLQReason, "the DLQ entry names attempt 2, not this one")
	require.Equal(t, readdb.OutcomeTimedOut, earlier.Outcome(),
		"a timeout with budget left, whatever a later attempt did to the job")

	last := byNumber(secondKill, 2)
	require.NotNil(t, last.DLQReason)
	require.Equal(t, "ATTEMPTS_EXHAUSTED", *last.DLQReason)
	require.Equal(t, readdb.OutcomeAbandonedDeadLetter, last.Outcome(), "the abandonment that exhausted the budget")
}
