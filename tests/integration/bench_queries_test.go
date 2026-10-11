//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// These tests prove the queries scripts/bench measures with, against rows whose
// timestamps are written by the test itself. The harness's numbers are only as
// good as these queries: a query that measured the wrong column would produce a
// confident, plausible, wrong benchmark, and nothing in the harness could tell.
//
// So each case seeds instants that are all different (created, started and
// leased are never equal) and asserts the exact difference the definition in
// docs/adr/0020-benchmark-methodology.md names. The harness imports the same
// functions; there is no second copy of any query.

const benchScope = "bench-queries"

// benchBase is a fixed instant, so every expected duration is a literal.
var benchBase = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

// benchSeed writes rows directly, with the timestamps the test chooses. It uses
// the same shapes the application writes, so the schema's own constraints still
// apply to them.
type benchSeed struct {
	t     *testing.T
	scope string
}

func (s benchSeed) job(id uuid.UUID, status string, created time.Time, scheduled *time.Time) {
	s.t.Helper()
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO jobs (
			id, scope, queue, job_type, payload, status, priority, max_attempts,
			timeout_seconds, available_at, created_at, updated_at,
			notification_generation, last_notification_at, scheduled_at
		) VALUES ($1, $2, 'default', 'demo.sleep', '{"duration_ms":50}', $3, 50, 3, 30,
		          $4, $4, $4, 1, $4, $5)`,
		id, s.scope, status, created, scheduled)
	require.NoError(s.t, err)
}

func (s benchSeed) worker(name string) uuid.UUID {
	s.t.Helper()
	id := uuid.New()
	_, err := testPool.Exec(context.Background(),
		`INSERT INTO workers (id, scope, name) VALUES ($1, $2, $3)`, id, s.scope, name)
	require.NoError(s.t, err)
	return id
}

// session writes one process boot. A session that is not current needs an end
// time; the one-current-session-per-worker index forbids two current ones.
func (s benchSeed) session(workerID uuid.UUID, registered time.Time, status string, ended *time.Time) uuid.UUID {
	s.t.Helper()
	id := uuid.New()
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO worker_sessions (
			id, worker_id, scope, hostname, worker_group, concurrency_limit,
			capabilities, supported_job_types, status, registered_at,
			last_heartbeat_at, ended_at
		) VALUES ($1, $2, $3, 'bench.local', 'default', 4, '{cpu}', '{demo.sleep}',
		          $4, $5, $5, $6)`,
		id, workerID, s.scope, status, registered, ended)
	require.NoError(s.t, err)
	return id
}

func (s benchSeed) attempt(jobID uuid.UUID, number int, workerID, sessionID uuid.UUID,
	status string, created time.Time, started, finished *time.Time) uuid.UUID {
	s.t.Helper()
	id := uuid.New()
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO job_attempts (
			id, job_id, scope, queue, attempt_number, worker_id, worker_session_id,
			status, created_at, started_at, finished_at
		) VALUES ($1, $2, $3, 'default', $4, $5, $6, $7, $8, $9, $10)`,
		id, jobID, s.scope, number, workerID, sessionID, status, created, started, finished)
	require.NoError(s.t, err)
	return id
}

func (s benchSeed) lease(attemptID, jobID, workerID, sessionID uuid.UUID, acquired time.Time) {
	s.t.Helper()
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO leases (
			id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
			claim_request_id, status, acquired_at, renewed_at, expires_at
		) VALUES (gen_random_uuid(), $1, $2, $3, 'default', $4, $5, gen_random_uuid(),
		          'ACTIVE', $6::timestamptz, $6::timestamptz, $6::timestamptz + interval '30 seconds')`,
		jobID, attemptID, s.scope, workerID, sessionID, acquired)
	require.NoError(s.t, err)
}

func at(offset time.Duration) time.Time { return benchBase.Add(offset) }
func ptr(t time.Time) *time.Time        { return &t }

func TestBenchQueries_DispatchLatencyIsTheClaimMinusTheSubmission(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}
	worker := seed.worker("w1")
	session := seed.session(worker, at(-time.Minute), "HEALTHY", nil)

	from, to := at(0), at(10*time.Second)

	// In the window and claimed. The claim (attempt 1's created_at) is 40ms after
	// the submission, the start report is 90ms after it, and the lease is issued
	// 55ms after it: three different instants, so asserting 40ms can only be
	// satisfied by reading created_at.
	claimed := uuid.New()
	seed.job(claimed, "SUCCEEDED", at(time.Second), nil)
	a1 := seed.attempt(claimed, 1, worker, session, "SUCCEEDED",
		at(time.Second+40*time.Millisecond), ptr(at(time.Second+90*time.Millisecond)), ptr(at(time.Second+140*time.Millisecond)))
	seed.lease(a1, claimed, worker, session, at(time.Second+55*time.Millisecond))

	// A second claimed job, whose first attempt was abandoned and retried: only
	// attempt 1 is dispatch, and the retry (attempt 2) must not be read instead.
	retried := uuid.New()
	seed.job(retried, "SUCCEEDED", at(2*time.Second), nil)
	seed.attempt(retried, 1, worker, session, "ABANDONED",
		at(2*time.Second+250*time.Millisecond), nil, ptr(at(2*time.Second+30*time.Second)))
	seed.attempt(retried, 2, worker, session, "SUCCEEDED",
		at(2*time.Second+31*time.Second), ptr(at(2*time.Second+31*time.Second+10*time.Millisecond)),
		ptr(at(2*time.Second+31*time.Second+60*time.Millisecond)))

	// Exactly on the window's start: included. The window is [from, to).
	onStart := uuid.New()
	seed.job(onStart, "SUCCEEDED", from, nil)
	seed.attempt(onStart, 1, worker, session, "SUCCEEDED",
		from.Add(7*time.Millisecond), ptr(from.Add(9*time.Millisecond)), ptr(from.Add(12*time.Millisecond)))

	// In the window and never claimed: kept, with no claim, so the harness can
	// count it instead of silently averaging it away.
	unclaimed := uuid.New()
	seed.job(unclaimed, "QUEUED", at(3*time.Second), nil)

	// Excluded: a delayed job, however it is spelled in the window.
	delayed := uuid.New()
	seed.job(delayed, "PENDING", at(4*time.Second), ptr(at(time.Hour)))

	// Excluded: before the window, exactly at its end, and in another scope.
	seed.job(uuid.New(), "QUEUED", from.Add(-time.Nanosecond*1000), nil)
	seed.job(uuid.New(), "QUEUED", to, nil)
	other := benchSeed{t: t, scope: "bench-queries-other"}
	other.job(uuid.New(), "QUEUED", at(5*time.Second), nil)

	got, err := readdb.Dispatches(ctx, testPool, benchScope, from, to)
	require.NoError(t, err)

	byID := map[uuid.UUID]readdb.Dispatch{}
	for _, d := range got {
		byID[d.JobID] = d
	}
	require.Len(t, byID, 4, "claimed, retried, on-the-start and unclaimed; nothing else")
	require.NotContains(t, byID, delayed)

	latency, ok := byID[claimed].Latency()
	require.True(t, ok)
	require.Equal(t, 40*time.Millisecond, latency, "claim = attempt 1's created_at, not its started_at (90ms)")

	leaseLatency, ok := byID[claimed].LeaseLatency()
	require.True(t, ok)
	require.Equal(t, 55*time.Millisecond, leaseLatency, "the supplementary figure reads the lease's acquired_at")

	latency, ok = byID[retried].Latency()
	require.True(t, ok)
	require.Equal(t, 250*time.Millisecond, latency, "attempt 1, not the retry 31s later")
	_, ok = byID[retried].LeaseLatency()
	require.False(t, ok, "no lease was seeded for it, so there is no supplementary figure")

	latency, ok = byID[onStart].Latency()
	require.True(t, ok)
	require.Equal(t, 7*time.Millisecond, latency)

	_, ok = byID[unclaimed].Latency()
	require.False(t, ok, "a job nobody has claimed has no dispatch latency yet")
	require.Nil(t, byID[unclaimed].ClaimedAt)
}

func TestBenchQueries_FinishTimeIsTheSucceededAttemptsAndCountedOncePerJob(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}
	worker := seed.worker("w1")
	session := seed.session(worker, at(-time.Minute), "HEALTHY", nil)

	// Succeeded on attempt 2 after attempt 1 was abandoned: one finish, the
	// SUCCEEDED attempt's, never the abandoned attempt's.
	retried := uuid.New()
	seed.job(retried, "SUCCEEDED", at(0), nil)
	seed.attempt(retried, 1, worker, session, "ABANDONED", at(10*time.Millisecond), nil, ptr(at(31*time.Second)))
	seed.attempt(retried, 2, worker, session, "SUCCEEDED", at(32*time.Second),
		ptr(at(32*time.Second+5*time.Millisecond)), ptr(at(32*time.Second+80*time.Millisecond)))

	plain := uuid.New()
	seed.job(plain, "SUCCEEDED", at(time.Second), nil)
	seed.attempt(plain, 1, worker, session, "SUCCEEDED", at(time.Second+20*time.Millisecond),
		ptr(at(time.Second+30*time.Millisecond)), ptr(at(time.Second+100*time.Millisecond)))

	// Not finished successfully: neither contributes.
	failed := uuid.New()
	seed.job(failed, "DEAD_LETTERED", at(2*time.Second), nil)
	seed.attempt(failed, 1, worker, session, "FAILED", at(2*time.Second+time.Millisecond),
		ptr(at(2*time.Second+2*time.Millisecond)), ptr(at(2*time.Second+3*time.Millisecond)))
	seed.job(uuid.New(), "RUNNING", at(3*time.Second), nil)

	other := benchSeed{t: t, scope: "bench-queries-other"}
	otherWorker := other.worker("w1")
	otherSession := other.session(otherWorker, at(-time.Minute), "HEALTHY", nil)
	foreign := uuid.New()
	other.job(foreign, "SUCCEEDED", at(0), nil)
	other.attempt(foreign, 1, otherWorker, otherSession, "SUCCEEDED", at(time.Millisecond),
		ptr(at(2*time.Millisecond)), ptr(at(3*time.Millisecond)))

	got, err := readdb.SucceededFinishes(ctx, testPool, benchScope)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.True(t, got[0].Equal(at(time.Second+100*time.Millisecond)), "ordered by finish time: %v", got)
	require.True(t, got[1].Equal(at(32*time.Second+80*time.Millisecond)), "%v", got)

	counts, err := readdb.StatusCounts(ctx, testPool, benchScope)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"SUCCEEDED": 2, "DEAD_LETTERED": 1, "RUNNING": 1}, counts)
}

func TestBenchQueries_RecoveryIsTheReplacementsClaimMinusTheKill(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}

	// One logical worker, killed and restarted under the same name: two sessions.
	worker := seed.worker("worker-3")
	killedAt := at(10 * time.Second)
	killed := seed.session(worker, at(0), "UNHEALTHY", ptr(at(41*time.Second)))
	restarted := seed.session(worker, at(10*time.Second+500*time.Millisecond), "HEALTHY", nil)

	// Affected, and replaced: abandoned on the killed session, and attempt 2 was
	// claimed 31s after the kill. Its start report is 2s after that claim, so the
	// answer is 31s only if the replacement's created_at is what is read.
	replaced := uuid.New()
	seed.job(replaced, "SUCCEEDED", at(9*time.Second), nil)
	seed.attempt(replaced, 1, worker, killed, "ABANDONED", at(9*time.Second+900*time.Millisecond), nil, ptr(at(40*time.Second)))
	seed.attempt(replaced, 2, worker, restarted, "SUCCEEDED", at(41*time.Second),
		ptr(at(43*time.Second)), ptr(at(43*time.Second+50*time.Millisecond)))

	// Affected, and never replaced: its budget ran out, so nothing was claimed.
	exhausted := uuid.New()
	seed.job(exhausted, "DEAD_LETTERED", at(9*time.Second), nil)
	seed.attempt(exhausted, 1, worker, killed, "ABANDONED", at(9*time.Second+950*time.Millisecond), nil, ptr(at(40*time.Second)))

	// Not affected: finished before the kill, bound to the killed session.
	finished := uuid.New()
	seed.job(finished, "SUCCEEDED", at(8*time.Second), nil)
	seed.attempt(finished, 1, worker, killed, "SUCCEEDED", at(8*time.Second+time.Millisecond),
		ptr(at(8*time.Second+2*time.Millisecond)), ptr(at(8*time.Second+60*time.Millisecond)))

	// Not affected: abandoned, but under a different session.
	elsewhere := seed.worker("worker-4")
	elsewhereSession := seed.session(elsewhere, at(0), "HEALTHY", nil)
	unrelated := uuid.New()
	seed.job(unrelated, "QUEUED", at(9*time.Second), nil)
	seed.attempt(unrelated, 1, elsewhere, elsewhereSession, "ABANDONED", at(9*time.Second+time.Millisecond), nil, ptr(at(40*time.Second)))

	got, err := readdb.Recoveries(ctx, testPool, benchScope, killed)
	require.NoError(t, err)
	require.Len(t, got, 2, "the replaced and the exhausted attempt; not the finished or the other session's")

	byJob := map[uuid.UUID]readdb.Recovery{}
	for _, r := range got {
		byJob[r.JobID] = r
	}
	recovery, ok := byJob[replaced].After(killedAt)
	require.True(t, ok)
	require.Equal(t, 31*time.Second, recovery, "claim of attempt 2 (41s) minus the kill (10s), not its start (43s)")
	require.Equal(t, 1, byJob[replaced].AbandonedAttempt)

	_, ok = byJob[exhausted].After(killedAt)
	require.False(t, ok, "no replacement was ever claimed, and that is reported rather than dropped")
	require.Nil(t, byJob[exhausted].ReplacementClaimedAt)

	none, err := readdb.Recoveries(ctx, testPool, benchScope, restarted)
	require.NoError(t, err)
	require.Empty(t, none, "nothing was abandoned on the restarted session")
}

func TestBenchQueries_CurrentSessionAndOccupancy(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}

	worker := seed.worker("worker-1")
	old := seed.session(worker, at(0), "OFFLINE", ptr(at(5*time.Second)))
	current := seed.session(worker, at(6*time.Second), "HEALTHY", nil)
	require.NotEqual(t, old, current)

	got, err := readdb.CurrentSession(ctx, testPool, benchScope, "worker-1")
	require.NoError(t, err)
	require.Equal(t, current, got, "the newest boot of the logical worker is the process that is running")

	_, err = readdb.CurrentSession(ctx, testPool, benchScope, "no-such-worker")
	require.Error(t, err)

	// Occupancy counts attempts that hold a slot: LEASED and RUNNING, by worker.
	idle := seed.worker("worker-2")
	idleSession := seed.session(idle, at(0), "HEALTHY", nil)
	for i, status := range []string{"LEASED", "RUNNING", "RUNNING", "SUCCEEDED"} {
		job := uuid.New()
		seed.job(job, "RUNNING", at(time.Duration(i)*time.Second), nil)
		var started, finished *time.Time
		switch status {
		case "RUNNING":
			started = ptr(at(time.Duration(i)*time.Second + time.Millisecond))
		case "SUCCEEDED":
			started = ptr(at(time.Duration(i)*time.Second + time.Millisecond))
			finished = ptr(at(time.Duration(i)*time.Second + 2*time.Millisecond))
		}
		seed.attempt(job, 1, worker, current, status, at(time.Duration(i)*time.Second), started, finished)
	}
	_ = idleSession

	occupied, err := readdb.Occupancy(ctx, testPool, benchScope)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"worker-1": 3}, occupied, "worker-2 holds nothing and is absent")

	_, err = readdb.ClockNow(ctx, testPool)
	require.NoError(t, err)
}

// TestBenchQueries_TargetableAttempts proves the query the smoke aims its kill with,
// against real PostgreSQL and rows whose timestamps the test writes.
//
// An attempt is targetable if it is LEASED (its sleep has not begun), or RUNNING and
// started no longer ago than duration-margin, so at least margin of its duration is
// left. The cases sit on and either side of that boundary, judged at an explicit
// instant so they are exact, and one case reads PostgreSQL's own clock.
//
// The boundary is pinned: an attempt that started EXACTLY duration-margin ago is
// targetable (the comparison is >=). It has exactly margin left, which is what
// "at least margin" means.
func TestBenchQueries_TargetableAttempts(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}

	const duration, margin = 3 * time.Second, 1500 * time.Millisecond
	asOf := at(10 * time.Second)
	boundary := asOf.Add(-(duration - margin)) // started exactly this long ago: margin left

	busy := seed.worker("worker-busy")
	busySession := seed.session(busy, at(0), "HEALTHY", nil)
	idle := seed.worker("worker-idle")
	idleSession := seed.session(idle, at(0), "HEALTHY", nil)

	attempt := func(worker, session uuid.UUID, status string, started, finished *time.Time) {
		job := uuid.New()
		seed.job(job, "RUNNING", at(time.Second), nil)
		seed.attempt(job, 1, worker, session, status, at(time.Second), started, finished)
	}
	// Targetable: its sleep has not begun.
	attempt(busy, busySession, "LEASED", nil, nil)
	// Targetable: it has just started, with nearly all of its duration left.
	attempt(busy, busySession, "RUNNING", ptr(asOf.Add(-100*time.Millisecond)), nil)
	// Targetable, and pinned: it started exactly duration-margin ago, so exactly
	// margin is left.
	attempt(busy, busySession, "RUNNING", ptr(boundary), nil)
	// Not targetable: one millisecond past the boundary, so just under margin left.
	attempt(busy, busySession, "RUNNING", ptr(boundary.Add(-time.Millisecond)), nil)
	// Not targetable: nearly finished.
	attempt(busy, busySession, "RUNNING", ptr(asOf.Add(-(duration - 10*time.Millisecond))), nil)
	// Not targetable: no longer holds a slot at all.
	attempt(busy, busySession, "SUCCEEDED", ptr(asOf.Add(-2*duration)), ptr(asOf.Add(-duration)))
	// A worker whose only attempt is too old holds nothing a kill could catch.
	attempt(idle, idleSession, "RUNNING", ptr(asOf.Add(-(duration - time.Millisecond))), nil)

	// Another scope's attempts are not this scope's, however targetable.
	other := benchSeed{t: t, scope: "bench-queries-other"}
	otherWorker := other.worker("worker-busy")
	otherSession := other.session(otherWorker, at(0), "HEALTHY", nil)
	otherJob := uuid.New()
	other.job(otherJob, "RUNNING", at(time.Second), nil)
	other.attempt(otherJob, 1, otherWorker, otherSession, "LEASED", at(time.Second), nil, nil)

	got, err := readdb.TargetableAttemptsAsOf(ctx, testPool, benchScope, duration, margin, asOf)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"worker-busy": 3}, got,
		"LEASED, just started and exactly on the boundary count; one millisecond past it, a nearly finished attempt and a SUCCEEDED one do not; worker-idle holds nothing a kill could catch; another scope's attempt is not counted")

	t.Run("margin decides where the boundary is", func(t *testing.T) {
		// Demanding 2 s left moves the boundary to 1 s ago: the attempt started exactly
		// 1.5 s ago is no longer a target, and only the LEASED and the just-started
		// ones are.
		got, err := readdb.TargetableAttemptsAsOf(ctx, testPool, benchScope, duration, 2*time.Second, asOf)
		require.NoError(t, err)
		require.Equal(t, map[string]int{"worker-busy": 2}, got)

		// A margin of 0 asks for any time left at all: every RUNNING attempt that
		// started within one duration, the nearly finished one included, and the
		// LEASED one. worker-idle's attempt, 1 ms inside a full duration, counts.
		got, err = readdb.TargetableAttemptsAsOf(ctx, testPool, benchScope, duration, 0, asOf)
		require.NoError(t, err)
		require.Equal(t, map[string]int{"worker-busy": 5, "worker-idle": 1}, got)
	})

	t.Run("the parameters are checked", func(t *testing.T) {
		_, err := readdb.TargetableAttemptsAsOf(ctx, testPool, benchScope, duration, duration+time.Millisecond, asOf)
		require.Error(t, err, "a margin longer than the job can never be left")
		_, err = readdb.TargetableAttemptsAsOf(ctx, testPool, benchScope, 0, 0, asOf)
		require.Error(t, err)
	})

	t.Run("only the worker's current session counts", func(t *testing.T) {
		// A worker the harness killed and restarted keeps its name, so it has two
		// sessions: the dead boot and the running one. The dead boot's attempts stay
		// LEASED or RUNNING until their leases expire, but a kill now ends the running
		// boot, which cannot catch them. Only the current session's attempt counts.
		reset(t)
		seed := benchSeed{t: t, scope: benchScope}
		worker := seed.worker("worker-restarted")
		dead := seed.session(worker, at(0), "OFFLINE", ptr(at(5*time.Second)))
		current := seed.session(worker, at(6*time.Second), "HEALTHY", nil)

		heldByDead := uuid.New()
		seed.job(heldByDead, "RUNNING", at(time.Second), nil)
		seed.attempt(heldByDead, 1, worker, dead, "RUNNING", at(time.Second), ptr(asOf.Add(-100*time.Millisecond)), nil)
		heldByCurrent := uuid.New()
		seed.job(heldByCurrent, "LEASED", at(7*time.Second), nil)
		seed.attempt(heldByCurrent, 1, worker, current, "LEASED", at(7*time.Second), nil, nil)

		got, err := readdb.TargetableAttemptsAsOf(ctx, testPool, benchScope, duration, margin, asOf)
		require.NoError(t, err)
		require.Equal(t, map[string]int{"worker-restarted": 1}, got,
			"the dead session's RUNNING attempt has time left but is not the running process's; only the current session's LEASED attempt counts")

		// A worker whose only targetable attempt belongs to a dead session is absent.
		reset(t)
		seed = benchSeed{t: t, scope: benchScope}
		worker = seed.worker("worker-restarted")
		dead = seed.session(worker, at(0), "OFFLINE", ptr(at(5*time.Second)))
		seed.session(worker, at(6*time.Second), "HEALTHY", nil)
		orphan := uuid.New()
		seed.job(orphan, "RUNNING", at(time.Second), nil)
		seed.attempt(orphan, 1, worker, dead, "RUNNING", at(time.Second), ptr(asOf.Add(-100*time.Millisecond)), nil)

		got, err = readdb.TargetableAttemptsAsOf(ctx, testPool, benchScope, duration, margin, asOf)
		require.NoError(t, err)
		require.Empty(t, got, "nothing the running process holds; the restarted worker is not a target")
	})

	t.Run("two sessions registered at the same instant: the id decides, as CurrentSession reads it", func(t *testing.T) {
		reset(t)
		seed := benchSeed{t: t, scope: benchScope}
		worker := seed.worker("worker-tie")
		first := seed.session(worker, at(0), "OFFLINE", ptr(at(5*time.Second)))
		second := seed.session(worker, at(0), "HEALTHY", nil)

		// Two attempts on one session and one on the other, so the count says which
		// session was taken as current.
		for i, session := range []uuid.UUID{first, first, second} {
			job := uuid.New()
			seed.job(job, "LEASED", at(time.Duration(i+1)*time.Second), nil)
			seed.attempt(job, 1, worker, session, "LEASED", at(time.Duration(i+1)*time.Second), nil, nil)
		}
		currentID, err := readdb.CurrentSession(ctx, testPool, benchScope, "worker-tie")
		require.NoError(t, err)
		want := 1
		if currentID == first {
			want = 2
		}

		got, err := readdb.TargetableAttemptsAsOf(ctx, testPool, benchScope, duration, margin, asOf)
		require.NoError(t, err)
		require.Equal(t, map[string]int{"worker-tie": want}, got,
			"the session counted is the one CurrentSession names (%s)", currentID)
	})

	t.Run("without an instant it reads PostgreSQL's own clock", func(t *testing.T) {
		reset(t)
		seed := benchSeed{t: t, scope: benchScope}
		worker := seed.worker("worker-live")
		session := seed.session(worker, at(0), "HEALTHY", nil)
		now, err := readdb.ClockNow(ctx, testPool)
		require.NoError(t, err)

		young, old := uuid.New(), uuid.New()
		seed.job(young, "RUNNING", at(time.Second), nil)
		seed.job(old, "RUNNING", at(time.Second), nil)
		seed.attempt(young, 1, worker, session, "RUNNING", at(time.Second), ptr(now.Add(-100*time.Millisecond)), nil)
		seed.attempt(old, 1, worker, session, "RUNNING", at(time.Second), ptr(now.Add(-10*time.Second)), nil)

		got, err := readdb.TargetableAttempts(ctx, testPool, benchScope, duration, margin)
		require.NoError(t, err)
		require.Equal(t, map[string]int{"worker-live": 1}, got,
			"the attempt that started 100 ms before PostgreSQL's now is a target; the one 10 s before it is not")
	})
}

// releasedLease writes a lease with every instant chosen by the test and the claim
// request id given, so a case can say which notification produced which claim.
// released nil writes an ACTIVE lease.
func (s benchSeed) releasedLease(attemptID, jobID, workerID, sessionID, claimRequestID uuid.UUID,
	acquired, expires time.Time, released *time.Time) {
	s.t.Helper()
	status := "ACTIVE"
	if released != nil {
		status = "EXPIRED"
	}
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO leases (
			id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
			claim_request_id, status, acquired_at, renewed_at, expires_at, released_at
		) VALUES (gen_random_uuid(), $1, $2, $3, 'default', $4, $5, $6, $7, $8, $8, $9, $10)`,
		jobID, attemptID, s.scope, workerID, sessionID, claimRequestID, status, acquired, expires, released)
	require.NoError(s.t, err)
}

// event writes one work.available outbox event for a job, published or not.
func (s benchSeed) event(jobID uuid.UUID, generation int, created time.Time, published *time.Time,
	attempts int, lastError *string) uuid.UUID {
	s.t.Helper()
	id := uuid.New()
	status := "PENDING"
	if published != nil {
		status = "PUBLISHED"
	}
	_, err := testPool.Exec(context.Background(), `
		INSERT INTO outbox_events (
			id, event_type, schema_version, payload, job_id, notification_generation,
			status, attempts, available_at, created_at, published_at, last_error
		) VALUES ($1, 'work.available', 1, $9::jsonb, $2, $3, $4, $5, $6, $6, $7, $8)`,
		id, jobID, generation, status, attempts, created, published, lastError,
		`{"queue":"default","job_id":"`+jobID.String()+`"}`)
	require.NoError(s.t, err)
	return id
}

// TestBenchQueries_RecoveryTimeline proves the query the recovery probe splits a
// recovery with (M8D3), on rows whose every instant the test writes.
//
// The story it seeds is the one the probe has to be able to tell: a job (X) whose
// abandoned attempt was recovered, but whose recovery event's notification claimed
// a different job (Y), so that X was claimed later by Y's notification. The recovery
// event must be found exactly, between the job's earlier submission event and a
// later re-notification, neither of which may be picked.
func TestBenchQueries_RecoveryTimeline(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}

	worker := seed.worker("worker-killed")
	killedAt := at(10 * time.Second)
	killed := seed.session(worker, at(0), "OFFLINE", ptr(at(10*time.Second+200*time.Millisecond)))
	seed.session(worker, at(10*time.Second+200*time.Millisecond), "HEALTHY", nil)
	other := seed.worker("worker-other")
	otherSession := seed.session(other, at(0), "HEALTHY", nil)

	// Job X. Its submission event (E0) produced attempt 1, on the killed session.
	jobX := uuid.New()
	seed.job(jobX, "RUNNING", at(0), nil)
	e0 := seed.event(jobX, 1, at(0), ptr(at(100*time.Millisecond)), 1, nil)
	x1 := seed.attempt(jobX, 1, worker, killed, "ABANDONED", at(time.Second), nil, ptr(at(33*time.Second+200*time.Millisecond)))
	seed.releasedLease(x1, jobX, worker, killed, e0, at(time.Second), at(31*time.Second), ptr(at(33*time.Second+200*time.Millisecond)))
	// The recovery event (E1): created at the reconciler transaction's start, 50 ms
	// BEFORE the lease's released_at, which that transaction sampled after its locks.
	e1 := seed.event(jobX, 2, at(33*time.Second+150*time.Millisecond), ptr(at(34*time.Second)), 1, nil)
	// A later re-notification of X (E3), after the lease was released: not it either.
	seed.event(jobX, 3, at(95*time.Second), nil, 0, nil)

	// Job Y, submitted before the kill. Its own notification (E2) was received and
	// not claimed; E1's notification claimed Y, the queue's oldest eligible job.
	jobY := uuid.New()
	seed.job(jobY, "RUNNING", at(20*time.Second), nil)
	e2 := seed.event(jobY, 1, at(20*time.Second), ptr(at(20*time.Second+500*time.Millisecond)), 1, nil)
	y1 := seed.attempt(jobY, 1, other, otherSession, "RUNNING", at(34*time.Second+100*time.Millisecond),
		ptr(at(34*time.Second+150*time.Millisecond)), nil)
	seed.releasedLease(y1, jobY, other, otherSession, e1, at(34*time.Second+100*time.Millisecond), at(64*time.Second+100*time.Millisecond), nil)
	// ...and E2's notification, back from the broker, claimed X: attempt 2, at 52 s.
	x2 := seed.attempt(jobX, 2, other, otherSession, "RUNNING", at(52*time.Second),
		ptr(at(52*time.Second+50*time.Millisecond)), nil)
	seed.releasedLease(x2, jobX, other, otherSession, e2, at(52*time.Second), at(82*time.Second), nil)

	// Job Z: abandoned on the killed session, and its recovery event has failed to
	// publish twice. No replacement.
	jobZ := uuid.New()
	seed.job(jobZ, "QUEUED", at(0), nil)
	z0 := seed.event(jobZ, 1, at(0), ptr(at(100*time.Millisecond)), 1, nil)
	z1 := seed.attempt(jobZ, 1, worker, killed, "ABANDONED", at(2*time.Second), nil, ptr(at(33*time.Second+300*time.Millisecond)))
	seed.releasedLease(z1, jobZ, worker, killed, z0, at(2*time.Second), at(32*time.Second), ptr(at(33*time.Second+300*time.Millisecond)))
	boom := "send message: connection refused"
	e4 := seed.event(jobZ, 2, at(33*time.Second+250*time.Millisecond), nil, 2, &boom)

	// Not returned: an attempt of the killed session that finished before the kill.
	done := uuid.New()
	seed.job(done, "SUCCEEDED", at(0), nil)
	seed.attempt(done, 1, worker, killed, "SUCCEEDED", at(3*time.Second), ptr(at(3*time.Second+time.Millisecond)), ptr(at(3*time.Second+60*time.Millisecond)))

	got, err := readdb.RecoveryTimeline(ctx, testPool, benchScope, killed)
	require.NoError(t, err)
	require.Len(t, got, 2, "X and Z, the two abandoned attempts; not the finished one")
	byJob := map[uuid.UUID]readdb.RecoveryHop{}
	for _, h := range got {
		byJob[h.JobID] = h
	}

	x := byJob[jobX]
	require.Equal(t, 1, x.AbandonedAttempt)
	require.True(t, x.LeaseAcquiredAt.Equal(at(time.Second)))
	require.True(t, x.LeaseRenewedAt.Equal(at(time.Second)))
	require.True(t, x.LeaseExpiresAt.Equal(at(31*time.Second)))
	require.NotNil(t, x.LeaseReleasedAt)
	require.True(t, x.LeaseReleasedAt.Equal(at(33*time.Second+200*time.Millisecond)))
	require.NotNil(t, x.EventID)
	require.Equal(t, e1, *x.EventID, "the recovery event, not the submission event (%s) and not the later re-notification", e0)
	require.True(t, x.EventCreatedAt.Equal(at(33*time.Second+150*time.Millisecond)))
	require.True(t, x.EventPublishedAt.Equal(at(34*time.Second)))
	require.Equal(t, 1, *x.EventAttempts)
	require.Nil(t, x.EventLastError)
	require.NotNil(t, x.EventClaimedJobID)
	require.Equal(t, jobY, *x.EventClaimedJobID, "the recovery event's notification claimed the oldest eligible job, Y")
	require.True(t, x.ReplacementCreatedAt.Equal(at(52*time.Second)))
	require.Equal(t, otherSession, *x.ReplacementSessionID)
	require.Equal(t, e2, *x.ReplacementEventID, "X's replacement was claimed by Y's notification")
	require.Equal(t, jobY, *x.ReplacementEventJobID)
	require.True(t, x.ReplacementEventCreatedAt.Equal(at(20*time.Second)))
	require.True(t, x.ReplacementEventPublishedAt.Equal(at(20*time.Second+500*time.Millisecond)))

	segments, ok := x.Segments(killedAt)
	require.True(t, ok)
	require.Equal(t, readdb.Segments{
		S1: 21 * time.Second,                     // expires 31 s - kill 10 s
		S2: 2*time.Second + 200*time.Millisecond, // released 33.2 s - expires 31 s
		S3: -50 * time.Millisecond,               // created 33.15 s - released 33.2 s
		S4: 850 * time.Millisecond,               // published 34 s - created 33.15 s
		S5: 18 * time.Second,                     // replacement 52 s - published 34 s
	}, segments)

	// The identity: the five segments sum to the recovery Recoveries measures.
	recoveries, err := readdb.Recoveries(ctx, testPool, benchScope, killed)
	require.NoError(t, err)
	var recovery time.Duration
	for _, r := range recoveries {
		if r.JobID == jobX {
			recovery, ok = r.After(killedAt)
			require.True(t, ok)
		}
	}
	require.Equal(t, 42*time.Second, recovery)
	require.Equal(t, recovery, segments.Sum())

	z := byJob[jobZ]
	require.NotNil(t, z.EventID)
	require.Equal(t, e4, *z.EventID)
	require.Nil(t, z.EventPublishedAt, "not published yet")
	require.Equal(t, 2, *z.EventAttempts)
	require.Equal(t, boom, *z.EventLastError)
	require.Nil(t, z.EventClaimedJobID)
	require.Nil(t, z.ReplacementCreatedAt)
	require.Nil(t, z.ReplacementEventID)
	_, ok = z.Segments(killedAt)
	require.False(t, ok, "no publish and no replacement, so no segments")

	t.Run("two events inside one lease's window are refused, not chosen between", func(t *testing.T) {
		seed.event(jobX, 4, at(33*time.Second+180*time.Millisecond), nil, 0, nil)
		_, err := readdb.RecoveryTimeline(ctx, testPool, benchScope, killed)
		require.ErrorContains(t, err, "2 work.available events")
	})
}

func TestBenchQueries_HeldBySessionAndForeignActivity(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}
	worker := seed.worker("worker-1")
	dead := seed.session(worker, at(0), "OFFLINE", ptr(at(5*time.Second)))
	current := seed.session(worker, at(6*time.Second), "HEALTHY", nil)
	for i, status := range []string{"LEASED", "RUNNING", "SUCCEEDED", "ABANDONED"} {
		job := uuid.New()
		seed.job(job, "RUNNING", at(time.Duration(i)*time.Second), nil)
		var started, finished *time.Time
		if status != "LEASED" {
			started = ptr(at(time.Duration(i)*time.Second + time.Millisecond))
		}
		if status == "SUCCEEDED" || status == "ABANDONED" {
			finished = ptr(at(time.Duration(i)*time.Second + 2*time.Millisecond))
		}
		seed.attempt(job, 1, worker, dead, status, at(time.Duration(i)*time.Second), started, finished)
	}
	held, err := readdb.HeldBySession(ctx, testPool, benchScope, dead)
	require.NoError(t, err)
	require.Equal(t, 2, held, "LEASED and RUNNING; not SUCCEEDED or ABANDONED")
	held, err = readdb.HeldBySession(ctx, testPool, benchScope, current)
	require.NoError(t, err)
	require.Zero(t, held)

	// Active leases of the dead session, soonest first; a released one is not listed.
	soon, later, released := uuid.New(), uuid.New(), uuid.New()
	for i, job := range []uuid.UUID{soon, later, released} {
		seed.job(job, "RUNNING", at(time.Duration(10+i)*time.Second), nil)
	}
	a := seed.attempt(later, 1, worker, dead, "RUNNING", at(10*time.Second), ptr(at(10*time.Second)), nil)
	seed.releasedLease(a, later, worker, dead, uuid.New(), at(10*time.Second), at(40*time.Second), nil)
	a = seed.attempt(soon, 1, worker, dead, "RUNNING", at(11*time.Second), ptr(at(11*time.Second)), nil)
	seed.releasedLease(a, soon, worker, dead, uuid.New(), at(11*time.Second), at(27*time.Second), nil)
	a = seed.attempt(released, 1, worker, dead, "SUCCEEDED", at(12*time.Second), ptr(at(12*time.Second)), ptr(at(13*time.Second)))
	seed.releasedLease(a, released, worker, dead, uuid.New(), at(12*time.Second), at(42*time.Second), ptr(at(13*time.Second)))
	expiries, err := readdb.ActiveLeaseExpiries(ctx, testPool, benchScope, dead)
	require.NoError(t, err)
	require.Len(t, expiries, 2)
	require.True(t, expiries[0].Equal(at(27*time.Second)), "a renewed lease's earlier expiry comes first: %v", expiries)
	require.True(t, expiries[1].Equal(at(40*time.Second)))

	// This scope's own work is never foreign. reset leaves no job and no event.
	own := uuid.New()
	seed.job(own, "QUEUED", at(time.Second), nil)
	seed.event(own, 1, at(time.Second), nil, 0, nil)
	f, err := readdb.ForeignActivity(ctx, testPool, benchScope)
	require.NoError(t, err)
	require.True(t, f.None(), "%+v", f)

	// Another scope's terminal job is not; its QUEUED job and its pending event are.
	other := benchSeed{t: t, scope: "bench-queries-other"}
	other.job(uuid.New(), "SUCCEEDED", at(0), nil)
	f, err = readdb.ForeignActivity(ctx, testPool, benchScope)
	require.NoError(t, err)
	require.True(t, f.None(), "a terminal job of another scope gives the services nothing to do: %+v", f)

	queued := uuid.New()
	other.job(queued, "QUEUED", at(0), nil)
	other.event(queued, 1, at(0), nil, 0, nil)
	f, err = readdb.ForeignActivity(ctx, testPool, benchScope)
	require.NoError(t, err)
	require.Equal(t, readdb.Foreign{NonTerminalJobs: 1, PendingEvents: 1}, f)
	require.False(t, f.None())
}

// TestBenchQueries_WorkersAtCapacityDuringCountsADeadBootsLeases proves the
// reconstruction the probe uses to say which logical worker was at its limit while
// a claim that took nothing was in progress: leases are counted by logical worker,
// a dead boot's included, against the limit of the newest session registered by
// then, at any instant of the interval and not only at one sample of it.
func TestBenchQueries_WorkersAtCapacityDuringCountsADeadBootsLeases(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}
	restarted := seed.worker("w01")
	dead := seed.session(restarted, at(0), "OFFLINE", ptr(at(10*time.Second)))
	current := seed.session(restarted, at(10*time.Second), "HEALTHY", nil)
	other := seed.worker("w02")
	otherSession := seed.session(other, at(0), "HEALTHY", nil)

	lease := func(worker, session uuid.UUID, acquired time.Time, released *time.Time) {
		job := uuid.New()
		seed.job(job, "RUNNING", acquired, nil)
		a := seed.attempt(job, 1, worker, session, "RUNNING", acquired, ptr(acquired), nil)
		seed.releasedLease(a, job, worker, session, uuid.New(), acquired, acquired.Add(30*time.Second), released)
	}
	// The dead boot's lease, active until it is released at 40 s. The new boot takes
	// three more at 20.030, 20.036 and 20.039 s, each held for 150 ms, which is the
	// shape a 50 ms workload leaves: the logical worker is at its limit of 4 only
	// from 20.039 s to 20.180 s.
	lease(restarted, dead, at(9*time.Second), ptr(at(40*time.Second)))
	for _, ms := range []int{30, 36, 39} {
		acquired := at(20*time.Second + time.Duration(ms)*time.Millisecond)
		lease(restarted, current, acquired, ptr(acquired.Add(150*time.Millisecond)))
	}
	// Another worker with three: under its limit throughout.
	for range 3 {
		lease(other, otherSession, at(20*time.Second), nil)
	}

	// A claim from 19.976 s to 20.045 s: its midpoint (20.010 s) sees one lease, but
	// the worker reaches its limit at 20.039 s, inside the claim.
	got, err := readdb.WorkersAtCapacityDuring(ctx, testPool, benchScope,
		at(19*time.Second+976*time.Millisecond), at(20*time.Second+45*time.Millisecond))
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "w01", got[0].Worker)
	require.Equal(t, 4, got[0].Active)
	require.Equal(t, 1, got[0].DeadBoots, "one of the four is the dead boot's")
	require.Equal(t, 4, got[0].Limit)
	require.True(t, got[0].At.Equal(at(20*time.Second+39*time.Millisecond)), "the first instant at the limit: %v", got[0].At)

	// The same claim ending one millisecond before the third acquisition: never at the limit.
	got, err = readdb.WorkersAtCapacityDuring(ctx, testPool, benchScope,
		at(19*time.Second+976*time.Millisecond), at(20*time.Second+38*time.Millisecond))
	require.NoError(t, err)
	require.Empty(t, got)

	// An interval that starts while it is at the limit counts from its start.
	got, err = readdb.WorkersAtCapacityDuring(ctx, testPool, benchScope,
		at(20*time.Second+100*time.Millisecond), at(20*time.Second+110*time.Millisecond))
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.True(t, got[0].At.Equal(at(20*time.Second+100*time.Millisecond)))

	// After the dead boot's lease is released, three of four.
	got, err = readdb.WorkersAtCapacityDuring(ctx, testPool, benchScope, at(41*time.Second), at(42*time.Second))
	require.NoError(t, err)
	require.Empty(t, got)
}
