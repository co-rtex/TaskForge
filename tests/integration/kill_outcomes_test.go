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

// The queries the recovery probe reads a kill's outcomes with (M8D4), against rows
// whose every instant the test writes, as bench_queries_test.go does for the
// measurement queries.

// seededAttempt is one attempt and its lease, every instant chosen by the test.
type seededAttempt struct {
	job      uuid.UUID
	number   int
	worker   uuid.UUID
	session  uuid.UUID
	status   string
	created  time.Time
	started  *time.Time
	timeout  *time.Time
	finished *time.Time
	class    *string
	code     *string
	// retryDelay is set for an attempt whose outcome decided a retry.
	retryDelay *time.Duration

	leaseStatus string // "" is ACTIVE
	acquired    time.Time
	renewed     *time.Time // nil: never renewed
	expires     time.Time
	released    *time.Time
}

func (s benchSeed) seededAttempt(a seededAttempt) uuid.UUID {
	s.t.Helper()
	ctx := context.Background()
	id := uuid.New()
	var retryMS *int64
	var retryAt *time.Time
	if a.retryDelay != nil {
		ms := a.retryDelay.Milliseconds()
		at := a.finished.Add(*a.retryDelay)
		retryMS, retryAt = &ms, &at
	}
	_, err := testPool.Exec(ctx, `
		INSERT INTO job_attempts (
			id, job_id, scope, queue, attempt_number, worker_id, worker_session_id,
			status, created_at, started_at, timeout_at, finished_at,
			failure_class, error_code, retry_delay_ms, retry_at
		) VALUES ($1, $2, $3, 'default', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		id, a.job, s.scope, a.number, a.worker, a.session, a.status, a.created, a.started, a.timeout, a.finished,
		a.class, a.code, retryMS, retryAt)
	require.NoError(s.t, err)

	status, renewed, version := a.leaseStatus, a.acquired, 0
	if status == "" {
		status = "ACTIVE"
	}
	var renewalID *uuid.UUID
	if a.renewed != nil {
		renewed, version = *a.renewed, 1
		v := uuid.New()
		renewalID = &v
	}
	_, err = testPool.Exec(ctx, `
		INSERT INTO leases (
			id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
			claim_request_id, status, acquired_at, renewed_at, expires_at, released_at,
			renewal_version, last_renewal_request_id
		) VALUES (gen_random_uuid(), $1, $2, $3, 'default', $4, $5, gen_random_uuid(),
		          $6, $7, $8, $9, $10, $11, $12)`,
		a.job, id, s.scope, a.worker, a.session, status, a.acquired, renewed, a.expires, a.released, version, renewalID)
	require.NoError(s.t, err)
	return id
}

func strp(s string) *string                            { return &s }
func durp(d time.Duration) *time.Duration              { return &d }
func timep(base time.Time, d time.Duration) *time.Time { v := base.Add(d); return &v }

// TestKillOutcomes_EveryAttemptUnfinishedAtTheKillAndWhatBecameOfIt seeds a killed
// session with one attempt of each kind a kill can leave, and the boundaries of
// the selection on both sides.
func TestKillOutcomes_EveryAttemptUnfinishedAtTheKillAndWhatBecameOfIt(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}
	worker := seed.worker("worker-killed")
	other := seed.worker("worker-other")
	killed := seed.session(worker, at(0), "UNHEALTHY", ptr(at(50*time.Second)))
	otherSession := seed.session(other, at(0), "HEALTHY", nil)
	kill := at(10 * time.Second)
	chosen := kill.Add(-4 * time.Millisecond)
	ms := time.Millisecond

	newJob := func(status string) uuid.UUID {
		id := uuid.New()
		seed.job(id, status, at(time.Second), nil)
		return id
	}
	// replace writes the job's next attempt on the other worker: the replacement.
	replace := func(job uuid.UUID, number int, claimed time.Time) {
		seed.seededAttempt(seededAttempt{job: job, number: number, worker: other, session: otherSession,
			status: "LEASED", created: claimed, acquired: claimed, expires: claimed.Add(30 * time.Second)})
	}

	// 1. LEASED at the kill, never started: ABANDONED, requeued, replaced at kill + 33 s.
	leasedJob := newJob("RUNNING")
	seed.seededAttempt(seededAttempt{job: leasedJob, number: 1, worker: worker, session: killed, status: "ABANDONED",
		created: kill.Add(-20 * ms), finished: timep(kill, 32*time.Second), class: strp("ABANDONED"), code: strp("attempt_abandoned"),
		retryDelay: durp(0), leaseStatus: "EXPIRED", acquired: kill.Add(-20 * ms), expires: kill.Add(30*time.Second - 20*ms),
		released: timep(kill, 32*time.Second)})
	replace(leasedJob, 2, kill.Add(33*time.Second))

	// 2. RUNNING at the kill, never renewed, reached after its deadline: TIMED_OUT on
	//    the expired-lease path, RETRY_WAIT, replaced later.
	timedOutJob := newJob("RUNNING")
	seed.seededAttempt(seededAttempt{job: timedOutJob, number: 1, worker: worker, session: killed, status: "TIMED_OUT",
		created: kill.Add(-30 * ms), started: timep(kill, -18*ms), timeout: timep(kill, 30*time.Second-18*ms),
		finished: timep(kill, 31*time.Second), class: strp("TIMED_OUT"), code: strp("attempt_timeout"),
		retryDelay: durp(900 * ms), leaseStatus: "EXPIRED", acquired: kill.Add(-30 * ms),
		expires: kill.Add(30*time.Second - 30*ms), released: timep(kill, 31*time.Second)})
	replace(timedOutJob, 2, kill.Add(34*time.Second))

	// 3. RUNNING, the third and last attempt, TIMED_OUT and dead-lettered.
	dlqJob := newJob("DEAD_LETTERED")
	dlqAttempt := seed.seededAttempt(seededAttempt{job: dlqJob, number: 3, worker: worker, session: killed, status: "TIMED_OUT",
		created: kill.Add(-40 * ms), started: timep(kill, -35*ms), timeout: timep(kill, 30*time.Second-35*ms),
		finished: timep(kill, 31*time.Second), class: strp("TIMED_OUT"), code: strp("attempt_timeout"),
		leaseStatus: "EXPIRED", acquired: kill.Add(-40 * ms), expires: kill.Add(30*time.Second - 40*ms),
		released: timep(kill, 31*time.Second)})
	_, err := testPool.Exec(ctx, `
		INSERT INTO dlq_entries (id, scope, queue, job_id, terminal_attempt_id, reason, created_at)
		VALUES (gen_random_uuid(), $1, 'default', $2, $3, 'ATTEMPTS_EXHAUSTED', $4)`,
		benchScope, dlqJob, dlqAttempt, at(41*time.Second))
	require.NoError(t, err)

	// 4. RUNNING, its job cancel-requested: the lapse finalized the cancellation (g).
	canceledJob := newJob("RUNNING")
	_, err = testPool.Exec(ctx, `UPDATE jobs SET status = 'CANCELED', cancel_requested_at = $2 WHERE id = $1`,
		canceledJob, at(5*time.Second))
	require.NoError(t, err)
	seed.seededAttempt(seededAttempt{job: canceledJob, number: 1, worker: worker, session: killed, status: "CANCELED",
		created: kill.Add(-50 * ms), started: timep(kill, -45*ms), timeout: timep(kill, 30*time.Second-45*ms),
		finished: timep(kill, 31*time.Second), class: strp("CANCELED"), code: strp("canceled"),
		leaseStatus: "EXPIRED", acquired: kill.Add(-50 * ms), expires: kill.Add(30*time.Second - 50*ms),
		released: timep(kill, 31*time.Second)})

	// 5. Created exactly at the kill, its Start committed 2 ms after it (in flight
	//    when the worker died): LEASED at the kill.
	lateStartJob := newJob("QUEUED")
	seed.seededAttempt(seededAttempt{job: lateStartJob, number: 1, worker: worker, session: killed, status: "ABANDONED",
		created: kill, started: timep(kill, 2*ms), timeout: timep(kill, 30*time.Second+2*ms),
		finished: timep(kill, 31*time.Second), class: strp("ABANDONED"), code: strp("attempt_abandoned"),
		retryDelay: durp(0), leaseStatus: "EXPIRED", acquired: kill, expires: kill.Add(30 * time.Second),
		released: timep(kill, 31*time.Second)})

	// 6. RUNNING, its success committing as the kill was stamped: SUCCEEDED 1 ms after it.
	inFlightJob := newJob("SUCCEEDED")
	seed.seededAttempt(seededAttempt{job: inFlightJob, number: 1, worker: worker, session: killed, status: "SUCCEEDED",
		created: kill.Add(-60 * ms), started: timep(kill, -55*ms), timeout: timep(kill, 30*time.Second-55*ms),
		finished: timep(kill, ms), leaseStatus: "COMPLETED", acquired: kill.Add(-60 * ms),
		expires: kill.Add(30*time.Second - 60*ms), released: timep(kill, ms)})

	// Not selected: finished exactly at the kill (finished, not unfinished), finished
	// between the choice and the kill, finished just before the choice, created
	// after the kill, and the other session's attempt.
	finishedAtKill := newJob("SUCCEEDED")
	seed.seededAttempt(seededAttempt{job: finishedAtKill, number: 1, worker: worker, session: killed, status: "SUCCEEDED",
		created: kill.Add(-70 * ms), started: timep(kill, -65*ms), timeout: timep(kill, 30*time.Second),
		finished: timep(kill, 0), leaseStatus: "COMPLETED", acquired: kill.Add(-70 * ms),
		expires: kill.Add(30 * time.Second), released: timep(kill, 0)})
	finishedAfterChoice := newJob("SUCCEEDED")
	seed.seededAttempt(seededAttempt{job: finishedAfterChoice, number: 1, worker: worker, session: killed, status: "SUCCEEDED",
		created: kill.Add(-80 * ms), started: timep(kill, -75*ms), timeout: timep(kill, 30*time.Second),
		finished: &chosen, leaseStatus: "COMPLETED", acquired: kill.Add(-80 * ms),
		expires: kill.Add(30 * time.Second), released: &chosen})
	finishedBeforeChoice := newJob("SUCCEEDED")
	seed.seededAttempt(seededAttempt{job: finishedBeforeChoice, number: 1, worker: worker, session: killed, status: "SUCCEEDED",
		created: kill.Add(-90 * ms), started: timep(kill, -85*ms), timeout: timep(kill, 30*time.Second),
		finished: timep(chosen, -ms), leaseStatus: "COMPLETED", acquired: kill.Add(-90 * ms),
		expires: kill.Add(30 * time.Second), released: timep(chosen, -ms)})
	createdAfterKill := newJob("LEASED")
	seed.seededAttempt(seededAttempt{job: createdAfterKill, number: 1, worker: worker, session: killed, status: "LEASED",
		created: kill.Add(ms), acquired: kill.Add(ms), expires: kill.Add(30*time.Second + ms)})
	otherJob := newJob("RUNNING")
	seed.seededAttempt(seededAttempt{job: otherJob, number: 1, worker: other, session: otherSession, status: "LEASED",
		created: kill.Add(-ms), acquired: kill.Add(-ms), expires: kill.Add(30*time.Second - ms)})

	outcomes, err := readdb.KillOutcomes(ctx, testPool, benchScope, killed, kill)
	require.NoError(t, err)
	byJob := map[uuid.UUID]readdb.KillOutcome{}
	for _, o := range outcomes {
		byJob[o.JobID] = o
	}
	require.Len(t, outcomes, 6, "the six attempts unfinished at the kill, and nothing else")
	for _, excluded := range []uuid.UUID{finishedAtKill, finishedAfterChoice, finishedBeforeChoice, createdAfterKill, otherJob} {
		require.NotContains(t, byJob, excluded)
	}

	leased := byJob[leasedJob]
	require.Equal(t, readdb.HeldLeased, leased.StateAtKill)
	require.Equal(t, readdb.OutcomeAbandoned, leased.Outcome())
	require.Equal(t, "RUNNING", leased.JobStatus, "the job as it is now (the seed's)")
	_, started := leased.ClaimToStart()
	require.False(t, started)
	d, ok := leased.ToReplacement(kill)
	require.True(t, ok)
	require.Equal(t, 33*time.Second, d, "the recovery figure: replacement claim minus the kill")

	timedOut := byJob[timedOutJob]
	require.Equal(t, readdb.HeldRunning, timedOut.StateAtKill)
	require.Equal(t, readdb.OutcomeTimedOut, timedOut.Outcome())
	require.Equal(t, "EXPIRED", timedOut.LeaseStatus)
	require.Equal(t, "attempt_timeout", *timedOut.ErrorCode)
	require.Equal(t, "TIMED_OUT", *timedOut.FailureClass)
	require.True(t, timedOut.RetryAt.Equal(at(10*time.Second+31*time.Second+900*ms)))
	d, ok = timedOut.ClaimToStart()
	require.True(t, ok)
	require.Equal(t, 12*ms, d, "started_at minus the lease's acquired_at")
	require.Equal(t, 12*ms, timedOut.TimeoutAt.Sub(timedOut.LeaseExpiresAt),
		"with timeout_seconds equal to the lease, the window is the claim-to-start latency")
	d, ok = timedOut.ToReplacement(kill)
	require.True(t, ok)
	require.Equal(t, 34*time.Second, d)

	dlq := byJob[dlqJob]
	require.Equal(t, 3, dlq.AttemptNumber)
	require.Equal(t, readdb.OutcomeTimedOutDeadLetter, dlq.Outcome())
	require.Equal(t, "ATTEMPTS_EXHAUSTED", *dlq.DLQReason)
	require.Nil(t, dlq.ReplacementClaimedAt)

	canceled := byJob[canceledJob]
	require.Equal(t, readdb.OutcomeCanceled, canceled.Outcome(), "a cancellation is its own bucket")
	require.Equal(t, readdb.HeldRunning, canceled.StateAtKill)

	lateStart := byJob[lateStartJob]
	require.Equal(t, readdb.HeldLeased, lateStart.StateAtKill, "created at the kill is included; started after it is LEASED at it")

	inFlight := byJob[inFlightJob]
	require.Equal(t, readdb.OutcomeOther, inFlight.Outcome())
	require.Equal(t, "SUCCEEDED", inFlight.Status)

	// The same kill read for the other session catches only its own attempt.
	others, err := readdb.KillOutcomes(ctx, testPool, benchScope, otherSession, kill)
	require.NoError(t, err)
	require.Len(t, others, 1)
	require.Equal(t, otherJob, others[0].JobID)

	// Finished between the choice and the kill, both ends included: the one at the
	// choice and the one at the kill, not the one a millisecond before the choice.
	finished, err := readdb.FinishedBetween(ctx, testPool, benchScope, killed, chosen, kill)
	require.NoError(t, err)
	require.Len(t, finished, 2)
	require.Equal(t, finishedAfterChoice, finished[0].JobID)
	require.True(t, finished[0].FinishedAt.Equal(chosen))
	require.Equal(t, finishedAtKill, finished[1].JobID)
	require.Equal(t, "SUCCEEDED", finished[1].Status)
}

// TestKillOutcomes_RenewedAttempts proves condition E's victim selection: a worker
// whose current session holds attempts, every one RUNNING, renewed at least once,
// and with its deadline still margin away on PostgreSQL's clock.
func TestKillOutcomes_RenewedAttempts(t *testing.T) {
	reset(t)
	ctx := context.Background()
	seed := benchSeed{t: t, scope: benchScope}
	now := serverNow(t)
	base := now.Add(-time.Minute)
	margin := 5 * time.Second

	hold := func(worker, session uuid.UUID, renewed bool, timeoutIn time.Duration, status string) {
		job := uuid.New()
		seed.job(job, "RUNNING", base, nil)
		a := seededAttempt{job: job, number: 1, worker: worker, session: session, status: status,
			created: base, acquired: base, expires: now.Add(time.Hour)}
		if status == "RUNNING" {
			a.started, a.timeout = timep(base, time.Millisecond), timep(now, timeoutIn)
		}
		if renewed {
			a.renewed = timep(base, 10*time.Second)
		}
		seed.seededAttempt(a)
	}
	worker := func(name string) uuid.UUID {
		id := seed.worker(name)
		return seed.session(id, base, "HEALTHY", nil)
	}

	// Two renewed RUNNING attempts, and an ABANDONED one that no longer counts.
	allRenewed := seed.worker("all-renewed")
	allRenewedSession := seed.session(allRenewed, base, "HEALTHY", nil)
	hold(allRenewed, allRenewedSession, true, time.Hour, "RUNNING")
	hold(allRenewed, allRenewedSession, true, time.Hour, "RUNNING")

	partly := seed.worker("one-not-renewed")
	partlySession := seed.session(partly, base, "HEALTHY", nil)
	hold(partly, partlySession, true, time.Hour, "RUNNING")
	hold(partly, partlySession, false, time.Hour, "RUNNING") // a kill now would catch an unrenewed attempt

	leased := seed.worker("one-leased")
	leasedSession := seed.session(leased, base, "HEALTHY", nil)
	hold(leased, leasedSession, true, time.Hour, "RUNNING")
	hold(leased, leasedSession, true, 0, "LEASED") // renewed but never started

	soon := seed.worker("deadline-soon")
	soonSession := seed.session(soon, base, "HEALTHY", nil)
	hold(soon, soonSession, true, margin-time.Second, "RUNNING") // about to time out by itself

	restarted := seed.worker("restarted")
	deadBoot := seed.session(restarted, base, "UNHEALTHY", ptr(base.Add(time.Second)))
	seed.session(restarted, base.Add(2*time.Second), "HEALTHY", nil)
	hold(restarted, deadBoot, true, time.Hour, "RUNNING") // the dead boot's: a kill would end the new one

	worker("idle")

	got, err := readdb.RenewedAttempts(ctx, testPool, benchScope, margin)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"all-renewed": 2}, got)

	got, err = readdb.RenewedAttempts(ctx, testPool, benchScope, 0)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"all-renewed": 2, "deadline-soon": 1}, got, "with no margin the near deadline counts")

	_, err = readdb.RenewedAttempts(ctx, testPool, benchScope, -time.Second)
	require.Error(t, err)
}
