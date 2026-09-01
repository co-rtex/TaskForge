package workers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/outbox"
)

// Fail records one fenced terminal failure and decides what it does to the job.
//
// The operation is fenced by the complete five-part fence and additionally by a
// client-generated outcome request id, for the same reason renewal carries a
// renewal identity (ADR-0008): every network call has an ambiguous outcome, and
// a worker whose failure report commits but whose response is lost must retry.
// Without a retained identity that retry would consume a second place in the
// attempt budget, draw fresh jitter for a different retry instant, and — at the
// end of the budget — create a second dead-letter entry.
//
// So the decision is computed exactly once and persisted ON the attempt:
// classification, safe code and message, chosen delay, and retry_at. A replay
// returns those stored values unchanged. Reusing the identity for a different
// attempt, or replaying it with a different body, is a stable domain conflict.
//
// Locking is the established queue -> worker session -> job -> attempt -> lease
// order, extended by dlq_entries when the decision is terminal. PostgreSQL time
// is sampled after every lock, so a report that waited across the attempt's
// deadline or its lease expiry is rejected against the fresh sample rather than
// accepted on a stale one.
func (s *Store) Fail(ctx context.Context, scope string, report FailureReport) (_ OutcomeResult, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if err := ValidateFailureReport(report); err != nil {
		return OutcomeResult{}, err
	}

	// lockAuthorityRows, not lockFence: an exact replay is a read of immutable
	// terminal history and must survive everything that ends live authority —
	// the lease closing, its window lapsing, and the worker session being
	// replaced. lockFence would refuse a replaced session before the replay was
	// even considered, which is how a retrying worker ends up being told its
	// report never landed when it did.
	//
	// The complete fence is still verified: lockAuthorityRows matched lease,
	// job, attempt, worker, session, and scope together, so a replay is only
	// recognized for the exact binding that committed it.
	tx, state, err := s.lockAuthorityRows(ctx, scope, report.Fence)
	if err != nil {
		return OutcomeResult{}, err
	}
	defer rollback(ctx, tx)

	// An exact replay is recognized FIRST, before any liveness check, and that
	// ordering is the opposite of renewal's on purpose. Renewal replays return
	// live authority, so they must be refused once the lease is gone. A failure
	// replay returns a decision that is already durable history; refusing it
	// because the lease has since lapsed, or because this process boot has been
	// replaced, would leave the worker no recourse but to send the same identity
	// forever.
	if state.outcomeRequestID != nil && *state.outcomeRequestID == report.OutcomeRequestID {
		result, err := replayedOutcome(state, report)
		if err != nil {
			return OutcomeResult{}, err
		}
		result.JobID = report.Fence.JobID
		if err := tx.Commit(ctx); err != nil {
			return OutcomeResult{}, fmt.Errorf("commit replayed failure outcome: %w", err)
		}
		return result, nil
	}
	// The identity is unique for the lifetime of attempt history, so presenting
	// one that already belongs to another attempt is a caller error with a
	// deterministic answer. Looking it up under the same locks turns what would
	// otherwise surface as a raw unique violation into a stable domain conflict.
	if err := s.rejectForeignOutcomeIdentity(ctx, tx, report.OutcomeRequestID, report.Fence.AttemptID); err != nil {
		return OutcomeResult{}, err
	}

	// Everything past here mutates, so it is an assertion of live authority and
	// a replaced session must be refused. Recognizing the replay above needed no
	// such assertion; recording a FIRST outcome does.
	if !state.sessionHealthy {
		return OutcomeResult{}, ErrFenceRejected
	}

	// A failure observed at or after the deadline must NOT become an ordinary
	// FAILED attempt. The truthful outcome is TIMED_OUT, and only a PostgreSQL
	// transaction driven by reconciliation may record that; letting this request
	// write FAILED would both mislabel the attempt and let a handler's own
	// classification override a server-authoritative one.
	//
	// Checked before lease usability for the same reason Succeed checks it
	// first: a timeout that already won released the lease too, and "your
	// execution budget ran out" is the more specific and more useful of the two
	// true answers.
	if state.timedOut() {
		return OutcomeResult{}, ErrAttemptTimedOut
	}
	if !state.leaseUsable() {
		return OutcomeResult{}, ErrLeaseExpired
	}
	if state.jobStatus != "RUNNING" || state.attemptStatus != AttemptRunning {
		return OutcomeResult{}, ErrStateConflict
	}

	decision, err := s.retryPolicy.Decide(
		report.Class, state.attemptNumber, state.attemptNumber, state.maxAttempts, s.jitter)
	if err != nil {
		return OutcomeResult{}, fmt.Errorf("%w: %v", ErrStateConflict, err)
	}

	outcome := attemptOutcome{
		status:           AttemptFailed,
		class:            report.Class,
		errorCode:        report.ErrorCode,
		errorMessage:     report.ErrorMessage,
		outcomeRequestID: &report.OutcomeRequestID,
		leaseStatus:      LeaseReleased,
	}
	result, err := s.finalizeAttempt(ctx, tx, state, report.Fence, outcome, decision)
	if err != nil {
		return OutcomeResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OutcomeResult{}, fmt.Errorf("commit failure outcome: %w", err)
	}
	return result, nil
}

// AcknowledgeCancellation records that a worker cooperatively stopped a canceled
// attempt.
//
// It is a dedicated operation rather than a flavor of Fail because cancellation
// is not a failure: it never retries, never dead-letters, and never consults the
// attempt budget. It also deliberately does not check the attempt's execution
// deadline. Once cancellation has durably won, it is the outcome; a job that was
// canceled while it happened to be running long is canceled, not timed out.
//
// The attempt may be LEASED rather than RUNNING. Cancellation can win after a
// claim but before Start, and the honest record of that is a CANCELED attempt
// with no start time — which is exactly the case migration 0009 revised the
// timeline constraint to allow.
func (s *Store) AcknowledgeCancellation(
	ctx context.Context,
	scope string,
	ack CancelAcknowledgment,
) (_ OutcomeResult, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if err := ValidateCancelAcknowledgment(ack); err != nil {
		return OutcomeResult{}, err
	}

	// lockAuthorityRows for the same reason Fail uses it: an acknowledgment that
	// already committed is immutable history, and a worker retrying an ambiguous
	// response must get the stored answer even after its session was replaced.
	tx, state, err := s.lockAuthorityRows(ctx, scope, ack.Fence)
	if err != nil {
		return OutcomeResult{}, err
	}
	defer rollback(ctx, tx)

	if state.outcomeRequestID != nil && *state.outcomeRequestID == ack.OutcomeRequestID {
		if state.attemptStatus != AttemptCanceled {
			return OutcomeResult{}, ErrOutcomeConflict
		}
		result := OutcomeResult{
			JobID: ack.Fence.JobID, JobStatus: state.jobStatus,
			AttemptStatus: state.attemptStatus, Replayed: true,
		}
		if err := tx.Commit(ctx); err != nil {
			return OutcomeResult{}, fmt.Errorf("commit replayed cancellation acknowledgment: %w", err)
		}
		return result, nil
	}
	if err := s.rejectForeignOutcomeIdentity(ctx, tx, ack.OutcomeRequestID, ack.Fence.AttemptID); err != nil {
		return OutcomeResult{}, err
	}

	// Recording a first acknowledgment mutates state, so it asserts live
	// authority and a replaced session must be refused.
	if !state.sessionHealthy {
		return OutcomeResult{}, ErrFenceRejected
	}

	// A lapsed lease is a real rejection here, unlike in the replay path above:
	// this request is asking to change state, and reconciliation owns
	// finalization once the lease can no longer commit.
	if !state.leaseUsable() {
		return OutcomeResult{}, ErrLeaseExpired
	}
	if state.jobStatus != "CANCEL_REQUESTED" || !isExecutingAttemptStatus(state.attemptStatus) {
		return OutcomeResult{}, ErrStateConflict
	}

	result, err := s.finalizeCancellation(ctx, tx, state, ack.Fence, &ack.OutcomeRequestID, LeaseReleased)
	if err != nil {
		return OutcomeResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OutcomeResult{}, fmt.Errorf("commit cancellation acknowledgment: %w", err)
	}
	return result, nil
}

// replayedOutcome answers an exact replay from what was persisted, and refuses a
// replay whose body changed.
//
// "Same identity, different body" is not a harmless retry: it is a caller
// claiming the committed decision was made on inputs it was not. Returning the
// stored decision anyway would hide that, and recomputing would violate the
// promise that an ambiguous response never redraws jitter.
func replayedOutcome(state fenceState, report FailureReport) (OutcomeResult, error) {
	if state.failureClass == nil || lifecycle.FailureClass(*state.failureClass) != report.Class {
		return OutcomeResult{}, ErrOutcomeConflict
	}
	if derefString(state.errorCode) != report.ErrorCode ||
		derefString(state.errorMessage) != report.ErrorMessage {
		return OutcomeResult{}, ErrOutcomeConflict
	}
	result := OutcomeResult{
		JobStatus:     state.jobStatus,
		AttemptStatus: state.attemptStatus,
		RetryAt:       state.retryAt,
		Replayed:      true,
	}
	if state.retryDelayMillis != nil {
		delay := time.Duration(*state.retryDelayMillis) * time.Millisecond
		result.RetryDelay = &delay
	}
	if state.jobStatus == "DEAD_LETTERED" {
		if report.Class == lifecycle.ClassPermanent {
			result.DeadLetterReason = lifecycle.ReasonPermanentFailure
		} else {
			result.DeadLetterReason = lifecycle.ReasonAttemptsExhausted
		}
	}
	return result, nil
}

func (s *Store) rejectForeignOutcomeIdentity(
	ctx context.Context,
	tx pgx.Tx,
	outcomeRequestID, attemptID uuid.UUID,
) error {
	var owner uuid.UUID
	err := tx.QueryRow(ctx,
		`SELECT id FROM job_attempts WHERE outcome_request_id = $1`, outcomeRequestID).Scan(&owner)
	switch {
	case err == nil && owner != attemptID:
		return ErrOutcomeConflict
	case err != nil && !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read outcome identity owner: %w", err)
	}
	return nil
}

// attemptOutcome is the terminal shape one attempt takes, independent of what
// the job then does. Keeping it separate from the retry decision is what lets
// failure, timeout, and abandonment share finalizeAttempt without any of them
// inheriting another's classification.
type attemptOutcome struct {
	status           AttemptStatus
	class            lifecycle.FailureClass
	errorCode        string
	errorMessage     string
	outcomeRequestID *uuid.UUID
	// leaseStatus is RELEASED when a worker reported the outcome and EXPIRED when
	// the lease lapsed and reconciliation finalized it. The distinction is real
	// history: it says whether authority was handed back or taken away.
	leaseStatus LeaseStatus
}

// finalizeAttempt writes one terminal attempt outcome and the job transition it
// implies, inside a transaction that already holds every authority row.
//
// Every non-cancellation terminal path funnels through here — worker-reported
// failure, reconciler-detected timeout, and ADR-0009 abandonment — so the
// attempt stamp, the capacity release, the retry bookkeeping, the DLQ insertion,
// and the transactional notification cannot drift apart between them.
func (s *Store) finalizeAttempt(
	ctx context.Context,
	tx pgx.Tx,
	state fenceState,
	fence Fence,
	outcome attemptOutcome,
	decision lifecycle.Decision,
) (OutcomeResult, error) {
	result := OutcomeResult{JobID: fence.JobID, AttemptStatus: outcome.status}

	var retryAt *time.Time
	var retryDelayMillis *int64
	if decision.Retry {
		// Both the chosen delay and the instant it produced are persisted. The
		// delay alone would not survive a replay (it would have to be re-added to
		// a different "now"), and the instant alone would hide what policy
		// produced it from anyone reading attempt history.
		//
		// The delay is truncated to the granularity it is stored at, and the
		// instant is derived from the truncated value. Deriving the instant from
		// the untruncated delay instead would make the two disagree by up to a
		// millisecond, so a replay reading them back would answer a question the
		// first response answered differently.
		//
		// A zero delay is still recorded, so "requeued immediately" (ADR-0009)
		// and "no decision was made" stay distinguishable in attempt history.
		millis := decision.Delay.Milliseconds()
		at := state.serverNow.Add(time.Duration(millis) * time.Millisecond)
		retryAt, retryDelayMillis = &at, &millis
	}

	// RETURNING, not the values computed above. PostgreSQL stores timestamps at
	// microsecond granularity, so the instant this transaction committed is not
	// necessarily the instant Go computed. Reporting the computed value would
	// make the first response and its own replay disagree — which is precisely
	// the promise the retained outcome identity exists to keep.
	var storedRetryAt *time.Time
	var storedRetryDelayMillis *int64
	err := tx.QueryRow(ctx, `
		UPDATE job_attempts
		SET status = $2, finished_at = $3, failure_class = $4,
		    error_code = $5, error_message = $6,
		    outcome_request_id = COALESCE($7, outcome_request_id),
		    retry_delay_ms = $8, retry_at = $9
		WHERE id = $1 AND status IN ('LEASED', 'RUNNING')
		RETURNING retry_delay_ms, retry_at`,
		fence.AttemptID, string(outcome.status), state.serverNow, string(outcome.class),
		nullableString(outcome.errorCode), nullableString(outcome.errorMessage),
		outcome.outcomeRequestID, retryDelayMillis, retryAt,
	).Scan(&storedRetryDelayMillis, &storedRetryAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return OutcomeResult{}, fmt.Errorf("%w: attempt changed during outcome", ErrStateConflict)
	}
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return OutcomeResult{}, ErrOutcomeConflict
		}
		return OutcomeResult{}, fmt.Errorf("record attempt outcome: %w", err)
	}
	retryAt = storedRetryAt

	// Capacity is released solely by the lease ceasing to be ACTIVE. There is no
	// counter to decrement, so it cannot drift or go negative.
	if tag, err := tx.Exec(ctx, `
		UPDATE leases SET status = $2, released_at = $3
		WHERE id = $1 AND status = 'ACTIVE'`,
		fence.LeaseID, string(outcome.leaseStatus), state.serverNow); err != nil {
		return OutcomeResult{}, fmt.Errorf("release lease after outcome: %w", err)
	} else if tag.RowsAffected() != 1 {
		return OutcomeResult{}, fmt.Errorf("%w: lease changed during outcome", ErrStateConflict)
	}

	if decision.Retry {
		result.RetryAt = retryAt
		if storedRetryDelayMillis != nil {
			result.RetryDelay = durationPointer(
				time.Duration(*storedRetryDelayMillis) * time.Millisecond)
		}
		if decision.Delay > 0 {
			// RETRY_WAIT, and deliberately NO outbox event. The job is durable and
			// scheduled; creating a notification now would advertise work no worker
			// may claim until available_at passes. The scheduler creates exactly one
			// event when the job actually becomes eligible.
			result.JobStatus = "RETRY_WAIT"
			if err := setJobStatus(ctx, tx, fence.JobID, "RETRY_WAIT", *retryAt, state.serverNow); err != nil {
				return OutcomeResult{}, err
			}
			return result, nil
		}
		// Immediate recovery (ADR-0009). The job becomes claimable now, so it
		// needs its notification now — in this same transaction, and on a fresh
		// generation, because the original event id was already consumed as this
		// attempt's claim identity.
		result.JobStatus = "QUEUED"
		generation, err := requeueJob(ctx, tx, fence.JobID, state.serverNow)
		if err != nil {
			return OutcomeResult{}, err
		}
		if _, err := outbox.InsertWorkAvailableTx(ctx, tx, fence.JobID, state.queue, generation); err != nil {
			return OutcomeResult{}, fmt.Errorf("record recovery notification: %w", err)
		}
		return result, nil
	}

	result.JobStatus = "DEAD_LETTERED"
	result.DeadLetterReason = decision.DeadLetterReason
	if err := setJobStatus(ctx, tx, fence.JobID, "DEAD_LETTERED", time.Time{}, state.serverNow); err != nil {
		return OutcomeResult{}, err
	}
	attemptID := fence.AttemptID
	if _, err := lifecycle.InsertDLQEntryTx(ctx, tx, state.scope, state.queue,
		fence.JobID, &attemptID, decision.DeadLetterReason, state.serverNow); err != nil {
		return OutcomeResult{}, fmt.Errorf("record dead-letter entry: %w", err)
	}
	return result, nil
}

// finalizeCancellation writes the terminal cancellation of one attempt.
//
// It is shared by the cooperative acknowledgment path and by reconciliation's
// fallback, which differ only in whether the lease was handed back (RELEASED) or
// taken away (EXPIRED). Cancellation produces neither a retry nor a DLQ entry,
// so it never touches the attempt budget.
func (s *Store) finalizeCancellation(
	ctx context.Context,
	tx pgx.Tx,
	state fenceState,
	fence Fence,
	outcomeRequestID *uuid.UUID,
	leaseStatus LeaseStatus,
) (OutcomeResult, error) {
	if tag, err := tx.Exec(ctx, `
		UPDATE job_attempts
		SET status = 'CANCELED', finished_at = $2, failure_class = $3,
		    error_code = $4, error_message = $5,
		    outcome_request_id = COALESCE($6, outcome_request_id)
		WHERE id = $1 AND status IN ('LEASED', 'RUNNING')`,
		fence.AttemptID, state.serverNow, string(lifecycle.ClassCanceled),
		lifecycle.CodeCanceled, lifecycle.MessageCanceled, outcomeRequestID); err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return OutcomeResult{}, ErrOutcomeConflict
		}
		return OutcomeResult{}, fmt.Errorf("cancel attempt: %w", err)
	} else if tag.RowsAffected() != 1 {
		return OutcomeResult{}, fmt.Errorf("%w: attempt changed during cancellation", ErrStateConflict)
	}

	if tag, err := tx.Exec(ctx, `
		UPDATE leases SET status = $2, released_at = $3
		WHERE id = $1 AND status = 'ACTIVE'`,
		fence.LeaseID, string(leaseStatus), state.serverNow); err != nil {
		return OutcomeResult{}, fmt.Errorf("release lease after cancellation: %w", err)
	} else if tag.RowsAffected() != 1 {
		return OutcomeResult{}, fmt.Errorf("%w: lease changed during cancellation", ErrStateConflict)
	}

	if tag, err := tx.Exec(ctx, `
		UPDATE jobs SET status = 'CANCELED', updated_at = $2
		WHERE id = $1 AND status = 'CANCEL_REQUESTED'`,
		fence.JobID, state.serverNow); err != nil {
		return OutcomeResult{}, fmt.Errorf("finalize job cancellation: %w", err)
	} else if tag.RowsAffected() != 1 {
		return OutcomeResult{}, fmt.Errorf("%w: job changed during cancellation", ErrStateConflict)
	}

	return OutcomeResult{
		JobID: fence.JobID, JobStatus: "CANCELED", AttemptStatus: AttemptCanceled,
	}, nil
}

// setJobStatus applies one terminal or waiting job transition from an executing
// state. availableAt is applied only when it is non-zero, which is what
// distinguishes RETRY_WAIT (eligible later) from DEAD_LETTERED (never again).
func setJobStatus(
	ctx context.Context,
	tx pgx.Tx,
	jobID uuid.UUID,
	status string,
	availableAt, serverNow time.Time,
) error {
	query := `UPDATE jobs SET status = $2, updated_at = $3
	          WHERE id = $1 AND status IN ('LEASED', 'RUNNING')`
	args := []any{jobID, status, serverNow}
	if !availableAt.IsZero() {
		query = `UPDATE jobs SET status = $2, updated_at = $3, available_at = $4
		         WHERE id = $1 AND status IN ('LEASED', 'RUNNING')`
		args = append(args, availableAt)
	}
	tag, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("transition job to %s: %w", status, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: job changed during outcome", ErrStateConflict)
	}
	return nil
}

// requeueJob returns an executing job to QUEUED and opens a new notification
// generation for it, returning that generation so the caller can write the
// matching outbox event in the same transaction.
//
// The generation increment and the last_notification_at stamp happen here rather
// than in the caller because they are inseparable from the transition: a job
// that becomes claimable without opening a new generation would be invisible to
// bounded re-notification, and one whose generation moved without an event would
// be advertised by nothing.
func requeueJob(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, serverNow time.Time) (int, error) {
	var generation int
	err := tx.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'QUEUED', available_at = $2, updated_at = $2,
		    notification_generation = notification_generation + 1,
		    last_notification_at = $2
		WHERE id = $1 AND status IN ('LEASED', 'RUNNING')
		RETURNING notification_generation`, jobID, serverNow).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("%w: job changed during requeue", ErrStateConflict)
	}
	if err != nil {
		return 0, fmt.Errorf("requeue job: %w", err)
	}
	return generation, nil
}

func nullableString(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func durationPointer(d time.Duration) *time.Duration { return &d }
