package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the outbox's PostgreSQL persistence.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds a Store over an existing pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// InsertWorkAvailableTx writes one work-availability notification inside a
// caller-supplied transaction.
//
// Taking pgx.Tx rather than a pool is the whole point of this package: the event
// must commit atomically with the state change that caused it. There is no
// pool-based insert on purpose — offering one would make the dual-write bug easy
// to reintroduce.
//
// generation is the job's notification generation at the moment this event is
// written. It is stored in a real column, alongside a real job reference,
// because the scheduler has to decide whether the CURRENT eligibility
// transition still has an unpublished notification — and a hint buried in the
// published JSON envelope is not something a correctness decision may rest on.
// The wire contract is unchanged: neither column is serialized to the broker.
func InsertWorkAvailableTx(
	ctx context.Context,
	tx pgx.Tx,
	jobID uuid.UUID,
	queue string,
	generation int,
) (uuid.UUID, error) {
	if generation < 1 {
		return uuid.Nil, fmt.Errorf("work.available notification generation must be at least 1, got %d", generation)
	}
	raw, err := json.Marshal(WorkAvailableData{Queue: queue, JobID: jobID.String()})
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal outbox data: %w", err)
	}
	id := uuid.New()
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (
			id, event_type, schema_version, payload, job_id, notification_generation
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		id, EventWorkAvailable, WorkAvailableSchemaVersion, raw, jobID, generation)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert outbox event: %w", err)
	}
	return id, nil
}

// HasPendingWorkAvailableTx reports whether an unpublished work.available event
// already exists for this job's given notification generation.
//
// This is the check that keeps bounded re-notification from multiplying events,
// and the reason the generation is part of it rather than just the job id: a
// stale event left behind by the publish-before-mark window belongs to an
// earlier generation, so it correctly fails to satisfy the current one and
// cannot suppress the notification a new eligibility transition requires.
func HasPendingWorkAvailableTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, generation int) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM outbox_events
			WHERE job_id = $1 AND notification_generation = $2 AND status = 'PENDING'
		)`, jobID, generation).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check pending work notification: %w", err)
	}
	return exists, nil
}

// ClaimDue atomically claims up to limit due pending events.
//
// Concurrency: FOR UPDATE SKIP LOCKED means two publishers scanning at the same
// instant never select the same row — the second skips locked rows instead of
// blocking. Claiming pushes available_at forward by claimTimeout, which acts as a
// visibility timeout: a publisher that dies between claiming and publishing
// releases its events automatically once that window passes, instead of pinning
// them forever.
//
// The claim commits before publishing so that no row lock is held across network
// I/O to the broker.
func (s *Store) ClaimDue(ctx context.Context, limit int, claimTimeout time.Duration) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT id
			FROM outbox_events
			WHERE status = 'PENDING'
			  AND available_at <= now()
			ORDER BY available_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE outbox_events o
		SET attempts     = o.attempts + 1,
		    claimed_at   = now(),
		    available_at = now() + make_interval(secs => $2::double precision)
		FROM due
		WHERE o.id = due.id
		RETURNING o.id, o.event_type, o.schema_version, o.payload, o.attempts, o.created_at`,
		limit, claimTimeout.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim due outbox events: %w", err)
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.Type, &e.SchemaVersion, &e.Data, &e.Attempts, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return out, nil
}

// MarkPublished records successful publication.
//
// The status predicate makes this idempotent: it reports false when the row was
// already published, which happens when a duplicate claim raced. The caller logs
// that rather than treating it as an error.
func (s *Store) MarkPublished(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PUBLISHED', published_at = now(), last_error = NULL
		WHERE id = $1 AND status = 'PENDING'`, id)
	if err != nil {
		return false, fmt.Errorf("mark outbox event published: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RecordFailure stores a publish error and schedules the retry.
//
// It never marks the event failed-terminal: an undeliverable notification must
// stay visible and retryable, because the job it refers to is already durable and
// would otherwise sit queued forever.
func (s *Store) RecordFailure(ctx context.Context, id uuid.UUID, cause string, retryAfter time.Duration) error {
	const maxStoredError = 1000
	if len(cause) > maxStoredError {
		cause = cause[:maxStoredError]
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET last_error = $2,
		    available_at = now() + make_interval(secs => $3::double precision)
		WHERE id = $1 AND status = 'PENDING'`, id, cause, retryAfter.Seconds())
	if err != nil {
		return fmt.Errorf("record outbox failure: %w", err)
	}
	return nil
}

// PendingCount reports how many events are waiting. A rising value is the
// primary signal that delivery is broken.
func (s *Store) PendingCount(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE status = 'PENDING'`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count pending outbox events: %w", err)
	}
	return n, nil
}

// ReleaseClaim makes a claimed event immediately due again.
//
// This exists for tests that need to reproduce the publish-before-mark crash
// window deterministically instead of waiting out a real visibility timeout.
func (s *Store) ReleaseClaim(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events SET available_at = now()
		WHERE id = $1 AND status = 'PENDING'`, id)
	if err != nil {
		return fmt.Errorf("release outbox claim: %w", err)
	}
	return nil
}
