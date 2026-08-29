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

// InsertTx writes an outbox event inside a caller-supplied transaction.
//
// Taking pgx.Tx rather than a pool is the whole point of this package: the event
// must commit atomically with the state change that caused it. There is no
// pool-based insert on purpose — offering one would make the dual-write bug easy
// to reintroduce.
func InsertTx(ctx context.Context, tx pgx.Tx, eventType string, schemaVersion int, data any) (uuid.UUID, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal outbox data: %w", err)
	}
	id := uuid.New()
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, schema_version, payload)
		VALUES ($1, $2, $3, $4)`,
		id, eventType, schemaVersion, raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert outbox event: %w", err)
	}
	return id, nil
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
