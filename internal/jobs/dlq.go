package jobs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/outbox"
)

// Errors the DLQ and replay operations return.
var (
	// ErrNotDeadLettered means the job exists in this scope but is not in the
	// logical dead-letter queue, so there is nothing to replay.
	ErrNotDeadLettered = errors.New("job is not dead-lettered")
	// ErrInvalidCursor means the pagination cursor was not one this API issued.
	ErrInvalidCursor = errors.New("invalid pagination cursor")
)

// DLQ listing bounds. A page is capped so one request cannot ask the database
// for an unbounded scan.
const (
	DefaultDLQPageSize = 25
	MaxDLQPageSize     = 100
)

// DLQEntry is one operator-visible row of the logical dead-letter queue.
//
// It carries bounded metadata joined from the immutable job and its terminal
// attempt, and deliberately NOT the job payload: a payload is unbounded user
// data, and putting it in a list endpoint would make one request able to return
// an arbitrary amount of it. A single job is still readable through
// GET /v1/jobs/{job_id}.
type DLQEntry struct {
	ID                uuid.UUID
	JobID             uuid.UUID
	Queue             string
	JobType           string
	Priority          int
	MaxAttempts       int
	Reason            lifecycle.DLQReason
	CreatedAt         time.Time
	TerminalAttemptID *uuid.UUID
	AttemptNumber     *int
	AttemptStatus     *string
	FailureClass      *string
	ErrorCode         *string
	ErrorMessage      *string
	// ReplayCount is how many replacement jobs this entry has produced. Different
	// idempotency keys deliberately create different replacements, so this can
	// legitimately exceed one.
	ReplayCount int
}

// DLQPage is one bounded page of the dead-letter queue.
type DLQPage struct {
	Entries []DLQEntry
	// NextCursor is empty when this is the last page. A page that is full may
	// still be the last one; the only way to know is to ask for the next.
	NextCursor string
}

// EncodeDLQCursor renders a keyset position as an opaque token.
//
// Opaque, but not secret: it is a position, not an authorization. Encoding it
// keeps clients from building cursors by hand and then depending on the ordering
// columns, which would freeze an implementation detail into the public contract.
func EncodeDLQCursor(createdAt time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(createdAt.UTC().Format(time.RFC3339Nano) + "|" + id.String()))
}

func decodeDLQCursor(cursor string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	stamp, rest, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	createdAt, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	id, err := uuid.Parse(rest)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	return createdAt, id, nil
}

// ListDLQ returns one bounded, scope-filtered page of dead-lettered jobs,
// newest first.
//
// Pagination is keyset on (created_at DESC, id DESC) rather than OFFSET. Two
// jobs can dead-letter in the same instant — two reconciler replicas finishing
// two exhausted attempts, for instance — so ordering by timestamp alone is not a
// total order, and OFFSET over a table that is still growing skips and repeats
// rows. The composite comparison below is a real total order, so a page boundary
// can neither duplicate nor omit an entry.
func (s *Store) ListDLQ(ctx context.Context, scope, cursor string, limit int) (DLQPage, error) {
	if limit <= 0 {
		limit = DefaultDLQPageSize
	}
	if limit > MaxDLQPageSize {
		limit = MaxDLQPageSize
	}

	var cursorAt *time.Time
	var cursorID *uuid.UUID
	if cursor != "" {
		at, id, err := decodeDLQCursor(cursor)
		if err != nil {
			return DLQPage{}, err
		}
		cursorAt, cursorID = &at, &id
	}

	// Matches dlq_entries_scope_keyset_idx. One extra row is requested so the
	// presence of a next page is a fact rather than a guess: returning a cursor
	// on every full page would hand clients a cursor to an empty page.
	rows, err := s.pool.Query(ctx, `
		SELECT d.id, d.job_id, d.queue, d.reason, d.created_at, d.terminal_attempt_id,
		       j.job_type, j.priority, j.max_attempts,
		       a.attempt_number, a.status, a.failure_class, a.error_code, a.error_message,
		       (SELECT count(*) FROM dlq_replays r WHERE r.original_job_id = d.job_id)
		FROM dlq_entries d
		JOIN jobs j ON j.id = d.job_id
		LEFT JOIN job_attempts a ON a.id = d.terminal_attempt_id
		WHERE d.scope = $1
		  AND ($2::timestamptz IS NULL OR (d.created_at, d.id) < ($2, $3))
		ORDER BY d.created_at DESC, d.id DESC
		LIMIT $4`, scope, cursorAt, cursorID, limit+1)
	if err != nil {
		return DLQPage{}, fmt.Errorf("list dead-letter entries: %w", err)
	}
	defer rows.Close()

	page := DLQPage{Entries: make([]DLQEntry, 0, limit)}
	for rows.Next() {
		var entry DLQEntry
		var reason string
		if err := rows.Scan(&entry.ID, &entry.JobID, &entry.Queue, &reason, &entry.CreatedAt,
			&entry.TerminalAttemptID, &entry.JobType, &entry.Priority, &entry.MaxAttempts,
			&entry.AttemptNumber, &entry.AttemptStatus, &entry.FailureClass,
			&entry.ErrorCode, &entry.ErrorMessage, &entry.ReplayCount); err != nil {
			return DLQPage{}, fmt.Errorf("scan dead-letter entry: %w", err)
		}
		entry.Reason = lifecycle.DLQReason(reason)
		page.Entries = append(page.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return DLQPage{}, fmt.Errorf("iterate dead-letter entries: %w", err)
	}

	if len(page.Entries) > limit {
		last := page.Entries[limit-1]
		page.Entries = page.Entries[:limit]
		page.NextCursor = EncodeDLQCursor(last.CreatedAt, last.ID)
	}
	return page, nil
}

// ReplayResult is the outcome of a replay or operator retry.
type ReplayResult struct {
	// OriginalJobID is the terminal job, unchanged by this operation.
	OriginalJobID uuid.UUID
	// Replacement is the new job. It has its own id, a fresh attempt budget, and
	// a fresh notification generation.
	Replacement *Job
	// Replayed is true when this replay identity had already created the
	// replacement. The HTTP layer answers 200 rather than 201 in that case.
	Replayed bool
}

// Replay creates a new job from a dead-lettered one.
//
// "Retry this job" and "replay this DLQ entry" are the same operation, and this
// is the one implementation of it. Two routes reach it, they share one
// idempotency namespace, and the same identity presented through either returns
// the same replacement rather than two jobs.
//
// What it deliberately does not do is resurrect the original. A terminal job
// never returns to a non-terminal state (reliability invariant 2), and its
// attempts, leases, failure metadata, and DLQ entry are the durable record of
// what happened. So the original is left exactly as it is, and a distinct new
// job is created with replayed_from_job_id pointing back at it.
//
// Idempotency works exactly like submission's, for exactly the same reason: the
// (scope, original job, key) primary key on dlq_replays is the guarantee, not
// application logic. Two concurrent identical requests both insert a job, one
// wins the identity insert, and the loser rolls back — discarding its own job,
// leaving no orphan — and reads the winner's replacement.
//
// Locking is queue -> original job, extended by the dlq_replays row at the end.
func (s *Store) Replay(ctx context.Context, scope string, originalJobID uuid.UUID, key string) (ReplayResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("begin replay: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var queue string
	err = tx.QueryRow(ctx,
		`SELECT queue FROM jobs WHERE id = $1 AND scope = $2`, originalJobID, scope).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReplayResult{}, ErrJobNotFound
	}
	if err != nil {
		return ReplayResult{}, fmt.Errorf("route replay: %w", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT name FROM queues WHERE name = $1 FOR UPDATE`, queue).Scan(&queue); err != nil {
		return ReplayResult{}, fmt.Errorf("lock queue for replay: %w", err)
	}

	original, err := scanJob(tx.QueryRow(ctx, jobSelect+`
		WHERE id = $1 AND scope = $2
		FOR UPDATE`, originalJobID, scope))
	if err != nil {
		return ReplayResult{}, err
	}
	// Revalidated under the lock. Both conditions are checked, not just the
	// status: a job could in principle be DEAD_LETTERED with no entry if a future
	// path forgot the shared insertion helper, and replaying something the DLQ
	// does not list would hide that bug rather than surface it.
	if original.Status != StatusDeadLettered {
		return ReplayResult{}, ErrNotDeadLettered
	}
	var hasEntry bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM dlq_entries WHERE job_id = $1)`, originalJobID).Scan(&hasEntry); err != nil {
		return ReplayResult{}, fmt.Errorf("read dead-letter entry: %w", err)
	}
	if !hasEntry {
		return ReplayResult{}, ErrNotDeadLettered
	}

	var serverNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&serverNow); err != nil {
		return ReplayResult{}, fmt.Errorf("read replay time: %w", err)
	}

	// The replacement copies the original's definition exactly and nothing else.
	// It is immediately eligible: a replay is an operator saying "run this now",
	// so carrying the original's schedule forward would delay it by an instant
	// that has already passed, and carrying its retry state forward would make
	// the operator wait out a backoff for an attempt that never happens.
	replacement := &Job{
		ID:                   uuid.New(),
		Scope:                scope,
		Queue:                original.Queue,
		Type:                 original.Type,
		Payload:              original.Payload,
		Status:               StatusQueued,
		Priority:             original.Priority,
		MaxAttempts:          original.MaxAttempts,
		TimeoutSeconds:       original.TimeoutSeconds,
		RequiredCapabilities: original.RequiredCapabilities,
		AvailableAt:          serverNow,
		ReplayedFromJobID:    &originalJobID,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (
			id, scope, queue, job_type, payload, status,
			priority, max_attempts, timeout_seconds, required_capabilities,
			scheduled_at, available_at, replayed_from_job_id,
			notification_generation, last_notification_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'QUEUED', $6, $7, $8, $9,
		          NULL, $10, $11, 1, $10, $10, $10)
		RETURNING created_at, updated_at`,
		replacement.ID, scope, replacement.Queue, replacement.Type, replacement.Payload,
		replacement.Priority, replacement.MaxAttempts, replacement.TimeoutSeconds,
		replacement.RequiredCapabilities, serverNow, originalJobID,
	).Scan(&replacement.CreatedAt, &replacement.UpdatedAt)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("insert replacement job: %w", err)
	}

	var winner uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO dlq_replays (scope, original_job_id, idempotency_key, replacement_job_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (scope, original_job_id, idempotency_key) DO NOTHING
		RETURNING replacement_job_id`,
		scope, originalJobID, key, replacement.ID).Scan(&winner)
	if errors.Is(err, pgx.ErrNoRows) {
		// Someone else owns this replay identity. Discard our replacement and
		// defer to theirs, exactly as a losing submission does.
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return ReplayResult{}, fmt.Errorf("rollback losing replay: %w", rbErr)
		}
		return s.resolveExistingReplay(ctx, scope, originalJobID, key)
	}
	if err != nil {
		return ReplayResult{}, fmt.Errorf("insert replay identity: %w", err)
	}

	// Same transaction as the replacement job and its identity, so a replay can
	// never be durable without the notification that makes it reachable.
	if _, err := outbox.InsertWorkAvailableTx(ctx, tx, replacement.ID, replacement.Queue, 1); err != nil {
		return ReplayResult{}, fmt.Errorf("record replay notification: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ReplayResult{}, fmt.Errorf("commit replay: %w", err)
	}
	return ReplayResult{OriginalJobID: originalJobID, Replacement: replacement}, nil
}

// resolveExistingReplay answers a replay whose identity is already taken by
// returning the replacement that identity created.
//
// There is no fingerprint to compare, unlike submission: everything about the
// replacement is derived from the original job, so the same (scope, original,
// key) can only ever have meant one request. A retry after an ambiguous response
// therefore always returns the committed replacement rather than a conflict.
func (s *Store) resolveExistingReplay(ctx context.Context, scope string, originalJobID uuid.UUID, key string) (ReplayResult, error) {
	var replacementID uuid.UUID
	err := s.pool.QueryRow(ctx, `
		SELECT replacement_job_id FROM dlq_replays
		WHERE scope = $1 AND original_job_id = $2 AND idempotency_key = $3`,
		scope, originalJobID, key).Scan(&replacementID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ReplayResult{}, fmt.Errorf("replay record disappeared during replay; retry the request")
		}
		return ReplayResult{}, fmt.Errorf("read replay identity: %w", err)
	}
	replacement, err := s.getByID(ctx, scope, replacementID)
	if err != nil {
		return ReplayResult{}, err
	}
	return ReplayResult{OriginalJobID: originalJobID, Replacement: replacement, Replayed: true}, nil
}
