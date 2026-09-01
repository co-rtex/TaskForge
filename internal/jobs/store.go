package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-rtex/TaskForge/internal/outbox"
)

// Errors returned by Store that callers must distinguish.
var (
	// ErrIdempotencyConflict means the key was reused with a different request.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	// ErrUnknownQueue means the named queue does not exist.
	ErrUnknownQueue = errors.New("unknown queue")
	// ErrJobNotFound means no job with that id exists in the caller's scope.
	ErrJobNotFound = errors.New("job not found")
)

// Store persists jobs.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds a Store over an existing pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// SubmitResult reports what a submission did.
type SubmitResult struct {
	Job *Job
	// Replayed is true when an earlier identical submission already created this
	// job. The HTTP layer answers 200 rather than 201 in that case.
	Replayed bool
}

// Submit durably accepts a job exactly once per (scope, idempotency key).
//
// One transaction writes the job, the idempotency record, and the outbox event,
// so there is no window in which a job exists without its notification, or a
// notification exists for a job that does not.
//
// Concurrency: the idempotency insert uses ON CONFLICT DO NOTHING. PostgreSQL
// makes a conflicting inserter wait for the in-flight transaction to finish, so
// two identical concurrent submissions cannot both win — the loser sees zero rows
// returned, rolls back (discarding its own job insert, leaving no orphan), and
// then reads the winner's record. The primary key on
// (scope, idempotency_key) is what actually enforces this; nothing here depends
// on application-level check-then-insert.
func (s *Store) Submit(ctx context.Context, scope, key string, req NormalizedRequest) (SubmitResult, error) {
	fingerprint := req.Fingerprint()

	exists, err := s.queueExists(ctx, req.Queue)
	if err != nil {
		return SubmitResult{}, err
	}
	if !exists {
		return SubmitResult{}, fmt.Errorf("%w: %q", ErrUnknownQueue, req.Queue)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("begin submit transaction: %w", err)
	}
	// Rollback is a no-op once the transaction has committed.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// PostgreSQL decides whether the requested schedule is still in the future.
	// A worker- or client-supplied clock is never authoritative for eligibility
	// (ADR-0001), so "is this delayed job due yet" is answered by the database
	// even at submission time, where a caller's clock skew would otherwise decide
	// whether the job is immediately claimable.
	var serverNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&serverNow); err != nil {
		return SubmitResult{}, fmt.Errorf("read submission time: %w", err)
	}

	job := &Job{
		ID:                   uuid.New(),
		Scope:                scope,
		Queue:                req.Queue,
		Type:                 req.Type,
		Payload:              req.Payload,
		Status:               StatusQueued,
		Priority:             req.Priority,
		MaxAttempts:          req.MaxAttempts,
		TimeoutSeconds:       req.TimeoutSeconds,
		RequiredCapabilities: req.RequiredCapabilities,
		ScheduledAt:          req.ScheduledAt,
		AvailableAt:          serverNow,
	}
	// An absent, null, or already-due schedule is an immediate submission and is
	// notified now. A future one is durable but not yet eligible, and gets NO
	// outbox event: publishing a notification for work no worker may claim yet
	// would make every delayed job a wasted broker round trip, and the scheduler
	// is what creates the event when the job actually becomes claimable.
	notificationGeneration := 1
	var lastNotificationAt *time.Time
	if req.ScheduledAt != nil && req.ScheduledAt.After(serverNow) {
		job.Status = StatusPending
		job.AvailableAt = *req.ScheduledAt
		notificationGeneration = 0
	} else {
		lastNotificationAt = &serverNow
	}

	// created_at and updated_at deliberately keep their DEFAULT now(), which is
	// the transaction's own start time and therefore identical to the outbox
	// event's. That shared timestamp is what makes "these two rows committed
	// together" observable in the database rather than merely asserted in prose.
	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (
			id, scope, queue, job_type, payload, status,
			priority, max_attempts, timeout_seconds, required_capabilities,
			scheduled_at, available_at, notification_generation, last_notification_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		RETURNING created_at, updated_at`,
		job.ID, job.Scope, job.Queue, job.Type, job.Payload, string(job.Status),
		job.Priority, job.MaxAttempts, job.TimeoutSeconds, job.RequiredCapabilities,
		job.ScheduledAt, job.AvailableAt, notificationGeneration, lastNotificationAt,
	).Scan(&job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("insert job: %w", err)
	}

	var winnerJobID uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO idempotency_records (scope, idempotency_key, request_fingerprint, job_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (scope, idempotency_key) DO NOTHING
		RETURNING job_id`,
		scope, key, fingerprint, job.ID,
	).Scan(&winnerJobID)

	if errors.Is(err, pgx.ErrNoRows) {
		// Someone else owns this key. Discard our job and defer to them.
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return SubmitResult{}, fmt.Errorf("rollback losing submission: %w", rbErr)
		}
		return s.resolveExisting(ctx, scope, key, fingerprint)
	}
	if err != nil {
		return SubmitResult{}, fmt.Errorf("insert idempotency record: %w", err)
	}

	// Same transaction as the job: this is the whole point of the outbox. A
	// delayed job deliberately gets no event here; the scheduler writes one, also
	// transactionally, when it promotes the job to QUEUED.
	if job.Status == StatusQueued {
		if _, err := outbox.InsertWorkAvailableTx(ctx, tx, job.ID, job.Queue, notificationGeneration); err != nil {
			return SubmitResult{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return SubmitResult{}, fmt.Errorf("commit submit transaction: %w", err)
	}
	return SubmitResult{Job: job, Replayed: false}, nil
}

// resolveExisting answers a submission whose idempotency key is already taken:
// the same request replays the original job, a different one is a conflict.
func (s *Store) resolveExisting(ctx context.Context, scope, key, fingerprint string) (SubmitResult, error) {
	var existingJobID uuid.UUID
	var existingFingerprint string
	err := s.pool.QueryRow(ctx, `
		SELECT job_id, request_fingerprint
		FROM idempotency_records
		WHERE scope = $1 AND idempotency_key = $2`, scope, key,
	).Scan(&existingJobID, &existingFingerprint)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// The holder rolled back between our conflict and this read. Report
			// it rather than inventing a result; the client can safely retry.
			return SubmitResult{}, fmt.Errorf("idempotency record disappeared during submission; retry the request")
		}
		return SubmitResult{}, fmt.Errorf("read idempotency record: %w", err)
	}

	if existingFingerprint != fingerprint {
		return SubmitResult{}, ErrIdempotencyConflict
	}

	job, err := s.getByID(ctx, scope, existingJobID)
	if err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Job: job, Replayed: true}, nil
}

// Get returns a job by id within a scope. Scoping the lookup means a caller can
// never read another tenant's job by guessing an id.
func (s *Store) Get(ctx context.Context, scope string, id uuid.UUID) (*Job, error) {
	return s.getByID(ctx, scope, id)
}

func (s *Store) getByID(ctx context.Context, scope string, id uuid.UUID) (*Job, error) {
	return scanJob(s.pool.QueryRow(ctx, jobSelect+`
		WHERE id = $1 AND scope = $2`, id, scope))
}

// jobSelect is the one column list every job read uses, so a new column cannot
// be returned by one path and silently missing from another.
const jobSelect = `
	SELECT id, scope, queue, job_type, payload, status,
	       priority, max_attempts, timeout_seconds, required_capabilities,
	       scheduled_at, available_at, cancel_requested_at, replayed_from_job_id,
	       created_at, updated_at
	FROM jobs
`

func scanJob(row pgx.Row) (*Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Scope, &j.Queue, &j.Type, &j.Payload, &j.Status,
		&j.Priority, &j.MaxAttempts, &j.TimeoutSeconds, &j.RequiredCapabilities,
		&j.ScheduledAt, &j.AvailableAt, &j.CancelRequestedAt, &j.ReplayedFromJobID,
		&j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("read job: %w", err)
	}
	return &j, nil
}

func (s *Store) queueExists(ctx context.Context, name string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM queues WHERE name = $1)`, name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check queue exists: %w", err)
	}
	return exists, nil
}
