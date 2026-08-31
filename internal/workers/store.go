package workers

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store serializes worker-control operations through PostgreSQL.
type Store struct {
	pool          *pgxpool.Pool
	leaseDuration time.Duration
}

// NewStore builds a worker control-plane store. Lease duration is a
// server-owned setting; a worker never supplies its own expiry.
func NewStore(pool *pgxpool.Pool, leaseDuration time.Duration) *Store {
	return &Store{pool: pool, leaseDuration: leaseDuration}
}

// Register creates or reuses the logical worker and idempotently registers one
// process session.
//
// A new session for the same logical worker marks the prior current session
// OFFLINE in the same transaction. Because both registration and control
// transitions lock the session row, either an old operation commits first or
// it observes that it has been fenced; there is no check-then-update window.
func (s *Store) Register(ctx context.Context, scope string, registration Registration) (_ Session, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	reg, err := NormalizeRegistration(registration)
	if err != nil {
		return Session{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Session{}, fmt.Errorf("begin worker registration: %w", err)
	}
	defer rollback(ctx, tx)

	workerID := uuid.New()
	// This upsert also locks the logical-worker row. It serializes two process
	// boots that try to replace the same current session at once.
	err = tx.QueryRow(ctx, `
		INSERT INTO workers (id, scope, name)
		VALUES ($1, $2, $3)
		ON CONFLICT (scope, name) DO UPDATE SET updated_at = clock_timestamp()
		RETURNING id`, workerID, scope, reg.Name).Scan(&workerID)
	if err != nil {
		return Session{}, fmt.Errorf("upsert logical worker: %w", err)
	}

	existing, err := readSessionForUpdate(ctx, tx, reg.SessionID)
	switch {
	case err == nil:
		if !sameRegistration(existing, workerID, reg) {
			return Session{}, ErrSessionConflict
		}
		if existing.Status != SessionHealthy {
			return Session{}, ErrSessionUnavailable
		}
		if err := tx.Commit(ctx); err != nil {
			return Session{}, fmt.Errorf("commit replayed worker registration: %w", err)
		}
		return existing, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return Session{}, fmt.Errorf("read existing worker session: %w", err)
	}
	// A process boot replaces the prior current boot. Its active leases remain
	// durable and consume logical-worker capacity until M3 reconciliation; they
	// are not silently transferred to this session.
	if _, err := tx.Exec(ctx, `
		UPDATE worker_sessions
		SET status = 'OFFLINE', ended_at = clock_timestamp()
		WHERE worker_id = $1
		  AND status IN ('STARTING', 'HEALTHY', 'DRAINING')`, workerID); err != nil {
		return Session{}, fmt.Errorf("fence prior worker session: %w", err)
	}
	// UPDATE above acquires any prior-session row lock. Sample only afterward so
	// a registration that waited cannot begin its heartbeat timeline in the past.
	var registeredAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&registeredAt); err != nil {
		return Session{}, fmt.Errorf("read worker registration time: %w", err)
	}

	session := Session{
		ID:                reg.SessionID,
		WorkerID:          workerID,
		Name:              reg.Name,
		Hostname:          reg.Hostname,
		WorkerGroup:       reg.WorkerGroup,
		ConcurrencyLimit:  reg.ConcurrencyLimit,
		Capabilities:      reg.Capabilities,
		SupportedJobTypes: reg.SupportedJobTypes,
		Status:            SessionHealthy,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO worker_sessions (
			id, worker_id, scope, hostname, worker_group, concurrency_limit,
			capabilities, supported_job_types, status, registered_at, last_heartbeat_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
		RETURNING registered_at, last_heartbeat_at`,
		session.ID, session.WorkerID, scope, session.Hostname, session.WorkerGroup,
		session.ConcurrencyLimit, session.Capabilities, session.SupportedJobTypes,
		string(session.Status), registeredAt,
	).Scan(&session.RegisteredAt, &session.LastHeartbeatAt)
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return Session{}, ErrSessionConflict
		}
		return Session{}, fmt.Errorf("insert worker session: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Session{}, fmt.Errorf("commit worker registration: %w", err)
	}
	return session, nil
}

func readSessionForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Session, error) {
	var session Session
	err := tx.QueryRow(ctx, `
		SELECT s.id, s.worker_id, w.name, s.hostname, s.worker_group,
		       s.concurrency_limit, s.capabilities, s.supported_job_types,
		       s.status, s.registered_at, s.last_heartbeat_at
		FROM worker_sessions s
		JOIN workers w ON w.id = s.worker_id
		WHERE s.id = $1
		FOR UPDATE OF s`, id,
	).Scan(&session.ID, &session.WorkerID, &session.Name, &session.Hostname,
		&session.WorkerGroup, &session.ConcurrencyLimit, &session.Capabilities,
		&session.SupportedJobTypes, &session.Status, &session.RegisteredAt,
		&session.LastHeartbeatAt)
	return session, err
}

func sameRegistration(existing Session, workerID uuid.UUID, reg Registration) bool {
	return existing.ID == reg.SessionID &&
		existing.WorkerID == workerID &&
		existing.Name == reg.Name &&
		existing.Hostname == reg.Hostname &&
		existing.WorkerGroup == reg.WorkerGroup &&
		existing.ConcurrencyLimit == reg.ConcurrencyLimit &&
		slices.Equal(existing.Capabilities, reg.Capabilities) &&
		slices.Equal(existing.SupportedJobTypes, reg.SupportedJobTypes)
}

// Claim atomically reserves at most one eligible job for one current session.
//
// Claim requests first take a transaction-scoped advisory lock derived from the
// globally unique claim id, then use queue -> worker session -> job -> attempt ->
// lease row order. The advisory lock serializes same-id requests even when they
// name different queues and would otherwise lock disjoint authority rows. Every
// M2 operation that changes active lease capacity uses the same row-lock order.
// At PostgreSQL READ COMMITTED, a waiter acquires these locks after the prior
// transaction commits and its subsequent active-lease counts therefore see that
// commit. This is why derived counts are safe here and would not be safe as a
// bare count-then-insert.
func (s *Store) Claim(ctx context.Context, scope string, req ClaimRequest) (_ ClaimResult, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if err := ValidateClaim(req); err != nil {
		return ClaimResult{}, err
	}
	if s.leaseDuration <= 0 {
		return ClaimResult{}, fmt.Errorf("lease duration must be positive")
	}

	// A cheap pre-read identifies an exact retry before locks are chosen. The
	// assignment is re-read under locks below; this read carries no authority.
	if existing, err := s.lookupClaim(ctx, s.pool, req.ClaimRequestID, false); err == nil {
		if existing.Scope != scope || existing.Queue != req.Queue {
			return ClaimResult{}, ErrClaimConflict
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return ClaimResult{}, fmt.Errorf("preflight claim replay: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer rollback(ctx, tx)

	// The event id is global claim identity. A 64-bit PostgreSQL hash collision
	// can only over-serialize unrelated requests; it cannot weaken correctness.
	// Take this before any row lock so same-id, cross-queue requests cannot both
	// miss the replay lookup and race into the unique lease constraint.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`, req.ClaimRequestID); err != nil {
		return ClaimResult{}, fmt.Errorf("lock claim request identity: %w", err)
	}

	var queueGroup string
	var queueLimit int
	err = tx.QueryRow(ctx, `
		SELECT worker_group, max_concurrency
		FROM queues
		WHERE name = $1
		FOR UPDATE`, req.Queue).Scan(&queueGroup, &queueLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return ClaimResult{}, ErrUnknownQueue
	}
	if err != nil {
		return ClaimResult{}, fmt.Errorf("lock queue capacity: %w", err)
	}

	var (
		sessionStatus SessionStatus
		workerGroup   string
		workerLimit   int
		capabilities  []string
		jobTypes      []string
	)
	err = tx.QueryRow(ctx, `
		SELECT status, worker_group, concurrency_limit, capabilities, supported_job_types
		FROM worker_sessions
		WHERE id = $1 AND worker_id = $2 AND scope = $3
		FOR UPDATE`, req.SessionID, req.WorkerID, scope,
	).Scan(&sessionStatus, &workerGroup, &workerLimit, &capabilities, &jobTypes)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && sessionStatus != SessionHealthy) {
		return ClaimResult{}, ErrSessionUnavailable
	}
	if err != nil {
		return ClaimResult{}, fmt.Errorf("lock worker session capacity: %w", err)
	}

	// Re-read after both capacity locks. A concurrent retry of this same claim
	// request is now committed and visible, so it returns one assignment rather
	// than reserving a second job.
	if existing, err := s.lookupClaim(ctx, tx, req.ClaimRequestID, true); err == nil {
		if existing.Scope != scope || existing.Queue != req.Queue {
			return ClaimResult{}, ErrClaimConflict
		}
		if existing.WorkerID != req.WorkerID || existing.SessionID != req.SessionID {
			return commitNoClaim(ctx, tx, DuplicateNotification)
		}
		if err := setLeaseRemaining(ctx, tx, existing); err != nil {
			return ClaimResult{}, fmt.Errorf("read replayed lease window: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return ClaimResult{}, fmt.Errorf("commit replayed claim: %w", err)
		}
		return ClaimResult{Disposition: Claimed, Assignment: existing, Replayed: true}, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return ClaimResult{}, fmt.Errorf("read claim replay: %w", err)
	}

	if workerGroup != queueGroup {
		return commitNoClaim(ctx, tx, NoEligibleJob)
	}

	var activeForQueue int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM leases WHERE queue = $1 AND status = 'ACTIVE'`,
		req.Queue).Scan(&activeForQueue); err != nil {
		return ClaimResult{}, fmt.Errorf("count active queue leases: %w", err)
	}
	if activeForQueue >= queueLimit {
		return commitNoClaim(ctx, tx, CapacityExhausted)
	}

	// Count by logical worker, not just process session. Leases held by a
	// replaced boot remain reservations until M3 reconciliation and cannot be
	// bypassed by restarting the process.
	var activeForWorker int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM leases WHERE worker_id = $1 AND status = 'ACTIVE'`,
		req.WorkerID).Scan(&activeForWorker); err != nil {
		return ClaimResult{}, fmt.Errorf("count active worker leases: %w", err)
	}
	if activeForWorker >= workerLimit {
		return commitNoClaim(ctx, tx, CapacityExhausted)
	}

	assignment := &Assignment{}
	// SKIP LOCKED prevents two claimers from waiting on and then both acting on
	// the same candidate. The queue capacity lock serializes claimers for one
	// queue today; SKIP LOCKED also keeps the selection correct if capacity moves
	// to a sharded ledger later. PostgreSQL server time owns eligibility.
	err = tx.QueryRow(ctx, `
		SELECT j.id, j.queue, j.job_type, j.payload, j.priority,
		       j.timeout_seconds, j.required_capabilities
		FROM jobs j
		WHERE j.scope = $1
		  AND j.queue = $2
		  AND j.status = 'QUEUED'
		  AND j.available_at <= clock_timestamp()
		  AND j.required_capabilities <@ $3::text[]
		  AND j.job_type = ANY($4::text[])
		  AND (SELECT count(*) FROM job_attempts a WHERE a.job_id = j.id) < j.max_attempts
		ORDER BY j.priority DESC, j.available_at ASC, j.created_at ASC, j.id ASC
		FOR UPDATE OF j SKIP LOCKED
		LIMIT 1`, scope, req.Queue, capabilities, jobTypes,
	).Scan(&assignment.JobID, &assignment.Queue, &assignment.JobType,
		&assignment.Payload, &assignment.Priority, &assignment.TimeoutSeconds,
		&assignment.RequiredCapabilities)
	if errors.Is(err, pgx.ErrNoRows) {
		var anyDue bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM jobs
				WHERE scope = $1 AND queue = $2 AND status = 'QUEUED'
				  AND available_at <= clock_timestamp()
			)`, scope, req.Queue).Scan(&anyDue); err != nil {
			return ClaimResult{}, fmt.Errorf("check queue after empty claim: %w", err)
		}
		if !anyDue {
			return commitNoClaim(ctx, tx, QueueEmpty)
		}
		return commitNoClaim(ctx, tx, NoEligibleJob)
	}
	if err != nil {
		return ClaimResult{}, fmt.Errorf("select eligible job: %w", err)
	}

	assignment.AttemptID = uuid.New()
	assignment.LeaseID = uuid.New()
	assignment.WorkerID = req.WorkerID
	assignment.SessionID = req.SessionID
	assignment.Scope = scope
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(max(attempt_number), 0) + 1
		FROM job_attempts
		WHERE job_id = $1`, assignment.JobID).Scan(&assignment.AttemptNumber)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("allocate attempt number: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO job_attempts (
			id, job_id, scope, queue, attempt_number,
			worker_id, worker_session_id, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'LEASED')`,
		assignment.AttemptID, assignment.JobID, scope, assignment.Queue,
		assignment.AttemptNumber, req.WorkerID, req.SessionID)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("insert job attempt: %w", err)
	}

	// PostgreSQL now() is fixed at transaction start and is therefore unsafe
	// after any lock wait. Sample wall-clock database time only after every
	// authority row is locked, then use that one instant for lease issuance.
	var acquiredAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&acquiredAt); err != nil {
		return ClaimResult{}, fmt.Errorf("read lease issuance time: %w", err)
	}
	assignment.LeaseExpiresAt = acquiredAt.Add(s.leaseDuration)
	assignment.LeaseRemaining = s.leaseDuration
	err = tx.QueryRow(ctx, `
		INSERT INTO leases (
			id, job_id, attempt_id, scope, queue, worker_id, worker_session_id,
			claim_request_id, status, acquired_at, renewed_at, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'ACTIVE',
		          $9, $9, $10)
		RETURNING expires_at`,
		assignment.LeaseID, assignment.JobID, assignment.AttemptID, scope,
		assignment.Queue, req.WorkerID, req.SessionID, req.ClaimRequestID,
		acquiredAt, assignment.LeaseExpiresAt,
	).Scan(&assignment.LeaseExpiresAt)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("insert active lease: %w", err)
	}

	commandTag, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'LEASED', updated_at = $3
		WHERE id = $1 AND scope = $2 AND status = 'QUEUED'`, assignment.JobID, scope, acquiredAt)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("lease job: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ClaimResult{}, fmt.Errorf("%w: queued job changed during claim", ErrStateConflict)
	}

	if err := tx.Commit(ctx); err != nil {
		return ClaimResult{}, fmt.Errorf("commit job claim: %w", err)
	}
	return ClaimResult{Disposition: Claimed, Assignment: assignment}, nil
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Store) lookupClaim(ctx context.Context, q rowQuerier, claimID uuid.UUID, lock bool) (*Assignment, error) {
	query := `
		SELECT a.scope, j.id, j.queue, j.job_type, j.payload, j.priority,
		       j.timeout_seconds, j.required_capabilities,
		       a.id, a.attempt_number, l.id, l.expires_at,
		       a.worker_id, a.worker_session_id
		FROM job_attempts a
		JOIN leases l ON l.attempt_id = a.id
		JOIN jobs j ON j.id = a.job_id
		WHERE l.claim_request_id = $1`
	if lock {
		query += ` FOR UPDATE OF j, a, l`
	}
	assignment := &Assignment{}
	err := q.QueryRow(ctx, query, claimID).Scan(
		&assignment.Scope, &assignment.JobID, &assignment.Queue, &assignment.JobType, &assignment.Payload,
		&assignment.Priority, &assignment.TimeoutSeconds, &assignment.RequiredCapabilities,
		&assignment.AttemptID, &assignment.AttemptNumber, &assignment.LeaseID,
		&assignment.LeaseExpiresAt, &assignment.WorkerID, &assignment.SessionID,
	)
	return assignment, err
}

func setLeaseRemaining(ctx context.Context, q rowQuerier, assignment *Assignment) error {
	var serverNow time.Time
	if err := q.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&serverNow); err != nil {
		return err
	}
	assignment.LeaseRemaining = assignment.LeaseExpiresAt.Sub(serverNow)
	if assignment.LeaseRemaining < 0 {
		assignment.LeaseRemaining = 0
	}
	return nil
}

func commitNoClaim(ctx context.Context, tx pgx.Tx, disposition ClaimDisposition) (ClaimResult, error) {
	if err := tx.Commit(ctx); err != nil {
		return ClaimResult{}, fmt.Errorf("commit empty claim: %w", err)
	}
	return ClaimResult{Disposition: disposition}, nil
}

// Start performs the dedicated LEASED -> RUNNING transition. Replaying the
// exact fence while it remains current is idempotent.
func (s *Store) Start(ctx context.Context, scope string, fence Fence) (err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if err := ValidateFence(fence); err != nil {
		return err
	}
	tx, state, err := s.lockFence(ctx, scope, fence)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)

	if state.leaseStatus != LeaseActive || !state.serverNow.Before(state.expiresAt) {
		return ErrLeaseExpired
	}
	if state.jobStatus == "RUNNING" && state.attemptStatus == AttemptRunning {
		return tx.Commit(ctx)
	}
	if state.jobStatus != "LEASED" || state.attemptStatus != AttemptLeased {
		return ErrStateConflict
	}

	if tag, err := tx.Exec(ctx, `
		UPDATE job_attempts SET status = 'RUNNING', started_at = $2
		WHERE id = $1 AND status = 'LEASED'`, fence.AttemptID, state.serverNow); err != nil {
		return fmt.Errorf("start attempt: %w", err)
	} else if tag.RowsAffected() != 1 {
		return ErrStateConflict
	}
	if tag, err := tx.Exec(ctx, `
		UPDATE jobs SET status = 'RUNNING', updated_at = $2
		WHERE id = $1 AND status = 'LEASED'`, fence.JobID, state.serverNow); err != nil {
		return fmt.Errorf("start job: %w", err)
	} else if tag.RowsAffected() != 1 {
		return ErrStateConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit attempt start: %w", err)
	}
	return nil
}

// Succeed atomically accepts one fenced successful outcome and releases both
// queue and logical-worker capacity. An exact replay after success is a no-op.
func (s *Store) Succeed(ctx context.Context, scope string, fence Fence) (err error) {
	defer func() { err = classifyDatabaseError(err) }()

	if err := ValidateFence(fence); err != nil {
		return err
	}
	tx, state, err := s.lockFence(ctx, scope, fence)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)

	if state.jobStatus == "SUCCEEDED" && state.attemptStatus == AttemptSucceeded && state.leaseStatus == LeaseCompleted {
		return tx.Commit(ctx)
	}
	if state.leaseStatus != LeaseActive || !state.serverNow.Before(state.expiresAt) {
		return ErrLeaseExpired
	}
	if state.jobStatus != "RUNNING" || state.attemptStatus != AttemptRunning {
		return ErrStateConflict
	}

	if tag, err := tx.Exec(ctx, `
		UPDATE jobs SET status = 'SUCCEEDED', updated_at = $2
		WHERE id = $1 AND status = 'RUNNING'`, fence.JobID, state.serverNow); err != nil {
		return fmt.Errorf("complete job: %w", err)
	} else if tag.RowsAffected() != 1 {
		return ErrStateConflict
	}
	if tag, err := tx.Exec(ctx, `
		UPDATE job_attempts SET status = 'SUCCEEDED', finished_at = $2
		WHERE id = $1 AND status = 'RUNNING'`, fence.AttemptID, state.serverNow); err != nil {
		return fmt.Errorf("complete attempt: %w", err)
	} else if tag.RowsAffected() != 1 {
		return ErrStateConflict
	}
	if tag, err := tx.Exec(ctx, `
		UPDATE leases SET status = 'COMPLETED', released_at = $2
		WHERE id = $1 AND status = 'ACTIVE'`, fence.LeaseID, state.serverNow); err != nil {
		return fmt.Errorf("release completed lease: %w", err)
	} else if tag.RowsAffected() != 1 {
		return ErrStateConflict
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit successful outcome: %w", err)
	}
	return nil
}

type fenceState struct {
	jobStatus     string
	attemptStatus AttemptStatus
	leaseStatus   LeaseStatus
	expiresAt     time.Time
	serverNow     time.Time
}

// lockFence uses the same queue -> worker session -> job -> attempt -> lease
// order as Claim. The first lease read is only an immutable routing hint; every
// authority field is revalidated after all rows are locked.
func (s *Store) lockFence(ctx context.Context, scope string, fence Fence) (pgx.Tx, fenceState, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fenceState{}, fmt.Errorf("begin fenced transition: %w", err)
	}
	fail := func(err error) (pgx.Tx, fenceState, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, fenceState{}, err
	}

	var queue string
	err = tx.QueryRow(ctx, `
		SELECT queue FROM leases
		WHERE id = $1 AND job_id = $2 AND attempt_id = $3
		  AND worker_id = $4 AND worker_session_id = $5 AND scope = $6`,
		fence.LeaseID, fence.JobID, fence.AttemptID, fence.WorkerID, fence.SessionID, scope,
	).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail(ErrFenceRejected)
	}
	if err != nil {
		return fail(fmt.Errorf("route fenced transition: %w", err))
	}

	if err := tx.QueryRow(ctx, `SELECT name FROM queues WHERE name = $1 FOR UPDATE`, queue).Scan(&queue); err != nil {
		return fail(fmt.Errorf("lock queue for fenced transition: %w", err))
	}
	var sessionStatus SessionStatus
	err = tx.QueryRow(ctx, `
		SELECT status FROM worker_sessions
		WHERE id = $1 AND worker_id = $2 AND scope = $3
		FOR UPDATE`, fence.SessionID, fence.WorkerID, scope).Scan(&sessionStatus)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && sessionStatus != SessionHealthy) {
		return fail(ErrFenceRejected)
	}
	if err != nil {
		return fail(fmt.Errorf("lock worker session for fenced transition: %w", err))
	}

	var state fenceState
	err = tx.QueryRow(ctx, `
		SELECT j.status, a.status, l.status, l.expires_at
		FROM jobs j
		JOIN job_attempts a ON a.job_id = j.id
		JOIN leases l ON l.attempt_id = a.id
		WHERE j.id = $1 AND a.id = $2 AND l.id = $3
		  AND a.worker_id = $4 AND a.worker_session_id = $5
		  AND j.scope = $6 AND a.scope = $6 AND l.scope = $6
		FOR UPDATE OF j, a, l`,
		fence.JobID, fence.AttemptID, fence.LeaseID, fence.WorkerID, fence.SessionID, scope,
	).Scan(&state.jobStatus, &state.attemptStatus, &state.leaseStatus, &state.expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail(ErrFenceRejected)
	}
	if err != nil {
		return fail(fmt.Errorf("lock fenced state: %w", err))
	}
	// This query is intentionally separate from the locking SELECT above. It
	// guarantees that a transaction which waited across the expiry boundary
	// cannot use its transaction-start timestamp to accept stale authority.
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&state.serverNow); err != nil {
		return fail(fmt.Errorf("read fenced transition time: %w", err))
	}
	return tx, state, nil
}

func rollback(ctx context.Context, tx pgx.Tx) {
	_ = tx.Rollback(context.WithoutCancel(ctx))
}

// classifyDatabaseError is the single translation point between a failed
// database call and the typed deadline sentinel.
//
// It inspects only the returned error. pgx wraps context.DeadlineExceeded when
// it aborts a query on an expiring deadline, so errors.Is is sufficient today;
// if a driver ever stops wrapping, this is the one function to change. It
// deliberately never consults ctx.Err(): an unrelated constraint, driver, or
// state failure that merely finishes after the deadline elapsed must keep its
// own identity rather than be laundered into a retryable deadline.
func classifyDatabaseError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrDeadlineExceeded) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrDeadlineExceeded, err)
	}
	return err
}
