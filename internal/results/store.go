package results

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrDeadlineExceeded reports that a database call in this operation failed
// because the operation's own deadline elapsed. It mirrors
// workers.ErrDeadlineExceeded and jobs.ErrDeadlineExceeded deliberately
// rather than being shared: each package owns its own translation from
// context.DeadlineExceeded so no package depends on another's error types
// for its own contract.
var ErrDeadlineExceeded = errors.New("operation deadline exceeded")

// Store reads recorded results from PostgreSQL. It never writes: the only
// writer is InsertResultTx, called from within internal/workers.Store.Succeed's
// own transaction, and it never touches the object store -- a caller that
// needs an object-located result's actual bytes fetches them separately
// through internal/objectstore, using the bucket and key this Store returns.
// internal/api.Server is where those two answers meet, mirroring how it is
// already the one place internal/workers and internal/workerauth's answers
// meet for worker-control authentication.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds a results reader.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Get reads the result recorded for jobID within scope.
func (s *Store) Get(ctx context.Context, scope string, jobID uuid.UUID) (_ *Result, err error) {
	defer func() { err = classifyDatabaseError(err) }()

	var result Result
	var objectBucket, objectKey, checksum *string
	scanErr := s.pool.QueryRow(ctx, `
		SELECT job_id, attempt_id, scope, location, inline_body,
		       object_bucket, object_key, size_bytes, checksum_sha256,
		       content_type, created_at
		FROM results
		WHERE job_id = $1 AND scope = $2`, jobID, scope,
	).Scan(&result.JobID, &result.AttemptID, &result.Scope, &result.Location, &result.InlineBody,
		&objectBucket, &objectKey, &result.SizeBytes, &checksum, &result.ContentType, &result.CreatedAt)
	if scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return nil, ErrResultNotFound
		}
		return nil, fmt.Errorf("read result: %w", scanErr)
	}
	if objectBucket != nil {
		result.ObjectBucket = *objectBucket
	}
	if objectKey != nil {
		result.ObjectKey = *objectKey
	}
	if checksum != nil {
		result.ChecksumSHA256 = *checksum
	}
	return &result, nil
}

// InsertResultTx records one job's result inside a caller-supplied
// transaction, mirroring outbox.InsertWorkAvailableTx and
// lifecycle.InsertDLQEntryTx: the row must commit atomically with the
// fenced transition that produced it, so there is no pool-based insert to
// accidentally call instead.
func InsertResultTx(ctx context.Context, tx pgx.Tx, result Result) error {
	if err := result.Validate(); err != nil {
		return err
	}
	contentType := result.ContentType
	if contentType == "" {
		contentType = "application/json"
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO results (
			job_id, attempt_id, scope, location, inline_body,
			object_bucket, object_key, size_bytes, checksum_sha256, content_type
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		result.JobID, result.AttemptID, result.Scope, string(result.Location),
		nullableJSON(result.InlineBody), nullableString(result.ObjectBucket),
		nullableString(result.ObjectKey), result.SizeBytes,
		nullableString(result.ChecksumSHA256), contentType)
	if err != nil {
		return fmt.Errorf("insert result: %w", err)
	}
	return nil
}

// nullableString returns a genuine untyped nil for an empty string, which
// pgx encodes as SQL NULL for any column type. Returning "" directly would
// insert an empty string, which the results_location_shape CHECK constraint
// does not treat as absent.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullableJSON is nullableString's equivalent for the one nullable jsonb
// column. An empty or nil json.RawMessage becomes SQL NULL rather than a
// stored JSON literal.
func nullableJSON(body []byte) any {
	if len(body) == 0 {
		return nil
	}
	return body
}

// classifyDatabaseError is the single translation point between a failed
// database call and the typed deadline sentinel, mirroring
// workers.classifyDatabaseError and jobs.classifyDatabaseError exactly.
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
