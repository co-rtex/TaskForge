package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrDLQEntryExists reports that this job already has a logical DLQ entry.
//
// It is a domain error rather than a leaked unique-violation because it is a
// real, expected outcome: two reconciler replicas can both decide a job is
// exhausted, and exactly one of them commits.
var ErrDLQEntryExists = errors.New("job already has a dead-letter entry")

// DLQEntry is one row of the authoritative logical dead-letter queue.
type DLQEntry struct {
	ID                uuid.UUID
	Scope             string
	Queue             string
	JobID             uuid.UUID
	TerminalAttemptID *uuid.UUID
	Reason            DLQReason
	CreatedAt         time.Time
}

// InsertDLQEntryTx records one dead-lettered job inside a caller-supplied
// transaction.
//
// Every path that sets a job to DEAD_LETTERED goes through here: permanent
// failure, exhausted retryable failure, exhausted timeout, and ADR-0009's
// exhausted abandonment. That is the point of the function existing rather than
// each caller writing its own INSERT — four statements would be four chances
// for the reason vocabulary, the scope binding, or the timestamp source to
// diverge.
//
// It takes pgx.Tx rather than a pool, for the same reason outbox insertion
// does: the entry must commit atomically with the transition that created it,
// or an operator could see a DEAD_LETTERED job that the DLQ does not list.
//
// createdAt must be the PostgreSQL time the caller already sampled AFTER its
// authority locks. Passing a fresh sample here would put a second, later
// timestamp on the same logical instant.
//
// Locking: this INSERT extends the established
// queue -> worker session -> job -> attempt -> lease order with dlq_entries at
// the end. It takes no lock of its own beyond the row it inserts, and it is
// always called after every authority row is already held.
func InsertDLQEntryTx(
	ctx context.Context,
	tx pgx.Tx,
	scope, queue string,
	jobID uuid.UUID,
	terminalAttemptID *uuid.UUID,
	reason DLQReason,
	createdAt time.Time,
) (DLQEntry, error) {
	if !reason.Valid() {
		return DLQEntry{}, fmt.Errorf("invalid dead-letter reason %q", reason)
	}
	entry := DLQEntry{
		ID: uuid.New(), Scope: scope, Queue: queue, JobID: jobID,
		TerminalAttemptID: terminalAttemptID, Reason: reason, CreatedAt: createdAt,
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO dlq_entries (
			id, scope, queue, job_id, terminal_attempt_id, reason, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		entry.ID, entry.Scope, entry.Queue, entry.JobID,
		entry.TerminalAttemptID, string(entry.Reason), entry.CreatedAt)
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return DLQEntry{}, ErrDLQEntryExists
		}
		return DLQEntry{}, fmt.Errorf("insert dead-letter entry: %w", err)
	}
	return entry, nil
}
