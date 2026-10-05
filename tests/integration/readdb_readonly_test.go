//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// readOnlyViolation is SQLSTATE 25006, read_only_sql_transaction. probeRaised is
// the code the probe raises itself, SQLSTATE P0001.
const (
	readOnlyViolation = "25006"
	probeRaised       = "P0001"
)

// writeProbe inserts a row and then raises, so it can never commit anything on
// any connection: where writes are allowed it fails at the RAISE, after the
// INSERT ran, and where they are not it fails at the INSERT itself. The two
// error codes are what tell the cases apart, and a missing guard cannot leave a
// stray row in the shared database behind it.
const writeProbe = `
	DO $$
	BEGIN
		INSERT INTO queues (name) VALUES ('readdb-write-probe');
		RAISE EXCEPTION 'probe aborted after the insert' USING ERRCODE = 'P0001';
	END
	$$`

func pgCode(t *testing.T, err error) string {
	t.Helper()
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected an error from PostgreSQL, got %v", err)
	return pgErr.Code
}

// TestReaddb_OpenRefusesWrites proves the connection the developer scripts
// read the control-plane database through cannot write by accident.
func TestReaddb_OpenRefusesWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Positive control: on an ordinary connection the probe gets as far as its own
	// RAISE, so the INSERT before it was a valid, permitted statement. Without
	// this, "the write failed" could only mean the statement was bad.
	plain, err := pgx.Connect(ctx, dsn())
	require.NoError(t, err)
	defer plain.Close(ctx)
	_, err = plain.Exec(ctx, writeProbe)
	require.Equal(t, probeRaised, pgCode(t, err), "the probe must be valid on an unguarded connection")

	guarded, err := readdb.Open(ctx, dsn())
	require.NoError(t, err)
	defer guarded.Close()

	// The setting is in force from the startup packet.
	var setting string
	require.NoError(t, guarded.QueryRow(ctx, `SHOW default_transaction_read_only`).Scan(&setting))
	require.Equal(t, "on", setting)

	// A statement outside any explicit transaction is refused...
	_, err = guarded.Exec(ctx, writeProbe)
	require.Equal(t, readOnlyViolation, pgCode(t, err), "an autocommit write must be refused")

	// ...and so is a transaction the caller begins without saying more.
	tx, err := guarded.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(ctx, writeProbe)
	require.Equal(t, readOnlyViolation, pgCode(t, err), "a write in a default transaction must be refused")
	require.NoError(t, tx.Rollback(ctx))

	// Reads are unaffected, and nothing was left behind.
	var n int
	require.NoError(t, guarded.QueryRow(ctx, `SELECT count(*) FROM queues WHERE name = 'readdb-write-probe'`).Scan(&n))
	require.Zero(t, n)
}

// TestReaddb_OpenIsSafeForConcurrentUse proves the handle can be shared by the
// goroutines of a run. The benchmark's watchdog, its fault injector and its main
// flow all read through the one handle the stack opens; a single pgx.Conn is not
// safe for that, and the failure is not a clean error: it is "conn busy" on one
// goroutine and, in pgx's statement cache, "fatal error: concurrent map writes"
// that kills the whole process. Run under -race, as `make test-race` does.
func TestReaddb_OpenIsSafeForConcurrentUse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	handle, err := readdb.Open(ctx, dsn())
	require.NoError(t, err)
	defer handle.Close()

	const goroutines, perGoroutine = 16, 40
	errs := make(chan error, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				if _, err := readdb.ClockNow(ctx, handle); err != nil {
					errs <- err
				}
				if _, err := readdb.StatusCounts(ctx, handle, "readdb-concurrency-scope"); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err, "a read through the shared handle failed")
	}
}
