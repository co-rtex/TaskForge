package workers

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClassifyDatabaseError_TranslatesOnlyARealDeadline pins the single
// translation point between a failed database call and the typed deadline
// sentinel. It reads the returned error and nothing else, so a failure that
// merely coincides with an elapsed deadline keeps its own identity.
func TestClassifyDatabaseError_TranslatesOnlyARealDeadline(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		require.NoError(t, classifyDatabaseError(nil))
	})

	t.Run("a wrapped driver deadline becomes the sentinel", func(t *testing.T) {
		// pgx wraps context.DeadlineExceeded when it aborts a query on an
		// expiring deadline; the store wraps that again with call context.
		driverErr := fmt.Errorf("timeout: %w", context.DeadlineExceeded)
		got := classifyDatabaseError(fmt.Errorf("lock queue capacity: %w", driverErr))
		require.ErrorIs(t, got, ErrDeadlineExceeded)
		require.ErrorIs(t, got, context.DeadlineExceeded, "the original cause stays inspectable")
		require.Contains(t, got.Error(), "lock queue capacity")
	})

	t.Run("an already-classified error is not re-wrapped", func(t *testing.T) {
		already := fmt.Errorf("%w: original", ErrDeadlineExceeded)
		require.Equal(t, already, classifyDatabaseError(already))
	})

	t.Run("cancellation is never a deadline", func(t *testing.T) {
		got := classifyDatabaseError(fmt.Errorf("lock queue capacity: %w", context.Canceled))
		require.NotErrorIs(t, got, ErrDeadlineExceeded)
		require.ErrorIs(t, got, context.Canceled)
	})

	for name, err := range map[string]error{
		"driver constraint violation": errors.New("SQLSTATE 23505 unique violation"),
		"domain state conflict":       ErrStateConflict,
		"domain fence rejection":      ErrFenceRejected,
		"deadline-shaped text only":   errors.New("context deadline exceeded"),
	} {
		t.Run("unrelated error passes through unchanged: "+name, func(t *testing.T) {
			got := classifyDatabaseError(err)
			require.Equal(t, err, got, "an unrelated failure must not be rewritten")
			require.NotErrorIs(t, got, ErrDeadlineExceeded)
		})
	}
}
