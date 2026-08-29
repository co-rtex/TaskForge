package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadMigrations_AreOrderedAndWellFormed(t *testing.T) {
	got, err := LoadMigrations()
	require.NoError(t, err)
	require.NotEmpty(t, got, "migrations must be embedded; check the //go:embed directive")

	seen := map[int]bool{}
	for i, m := range got {
		require.Positive(t, m.Version)
		require.NotEmpty(t, m.SQL)
		require.Len(t, m.Checksum, 64, "checksum must be a hex sha256")
		require.False(t, seen[m.Version], "duplicate migration version %d", m.Version)
		seen[m.Version] = true
		if i > 0 {
			require.Greater(t, m.Version, got[i-1].Version, "migrations must be sorted by version")
		}
	}
}

// Checksums are what let the runner refuse an edited migration. If loading were
// not deterministic, that protection would produce false alarms instead.
func TestLoadMigrations_ChecksumsAreStable(t *testing.T) {
	first, err := LoadMigrations()
	require.NoError(t, err)
	second, err := LoadMigrations()
	require.NoError(t, err)
	require.Equal(t, first, second)
}
