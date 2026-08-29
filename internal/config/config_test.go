package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func baseConfig() Config {
	c, _ := Load()
	return c
}

func TestLoad_DefaultsAreValid(t *testing.T) {
	c, err := Load()
	require.NoError(t, err)
	require.NoError(t, c.Validate())
	require.Equal(t, "local-dev", c.DevScope)
	require.Positive(t, c.MaxRequestBytes)
}

func TestLoad_ReadsEnvironment(t *testing.T) {
	t.Setenv("TASKFORGE_API_ADDR", "127.0.0.1:9999")
	t.Setenv("TASKFORGE_OUTBOX_BATCH_SIZE", "7")
	t.Setenv("TASKFORGE_OUTBOX_POLL_INTERVAL", "250ms")
	t.Setenv("TASKFORGE_OUTBOX_BACKOFF_MULTIPLIER", "1.5")

	c, err := Load()
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9999", c.APIAddr)
	require.Equal(t, 7, c.OutboxBatchSize)
	require.Equal(t, 250*time.Millisecond, c.OutboxPollInterval)
	require.InDelta(t, 1.5, c.OutboxBackoffMultiplier, 1e-9)
}

// An unparseable value falls back to the default rather than crashing, so one
// typo cannot take a service down.
func TestLoad_MalformedValuesFallBackToDefaults(t *testing.T) {
	t.Setenv("TASKFORGE_OUTBOX_BATCH_SIZE", "not-a-number")
	t.Setenv("TASKFORGE_OUTBOX_POLL_INTERVAL", "banana")

	c, err := Load()
	require.NoError(t, err)
	require.Equal(t, 50, c.OutboxBatchSize)
	require.Equal(t, time.Second, c.OutboxPollInterval)
}

func TestValidate_RejectsBadConfiguration(t *testing.T) {
	tests := map[string]func(*Config){
		"empty database url": func(c *Config) { c.DatabaseURL = "" },
		"empty api addr":     func(c *Config) { c.APIAddr = "" },
		"empty dev scope":    func(c *Config) { c.DevScope = "" },
		"empty queue name":   func(c *Config) { c.BrokerQueueName = "" },
		"tiny body limit":    func(c *Config) { c.MaxRequestBytes = 10 },
		"zero batch size":    func(c *Config) { c.OutboxBatchSize = 0 },
		"huge batch size":    func(c *Config) { c.OutboxBatchSize = 1001 },
		"zero poll interval": func(c *Config) { c.OutboxPollInterval = 0 },
		// A claim timeout below the poll interval lets a second publisher reclaim
		// an event while the first is still publishing it.
		"claim timeout below poll interval": func(c *Config) {
			c.OutboxPollInterval = time.Minute
			c.OutboxClaimTimeout = time.Second
		},
		"max below base":       func(c *Config) { c.OutboxBackoffMax = time.Millisecond },
		"multiplier below one": func(c *Config) { c.OutboxBackoffMultiplier = 0.5 },
		"jitter above one":     func(c *Config) { c.OutboxBackoffJitter = 1.5 },
		"negative jitter":      func(c *Config) { c.OutboxBackoffJitter = -0.1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			c := baseConfig()
			mutate(&c)
			require.Error(t, c.Validate())
		})
	}
}

// Every problem is reported at once so a misconfigured deployment can be fixed
// in a single pass.
func TestValidate_ReportsEveryProblem(t *testing.T) {
	c := baseConfig()
	c.DatabaseURL = ""
	c.APIAddr = ""
	c.OutboxBatchSize = 0

	err := c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "TASKFORGE_DATABASE_URL")
	require.Contains(t, err.Error(), "TASKFORGE_API_ADDR")
	require.Contains(t, err.Error(), "TASKFORGE_OUTBOX_BATCH_SIZE")
}

func TestLoadDotEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	require.NoError(t, os.WriteFile(path, []byte(`
# a comment
TASKFORGE_TEST_DOTENV_A=from-file
TASKFORGE_TEST_DOTENV_QUOTED="quoted value"

TASKFORGE_TEST_DOTENV_PRESET=from-file
malformed-line-without-equals
`), 0o600))

	// A real environment variable must win over the file, so a deployment is
	// never overridden by a developer's leftover .env.
	t.Setenv("TASKFORGE_TEST_DOTENV_PRESET", "from-environment")

	require.NoError(t, LoadDotEnv(path))
	t.Cleanup(func() {
		os.Unsetenv("TASKFORGE_TEST_DOTENV_A")
		os.Unsetenv("TASKFORGE_TEST_DOTENV_QUOTED")
	})

	require.Equal(t, "from-file", os.Getenv("TASKFORGE_TEST_DOTENV_A"))
	require.Equal(t, "quoted value", os.Getenv("TASKFORGE_TEST_DOTENV_QUOTED"))
	require.Equal(t, "from-environment", os.Getenv("TASKFORGE_TEST_DOTENV_PRESET"))
}

func TestLoadDotEnv_MissingFileIsNotAnError(t *testing.T) {
	require.NoError(t, LoadDotEnv(filepath.Join(t.TempDir(), "absent")))
}
