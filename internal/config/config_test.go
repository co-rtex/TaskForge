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
	require.Equal(t, 25*time.Second, c.APIRequestTimeout)
	require.Equal(t, 30*time.Second, c.LeaseDuration)
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
		"tiny api timeout":   func(c *Config) { c.APIRequestTimeout = time.Millisecond },
		"tiny lease":         func(c *Config) { c.LeaseDuration = time.Millisecond },
		"public api bind":    func(c *Config) { c.APIAddr = "0.0.0.0:8080" },
		"public outbox bind": func(c *Config) { c.OutboxAddr = "[::]:8081" },
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

func TestLoadWorker_DefaultsAreValid(t *testing.T) {
	c, err := LoadWorker()
	require.NoError(t, err)
	require.NoError(t, c.Validate())
	require.Equal(t, "local-worker", c.Name)
	require.Equal(t, []string{"cpu"}, c.Capabilities)
}

func TestLoadWorker_CanonicalizesCapabilities(t *testing.T) {
	t.Setenv("TASKFORGE_WORKER_CAPABILITIES", "gpu,cpu,gpu")
	c, err := LoadWorker()
	require.NoError(t, err)
	require.Equal(t, []string{"cpu", "gpu"}, c.Capabilities)
}

func TestWorkerConfig_ReportsEveryProblem(t *testing.T) {
	c, err := LoadWorker()
	require.NoError(t, err)
	c.APIBaseURL = "not-a-url"
	c.Name = "Bad Name"
	c.Concurrency = 0
	c.PollWait = 0

	err = c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "TASKFORGE_WORKER_API_URL")
	require.Contains(t, err.Error(), "TASKFORGE_WORKER_NAME")
	require.Contains(t, err.Error(), "TASKFORGE_WORKER_CONCURRENCY")
	require.Contains(t, err.Error(), "TASKFORGE_WORKER_POLL_WAIT")
}

// The SQS ReceiveMessage request converts the poll wait to whole seconds, so a
// sub-second value truncates to 0, disables long polling, and turns every idle
// slot into a busy receive loop. The bound is inclusive at both ends.
func TestWorkerConfig_PollWaitMustBeWholeSecondsWithinTheBrokerBound(t *testing.T) {
	tests := map[string]struct {
		pollWait time.Duration
		valid    bool
	}{
		"999ms truncates to zero long-poll seconds": {999 * time.Millisecond, false},
		"1s is the inclusive lower bound":           {time.Second, true},
		"10s is the shipped default":                {10 * time.Second, true},
		"20s is the inclusive SQS maximum":          {20 * time.Second, true},
		"20s001ms exceeds the SQS maximum":          {20*time.Second + time.Millisecond, false},
		"30s exceeds the SQS maximum":               {30 * time.Second, false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			c, err := LoadWorker()
			require.NoError(t, err)
			c.PollWait = test.pollWait

			err = c.Validate()
			if test.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), "TASKFORGE_WORKER_POLL_WAIT must be between 1s and 20s")
		})
	}
}

func TestWorkerConfig_RejectsUnauthenticatedNonLoopbackEndpoints(t *testing.T) {
	c, err := LoadWorker()
	require.NoError(t, err)
	c.APIBaseURL = "http://192.0.2.10:8080"
	c.HealthAddr = "0.0.0.0:8082"

	err = c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "TASKFORGE_WORKER_API_URL")
	require.Contains(t, err.Error(), "TASKFORGE_WORKER_ADDR")
}

func TestLoopbackValidationAcceptsIPv4IPv6AndLocalhost(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080"} {
		require.True(t, isLoopbackBind(address), address)
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

// TestValidateWorkerTimings_RejectsATransportTimeoutThatOutlivesASafetyWindow
// covers the gap neither Validate can see on its own: the transport timeout is a
// worker setting, while the windows it must fit inside are server-owned.
func TestValidateWorkerTimings_RejectsATransportTimeoutThatOutlivesASafetyWindow(t *testing.T) {
	shared, err := Load()
	require.NoError(t, err)
	worker, err := LoadWorker()
	require.NoError(t, err)

	t.Run("the documented defaults fit together", func(t *testing.T) {
		require.NoError(t, ValidateWorkerTimings(shared, worker))
	})

	t.Run("a timeout longer than the staleness window is rejected", func(t *testing.T) {
		bad := worker
		bad.RequestTimeout = shared.SessionStaleAfter + time.Second
		err := ValidateWorkerTimings(shared, bad)
		require.Error(t, err)
		require.Contains(t, err.Error(), "TASKFORGE_SESSION_STALE_AFTER")
	})

	t.Run("a timeout longer than the renewal cadence is rejected", func(t *testing.T) {
		bad := worker
		bad.RequestTimeout = shared.LeaseRenewInterval + time.Millisecond
		err := ValidateWorkerTimings(shared, bad)
		require.Error(t, err)
		require.Contains(t, err.Error(), "TASKFORGE_LEASE_RENEW_INTERVAL")
	})

	t.Run("both problems are reported together", func(t *testing.T) {
		bad := worker
		bad.RequestTimeout = time.Hour
		err := ValidateWorkerTimings(shared, bad)
		require.Error(t, err)
		require.Contains(t, err.Error(), "TASKFORGE_SESSION_STALE_AFTER")
		require.Contains(t, err.Error(), "TASKFORGE_LEASE_RENEW_INTERVAL")
	})
}
