package config

import (
	"math"
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

		"empty scheduler addr":    func(c *Config) { c.SchedulerAddr = "" },
		"public scheduler bind":   func(c *Config) { c.SchedulerAddr = "0.0.0.0:8084" },
		"zero scheduler poll":     func(c *Config) { c.SchedulerPollInterval = 0 },
		"zero scheduler batch":    func(c *Config) { c.SchedulerBatchSize = 0 },
		"huge scheduler batch":    func(c *Config) { c.SchedulerBatchSize = 1001 },
		"zero job retry base":     func(c *Config) { c.JobRetryBase = 0 },
		"retry max below base":    func(c *Config) { c.JobRetryMax = time.Millisecond },
		"retry multiplier below1": func(c *Config) { c.JobRetryMultiplier = 0.5 },
		"retry jitter above one":  func(c *Config) { c.JobRetryJitter = 1.5 },
		"negative retry jitter":   func(c *Config) { c.JobRetryJitter = -0.1 },

		// Re-notification repairs a notification that was genuinely lost. At or
		// near one polling cadence it would instead re-notify work whose first
		// notification is simply still on its way.
		"renotify at one poll interval": func(c *Config) {
			c.SchedulerPollInterval = 30 * time.Second
			c.SchedulerRenotifyAfter = 30 * time.Second
		},
		// A claimed-but-unpublished event is invisible to the scheduler's
		// pending-event check for exactly the claim timeout, so a shorter
		// interval would call an in-flight publish a lost notification.
		"renotify below the outbox claim timeout": func(c *Config) {
			c.OutboxClaimTimeout = 10 * time.Minute
			c.SchedulerRenotifyAfter = time.Minute
		},
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

// TestRetryPolicy_MirrorsTheValidatedSettings keeps the one place that turns
// configuration into policy honest. If these ever drift, two processes reading
// the same environment would enforce different retry cadences.
func TestRetryPolicy_MirrorsTheValidatedSettings(t *testing.T) {
	c := baseConfig()
	c.JobRetryBase = 2 * time.Second
	c.JobRetryMax = 90 * time.Second
	c.JobRetryMultiplier = 3
	c.JobRetryJitter = 0.15
	require.NoError(t, c.Validate())

	policy := c.RetryPolicy()
	require.NoError(t, policy.Validate())
	require.Equal(t, c.JobRetryBase, policy.Base)
	require.Equal(t, c.JobRetryMax, policy.Max)
	require.Equal(t, c.JobRetryMultiplier, policy.Multiplier)
	require.Equal(t, c.JobRetryJitter, policy.Jitter)

	// The defaults must themselves be a usable policy, not merely individually
	// in range.
	require.NoError(t, baseConfig().RetryPolicy().Validate())
}

// TestLoad_NonFiniteFloatEnvironmentValuesFallBackToTheDefault covers the first
// of two independent defences.
//
// strconv.ParseFloat accepts "NaN", "Inf", "+Inf", "-Inf", and "Infinity"
// without error, so a templating accident or a typo puts a non-finite float
// into the process rather than failing loudly. A NaN then compares false
// against every bound, so a range check alone lets it straight through into
// retry arithmetic. Parsing keeps the documented default instead.
func TestLoad_NonFiniteFloatEnvironmentValuesFallBackToTheDefault(t *testing.T) {
	for _, raw := range []string{"NaN", "nan", "Inf", "+Inf", "-Inf", "Infinity", "-Infinity"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("TASKFORGE_JOB_RETRY_MULTIPLIER", raw)
			t.Setenv("TASKFORGE_JOB_RETRY_JITTER", raw)
			t.Setenv("TASKFORGE_OUTBOX_BACKOFF_MULTIPLIER", raw)
			t.Setenv("TASKFORGE_OUTBOX_BACKOFF_JITTER", raw)

			c, err := Load()
			require.NoError(t, err, "a non-finite value must not become a usable setting")
			require.InDelta(t, 2.0, c.JobRetryMultiplier, 1e-9)
			require.InDelta(t, 0.2, c.JobRetryJitter, 1e-9)
			require.InDelta(t, 2.0, c.OutboxBackoffMultiplier, 1e-9)
			require.InDelta(t, 0.2, c.OutboxBackoffJitter, 1e-9)
			require.NoError(t, c.RetryPolicy().Validate())
		})
	}
}

// TestValidate_RejectsNonFiniteFloats covers the second defence, for a Config
// built in code rather than parsed from the environment — which is how every
// test, and every future embedding of this package, constructs one.
func TestValidate_RejectsNonFiniteFloats(t *testing.T) {
	nan := math.NaN()
	posInf := math.Inf(1)
	negInf := math.Inf(-1)

	for name, mutate := range map[string]func(*Config){
		"NaN job retry multiplier":  func(c *Config) { c.JobRetryMultiplier = nan },
		"+Inf job retry multiplier": func(c *Config) { c.JobRetryMultiplier = posInf },
		"-Inf job retry multiplier": func(c *Config) { c.JobRetryMultiplier = negInf },
		"NaN job retry jitter":      func(c *Config) { c.JobRetryJitter = nan },
		"+Inf job retry jitter":     func(c *Config) { c.JobRetryJitter = posInf },
		"-Inf job retry jitter":     func(c *Config) { c.JobRetryJitter = negInf },
		"NaN outbox multiplier":     func(c *Config) { c.OutboxBackoffMultiplier = nan },
		"+Inf outbox multiplier":    func(c *Config) { c.OutboxBackoffMultiplier = posInf },
		"-Inf outbox multiplier":    func(c *Config) { c.OutboxBackoffMultiplier = negInf },
		"NaN outbox jitter":         func(c *Config) { c.OutboxBackoffJitter = nan },
		"+Inf outbox jitter":        func(c *Config) { c.OutboxBackoffJitter = posInf },
		"-Inf outbox jitter":        func(c *Config) { c.OutboxBackoffJitter = negInf },
	} {
		t.Run(name, func(t *testing.T) {
			c := baseConfig()
			mutate(&c)
			err := c.Validate()
			require.Error(t, err, "a non-finite float must never validate")
			require.Contains(t, err.Error(), "finite",
				"it must be rejected as non-finite rather than as an out-of-range value")
		})
	}
}

// TestRetryPolicy_CannotCarryANonFiniteFloatPastValidation is the join between
// the two: a configuration that validates must produce a policy that validates,
// so no path exists from the environment to non-finite retry arithmetic.
func TestRetryPolicy_CannotCarryANonFiniteFloatPastValidation(t *testing.T) {
	c := baseConfig()
	c.JobRetryMultiplier = math.NaN()
	require.Error(t, c.Validate())
	require.Error(t, c.RetryPolicy().Validate(),
		"the policy must reject what the configuration rejected")
}
