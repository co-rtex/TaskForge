// Package config loads process configuration from the environment.
//
// Every value has an explicit default, an explicit validation rule, or both.
// Nothing reads a magic constant from the middle of the codebase.
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full configuration surface for the milestone-M1 services.
type Config struct {
	DatabaseURL string

	APIAddr         string
	MaxRequestBytes int64

	// DevScope is the single authentication scope every request is attributed
	// to until database-backed API keys land in milestone M5. It exists so the
	// idempotency and ownership model is already scoped correctly; it is not
	// authentication and must never be exposed off loopback.
	DevScope string

	BrokerEndpoint        string
	BrokerQueueName       string
	BrokerRegion          string
	BrokerAccessKeyID     string
	BrokerSecretAccessKey string

	OutboxAddr              string
	OutboxBatchSize         int
	OutboxPollInterval      time.Duration
	OutboxClaimTimeout      time.Duration
	OutboxBackoffBase       time.Duration
	OutboxBackoffMax        time.Duration
	OutboxBackoffMultiplier float64
	OutboxBackoffJitter     float64

	LogLevel string
}

// Load reads configuration from the environment, applying defaults and then
// validating. It returns every problem it finds rather than only the first, so
// a misconfigured deployment can be fixed in one pass.
func Load() (Config, error) {
	c := Config{
		DatabaseURL:     env("TASKFORGE_DATABASE_URL", "postgres://taskforge:taskforge@127.0.0.1:5442/taskforge?sslmode=disable"),
		APIAddr:         env("TASKFORGE_API_ADDR", "127.0.0.1:8080"),
		MaxRequestBytes: envInt64("TASKFORGE_MAX_REQUEST_BYTES", 256*1024),
		DevScope:        env("TASKFORGE_DEV_SCOPE", "local-dev"),

		BrokerEndpoint:        env("TASKFORGE_BROKER_ENDPOINT", "http://127.0.0.1:9324"),
		BrokerQueueName:       env("TASKFORGE_BROKER_QUEUE_NAME", "taskforge-work-available"),
		BrokerRegion:          env("TASKFORGE_BROKER_REGION", "us-east-1"),
		BrokerAccessKeyID:     env("TASKFORGE_BROKER_ACCESS_KEY_ID", "local"),
		BrokerSecretAccessKey: env("TASKFORGE_BROKER_SECRET_ACCESS_KEY", "local"),

		OutboxAddr:              env("TASKFORGE_OUTBOX_ADDR", "127.0.0.1:8081"),
		OutboxBatchSize:         envInt("TASKFORGE_OUTBOX_BATCH_SIZE", 50),
		OutboxPollInterval:      envDuration("TASKFORGE_OUTBOX_POLL_INTERVAL", time.Second),
		OutboxClaimTimeout:      envDuration("TASKFORGE_OUTBOX_CLAIM_TIMEOUT", 30*time.Second),
		OutboxBackoffBase:       envDuration("TASKFORGE_OUTBOX_BACKOFF_BASE", time.Second),
		OutboxBackoffMax:        envDuration("TASKFORGE_OUTBOX_BACKOFF_MAX", 5*time.Minute),
		OutboxBackoffMultiplier: envFloat("TASKFORGE_OUTBOX_BACKOFF_MULTIPLIER", 2.0),
		OutboxBackoffJitter:     envFloat("TASKFORGE_OUTBOX_BACKOFF_JITTER", 0.2),

		LogLevel: env("TASKFORGE_LOG_LEVEL", "info"),
	}
	return c, c.Validate()
}

// Validate reports every invalid setting at once.
func (c Config) Validate() error {
	var problems []string
	req := func(name, v string) {
		if strings.TrimSpace(v) == "" {
			problems = append(problems, name+" must not be empty")
		}
	}
	req("TASKFORGE_DATABASE_URL", c.DatabaseURL)
	req("TASKFORGE_API_ADDR", c.APIAddr)
	req("TASKFORGE_DEV_SCOPE", c.DevScope)
	req("TASKFORGE_BROKER_QUEUE_NAME", c.BrokerQueueName)
	req("TASKFORGE_BROKER_REGION", c.BrokerRegion)
	req("TASKFORGE_OUTBOX_ADDR", c.OutboxAddr)

	if c.MaxRequestBytes < 1024 {
		problems = append(problems, "TASKFORGE_MAX_REQUEST_BYTES must be at least 1024")
	}
	if c.OutboxBatchSize < 1 || c.OutboxBatchSize > 1000 {
		problems = append(problems, "TASKFORGE_OUTBOX_BATCH_SIZE must be between 1 and 1000")
	}
	if c.OutboxPollInterval <= 0 {
		problems = append(problems, "TASKFORGE_OUTBOX_POLL_INTERVAL must be positive")
	}
	// A claim timeout shorter than the poll interval would let a second
	// publisher reclaim an event while the first is still publishing it,
	// multiplying duplicates for no benefit.
	if c.OutboxClaimTimeout < c.OutboxPollInterval {
		problems = append(problems, "TASKFORGE_OUTBOX_CLAIM_TIMEOUT must be >= TASKFORGE_OUTBOX_POLL_INTERVAL")
	}
	if c.OutboxBackoffBase <= 0 {
		problems = append(problems, "TASKFORGE_OUTBOX_BACKOFF_BASE must be positive")
	}
	if c.OutboxBackoffMax < c.OutboxBackoffBase {
		problems = append(problems, "TASKFORGE_OUTBOX_BACKOFF_MAX must be >= TASKFORGE_OUTBOX_BACKOFF_BASE")
	}
	if c.OutboxBackoffMultiplier < 1 {
		problems = append(problems, "TASKFORGE_OUTBOX_BACKOFF_MULTIPLIER must be >= 1")
	}
	if c.OutboxBackoffJitter < 0 || c.OutboxBackoffJitter > 1 {
		problems = append(problems, "TASKFORGE_OUTBOX_BACKOFF_JITTER must be between 0 and 1")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
	}
	return nil
}

// LoadDotEnv reads KEY=VALUE lines from path into the environment without
// overriding variables that are already set, so a real environment always wins
// over a developer's local file. A missing file is not an error.
func LoadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, value); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(env(key, "")); err == nil {
		return v
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v, err := strconv.ParseInt(env(key, ""), 10, 64); err == nil {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(env(key, ""), 64); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(env(key, "")); err == nil {
		return v
	}
	return def
}
