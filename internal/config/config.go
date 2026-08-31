// Package config loads process configuration from the environment.
//
// Every value has an explicit default, an explicit validation rule, or both.
// Nothing reads a magic constant from the middle of the codebase.
package config

import (
	"bufio"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config is the shared configuration surface for the API, outbox, migration,
// and broker-dependent processes.
type Config struct {
	DatabaseURL string

	APIAddr           string
	MaxRequestBytes   int64
	APIRequestTimeout time.Duration
	LeaseDuration     time.Duration

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
		DatabaseURL:       env("TASKFORGE_DATABASE_URL", "postgres://taskforge:taskforge@127.0.0.1:5442/taskforge?sslmode=disable"),
		APIAddr:           env("TASKFORGE_API_ADDR", "127.0.0.1:8080"),
		MaxRequestBytes:   envInt64("TASKFORGE_MAX_REQUEST_BYTES", 256*1024),
		APIRequestTimeout: envDuration("TASKFORGE_API_REQUEST_TIMEOUT", 25*time.Second),
		LeaseDuration:     envDuration("TASKFORGE_LEASE_DURATION", 30*time.Second),
		DevScope:          env("TASKFORGE_DEV_SCOPE", "local-dev"),

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
	if strings.TrimSpace(c.APIAddr) != "" && !isLoopbackBind(c.APIAddr) {
		problems = append(problems, "TASKFORGE_API_ADDR must bind to a loopback address until authentication is implemented")
	}
	if strings.TrimSpace(c.OutboxAddr) != "" && !isLoopbackBind(c.OutboxAddr) {
		problems = append(problems, "TASKFORGE_OUTBOX_ADDR must bind to a loopback address until authentication is implemented")
	}

	if c.MaxRequestBytes < 1024 {
		problems = append(problems, "TASKFORGE_MAX_REQUEST_BYTES must be at least 1024")
	}
	if c.APIRequestTimeout < 100*time.Millisecond || c.APIRequestTimeout > 5*time.Minute {
		problems = append(problems, "TASKFORGE_API_REQUEST_TIMEOUT must be between 100ms and 5m")
	}
	if c.LeaseDuration < time.Second || c.LeaseDuration > 24*time.Hour {
		problems = append(problems, "TASKFORGE_LEASE_DURATION must be between 1s and 24h")
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

// WorkerConfig contains settings used only by taskforge-worker. Keeping these
// separate means API and outbox startup cannot fail because of an irrelevant
// worker-only environment value.
type WorkerConfig struct {
	APIBaseURL      string
	HealthAddr      string
	Name            string
	Hostname        string
	WorkerGroup     string
	Queue           string
	Concurrency     int
	Capabilities    []string
	PollWait        time.Duration
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
}

// LoadWorker reads and validates worker-process settings.
func LoadWorker() (WorkerConfig, error) {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "localhost"
	}
	c := WorkerConfig{
		APIBaseURL:      env("TASKFORGE_WORKER_API_URL", "http://127.0.0.1:8080"),
		HealthAddr:      env("TASKFORGE_WORKER_ADDR", "127.0.0.1:8082"),
		Name:            env("TASKFORGE_WORKER_NAME", "local-worker"),
		Hostname:        env("TASKFORGE_WORKER_HOSTNAME", hostname),
		WorkerGroup:     env("TASKFORGE_WORKER_GROUP", "default"),
		Queue:           env("TASKFORGE_WORKER_QUEUE", "default"),
		Concurrency:     envInt("TASKFORGE_WORKER_CONCURRENCY", 4),
		Capabilities:    envSet("TASKFORGE_WORKER_CAPABILITIES", "cpu"),
		PollWait:        envDuration("TASKFORGE_WORKER_POLL_WAIT", 10*time.Second),
		RequestTimeout:  envDuration("TASKFORGE_WORKER_REQUEST_TIMEOUT", 10*time.Second),
		ShutdownTimeout: envDuration("TASKFORGE_WORKER_SHUTDOWN_TIMEOUT", 15*time.Second),
	}
	return c, c.Validate()
}

var workerRoutingPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// Validate reports every invalid worker setting at once.
func (c WorkerConfig) Validate() error {
	var problems []string
	parsed, err := url.Parse(c.APIBaseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		problems = append(problems, "TASKFORGE_WORKER_API_URL must be an absolute http(s) URL")
	} else if !isLoopbackHost(parsed.Hostname()) {
		problems = append(problems, "TASKFORGE_WORKER_API_URL must use a loopback host until authentication is implemented")
	}
	if strings.TrimSpace(c.HealthAddr) == "" {
		problems = append(problems, "TASKFORGE_WORKER_ADDR must not be empty")
	} else if !isLoopbackBind(c.HealthAddr) {
		problems = append(problems, "TASKFORGE_WORKER_ADDR must bind to a loopback address until authentication is implemented")
	}
	if !workerRoutingPattern.MatchString(c.Name) {
		problems = append(problems, "TASKFORGE_WORKER_NAME must be a valid worker name")
	}
	if len(strings.TrimSpace(c.Hostname)) < 1 || len(c.Hostname) > 255 {
		problems = append(problems, "TASKFORGE_WORKER_HOSTNAME must contain between 1 and 255 characters")
	}
	for name, value := range map[string]string{
		"TASKFORGE_WORKER_GROUP": c.WorkerGroup,
		"TASKFORGE_WORKER_QUEUE": c.Queue,
	} {
		if len(value) > 64 || !workerRoutingPattern.MatchString(value) {
			problems = append(problems, name+" must be a valid routing name")
		}
	}
	if c.Concurrency < 1 || c.Concurrency > 256 {
		problems = append(problems, "TASKFORGE_WORKER_CONCURRENCY must be between 1 and 256")
	}
	if len(c.Capabilities) > 64 {
		problems = append(problems, "TASKFORGE_WORKER_CAPABILITIES must contain at most 64 values")
	}
	for _, capability := range c.Capabilities {
		if len(capability) > 64 || !workerRoutingPattern.MatchString(capability) {
			problems = append(problems, "TASKFORGE_WORKER_CAPABILITIES contains an invalid value")
			break
		}
	}
	// The SQS ReceiveMessage request carries WaitTimeSeconds as whole seconds, so
	// anything under 1s truncates to 0 and silently disables long polling, turning
	// every idle slot into a busy receive loop. 20s is the SQS maximum.
	if c.PollWait < time.Second || c.PollWait > 20*time.Second {
		problems = append(problems, "TASKFORGE_WORKER_POLL_WAIT must be between 1s and 20s; the broker request uses whole seconds")
	}
	if c.RequestTimeout <= 0 {
		problems = append(problems, "TASKFORGE_WORKER_REQUEST_TIMEOUT must be positive")
	}
	if c.ShutdownTimeout <= 0 {
		problems = append(problems, "TASKFORGE_WORKER_SHUTDOWN_TIMEOUT must be positive")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid worker configuration: %s", strings.Join(problems, "; "))
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

func envSet(key, def string) []string {
	raw := env(key, def)
	seen := map[string]struct{}{}
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func isLoopbackBind(address string) bool {
	host, _, err := net.SplitHostPort(address)
	return err == nil && isLoopbackHost(host)
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
