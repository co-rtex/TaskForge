package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/co-rtex/TaskForge/internal/config"
)

// timings is every duration the demo hands the services. Both profiles are
// checked against internal/config's own validation by a test, so a value here
// that the services would refuse fails `make test-unit` rather than a demo.
type timings struct {
	lease, heartbeat, stale, renew time.Duration
	workerRequest                  time.Duration

	// Shared by both profiles.
	pollInterval  time.Duration // reconciler and scheduler
	outboxPoll    time.Duration
	outboxClaim   time.Duration
	renotifyAfter time.Duration
	workerPollWait,
	workerShutdown,
	apiRequest time.Duration

	retryBase, retryMax time.Duration
	retryMultiplier     float64
	retryJitter         float64
}

// commonTimings are the settings both demonstrations share. They exist to make a
// run fast, not to change what it shows, and each satisfies the relationship
// internal/config validates for it.
func commonTimings() timings {
	return timings{
		// Fast scans, so a promoted retry or a recovered job is seen within a
		// fraction of a second instead of a poll interval of seconds.
		pollInterval: 250 * time.Millisecond,
		outboxPoll:   200 * time.Millisecond,
		// At least the outbox poll interval, which config requires.
		outboxClaim: time.Second,
		// At least 3x the scheduler poll interval (750ms) and at least the outbox
		// claim timeout (1s): both are config rules. It is only a repair for a
		// lost notification, so it should rarely matter in a demonstration.
		renotifyAfter: 5 * time.Second,
		// The minimum config allows: the broker request carries whole seconds.
		workerPollWait: time.Second,
		workerShutdown: 2 * time.Second,
		apiRequest:     5 * time.Second,

		// Retry settings, so two retries take about a second and a half rather
		// than minutes. The multiplier and jitter are the defaults; only the base
		// and the cap are shortened (cap >= base, whole milliseconds).
		retryBase:       500 * time.Millisecond,
		retryMax:        5 * time.Second,
		retryMultiplier: 2.0,
		retryJitter:     0.2,
	}
}

// successTimings keep the shipped liveness and lease settings. Nothing in the
// success demonstration depends on a worker dying, so there is no reason to run
// it with a lease short enough to be sensitive to a busy machine.
func successTimings() timings {
	t := commonTimings()
	t.lease = 30 * time.Second
	t.heartbeat = 5 * time.Second
	t.stale = 15 * time.Second        // >= 3x heartbeat
	t.renew = 10 * time.Second        // 3x renew <= lease
	t.workerRequest = 5 * time.Second // <= stale and <= renew
	return t
}

// failureTimings are short, so a killed worker's lease lapses and a frozen
// worker's session goes stale within seconds. Each value is as short as its
// relationship allows:
//
//	heartbeat      1s     the interval a worker proves it is alive
//	stale          3s     3x heartbeat: the minimum config accepts, so one lost
//	                      heartbeat cannot look like a dead process
//	lease          5s     the window an attempt owns without renewing
//	renew          1.5s   3x renew = 4.5s <= lease, so two renewals can fail and
//	                      a third still land inside the lease
//	workerRequest  1s     <= stale and <= renew, so one hung control call cannot
//	                      consume a whole window
func failureTimings() timings {
	t := commonTimings()
	t.lease = 5 * time.Second
	t.heartbeat = time.Second
	t.stale = 3 * time.Second
	t.renew = 1500 * time.Millisecond
	t.workerRequest = time.Second
	return t
}

// dialed lists, for each binary, the environment variables that carry the
// address of something it connects to. It was read off cmd/<binary>/main.go:
// which of the database URL, the broker endpoint, the object store endpoint and
// the worker's API URL that binary actually uses. A binary started without one
// of them would silently take its loopback default, which is wrong the moment
// the infrastructure is not on this machine's own loopback.
var dialed = map[string][]string{
	"taskforge-api":        {"TASKFORGE_DATABASE_URL", "TASKFORGE_RESULTS_ENDPOINT"},
	"taskforge-outbox":     {"TASKFORGE_DATABASE_URL", "TASKFORGE_BROKER_ENDPOINT"},
	"taskforge-reconciler": {"TASKFORGE_DATABASE_URL"},
	"taskforge-scheduler":  {"TASKFORGE_DATABASE_URL"},
	"taskforge-worker": {
		"TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_RESULTS_ENDPOINT", "TASKFORGE_WORKER_API_URL",
	},
}

// infra is where this run's infrastructure is, read from the same variables the
// services read.
type infra struct {
	databaseURL string

	brokerEndpoint, brokerRegion, brokerAccessKeyID, brokerSecretAccessKey string

	resultsEndpoint, resultsBucket, resultsRegion,
	resultsAccessKeyID, resultsSecretAccessKey string
}

// loadInfra resolves the infrastructure with the services' own loader, so the
// variable names, the .env handling and the compose-default fallbacks are not
// restated here. `make demo` depends on `make migrate`, which reads the same
// .env, so both land on the same database.
func loadInfra() (infra, error) {
	if err := config.LoadDotEnv(".env"); err != nil {
		return infra{}, fmt.Errorf("read .env: %w", err)
	}
	cfg, err := config.Load()
	if err != nil {
		return infra{}, fmt.Errorf("the TASKFORGE_* configuration is invalid: %w", err)
	}
	return infra{
		databaseURL:    cfg.DatabaseURL,
		brokerEndpoint: cfg.BrokerEndpoint, brokerRegion: cfg.BrokerRegion,
		brokerAccessKeyID: cfg.BrokerAccessKeyID, brokerSecretAccessKey: cfg.BrokerSecretAccessKey,
		resultsEndpoint: cfg.ResultsEndpoint, resultsBucket: cfg.ResultsBucket,
		resultsRegion: cfg.ResultsRegion, resultsAccessKeyID: cfg.ResultsAccessKeyID,
		resultsSecretAccessKey: cfg.ResultsSecretAccessKey,
	}, nil
}

// demo is one run: the processes it started, the credentials it created, and
// what it has concluded so far.
type demo struct {
	out     io.Writer
	mode    string
	started time.Time
	infra   infra
	timing  timings
	rep     *report

	// runID is unique to the run, and everything the run creates carries it: the
	// scope, the worker names and the broker queue. Nothing else in the database
	// can match them, which is what lets the demo assert on its own jobs only.
	runID string
	scope string

	binDir  string
	logDir  string
	workDir string // an empty directory: the working directory of every child

	queueName, queueURL string

	apiAddr, outboxAddr, schedulerAddr, reconcilerAddr string
	apiURL                                             string

	cli                   *cliClient
	apiKey, workerKey     string
	apiKeyID, workerKeyID string
	db                    *pgx.Conn

	mu      sync.Mutex
	procs   []*proc
	cleaned sync.Once
}

// binaries the demo runs. They must already be built into ./bin by `make build`.
var requiredBinaries = []string{
	"taskforge-api", "taskforge-outbox", "taskforge-scheduler",
	"taskforge-reconciler", "taskforge-worker", "taskforge-cli",
}

func newDemo(out io.Writer, mode string) (*demo, error) {
	in, err := loadInfra()
	if err != nil {
		return nil, err
	}
	binDir, err := filepath.Abs("bin")
	if err != nil {
		return nil, err
	}
	for _, name := range requiredBinaries {
		info, err := os.Stat(filepath.Join(binDir, name))
		if err != nil || info.IsDir() {
			return nil, fmt.Errorf("%s is missing: run `make build` from the repository root first", filepath.Join("bin", name))
		}
	}

	logDir, err := os.MkdirTemp("", "taskforge-demo-")
	if err != nil {
		return nil, fmt.Errorf("create the log directory: %w", err)
	}
	workDir := filepath.Join(logDir, "cwd")
	if err := os.Mkdir(workDir, 0o700); err != nil {
		return nil, fmt.Errorf("create the child working directory: %w", err)
	}

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	d := &demo{
		out: out, mode: mode, started: time.Now(), infra: in, rep: &report{},
		runID: runID, scope: "demo-" + runID,
		binDir: binDir, logDir: logDir, workDir: workDir,
		// A queue of this run's own. A shared queue would let any worker already
		// running against it receive this run's notifications, and would let a
		// message a killed worker was holding in flight surface in a later run.
		queueName: "taskforge-demo-" + runID,
	}
	if mode == modeFailure {
		d.timing = failureTimings()
	} else {
		d.timing = successTimings()
	}

	// Free ports, chosen now so every process's configuration can name all of
	// them. Each binds its own a moment later.
	var ports [4]string
	for i := range ports {
		if ports[i], err = freeLoopbackAddr(); err != nil {
			return nil, err
		}
	}
	d.apiAddr, d.outboxAddr, d.schedulerAddr, d.reconcilerAddr = ports[0], ports[1], ports[2], ports[3]
	d.apiURL = "http://" + d.apiAddr
	d.cli = &cliClient{bin: filepath.Join(binDir, "taskforge-cli"), apiURL: d.apiURL, dir: workDir}
	return d, nil
}

// freeLoopbackAddr asks the kernel for an unused loopback port and releases it.
// The process that is told to use it binds it moments later.
func freeLoopbackAddr() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("find a free loopback port: %w", err)
	}
	defer listener.Close()
	return listener.Addr().String(), nil
}

// say narrates one step with the time since the run began.
func (d *demo) say(format string, args ...any) {
	fmt.Fprintf(d.out, "[%6.1fs] %s\n", time.Since(d.started).Seconds(), fmt.Sprintf(format, args...))
}

// baseEnv is every setting each binary is started with. Children do not inherit
// the developer's environment: whatever is not named here is the binary's own
// default, and for an address that default is this machine's loopback.
func (d *demo) baseEnv() map[string]string {
	t := d.timing
	return map[string]string{
		"TASKFORGE_DATABASE_URL": d.infra.databaseURL,

		"TASKFORGE_BROKER_ENDPOINT":          d.infra.brokerEndpoint,
		"TASKFORGE_BROKER_QUEUE_NAME":        d.queueName,
		"TASKFORGE_BROKER_REGION":            d.infra.brokerRegion,
		"TASKFORGE_BROKER_ACCESS_KEY_ID":     d.infra.brokerAccessKeyID,
		"TASKFORGE_BROKER_SECRET_ACCESS_KEY": d.infra.brokerSecretAccessKey,

		// The API serves results from the object store and the worker uploads to
		// it and probes it for readiness, each through its own client.
		"TASKFORGE_RESULTS_ENDPOINT":          d.infra.resultsEndpoint,
		"TASKFORGE_RESULTS_BUCKET":            d.infra.resultsBucket,
		"TASKFORGE_RESULTS_REGION":            d.infra.resultsRegion,
		"TASKFORGE_RESULTS_ACCESS_KEY_ID":     d.infra.resultsAccessKeyID,
		"TASKFORGE_RESULTS_SECRET_ACCESS_KEY": d.infra.resultsSecretAccessKey,

		"TASKFORGE_API_ADDR":        d.apiAddr,
		"TASKFORGE_OUTBOX_ADDR":     d.outboxAddr,
		"TASKFORGE_SCHEDULER_ADDR":  d.schedulerAddr,
		"TASKFORGE_RECONCILER_ADDR": d.reconcilerAddr,

		"TASKFORGE_API_REQUEST_TIMEOUT":  t.apiRequest.String(),
		"TASKFORGE_LEASE_DURATION":       t.lease.String(),
		"TASKFORGE_HEARTBEAT_INTERVAL":   t.heartbeat.String(),
		"TASKFORGE_SESSION_STALE_AFTER":  t.stale.String(),
		"TASKFORGE_LEASE_RENEW_INTERVAL": t.renew.String(),

		"TASKFORGE_OUTBOX_POLL_INTERVAL":     t.outboxPoll.String(),
		"TASKFORGE_OUTBOX_CLAIM_TIMEOUT":     t.outboxClaim.String(),
		"TASKFORGE_RECONCILER_POLL_INTERVAL": t.pollInterval.String(),
		"TASKFORGE_SCHEDULER_POLL_INTERVAL":  t.pollInterval.String(),
		"TASKFORGE_SCHEDULER_RENOTIFY_AFTER": t.renotifyAfter.String(),

		"TASKFORGE_JOB_RETRY_BASE":       t.retryBase.String(),
		"TASKFORGE_JOB_RETRY_MAX":        t.retryMax.String(),
		"TASKFORGE_JOB_RETRY_MULTIPLIER": strconv.FormatFloat(t.retryMultiplier, 'f', -1, 64),
		"TASKFORGE_JOB_RETRY_JITTER":     strconv.FormatFloat(t.retryJitter, 'f', -1, 64),

		"TASKFORGE_LOG_LEVEL": "info",
	}
}

// env builds one binary's complete environment and refuses to return one that
// leaves an endpoint that binary dials unset. The check is on the very map that
// is handed to the operating system.
func (d *demo) env(binary string, extra map[string]string) (map[string]string, error) {
	env := d.baseEnv()
	for key, value := range extra {
		env[key] = value
	}
	endpoints, known := dialed[binary]
	if !known {
		return nil, fmt.Errorf("%s is not in the dialed table: read cmd/%s/main.go and list the endpoints it connects to", binary, binary)
	}
	for _, key := range endpoints {
		if strings.TrimSpace(env[key]) == "" {
			return nil, fmt.Errorf("%s would take %s from its loopback default", binary, key)
		}
	}
	return env, nil
}

// workerEnv is what a worker adds to the shared environment.
func (d *demo) workerEnv(name, addr string, concurrency int) map[string]string {
	t := d.timing
	return map[string]string{
		"TASKFORGE_WORKER_NAME":             name,
		"TASKFORGE_WORKER_ADDR":             addr,
		"TASKFORGE_WORKER_API_URL":          d.apiURL,
		"TASKFORGE_WORKER_API_KEY":          d.workerKey,
		"TASKFORGE_WORKER_QUEUE":            "default",
		"TASKFORGE_WORKER_GROUP":            "default",
		"TASKFORGE_WORKER_CONCURRENCY":      strconv.Itoa(concurrency),
		"TASKFORGE_WORKER_CAPABILITIES":     "cpu",
		"TASKFORGE_WORKER_POLL_WAIT":        t.workerPollWait.String(),
		"TASKFORGE_WORKER_REQUEST_TIMEOUT":  t.workerRequest.String(),
		"TASKFORGE_WORKER_SHUTDOWN_TIMEOUT": t.workerShutdown.String(),
	}
}

// sortedEnv renders an environment as the KEY=VALUE slice exec wants, in a
// stable order.
func sortedEnv(env map[string]string) []string {
	out := make([]string, 0, len(env))
	for key, value := range env {
		out = append(out, key+"="+value)
	}
	sort.Strings(out)
	return out
}

// setup creates what the run needs: its own broker queue, a database
// connection for the few facts the public API deliberately does not expose, the
// four background services, and a pair of keys in this run's scope.
func (d *demo) setup(ctx context.Context) error {
	var err error
	if d.queueURL, err = createQueue(ctx, d.infra.brokerEndpoint, d.queueName); err != nil {
		return fmt.Errorf("create this run's broker queue: %w", err)
	}
	d.say("Created broker queue %s for this run alone.", d.queueName)

	if d.db, err = pgx.Connect(ctx, d.infra.databaseURL); err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}

	d.say("Starting api, outbox, scheduler and reconciler on free loopback ports.")
	var services []*proc
	for _, svc := range []struct{ label, binary, addr string }{
		{"api", "taskforge-api", d.apiAddr},
		{"outbox", "taskforge-outbox", d.outboxAddr},
		{"scheduler", "taskforge-scheduler", d.schedulerAddr},
		{"reconciler", "taskforge-reconciler", d.reconcilerAddr},
	} {
		env, err := d.env(svc.binary, nil)
		if err != nil {
			return err
		}
		p, err := d.start(svc.label, svc.binary, svc.addr, kindService, env)
		if err != nil {
			return err
		}
		services = append(services, p)
	}
	for _, p := range services {
		if err := p.waitReady(ctx, 60*time.Second); err != nil {
			return err
		}
	}
	d.say("All four services report ready.")

	return d.createKeys(ctx)
}

// createKeys mints this run's credentials with taskforge-cli. The scope is
// unique to the run, so nothing another run or a developer created can be
// visible to, or claimed by, anything started here.
func (d *demo) createKeys(ctx context.Context) error {
	var apiKey, workerKey struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	}
	if err := d.cli.json(ctx, "", &apiKey, "api-keys", "create", "--scope", d.scope, "--name", "demo-"+d.runID); err != nil {
		return fmt.Errorf("create the run's API key: %w", err)
	}
	if err := d.cli.json(ctx, "", &workerKey, "worker-keys", "create", "--scope", d.scope, "--name", "demo-worker-"+d.runID); err != nil {
		return fmt.Errorf("create the run's worker key: %w", err)
	}
	if apiKey.Key == "" || workerKey.Key == "" {
		return fmt.Errorf("a key was created but its credential was not returned")
	}
	d.apiKey, d.apiKeyID = apiKey.Key, apiKey.ID
	d.workerKey, d.workerKeyID = workerKey.Key, workerKey.ID
	d.say("Created an API key and a worker key in scope %q.", d.scope)
	return nil
}

// cleanup stops everything the run started, in an order that lets it say what it
// is doing: workers first, then the run's keys are revoked while the API is
// still up, then the services, then the queue. It is idempotent.
//
// It deletes only what this run created: its own processes, its own queue, and
// (revoked, not deleted) its own two keys.
func (d *demo) cleanup() {
	d.cleaned.Do(func() {
		d.stopProcs(func(p *proc) bool { return p.kind == kindWorker })
		d.revokeKeys()
		d.stopProcs(func(p *proc) bool { return p.kind == kindService })
		d.mu.Lock()
		stillRunning := 0
		for _, p := range d.procs {
			if p.running() {
				stillRunning++
			}
		}
		started := len(d.procs)
		d.mu.Unlock()
		if started > 0 {
			d.say("Stopped the %d processes this run started; %d still running.", started, stillRunning)
		}
		if d.db != nil {
			_ = d.db.Close(context.Background())
		}
		if d.queueURL != "" {
			if err := deleteQueue(context.Background(), d.infra.brokerEndpoint, d.queueURL); err != nil {
				d.say("Could not delete broker queue %s: %v", d.queueName, err)
			} else {
				d.say("Deleted broker queue %s, which only this run used.", d.queueName)
			}
		}
	})
}

// revokeKeys revokes the credentials this run created. Best effort: a failure
// here is reported but never changes the result, because by now the
// demonstration is over.
func (d *demo) revokeKeys() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	revoked, failed := 0, 0
	for _, key := range []struct{ resource, id string }{
		{"api-keys", d.apiKeyID}, {"worker-keys", d.workerKeyID},
	} {
		if key.id == "" {
			continue
		}
		if _, err := d.cli.exec(ctx, "", key.resource, "revoke", key.id); err != nil {
			failed++
			d.say("Could not revoke one of the run's %s: %v", key.resource, err)
			continue
		}
		revoked++
	}
	if revoked > 0 && failed == 0 {
		d.say("Revoked this run's %d keys (revoked, not deleted).", revoked)
	}
}
