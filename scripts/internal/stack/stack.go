// Package stack runs TaskForge's real binaries from ./bin as one self-contained
// stack for a developer script: its own broker queue, its own key scope, the
// four background services on free loopback ports, and workers on demand. It
// is shared by scripts/demo and scripts/bench.
//
// It lives under scripts/ rather than internal/ or cmd/ on purpose: `make build`
// compiles ./cmd/... and nothing else, so none of this is built or shipped with
// the product. It is a client of the product, in the way taskforge-cli is.
//
// What a Stack does and does not touch:
//
//   - It starts processes in their own process groups and stops every one of
//     them on every way out. The caller owns signal handling and must call
//     Cleanup, which is idempotent.
//   - It reads the infrastructure it is pointed at from the same TASKFORGE_*
//     variables the services read (and the same .env), falling back to the
//     compose defaults, so CI can point it at its own services.
//   - It creates keys in a scope unique to the run. It never deletes or
//     truncates anything in the database, which may hold a developer's own
//     data, and its own database connection refuses writes (readdb.Open).
package stack

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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// Options is what a program chooses about its stack.
type Options struct {
	// Out receives the narration of each step.
	Out io.Writer
	// Prefix names everything the run creates: the scope, the broker queue, the
	// log directory, the keys and the workers. It is "demo" or "bench", so a
	// developer can tell whose leftovers they are looking at.
	Prefix string
	Timing Timings
	// QueueAttributes are SQS attributes the run's own queue is created with. Nil,
	// what scripts/demo, `make bench` and `make bench-smoke` pass, creates it with
	// the broker's defaults; only the recovery probe sets one, to vary the
	// visibility timeout under control.
	QueueAttributes map[string]string
}

// Stack is one run: the processes it started, the credentials it created, and
// the addresses everything listens on.
type Stack struct {
	Out     io.Writer
	Started time.Time
	Infra   Infra
	Timing  Timings
	Prefix  string

	// RunID is unique to the run, and everything the run creates carries it: the
	// scope, the worker names and the broker queue. Nothing else in the database
	// can match them, which is what lets a program assert on its own jobs only.
	RunID string
	Scope string

	BinDir  string
	LogDir  string
	WorkDir string // an empty directory: the working directory of every child

	QueueName, QueueURL string
	queueAttributes     map[string]string

	APIAddr, OutboxAddr, SchedulerAddr, ReconcilerAddr string
	APIURL                                             string

	CLI                   *CLI
	APIKey, WorkerKey     string
	APIKeyID, WorkerKeyID string

	// DB is a read-only pool (readdb.Open), for the few facts the public API
	// deliberately does not expose and for reading measurements back. It is safe
	// for use from several goroutines.
	DB *pgxpool.Pool

	mu      sync.Mutex
	procs   []*Proc
	cleaned sync.Once

	sayMu sync.Mutex // narration comes from several goroutines in a benchmark run
}

// binaries a stack runs. They must already be built into ./bin by `make build`.
var requiredBinaries = []string{
	"taskforge-api", "taskforge-outbox", "taskforge-scheduler",
	"taskforge-reconciler", "taskforge-worker", "taskforge-cli",
}

// RequiredBinaries is the names of the binaries a stack runs, for a caller that
// wants to check them (the benchmark verifies each was built from the commit it
// records). The slice is a copy.
func RequiredBinaries() []string { return append([]string(nil), requiredBinaries...) }

// New resolves the infrastructure, checks the binaries exist, and chooses the
// run's names and ports. It starts nothing.
func New(opts Options) (*Stack, error) {
	in, err := LoadInfra()
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

	logDir, err := os.MkdirTemp("", "taskforge-"+opts.Prefix+"-")
	if err != nil {
		return nil, fmt.Errorf("create the log directory: %w", err)
	}
	workDir := filepath.Join(logDir, "cwd")
	if err := os.Mkdir(workDir, 0o700); err != nil {
		return nil, fmt.Errorf("create the child working directory: %w", err)
	}

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	s := &Stack{
		Out: opts.Out, Started: time.Now(), Infra: in, Timing: opts.Timing, Prefix: opts.Prefix,
		RunID: runID, Scope: opts.Prefix + "-" + runID,
		BinDir: binDir, LogDir: logDir, WorkDir: workDir,
		// A queue of this run's own. A shared queue would let any worker already
		// running against it receive this run's notifications, and would let a
		// message a killed worker was holding in flight surface in a later run.
		QueueName:       "taskforge-" + opts.Prefix + "-" + runID,
		queueAttributes: opts.QueueAttributes,
	}

	// Free ports, chosen now so every process's configuration can name all of
	// them. Each binds its own a moment later.
	var ports [4]string
	for i := range ports {
		if ports[i], err = FreeLoopbackAddr(); err != nil {
			return nil, err
		}
	}
	s.APIAddr, s.OutboxAddr, s.SchedulerAddr, s.ReconcilerAddr = ports[0], ports[1], ports[2], ports[3]
	s.APIURL = "http://" + s.APIAddr
	s.CLI = &CLI{Bin: filepath.Join(binDir, "taskforge-cli"), APIURL: s.APIURL, Dir: workDir}
	return s, nil
}

// FreeLoopbackAddr asks the kernel for an unused loopback port and releases it.
// The process that is told to use it binds it moments later.
func FreeLoopbackAddr() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("find a free loopback port: %w", err)
	}
	defer listener.Close()
	return listener.Addr().String(), nil
}

// Say narrates one step with the time since the run began.
func (s *Stack) Say(format string, args ...any) {
	s.sayMu.Lock()
	defer s.sayMu.Unlock()
	fmt.Fprintf(s.Out, "[%6.1fs] %s\n", time.Since(s.Started).Seconds(), fmt.Sprintf(format, args...))
}

// BaseEnv is every setting each binary is started with. Children do not inherit
// the developer's environment: whatever is not named here is the binary's own
// default, and for an address that default is this machine's loopback.
func (s *Stack) BaseEnv() map[string]string {
	env := map[string]string{
		"TASKFORGE_DATABASE_URL": s.Infra.DatabaseURL,

		"TASKFORGE_BROKER_ENDPOINT":          s.Infra.BrokerEndpoint,
		"TASKFORGE_BROKER_QUEUE_NAME":        s.QueueName,
		"TASKFORGE_BROKER_REGION":            s.Infra.BrokerRegion,
		"TASKFORGE_BROKER_ACCESS_KEY_ID":     s.Infra.BrokerAccessKeyID,
		"TASKFORGE_BROKER_SECRET_ACCESS_KEY": s.Infra.BrokerSecretAccessKey,

		// The API serves results from the object store and the worker uploads to
		// it and probes it for readiness, each through its own client.
		"TASKFORGE_RESULTS_ENDPOINT":          s.Infra.ResultsEndpoint,
		"TASKFORGE_RESULTS_BUCKET":            s.Infra.ResultsBucket,
		"TASKFORGE_RESULTS_REGION":            s.Infra.ResultsRegion,
		"TASKFORGE_RESULTS_ACCESS_KEY_ID":     s.Infra.ResultsAccessKeyID,
		"TASKFORGE_RESULTS_SECRET_ACCESS_KEY": s.Infra.ResultsSecretAccessKey,

		"TASKFORGE_API_ADDR":        s.APIAddr,
		"TASKFORGE_OUTBOX_ADDR":     s.OutboxAddr,
		"TASKFORGE_SCHEDULER_ADDR":  s.SchedulerAddr,
		"TASKFORGE_RECONCILER_ADDR": s.ReconcilerAddr,

		"TASKFORGE_LOG_LEVEL": "info",
	}
	for name, value := range s.Timing.serviceEnv() {
		env[name] = value
	}
	return env
}

// Env builds one binary's complete environment and refuses to return one that
// leaves an endpoint that binary dials unset. The check is on the very map that
// is handed to the operating system.
func (s *Stack) Env(binary string, extra map[string]string) (map[string]string, error) {
	env := s.BaseEnv()
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

// WorkerEnv is what a worker adds to the shared environment.
func (s *Stack) WorkerEnv(name, addr string, concurrency int) map[string]string {
	env := map[string]string{
		"TASKFORGE_WORKER_NAME":         name,
		"TASKFORGE_WORKER_ADDR":         addr,
		"TASKFORGE_WORKER_API_URL":      s.APIURL,
		"TASKFORGE_WORKER_API_KEY":      s.WorkerKey,
		"TASKFORGE_WORKER_QUEUE":        "default",
		"TASKFORGE_WORKER_GROUP":        "default",
		"TASKFORGE_WORKER_CONCURRENCY":  strconv.Itoa(concurrency),
		"TASKFORGE_WORKER_CAPABILITIES": "cpu",
	}
	for key, value := range s.Timing.workerEnv() {
		env[key] = value
	}
	return env
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

// Setup creates what the run needs: its own broker queue, a read-only database
// connection for the few facts the public API deliberately does not expose, the
// four background services, and a pair of keys in this run's scope.
func (s *Stack) Setup(ctx context.Context) error {
	var err error
	if s.QueueURL, err = createQueue(ctx, s.Infra.BrokerEndpoint, s.QueueName, s.queueAttributes); err != nil {
		return fmt.Errorf("create this run's broker queue: %w", err)
	}
	s.Say("Created broker queue %s for this run alone.", s.QueueName)

	if s.DB, err = readdb.Open(ctx, s.Infra.DatabaseURL); err != nil {
		return err
	}

	s.Say("Starting api, outbox, scheduler and reconciler on free loopback ports.")
	var services []*Proc
	for _, svc := range []struct{ label, binary, addr string }{
		{"api", "taskforge-api", s.APIAddr},
		{"outbox", "taskforge-outbox", s.OutboxAddr},
		{"scheduler", "taskforge-scheduler", s.SchedulerAddr},
		{"reconciler", "taskforge-reconciler", s.ReconcilerAddr},
	} {
		env, err := s.Env(svc.binary, nil)
		if err != nil {
			return err
		}
		p, err := s.Start(svc.label, svc.binary, svc.addr, KindService, env)
		if err != nil {
			return err
		}
		services = append(services, p)
	}
	for _, p := range services {
		if err := p.WaitReady(ctx, 60*time.Second); err != nil {
			return err
		}
	}
	s.Say("All four services report ready.")

	return s.createKeys(ctx)
}

// QueueAttributes reads the run's own broker queue's attributes with
// GetQueueAttributes: its message counts and its configuration, as the broker
// reports them at that moment.
func (s *Stack) QueueAttributes(ctx context.Context) (map[string]string, error) {
	return queueAttributes(ctx, s.Infra.BrokerEndpoint, s.QueueURL)
}

// createKeys mints this run's credentials with taskforge-cli. The scope is
// unique to the run, so nothing another run or a developer created can be
// visible to, or claimed by, anything started here.
func (s *Stack) createKeys(ctx context.Context) error {
	var apiKey, workerKey struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	}
	if err := s.CLI.JSON(ctx, "", &apiKey, "api-keys", "create", "--scope", s.Scope, "--name", s.Prefix+"-"+s.RunID); err != nil {
		return fmt.Errorf("create the run's API key: %w", err)
	}
	if err := s.CLI.JSON(ctx, "", &workerKey, "worker-keys", "create", "--scope", s.Scope, "--name", s.Prefix+"-worker-"+s.RunID); err != nil {
		return fmt.Errorf("create the run's worker key: %w", err)
	}
	if apiKey.Key == "" || workerKey.Key == "" {
		return fmt.Errorf("a key was created but its credential was not returned")
	}
	s.APIKey, s.APIKeyID = apiKey.Key, apiKey.ID
	s.WorkerKey, s.WorkerKeyID = workerKey.Key, workerKey.ID
	s.Say("Created an API key and a worker key in scope %q.", s.Scope)
	return nil
}

// Cleanup stops everything the run started, in an order that lets it say what it
// is doing: workers first, then the run's keys are revoked while the API is
// still up, then the services, then the queue. It is idempotent.
//
// It deletes only what this run created: its own processes, its own queue, and
// (revoked, not deleted) its own two keys.
func (s *Stack) Cleanup() {
	s.cleaned.Do(func() {
		s.StopProcs(func(p *Proc) bool { return p.Kind == KindWorker })
		s.revokeKeys()
		s.StopProcs(func(p *Proc) bool { return p.Kind == KindService })
		s.mu.Lock()
		stillRunning := 0
		for _, p := range s.procs {
			if p.Running() {
				stillRunning++
			}
		}
		started := len(s.procs)
		s.mu.Unlock()
		if started > 0 {
			s.Say("Stopped the %d processes this run started; %d still running.", started, stillRunning)
		}
		if s.DB != nil {
			s.DB.Close()
		}
		if s.QueueURL != "" {
			if err := deleteQueue(context.Background(), s.Infra.BrokerEndpoint, s.QueueURL); err != nil {
				s.Say("Could not delete broker queue %s: %v", s.QueueName, err)
			} else {
				s.Say("Deleted broker queue %s, which only this run used.", s.QueueName)
			}
		}
	})
}

// revokeKeys revokes the credentials this run created. Best effort: a failure
// here is reported but never changes the result, because by now the run is over.
func (s *Stack) revokeKeys() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	revoked, failed := 0, 0
	for _, key := range []struct{ resource, id string }{
		{"api-keys", s.APIKeyID}, {"worker-keys", s.WorkerKeyID},
	} {
		if key.id == "" {
			continue
		}
		if _, err := s.CLI.Exec(ctx, "", key.resource, "revoke", key.id); err != nil {
			failed++
			s.Say("Could not revoke one of the run's %s: %v", key.resource, err)
			continue
		}
		revoked++
	}
	if revoked > 0 && failed == 0 {
		s.Say("Revoked this run's %d keys (revoked, not deleted).", revoked)
	}
}
