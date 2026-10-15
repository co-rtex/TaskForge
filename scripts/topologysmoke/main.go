// Command topologysmoke runs taskforge-api and taskforge-worker as separate
// containers, the topology M8C2's deployment will run, and checks that the
// listeners by surface (docs/adr/0026-listeners-by-surface.md) hold in it:
//
//  1. the worker registers through the control listener, and a job submitted
//     through the public listener with an API key reaches SUCCEEDED;
//  2. key administration (/internal/v1/api-keys, /internal/v1/worker-keys) is
//     404 on both network listeners;
//  3. /dashboard/ and /metrics are 404 on both;
//  4. the control listener answers 403 origin_refused to a Host off its
//     allowlist, and to the allowlisted Host carrying Origin or Sec-Fetch-Site;
//  5. the public listener serves no /internal route (and control no /v1 one);
//  6. the admin listener cannot be reached from another container;
//  7. `healthcheck` exits 0 against a healthy container and 1 when its probe is
//     down.
//
// It runs under `make topology-smoke` and in CI's container-images job, from the
// repository root, after `make images` and `make up`. It lives under scripts/,
// so `make build` neither builds nor ships it.
//
// What it creates, it removes on every way out, and it touches nothing else: one
// Docker network (taskforge-topology-<pid>), the api, outbox and worker containers
// on it, one database (taskforge_topology_<pid>) on the compose PostgreSQL, created
// over a separate read-write connection and dropped with FORCE, and one broker
// queue of its own. The containers reach the infrastructure by joining the compose
// project's own network, so the same code runs on Linux and on Docker Desktop.
//
// The first API key and worker key are minted the way M8C2's runbook will mint
// them: on the admin listener, from inside the api container, with the CLI
// against loopback. The api image holds no CLI (ADR-0021), so the smoke copies a
// linux build of taskforge-cli into the running container with `docker cp` and
// runs it there with `docker exec`.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
)

const (
	exitPassed = 0
	exitFailed = 1
)

// The topology, as M8C2 will configure it. The admin listener stays on the
// api container's own loopback; the two network listeners bind every interface.
const (
	adminAddr     = "127.0.0.1:8080"
	publicPort    = "8090"
	controlPort   = "8091"
	apiAlias      = "api"
	workerName    = "topology-worker"
	scope         = "topology-smoke"
	totalRows     = 7
	healthTimeout = 90 * time.Second
)

type check struct {
	row          int
	ok           bool
	what, detail string
}

type smoke struct {
	out        io.Writer
	infra      stack.Infra
	pid        int
	workdir    string
	builder    string
	network    string
	apiKey     string
	containers []string
	checks     []check
	cleanup    []func()
}

func (s *smoke) record(row int, what, problem, okDetail string) {
	detail := okDetail
	if problem != "" {
		detail = problem
	}
	s.checks = append(s.checks, check{row: row, ok: problem == "", what: what, detail: detail})
}

func (s *smoke) onCleanup(f func()) { s.cleanup = append(s.cleanup, f) }

func (s *smoke) say(format string, args ...any) {
	fmt.Fprintf(s.out, "topology-smoke: "+format+"\n", args...)
}

func main() { os.Exit(run(os.Stdout, os.Stderr)) }

func run(stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := &smoke{out: stdout, pid: os.Getpid()}
	defer func() {
		for i := len(s.cleanup) - 1; i >= 0; i-- {
			s.cleanup[i]()
		}
	}()
	if err := s.setUp(ctx); err != nil {
		fmt.Fprintf(stderr, "topology-smoke: %v\n", err)
		s.dumpLogs()
		return exitFailed
	}
	s.runProbe(ctx)
	s.checkHealthcheck(ctx)
	code := s.report(ctx)
	if code != exitPassed {
		s.dumpLogs()
	}
	return code
}

func (s *smoke) name(role string) string { return fmt.Sprintf("taskforge-topology-%s-%d", role, s.pid) }

// --- set-up --------------------------------------------------------------------

func (s *smoke) setUp(ctx context.Context) error {
	infra, err := stack.LoadInfra()
	if err != nil {
		return err
	}
	s.infra = infra
	dockerfile, err := os.ReadFile("Dockerfile")
	if err != nil {
		return fmt.Errorf("run from the repository root: %w", err)
	}
	if s.builder, err = builderImage(string(dockerfile)); err != nil {
		return err
	}
	for _, service := range []string{"api", "worker", "outbox", "migrate"} {
		if out, err := docker(ctx, "image", "inspect", "--format", "{{.Id}}", image(service)); err != nil || out.exit != 0 {
			return fmt.Errorf("%s is not built; run `make images` first", image(service))
		}
	}
	arch, err := dockerArch(ctx)
	if err != nil {
		return err
	}
	composeNetwork, err := composeNetwork(ctx)
	if err != nil {
		return err
	}
	s.say("docker engine linux/%s, compose network %s, pid %d", arch, composeNetwork, s.pid)

	// The probe and the CLI, built for the engine's architecture.
	if s.workdir, err = os.MkdirTemp("", "taskforge-topology-"); err != nil {
		return err
	}
	s.onCleanup(func() { _ = os.RemoveAll(s.workdir) })
	if err := crossBuild(ctx, arch, s.workdir, "./cmd/taskforge-cli", "./scripts/topologysmoke/probe"); err != nil {
		return err
	}

	// The smoke's own database, migrated by the migrate image.
	dbName := fmt.Sprintf("taskforge_topology_%d", s.pid)
	if err := createDatabase(ctx, infra.DatabaseURL, dbName); err != nil {
		return err
	}
	s.onCleanup(func() {
		if err := dropDatabase(infra.DatabaseURL, dbName); err != nil {
			s.say("could not drop %s: %v", dbName, err)
			return
		}
		s.say("dropped database %s", dbName)
	})
	containerDB, err := containerDatabaseURL(infra.DatabaseURL, dbName)
	if err != nil {
		return err
	}

	// The smoke's own broker queue, so no other consumer can take its
	// notification and no leftover message can reach its worker.
	queueName := fmt.Sprintf("taskforge-topology-%d", s.pid)
	queueURL, err := createQueue(ctx, infra.BrokerEndpoint, queueName)
	if err != nil {
		return err
	}
	s.onCleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := deleteQueue(cleanupCtx, infra.BrokerEndpoint, queueURL); err != nil {
			s.say("could not delete queue %s: %v", queueName, err)
		}
	})

	network := s.name("net")
	if out, err := docker(ctx, "network", "create", network); err != nil || out.exit != 0 {
		return fmt.Errorf("create network %s: %v %s", network, err, out.stderr)
	}
	s.onCleanup(func() { _, _ = dockerCleanup("network", "rm", network) })

	shared := map[string]string{
		"TASKFORGE_DATABASE_URL":              containerDB,
		"TASKFORGE_BROKER_ENDPOINT":           "http://elasticmq:9324",
		"TASKFORGE_BROKER_QUEUE_NAME":         queueName,
		"TASKFORGE_BROKER_REGION":             infra.BrokerRegion,
		"TASKFORGE_BROKER_ACCESS_KEY_ID":      infra.BrokerAccessKeyID,
		"TASKFORGE_BROKER_SECRET_ACCESS_KEY":  infra.BrokerSecretAccessKey,
		"TASKFORGE_RESULTS_ENDPOINT":          "http://objectstore:4566",
		"TASKFORGE_RESULTS_BUCKET":            infra.ResultsBucket,
		"TASKFORGE_RESULTS_REGION":            infra.ResultsRegion,
		"TASKFORGE_RESULTS_ACCESS_KEY_ID":     infra.ResultsAccessKeyID,
		"TASKFORGE_RESULTS_SECRET_ACCESS_KEY": infra.ResultsSecretAccessKey,
	}

	migrate, err := docker(ctx, append(append([]string{"run", "--rm", "--network", composeNetwork}, envArgs(shared)...), image("migrate"))...)
	if err != nil || migrate.exit != 0 {
		return fmt.Errorf("migrate %s: exit %d %v\n%s", dbName, migrate.exit, err, truncate(migrate.combined(), 600))
	}
	s.say("migrated %s with %s", dbName, image("migrate"))

	// The api: admin on its own loopback, public and control on every interface,
	// and "api" (the name the worker dials) the only allowlisted control Host.
	apiEnv := merge(shared, map[string]string{
		"TASKFORGE_API_ADDR":          adminAddr,
		"TASKFORGE_API_PUBLIC_ADDR":   "0.0.0.0:" + publicPort,
		"TASKFORGE_API_CONTROL_ADDR":  "0.0.0.0:" + controlPort,
		"TASKFORGE_API_CONTROL_HOSTS": apiAlias,
	})
	if err := s.startContainer(ctx, "api", network, []string{"--network-alias", apiAlias}, composeNetwork, apiEnv); err != nil {
		return err
	}
	if err := s.awaitHealthy(ctx, "api"); err != nil {
		return err
	}

	apiKey, workerKey, err := s.mintKeys(ctx)
	if err != nil {
		return err
	}
	s.apiKey = apiKey

	if err := s.startContainer(ctx, "outbox", "", nil, composeNetwork, shared); err != nil {
		return err
	}
	workerEnv := merge(shared, map[string]string{
		"TASKFORGE_WORKER_API_URL":          "http://" + apiAlias + ":" + controlPort,
		"TASKFORGE_WORKER_ALLOW_REMOTE_API": "true",
		"TASKFORGE_WORKER_API_KEY":          workerKey,
		"TASKFORGE_WORKER_NAME":             workerName,
		"TASKFORGE_WORKER_CONCURRENCY":      "1",
	})
	if err := s.startContainer(ctx, "worker", network, nil, composeNetwork, workerEnv); err != nil {
		return err
	}
	s.network = network
	return nil
}

// startContainer creates a container on its first network, connects it to the
// second, and starts it, so it is never running on one network alone.
func (s *smoke) startContainer(ctx context.Context, role, network string, networkArgs []string, second string, env map[string]string) error {
	name := s.name(role)
	first := second
	if network != "" {
		first = network
	}
	args := append([]string{"create", "--name", name, "--network", first}, networkArgs...)
	args = append(append(args, envArgs(env)...), image(role))
	if out, err := docker(ctx, args...); err != nil || out.exit != 0 {
		return fmt.Errorf("create the %s container: %v %s", role, err, truncate(out.stderr, 400))
	}
	s.containers = append(s.containers, name)
	s.onCleanup(func() { _, _ = dockerCleanup("rm", "--force", name) })
	if network != "" {
		if out, err := docker(ctx, "network", "connect", second, name); err != nil || out.exit != 0 {
			return fmt.Errorf("connect the %s container to %s: %v %s", role, second, err, out.stderr)
		}
	}
	if out, err := docker(ctx, "start", name); err != nil || out.exit != 0 {
		return fmt.Errorf("start the %s container: %v %s", role, err, out.stderr)
	}
	s.say("started %s", name)
	return nil
}

// awaitHealthy runs the container's own `healthcheck` until it exits 0, failing
// at once if the container has stopped.
func (s *smoke) awaitHealthy(ctx context.Context, role string) error {
	name := s.name(role)
	deadline := time.Now().Add(healthTimeout)
	var last string
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		state, _ := docker(ctx, "inspect", "--format", "{{.State.Running}}", name)
		if strings.TrimSpace(state.stdout) == "false" {
			return fmt.Errorf("the %s container exited before it became healthy", role)
		}
		out, err := docker(ctx, "exec", name, "/taskforge-"+role, "healthcheck")
		if err == nil && out.exit == 0 {
			return nil
		}
		last = out.combined()
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("the %s container was not healthy within %s: %s", role, healthTimeout, truncate(last, 300))
}

// mintKeys is the bootstrap path: the CLI, inside the api container, against
// the admin listener on that container's loopback.
func (s *smoke) mintKeys(ctx context.Context) (apiKey, workerKey string, err error) {
	api := s.name("api")
	if out, err := docker(ctx, "cp", filepath.Join(s.workdir, "taskforge-cli"), api+":/tmp/taskforge-cli"); err != nil || out.exit != 0 {
		return "", "", fmt.Errorf("copy taskforge-cli into the api container: %v %s", err, out.stderr)
	}
	mint := func(kind string) (string, error) {
		out, err := docker(ctx, "exec", "--env", "TASKFORGE_CLI_API_URL=http://"+adminAddr, api,
			"/tmp/taskforge-cli", kind, "create", "--scope", scope, "--name", fmt.Sprintf("topology-%s-%d", kind, s.pid))
		if err != nil || out.exit != 0 {
			return "", fmt.Errorf("%s create inside the api container: exit %d %v %s", kind, out.exit, err, truncate(out.stderr, 300))
		}
		var created struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal([]byte(out.stdout), &created); err != nil || created.Key == "" {
			return "", fmt.Errorf("%s create returned no key", kind)
		}
		return created.Key, nil
	}
	if apiKey, err = mint("api-keys"); err != nil {
		return "", "", err
	}
	if workerKey, err = mint("worker-keys"); err != nil {
		return "", "", err
	}
	s.say("minted an API key and a worker key with taskforge-cli inside %s, against http://%s", api, adminAddr)
	return apiKey, workerKey, nil
}

// --- rows 1 to 6: the probe container -------------------------------------------

var probeLine = regexp.MustCompile(`^CHECK (\d+) (PASS|FAIL) (.+?) :: (.*)$`)

func (s *smoke) runProbe(ctx context.Context) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	base := "http://" + apiAlias + ":"
	args := []string{"run", "--rm", "--name", s.name("probe"), "--network", s.network,
		"--volume", s.workdir + ":/smoke:ro",
		"--env", "TOPOLOGY_PUBLIC_URL=" + base + publicPort,
		"--env", "TOPOLOGY_CONTROL_URL=" + base + controlPort,
		"--env", "TOPOLOGY_PUBLIC_ADDR=" + apiAlias + ":" + publicPort,
		"--env", "TOPOLOGY_ADMIN_ADDR=" + apiAlias + ":8080",
		"--env", "TOPOLOGY_API_KEY=" + s.apiKey,
		"--env", "TOPOLOGY_WORKER_NAME=" + workerName,
		s.builder, "/smoke/probe"}
	s.onCleanup(func() { _, _ = dockerCleanup("rm", "--force", s.name("probe")) })
	out, err := docker(probeCtx, args...)
	if err != nil {
		s.record(0, "the probe container ran", err.Error(), "")
		return
	}
	var parsed int
	for _, line := range strings.Split(out.stdout, "\n") {
		m := probeLine.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		row, _ := strconv.Atoi(m[1])
		problem := ""
		if m[2] == "FAIL" {
			problem = m[4]
		}
		s.record(row, m[3], problem, m[4])
		parsed++
	}
	if parsed == 0 || (out.exit != 0 && !s.anyFailed()) {
		s.record(0, "the probe container ran", fmt.Sprintf("exit %d, %d check lines:\n%s", out.exit, parsed, truncate(out.combined(), 800)), "")
	}
}

func (s *smoke) anyFailed() bool {
	for _, c := range s.checks {
		if !c.ok {
			return true
		}
	}
	return false
}

// --- row 7: the healthcheck subcommand -------------------------------------------

func (s *smoke) checkHealthcheck(ctx context.Context) {
	const row = 7
	for _, role := range []string{"api", "worker", "outbox"} {
		out, err := docker(ctx, "exec", s.name(role), "/taskforge-"+role, "healthcheck")
		what := "`healthcheck` exits 0 in the running " + role + " container"
		switch {
		case err != nil:
			s.record(row, what, err.Error(), "")
		case out.exit != 0:
			s.record(row, what, fmt.Sprintf("exit %d: %s", out.exit, truncate(out.combined(), 200)), "")
		default:
			s.record(row, what, "", "exit 0: "+truncate(out.stdout, 120))
		}
	}
	// The same subcommand in a container of the same image whose probe is not
	// up: nothing in it serves, so the check must say so and exit 1, and for
	// the probe's reason, not for a configuration it could not load.
	for role, env := range map[string][]string{
		"api":    nil,
		"worker": {"--env", "TASKFORGE_WORKER_API_KEY=placeholder-not-a-key"},
	} {
		args := append(append([]string{"run", "--rm", "--network", "none"}, env...), image(role), "healthcheck")
		out, err := docker(ctx, args...)
		what := "`healthcheck` exits 1 in a " + role + " container whose probe is down"
		switch {
		case err != nil:
			s.record(row, what, err.Error(), "")
		case out.exit != 1:
			s.record(row, what, fmt.Sprintf("exit %d: %s", out.exit, truncate(out.combined(), 200)), "")
		case strings.Contains(out.combined(), "configuration invalid") || !strings.Contains(out.combined(), "unhealthy: GET"):
			s.record(row, what, "exit 1 but not because the probe was down: "+truncate(out.combined(), 200), "")
		default:
			s.record(row, what, "", "exit 1: "+truncate(out.stderr, 140))
		}
	}
}

// --- the report -----------------------------------------------------------------

func (s *smoke) report(ctx context.Context) int {
	if ctx.Err() != nil {
		fmt.Fprintln(s.out, "topology-smoke: interrupted")
		return exitFailed
	}
	sort.SliceStable(s.checks, func(i, j int) bool { return s.checks[i].row < s.checks[j].row })
	rowOK := map[int]bool{}
	rowSeen := map[int]int{}
	for row := 1; row <= totalRows; row++ {
		rowOK[row] = true
	}
	failedChecks := 0
	fmt.Fprintf(s.out, "\n=== topology-smoke: checks ===\n")
	for _, c := range s.checks {
		verdict := "PASS"
		if !c.ok {
			verdict, failedChecks = "FAIL", failedChecks+1
			rowOK[c.row] = false
		}
		rowSeen[c.row]++
		fmt.Fprintf(s.out, "  %s  row %d  %s  %s\n", verdict, c.row, c.what, c.detail)
	}
	fmt.Fprintf(s.out, "\n=== topology-smoke: rows ===\n")
	passedRows := 0
	for row := 1; row <= totalRows; row++ {
		ok := rowOK[row] && rowSeen[row] > 0 && rowSeen[0] == 0
		verdict := "FAIL"
		if ok {
			verdict = "PASS"
			passedRows++
		}
		fmt.Fprintf(s.out, "  %s  row %d  %s  (%d checks)\n", verdict, row, rowTitles[row], rowSeen[row])
	}
	if rowSeen[0] > 0 {
		fmt.Fprintf(s.out, "  FAIL  the probe itself did not run cleanly\n")
	}
	if passedRows != totalRows || failedChecks > 0 {
		fmt.Fprintf(s.out, "=== RESULT: FAIL (%d of %d rows passed; %d of %d checks failed) ===\n",
			passedRows, totalRows, failedChecks, len(s.checks))
		return exitFailed
	}
	fmt.Fprintf(s.out, "=== RESULT: PASS (%d of %d rows passed; %d of %d checks met) ===\n",
		passedRows, totalRows, len(s.checks), len(s.checks))
	return exitPassed
}

var rowTitles = map[int]string{
	1: "the worker registers, and a job submitted through public:8090 with an API key reaches SUCCEEDED",
	2: "/internal/v1/api-keys and /internal/v1/worker-keys are 404 on 8090 and 8091",
	3: "/dashboard/ and /metrics are 404 on 8090 and 8091",
	4: "control:8091 answers 403 origin_refused to a Host off the allowlist, and to the allowlisted Host with Origin or Sec-Fetch-Site",
	5: "public:8090 serves no /internal route (and control:8091 no /v1 route)",
	6: "the admin listener cannot be reached from the probe container",
	7: "`healthcheck` exits 0 against a healthy container and 1 when its probe is down",
}

// dumpLogs prints the tail of every container's log after a failure, so CI's
// output says why without a download. Service logs carry no credential.
func (s *smoke) dumpLogs() {
	for _, name := range s.containers {
		out, err := dockerCleanup("logs", "--tail", "60", name)
		if err != nil {
			continue
		}
		fmt.Fprintf(s.out, "\n--- %s (last 60 lines) ---\n%s\n", name, truncate(out.combined(), 6000))
	}
}

// --- helpers -----------------------------------------------------------------------

func image(service string) string { return "taskforge-" + service + ":dev" }

func merge(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func envArgs(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var args []string
	for _, k := range keys {
		args = append(args, "--env", k+"="+env[k])
	}
	return args
}

func dockerArch(ctx context.Context) (string, error) {
	out, err := docker(ctx, "version", "--format", "{{.Server.Arch}}")
	if err != nil || out.exit != 0 {
		return "", fmt.Errorf("read the Docker engine's architecture: %v %s", err, out.stderr)
	}
	arch := strings.TrimSpace(out.stdout)
	if arch != "amd64" && arch != "arm64" {
		return "", fmt.Errorf("unsupported Docker engine architecture %q", arch)
	}
	return arch, nil
}

// composeNetwork is the network the compose project's PostgreSQL is on, which is
// also where its broker and object store are, by their service names.
func composeNetwork(ctx context.Context) (string, error) {
	id, err := docker(ctx, "compose", "ps", "--quiet", "postgres")
	if err != nil || id.exit != 0 || strings.TrimSpace(id.stdout) == "" {
		return "", errors.New("the compose PostgreSQL is not running; run `make up` first")
	}
	out, err := docker(ctx, "inspect", "--format", "{{range $name, $_ := .NetworkSettings.Networks}}{{$name}} {{end}}", strings.TrimSpace(id.stdout))
	if err != nil || out.exit != 0 {
		return "", fmt.Errorf("read the compose network: %v %s", err, out.stderr)
	}
	networks := strings.Fields(out.stdout)
	if len(networks) != 1 {
		return "", fmt.Errorf("the compose PostgreSQL is on %d networks (%v); want exactly one", len(networks), networks)
	}
	return networks[0], nil
}

func crossBuild(ctx context.Context, arch, dir string, packages ...string) error {
	args := append([]string{"build", "-trimpath", "-o", dir + string(os.PathSeparator)}, packages...)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+arch, "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build %v for linux/%s: %v\n%s", packages, arch, err, truncate(string(out), 800))
	}
	return nil
}

// containerDatabaseURL is the database URL a container on the compose network
// dials: the same user, password and query string, the compose service name and
// PostgreSQL's own port, and the smoke's database.
func containerDatabaseURL(hostURL, name string) (string, error) {
	u, err := url.Parse(hostURL)
	if err != nil || u.Host == "" {
		return "", errors.New("the database URL is not a URL with a host")
	}
	u.Host = "postgres:5432"
	u.Path = "/" + name
	u.RawPath = ""
	return u.String(), nil
}

var smokeDatabase = regexp.MustCompile(`^taskforge_topology_\d+$`)

func createDatabase(ctx context.Context, serverURL, name string) error {
	if !smokeDatabase.MatchString(name) {
		return fmt.Errorf("%q is not a database name the smoke is willing to create", name)
	}
	conn, err := pgx.Connect(ctx, serverURL)
	if err != nil {
		return fmt.Errorf("connect to create %s: %w", name, err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	return nil
}

// dropDatabase uses a fresh context so it still runs after an interrupt.
func dropDatabase(serverURL, name string) error {
	if !smokeDatabase.MatchString(name) {
		return fmt.Errorf("%q is not a database name the smoke is willing to drop", name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, serverURL)
	if err != nil {
		return fmt.Errorf("connect to drop %s: %w", name, err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
		return fmt.Errorf("drop %s: %w", name, err)
	}
	return nil
}

var queueURLPattern = regexp.MustCompile(`<QueueUrl>([^<]+)</QueueUrl>`)

// sqsCall issues one unsigned SQS query-protocol request to ElasticMQ, as
// scripts/internal/stack does for its own queues.
func sqsCall(ctx context.Context, endpoint string, form url.Values) (string, error) {
	form.Set("Version", "2012-11-05")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s: HTTP %d: %s", form.Get("Action"), resp.StatusCode, body)
	}
	return string(body), nil
}

func createQueue(ctx context.Context, endpoint, name string) (string, error) {
	body, err := sqsCall(ctx, endpoint, url.Values{"Action": {"CreateQueue"}, "QueueName": {name}})
	if err != nil {
		return "", fmt.Errorf("create queue %s: %w", name, err)
	}
	m := queueURLPattern.FindStringSubmatch(body)
	if len(m) != 2 {
		return "", fmt.Errorf("CreateQueue returned no queue URL: %s", truncate(body, 200))
	}
	return m[1], nil
}

func deleteQueue(ctx context.Context, endpoint, queueURL string) error {
	_, err := sqsCall(ctx, endpoint, url.Values{"Action": {"DeleteQueue"}, "QueueUrl": {queueURL}})
	return err
}

var builderFrom = regexp.MustCompile(`(?m)^FROM\s+(\S+)\s+AS\s+builder\s*$`)

// builderImage is the Dockerfile's pinned builder image: the probe runs in it,
// so the smoke pulls no image the repository does not already pin.
func builderImage(dockerfile string) (string, error) {
	m := builderFrom.FindStringSubmatch(dockerfile)
	if m == nil {
		return "", errors.New("the Dockerfile has no `FROM <image> AS builder`")
	}
	return m[1], nil
}

type dockerOutput struct {
	stdout, stderr string
	exit           int
}

func (d dockerOutput) combined() string { return d.stdout + d.stderr }

// docker runs the docker CLI. A non-zero exit is an observation, not an error;
// the error is only for being unable to run docker at all.
func docker(ctx context.Context, args ...string) (dockerOutput, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	out := dockerOutput{stdout: stdout.String(), stderr: stderr.String()}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		out.exit = exitErr.ExitCode()
	default:
		return out, fmt.Errorf("run docker %s: %w", strings.Join(args[:min(len(args), 2)], " "), err)
	}
	return out, nil
}

// dockerCleanup runs docker with a context of its own, so teardown still runs
// after the smoke was interrupted.
func dockerCleanup(args ...string) (dockerOutput, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return docker(ctx, args...)
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
