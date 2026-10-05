// Command imagesmoke checks the six service images that `make images` builds. It
// is what turns the Dockerfile's claims into observations of the built image:
//
//   - every image runs as a non-root user, carries the source and revision labels,
//     has its own binary as an exec-form ENTRYPOINT, and holds no other TaskForge
//     binary and no shell;
//   - every image, given an environment that configuration validation rejects,
//     exits non-zero and says why, in the one variable that image reads;
//   - the migrate image, run against a database the smoke created a moment before
//     and which is empty, applies every embedded migration, finds nothing to do the
//     second time, and leaves the schema version the image embeds;
//   - the api image serves the real dashboard build and not the placeholder page.
//
// It runs under `make images-smoke` and in CI, from the repository root, after the
// images exist. It lives under scripts/, so `make build` neither builds nor ships
// it.
//
// It creates one database of its own, taskforge_imagesmoke_<pid>, on the server
// TASKFORGE_DATABASE_URL names, over a separate read-write connection (scripts/readdb
// is read-only on purpose), and drops it on every way out. That is what makes the
// migrate check mean something: a database someone has already migrated would let an
// image that applies nothing pass.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
	"github.com/co-rtex/TaskForge/scripts/readdb"
)

const (
	exitPassed = 0
	exitFailed = 1
)

// services are the six images, each with the one variable that image's own
// configuration validation rejects and the message it must print. The variable is
// chosen from internal/config's real rules, per service: a bare run is not used,
// because five of the six images accept the defaults and fail later on a refused
// database connection instead (docs/CURRENT_STATE.md, M8B).
var services = []struct {
	name    string
	env     map[string]string
	message string
}{
	{"api", map[string]string{"TASKFORGE_API_ADDR": "0.0.0.0:8080"}, "TASKFORGE_API_ADDR must bind to a loopback address"},
	{"outbox", map[string]string{"TASKFORGE_OUTBOX_ADDR": "0.0.0.0:8082"}, "TASKFORGE_OUTBOX_ADDR must bind to a loopback address"},
	{"scheduler", map[string]string{"TASKFORGE_SCHEDULER_ADDR": "0.0.0.0:8084"}, "TASKFORGE_SCHEDULER_ADDR must bind to a loopback address"},
	{"reconciler", map[string]string{"TASKFORGE_RECONCILER_ADDR": "0.0.0.0:8083"}, "TASKFORGE_RECONCILER_ADDR must bind to a loopback address"},
	{"worker", map[string]string{"TASKFORGE_WORKER_ADDR": "0.0.0.0:9000"}, "TASKFORGE_WORKER_ADDR must bind to a loopback address"},
	{"migrate", map[string]string{"TASKFORGE_MAX_REQUEST_BYTES": "1"}, "TASKFORGE_MAX_REQUEST_BYTES must be at least 1024"},
}

func imageName(service string) string { return "taskforge-" + service + ":dev" }

type check struct {
	name   string
	ok     bool
	detail string
}

type smoke struct {
	out      io.Writer
	revision string
	infra    stack.Infra
	// hostNetwork is true on Linux, where a container can share the host's
	// network and so reach services published on 127.0.0.1. Elsewhere (Docker
	// Desktop) containers get host.docker.internal instead.
	hostNetwork bool
	builder     string
	checks      []check
}

func (s *smoke) record(name string, problem string, okDetail string) {
	s.checks = append(s.checks, check{name: name, ok: problem == "", detail: firstNonEmpty(problem, okDetail)})
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func main() { os.Exit(run(os.Stdout, os.Stderr)) }

func run(stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s, err := newSmoke(stdout)
	if err != nil {
		fmt.Fprintf(stderr, "imagesmoke: %v\n", err)
		return exitFailed
	}
	fmt.Fprintf(stdout, "imagesmoke: revision %s, %s networking\n", s.revision, map[bool]string{true: "host", false: "Docker Desktop"}[s.hostNetwork])

	for _, svc := range services {
		s.checkImage(ctx, svc.name)
		s.checkRejection(ctx, svc.name, svc.env, svc.message)
	}
	s.checkMigrate(ctx)
	s.checkAPIDashboard(ctx)

	return s.report(ctx)
}

func newSmoke(out io.Writer) (*smoke, error) {
	infra, err := stack.LoadInfra()
	if err != nil {
		return nil, err
	}
	revision, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return nil, fmt.Errorf("read the commit the images should be labelled with: %w", err)
	}
	dockerfile, err := os.ReadFile("Dockerfile")
	if err != nil {
		return nil, fmt.Errorf("run from the repository root: %w", err)
	}
	builder, err := builderImage(string(dockerfile))
	if err != nil {
		return nil, err
	}
	return &smoke{
		out: out, revision: strings.TrimSpace(string(revision)), infra: infra,
		hostNetwork: runtime.GOOS == "linux", builder: builder,
	}, nil
}

func (s *smoke) report(ctx context.Context) int {
	if ctx.Err() != nil {
		fmt.Fprintln(s.out, "imagesmoke: interrupted")
		return exitFailed
	}
	width, failed := 0, 0
	for _, c := range s.checks {
		width = max(width, len(c.name))
	}
	fmt.Fprintf(s.out, "\n=== images-smoke: checks ===\n")
	for _, c := range s.checks {
		verdict := "PASS"
		if !c.ok {
			verdict, failed = "FAIL", failed+1
		}
		fmt.Fprintf(s.out, "  %s  %-*s  %s\n", verdict, width, c.name, c.detail)
	}
	if failed > 0 || len(s.checks) == 0 {
		fmt.Fprintf(s.out, "=== RESULT: FAIL (%d of %d checks failed) ===\n", failed, len(s.checks))
		return exitFailed
	}
	fmt.Fprintf(s.out, "=== RESULT: PASS (%d of %d checks met) ===\n", len(s.checks), len(s.checks))
	return exitPassed
}

// --- per-image checks -----------------------------------------------------------

func (s *smoke) checkImage(ctx context.Context, service string) {
	image := imageName(service)
	info, err := inspectImage(ctx, image)
	if err != nil {
		s.record(service+": the image exists", err.Error(), "")
		return
	}
	s.record(service+": the image exists", "", fmt.Sprintf("%s, %s/%s, %.1f MB", shortID(info.ID), info.OS, info.Architecture, float64(info.Size)/1e6))
	s.record(service+": runs as a non-root user", userProblem(info.Config.User), fmt.Sprintf("USER %q", info.Config.User))
	s.record(service+": exec-form entrypoint is its own binary", entrypointProblem(info.Config.Entrypoint, service), fmt.Sprint(info.Config.Entrypoint))
	s.record(service+": carries the source and revision labels", strings.Join(labelProblems(info.Config.Labels, s.revision), "; "), "revision "+shortID(info.Config.Labels[labelRevision]))

	files, err := imageFiles(ctx, image)
	if err != nil {
		s.record(service+": holds only its own binary, and no shell", err.Error(), "")
		return
	}
	s.record(service+": holds only its own binary, and no shell", strings.Join(binaryProblems(files, service), "; "),
		fmt.Sprintf("/taskforge-%s among %d paths", service, len(files)))
}

// checkRejection runs the image with no network and one invalid variable, and
// requires a non-zero exit and the specific validation message.
func (s *smoke) checkRejection(ctx context.Context, service string, env map[string]string, message string) {
	args := []string{"run", "--rm", "--network", "none"}
	for k, v := range env {
		args = append(args, "--env", k+"="+v)
	}
	args = append(args, imageName(service))

	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	out, err := docker(runCtx, args...)
	name := service + ": rejects an invalid configuration, saying which"
	if err != nil {
		s.record(name, err.Error(), "")
		return
	}
	s.record(name, rejectionProblem(out.exit, out.combined(), message), fmt.Sprintf("exit %d: %s", out.exit, message))
}

// --- migrate --------------------------------------------------------------------

// containerEnv is the environment for a container that dials the infrastructure
// the smoke was pointed at: as it stands on Linux, rewritten for Docker Desktop
// elsewhere.
func (s *smoke) containerEnv() map[string]string {
	env := map[string]string{
		"TASKFORGE_DATABASE_URL":              s.infra.DatabaseURL,
		"TASKFORGE_RESULTS_ENDPOINT":          s.infra.ResultsEndpoint,
		"TASKFORGE_RESULTS_BUCKET":            s.infra.ResultsBucket,
		"TASKFORGE_RESULTS_REGION":            s.infra.ResultsRegion,
		"TASKFORGE_RESULTS_ACCESS_KEY_ID":     s.infra.ResultsAccessKeyID,
		"TASKFORGE_RESULTS_SECRET_ACCESS_KEY": s.infra.ResultsSecretAccessKey,
	}
	if s.hostNetwork {
		return env
	}
	return rewriteForDesktop(env)
}

func (s *smoke) networkArgs() []string {
	if s.hostNetwork {
		return []string{"--network", "host"}
	}
	return []string{"--add-host", "host.docker.internal:host-gateway"}
}

func envArgs(env map[string]string) []string {
	var args []string
	for k, v := range env {
		args = append(args, "--env", k+"="+v)
	}
	return args
}

// containerDatabaseURL is a database URL as a container must dial it: unchanged on
// Linux, where containers share the host's network, and with the loopback host
// rewritten on Docker Desktop.
func (s *smoke) containerDatabaseURL(hostURL string) string {
	if s.hostNetwork {
		return hostURL
	}
	return rewriteForDesktop(map[string]string{"TASKFORGE_DATABASE_URL": hostURL})["TASKFORGE_DATABASE_URL"]
}

// createDatabase creates the smoke's own database over a separate read-write
// connection to the server. CREATE DATABASE cannot run in a transaction or on
// scripts/readdb's read-only handle.
func createDatabase(ctx context.Context, serverURL, name string) error {
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

// dropDatabase removes the smoke's database, forcing off any connection still on it.
// It uses a fresh context of its own, so that it still runs when the smoke was
// interrupted and its context is cancelled.
func dropDatabase(serverURL, name string) error {
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

// checkMigrate runs the migrate image twice against a database it was just handed,
// empty, and then reads that database itself.
//
// The first run must APPLY every embedded migration; "already up to date" fails it
// (firstRunProblem). That is only a meaningful test because the database is the
// smoke's own: a shared one may already have been migrated from the host.
func (s *smoke) checkMigrate(ctx context.Context) {
	const (
		applies  = "migrate: applies every embedded migration to an empty database"
		again    = "migrate: a second run finds the schema current"
		version  = "migrate: PostgreSQL's schema version is the embedded one"
		cleanup  = "migrate: drops its throwaway database"
		migrates = "migrations"
	)

	wantCount, wantHighest, err := expectedSchema(migrates)
	if err != nil {
		s.record(applies, err.Error(), "")
		return
	}
	name := throwawayDatabaseName(os.Getpid())
	hostURL, err := withDatabaseName(s.infra.DatabaseURL, name)
	if err != nil {
		s.record(applies, err.Error(), "")
		return
	}
	if err := createDatabase(ctx, s.infra.DatabaseURL, name); err != nil {
		s.record(applies, err.Error(), "")
		return
	}
	defer func() {
		s.record(cleanup, errString(dropDatabase(s.infra.DatabaseURL, name)), name+" dropped")
	}()

	run := func() (dockerOutput, error) {
		runCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
		defer cancel()
		args := append([]string{"run", "--rm"}, s.networkArgs()...)
		args = append(args, "--env", "TASKFORGE_DATABASE_URL="+s.containerDatabaseURL(hostURL), imageName("migrate"))
		return docker(runCtx, args...)
	}

	first, err := run()
	if err != nil {
		s.record(applies, err.Error(), "")
		return
	}
	outcome, outErr := migrationOutcome(first.combined())
	switch {
	case first.exit != 0:
		s.record(applies, fmt.Sprintf("exit %d:\n%s", first.exit, truncate(first.combined(), 600)), "")
	case outErr != nil:
		s.record(applies, outErr.Error(), "")
	default:
		s.record(applies, firstRunProblem(outcome, wantCount), fmt.Sprintf("exit 0, applied %d into an empty database", outcome.applied))
	}

	// A second run must find nothing left to do: the first one really finished.
	second, err := run()
	if err != nil {
		s.record(again, err.Error(), "")
	} else if rerun, outErr := migrationOutcome(second.combined()); second.exit != 0 || outErr != nil || rerun.kind != outcomeCurrent {
		s.record(again, fmt.Sprintf("exit %d, %v:\n%s", second.exit, outErr, truncate(second.combined(), 400)), "")
	} else {
		s.record(again, "", "exit 0, schema already up to date")
	}

	// Then read PostgreSQL itself, the throwaway database and not the shared one,
	// and not the command's account of what it did.
	pool, err := readdb.Open(ctx, hostURL)
	if err != nil {
		s.record(version, err.Error(), "")
		return
	}
	defer pool.Close()
	var gotCount, gotHighest int
	if err := pool.QueryRow(ctx, `SELECT count(*), coalesce(max(version), 0) FROM schema_migrations`).Scan(&gotCount, &gotHighest); err != nil {
		s.record(version, fmt.Sprintf("read schema_migrations: %v", err), "")
		return
	}
	s.record(version, schemaProblem(gotCount, gotHighest, wantCount, wantHighest),
		fmt.Sprintf("schema_migrations: %d migrations, highest version %d", gotCount, gotHighest))
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// --- api and the dashboard ------------------------------------------------------

// checkAPIDashboard runs the api image against the real infrastructure, fetches
// /dashboard/ from it, and requires the built dashboard. See
// docs/adr/0021-container-images-and-supply-chain-scanning.md for what this does
// and does not prove.
func (s *smoke) checkAPIDashboard(ctx context.Context) {
	const name = "api: serves the real dashboard build, not the placeholder"

	port, err := freePort()
	if err != nil {
		s.record(name, err.Error(), "")
		return
	}
	container := fmt.Sprintf("taskforge-smoke-api-%d", os.Getpid())
	env := s.containerEnv()
	env["TASKFORGE_API_ADDR"] = fmt.Sprintf("127.0.0.1:%d", port)

	args := append([]string{"run", "--detach", "--name", container}, s.networkArgs()...)
	args = append(args, envArgs(env)...)
	args = append(args, imageName("api"))
	started, err := docker(ctx, args...)
	if err != nil || started.exit != 0 {
		s.record(name, fmt.Sprintf("could not start the api image (exit %d): %v %s", started.exit, err, truncate(started.stderr, 300)), "")
		return
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = docker(cleanup, "rm", "--force", container)
	}()

	page, err := s.fetchEventually(ctx, container, port, "/dashboard/", 60*time.Second)
	if err != nil {
		logs, _ := docker(ctx, "logs", container)
		s.record(name, fmt.Sprintf("%v\napi log:\n%s", err, truncate(logs.combined(), 800)), "")
		return
	}
	if problem := dashboardProblem(string(page.body)); problem != "" {
		s.record(name, problem, "")
		return
	}

	// The page must reference a hashed script, and the api must serve exactly the
	// bytes `make dash-build` produced for it.
	scripts, _ := assetPaths(string(page.body))
	asset, err := s.fetch(ctx, container, port, scripts[0])
	if err != nil || asset.status != http.StatusOK {
		s.record(name, fmt.Sprintf("GET %s: status %d, %v", scripts[0], asset.status, err), "")
		return
	}
	onDisk, err := os.ReadFile(filepath.Join("internal", "dashboard", "dist", strings.TrimPrefix(scripts[0], "/dashboard/")))
	if err != nil {
		s.record(name, fmt.Sprintf("the build context has no %s: %v", scripts[0], err), "")
		return
	}
	served, want := sha256.Sum256(asset.body), sha256.Sum256(onDisk)
	if served != want {
		s.record(name, fmt.Sprintf("%s is served with sha256 %s but internal/dashboard/dist has %s",
			scripts[0], hex.EncodeToString(served[:])[:12], hex.EncodeToString(want[:])[:12]), "")
		return
	}

	// And the binary reports the same of itself.
	logs, _ := docker(ctx, "logs", container)
	if !strings.Contains(logs.combined(), `"dashboard_built":true`) {
		s.record(name, `the api's own log does not say "dashboard_built":true`, "")
		return
	}
	s.record(name, "", fmt.Sprintf("marker \"built\", %s served byte-identical to dist (%d bytes, sha256 %s), log says dashboard_built",
		scripts[0], len(asset.body), hex.EncodeToString(served[:])[:12]))
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find a free port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

type response struct {
	status int
	body   []byte
}

// fetch GETs a path from the api. With host networking the api is on this
// machine's loopback and a plain HTTP client reaches it. On Docker Desktop its
// loopback is inside the container, so curl runs in a sidecar that shares the
// container's network namespace, from the image the Dockerfile already pins as its
// builder (Debian, so it has curl) and not from a second pinned image.
func (s *smoke) fetch(ctx context.Context, container string, port int, path string) (response, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)
	if s.hostNetwork {
		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
		if err != nil {
			return response{}, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return response{}, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		return response{status: resp.StatusCode, body: body}, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := docker(reqCtx, "run", "--rm", "--network", "container:"+container, s.builder,
		"curl", "--silent", "--show-error", "--max-time", "10", "--dump-header", "/dev/stderr", url)
	if err != nil {
		return response{}, err
	}
	if out.exit != 0 {
		return response{}, fmt.Errorf("curl exit %d: %s", out.exit, truncate(out.stderr, 200))
	}
	// The first header line is the status line: "HTTP/1.1 200 OK".
	status := 0
	if fields := strings.Fields(strings.SplitN(out.stderr, "\n", 2)[0]); len(fields) >= 2 {
		status, _ = strconv.Atoi(fields[1])
	}
	return response{status: status, body: []byte(out.stdout)}, nil
}

// fetchEventually waits for the api to start serving, giving up at the deadline.
// It fails at once if the container has exited, since waiting would only hide why.
func (s *smoke) fetchEventually(ctx context.Context, container string, port int, path string, within time.Duration) (response, error) {
	deadline := time.Now().Add(within)
	var last error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return response{}, ctx.Err()
		}
		state, _ := docker(ctx, "inspect", "--format", "{{.State.Running}}", container)
		if strings.TrimSpace(state.stdout) == "false" {
			return response{}, fmt.Errorf("the api container exited before it served %s", path)
		}
		resp, err := s.fetch(ctx, container, port, path)
		if err == nil && resp.status == http.StatusOK {
			return resp, nil
		}
		last = fmt.Errorf("GET %s: status %d, %v", path, resp.status, err)
		time.Sleep(500 * time.Millisecond)
	}
	return response{}, fmt.Errorf("the api did not serve %s within %s: %v", path, within, last)
}

func shortID(id string) string {
	id = strings.TrimPrefix(id, "sha256:")
	return id[:min(len(id), 12)]
}
