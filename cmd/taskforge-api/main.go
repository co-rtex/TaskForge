// Command taskforge-api serves TaskForge's HTTP API.
//
// The public /v1 surface authenticates with database-backed API keys, and the
// internal worker-control surface authenticates registration with a separate
// database-backed worker key -- see docs/adr/0014-worker-control-authentication.md.
// The key-management routes for both credential types are unauthenticated
// operator plumbing, so they are served only on the admin listener, which binds
// to loopback. Two optional listeners serve the authenticated surfaces on
// network addresses: public (/v1 and the probes) and control (the worker-control
// routes and the probes) -- see docs/adr/0026-listeners-by-surface.md.
//
// `taskforge-api healthcheck` GETs the admin listener's /healthz and exits 0 or 1,
// for a container health check in an image with no shell.
//
// It also serves the operator dashboard's embedded static assets under
// /dashboard/, same-origin with the API the dashboard reads -- see
// docs/adr/0017-dashboard-toolchain-and-serving.md.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/auth"
	"github.com/co-rtex/TaskForge/internal/config"
	"github.com/co-rtex/TaskForge/internal/dashboard"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/healthcheck"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/metrics"
	"github.com/co-rtex/TaskForge/internal/objectstore"
	"github.com/co-rtex/TaskForge/internal/results"
	"github.com/co-rtex/TaskForge/internal/telemetry"
	"github.com/co-rtex/TaskForge/internal/workerauth"
	"github.com/co-rtex/TaskForge/internal/workers"
)

func main() {
	if healthcheck.Requested(os.Args) {
		os.Exit(runHealthcheck())
	}
	os.Exit(run())
}

// runHealthcheck probes the admin listener, which every configuration opens.
func runHealthcheck() int {
	if err := config.LoadDotEnv(".env"); err != nil {
		return healthcheck.Run(context.Background(), "taskforge-api", "", err, os.Stdout, os.Stderr)
	}
	cfg, err := config.Load()
	return healthcheck.Run(context.Background(), "taskforge-api", cfg.APIAddr, err, os.Stdout, os.Stderr)
}

func run() int {
	if err := config.LoadDotEnv(".env"); err != nil {
		slog.Error("read .env", slog.String("error", err.Error()))
		return 1
	}
	cfg, err := config.Load()
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, "taskforge-api")
	if err != nil {
		log.Error("configuration invalid", slog.String("error", err.Error()))
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Tracing is configured before anything else does work, so every span this
	// process emits belongs to one provider. Disabled by default: with
	// TASKFORGE_OTEL_EXPORTER unset or "none" no exporter is built, no
	// goroutine starts, and no endpoint is dialed.
	tracing, err := telemetry.StartTracing(ctx, telemetry.TracingConfig{
		Exporter: cfg.OTelExporter,
		Endpoint: cfg.OTelEndpoint,
		Service:  "taskforge-api",
	}, os.Stdout)
	if err != nil {
		log.Error("configure tracing", slog.String("error", err.Error()))
		return 1
	}
	// Deferred rather than placed on the graceful path, so it runs on every
	// exit including an early error return. It is bounded, idempotent, and a
	// no-op when tracing is disabled, and it deliberately survives ctx being
	// canceled by SIGTERM so buffered spans still flush.
	defer func() {
		if err := tracing.Shutdown(ctx); err != nil {
			log.Error("shut down tracing", slog.String("error", err.Error()))
		}
	}()

	pool, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect to database", slog.String("error", err.Error()))
		return 1
	}
	defer pool.Close()

	// Independently crypto-seeded per process. Two API replicas that shared a
	// deterministic seed would compute identical retry instants for jobs failing
	// at the same moment, which is exactly the stampede jitter exists to prevent.
	jitter, err := lifecycle.NewCryptoSeededJitter()
	if err != nil {
		log.Error("seed retry jitter", slog.String("error", err.Error()))
		return 1
	}

	// No network call here, deliberately: this process only ever reads an
	// object-located result on demand, so its boot must not depend on the
	// object store being reachable -- the same posture it already has
	// toward the broker, which it never touches directly at all.
	objects, err := objectstore.New(ctx, objectstore.OptionsFromConfig(cfg))
	if err != nil {
		log.Error("configure result object store client", slog.String("error", err.Error()))
		return 1
	}

	// One store serves both the fenced internal worker-control surface and the
	// authenticated public GET /v1/workers read. They are separate interfaces
	// on the server (WorkerControl, WorkerReads) so a public read cannot reach
	// a fenced transition, but there is only ever one control plane behind
	// them.
	workerStore := workers.NewStore(pool, workers.StoreConfig{
		LeaseDuration: cfg.LeaseDuration,
		RetryPolicy:   cfg.RetryPolicy(),
		Jitter:        jitter,
	})

	// The dashboard is embedded at compile time. Before `make dash-build` has
	// run, Assets is a single page saying so, which is logged here so an
	// operator who sees it knows it is a build state and not a fault.
	dashboardAssets, dashboardBuilt := dashboard.Assets()

	m := metrics.New("taskforge-api")
	metrics.NewStateCollector(m, pool, log)

	server := api.NewServer(
		jobs.NewStore(pool),
		api.Config{
			MaxRequestBytes: cfg.MaxRequestBytes,
			RequestTimeout:  cfg.APIRequestTimeout,
		},
		log,
		api.ReadinessCheck{
			Name:  "postgres",
			Check: func(ctx context.Context) error { return database.Ping(ctx, pool) },
		},
	).WithMetrics(m).
		WithWorkerControl(workerStore).
		WithWorkerReads(workerStore).
		WithAuth(auth.NewStore(pool)).WithWorkerAuth(workerauth.NewStore(pool)).
		WithResults(results.NewStore(pool), objects).
		WithDashboard(dashboardAssets)

	// One http.Server per configured listener, each serving its own share of
	// the one route table. The control listener's Host rule is the allowlist,
	// which Load has already validated.
	plan := planListeners(cfg)
	if cfg.APIControlAddr != "" {
		hosts, err := cfg.ControlHostAllowlist()
		if err != nil {
			log.Error("configuration invalid", slog.String("error", err.Error()))
			return 1
		}
		server.WithControlHosts(hosts)
	}
	servers := make([]namedServer, 0, len(plan))
	for _, p := range plan {
		servers = append(servers, namedServer{p.listener.String(), newHTTPServer(p.addr, server.ListenerHandler(p.listener))})
	}
	log.Info("api starting",
		slog.Int("listeners", len(servers)),
		slog.Int64("max_request_bytes", cfg.MaxRequestBytes),
		slog.String("dashboard_path", api.DashboardPath),
		slog.Bool("dashboard_built", dashboardBuilt))

	if err := serveAll(ctx, log, servers, 15*time.Second); err != nil {
		log.Error("api stopped with an error", slog.String("error", err.Error()))
		return 1
	}
	log.Info("api stopped")
	return 0
}
