//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/auth"
	"github.com/co-rtex/TaskForge/internal/database"
	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/metrics"
	"github.com/co-rtex/TaskForge/internal/results"
	"github.com/co-rtex/TaskForge/internal/workerauth"
)

// meteredAPI starts a real API with a real metrics registry and a real
// StateCollector over the test database.
func meteredAPI(t *testing.T) (*httptest.Server, *metrics.Metrics) {
	t.Helper()
	m := metrics.New("taskforge-api")
	metrics.NewStateCollector(m, testPool, discardLogger())

	srv := api.NewServer(
		jobs.NewStore(testPool),
		api.Config{MaxRequestBytes: 256 * 1024},
		discardLogger(),
		api.ReadinessCheck{
			Name:  "postgres",
			Check: func(ctx context.Context) error { return database.Ping(ctx, testPool) },
		},
	).WithMetrics(m).
		WithWorkerControl(workerControlForAPI()).
		WithWorkerReads(workerControlForAPI()).
		WithAuth(auth.NewStore(testPool)).
		WithWorkerAuth(workerauth.NewStore(testPool)).
		WithResults(results.NewStore(testPool), testObjects)

	server := httptest.NewServer(srv.Handler())
	t.Cleanup(server.Close)
	return server, m
}

// scrapeSeries fetches /metrics and returns metric name -> list of label sets.
func scrapeSeries(t *testing.T, base string) map[string][]map[string]string {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// NewTextParser, not a zero value: the zero value's validation scheme is
	// unset and panics on the first metric name.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err, "/metrics must emit parseable exposition format")

	out := map[string][]map[string]string{}
	for name, family := range families {
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			out[name] = append(out[name], labels)
		}
	}
	return out
}

// The durable counters and gauges are read from PostgreSQL at scrape time, so
// this asserts them against real rows rather than against an in-process count.
func TestMetrics_DurableCountersReflectRealRows(t *testing.T) {
	reset(t)
	srv, _ := meteredAPI(t)

	for i := 0; i < 3; i++ {
		createJob(t, "metrics-durable-"+itoa(i), "demo.echo", 50, nil)
	}

	series := scrapeSeries(t, srv.URL)

	submitted := series["taskforge_jobs_submitted_total"]
	require.NotEmpty(t, submitted, "the submitted counter must be exposed")
	var found bool
	for _, labels := range submitted {
		if labels["queue"] == "default" {
			found = true
		}
	}
	require.True(t, found, "the default queue must appear in the submitted counter")

	// The queued gauge must agree with a direct count, which is the whole
	// point of deriving it from the database rather than counting in process.
	var queued int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM jobs WHERE status = 'QUEUED' AND queue = 'default'`).Scan(&queued))
	require.Equal(t, 3, queued)
	require.NotEmpty(t, series["taskforge_jobs_queued"])
}

// A scrape must never emit a forbidden label, checked against what is actually
// on the wire from a live system with real rows in it.
func TestMetrics_LiveScrapeCarriesNoForbiddenLabel(t *testing.T) {
	reset(t)
	srv, _ := meteredAPI(t)

	// Real rows across several tables, so the StateCollector's queries all
	// return something rather than trivially emitting nothing.
	store := controlStore()
	session := registerWorker(t, store,
		workerRegistration("metrics-worker", 2, nil, []string{"demo.echo"}))
	createJob(t, "metrics-labels", "demo.echo", 50, nil)
	claim, err := store.Claim(context.Background(), testScope, claimRequest(session, "default"))
	require.NoError(t, err)
	require.NotNil(t, claim.Assignment)

	for name, all := range scrapeSeries(t, srv.URL) {
		for _, labels := range all {
			for label, value := range labels {
				for _, forbidden := range metrics.ForbiddenLabels {
					require.NotEqualf(t, forbidden, label,
						"%s exposed forbidden label %q", name, label)
				}
				// A uuid in a VALUE is just as bad as one in a name.
				_, err := uuid.Parse(value)
				require.Errorf(t, err,
					"%s label %q carries a uuid value %q", name, label, value)
			}
		}
	}
}

// The worker gauge counts a crashed worker's session, not only a current one.
//
// Same reasoning, and the same LATERAL query shape, as M6A's ListWorkers: a
// gauge built on the current-session index would report the crashed workers an
// operator is looking for as simply absent.
func TestMetrics_WorkerGaugeCountsCrashedSessions(t *testing.T) {
	reset(t)
	ctx := context.Background()
	srv, _ := meteredAPI(t)
	store := controlStore()

	registerWorker(t, store, workerRegistration("metrics-healthy", 2, nil, []string{"demo.echo"}))
	registerWorker(t, store, workerRegistration("metrics-crashed", 2, nil, []string{"demo.echo"}))
	_, err := testPool.Exec(ctx, `
		UPDATE worker_sessions
		SET registered_at = now() - interval '2 hours',
		    last_heartbeat_at = now() - interval '1 hour'
		WHERE worker_id = (SELECT id FROM workers WHERE name = 'metrics-crashed')`)
	require.NoError(t, err)
	// One SECOND, not one nanosecond: the healthy worker registered moments
	// ago must not also be swept up by the threshold under test.
	marked, err := store.MarkStaleSessions(ctx, time.Second, 20)
	require.NoError(t, err)
	require.Equal(t, 1, marked)

	statuses := map[string]bool{}
	for _, labels := range scrapeSeries(t, srv.URL)["taskforge_workers"] {
		statuses[labels["status"]] = true
	}
	require.True(t, statuses["UNHEALTHY"],
		"a crashed worker's session must be counted, not silently omitted")
	require.True(t, statuses["HEALTHY"])
}

// /metrics keeps serving when the database is unreachable.
//
// A scrape does real queries, so a database outage must degrade the affected
// gauges' absence rather than the endpoint: the metrics an operator reads to
// diagnose that outage are the ones they must not lose.
func TestMetrics_DatabaseOutageDegradesGaugesNotTheEndpoint(t *testing.T) {
	reset(t)
	m := metrics.New("taskforge-api")

	// A pool pointed at a closed connection: every collector query fails.
	dead, err := database.Connect(context.Background(), dsn())
	require.NoError(t, err)
	dead.Close()
	metrics.NewStateCollector(m, dead, discardLogger())

	// An in-process metric that does NOT depend on the database, so the test
	// can tell "endpoint still works" from "endpoint returned nothing".
	m.SchedulerPromotions.Add(7)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code,
		"a database outage must not take /metrics down with it")
	require.Contains(t, rec.Body.String(), "taskforge_scheduler_promotions_total",
		"metrics that do not need the database must still be served")
}
