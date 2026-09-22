package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/metrics"
)

// These are the first tests this binary has ever had.
//
// Before M6C the health server took its dependencies concretely -- a
// *pgxpool.Pool it called database.Ping on inside the handler -- so the only
// way to exercise a failing dependency was to have a real PostgreSQL that was
// genuinely down. That is why there were none: the cost of testing a 503 was
// standing up and breaking infrastructure. Taking named closures instead makes
// every branch reachable from `make test-unit` with nothing running.

func okCheck(name string) healthCheck {
	return healthCheck{name, func(context.Context) error { return nil }}
}

func failingCheck(name string) healthCheck {
	return healthCheck{name, func(context.Context) error { return errors.New("down") }}
}

func discard() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

// serve issues one request against a health server's handler.
func serve(t *testing.T, server *http.Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// readiness decodes the readiness body's per-component map.
func readiness(t *testing.T, rec *httptest.ResponseRecorder) (string, map[string]string) {
	t.Helper()
	var body struct {
		Status     string            `json:"status"`
		Components map[string]string `json:"components"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body.Status, body.Components
}

// Liveness is an intentional unconditional 200. It must not consult a single
// dependency: a liveness probe that failed on a database blip would restart a
// healthy process and turn a dependency outage into an outage of this service
// too.
func TestHealth_LivenessIsUnconditional(t *testing.T) {
	server := newHealthServer(":0", discard(), nil,
		failingCheck("postgres"), failingCheck("broker"))

	rec := serve(t, server, "/healthz")
	require.Equal(t, http.StatusOK, rec.Code,
		"liveness must answer 200 even with every dependency failing")

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "alive", body["status"])
}

// Readiness is 200 only when every dependency passes.
func TestHealth_ReadinessIsOKWhenEveryCheckPasses(t *testing.T) {
	server := newHealthServer(":0", discard(), nil,
		okCheck("postgres"), okCheck("broker"))

	rec := serve(t, server, "/readyz")
	require.Equal(t, http.StatusOK, rec.Code)

	status, components := readiness(t, rec)
	require.Equal(t, "ready", status)
	require.Equal(t, map[string]string{"postgres": "ok", "broker": "ok"}, components)
}

// Each dependency is failed one at a time, so a test proves which component a
// 503 actually blamed rather than only that something was wrong.
func TestHealth_ReadinessFailsPerDependencyAndNamesIt(t *testing.T) {
	names := []string{"postgres", "broker"}

	for _, failing := range names {
		t.Run(failing, func(t *testing.T) {
			checks := make([]healthCheck, 0, len(names))
			for _, name := range names {
				if name == failing {
					checks = append(checks, failingCheck(name))
					continue
				}
				checks = append(checks, okCheck(name))
			}

			rec := serve(t, newHealthServer(":0", discard(), nil, checks...), "/readyz")
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)

			status, components := readiness(t, rec)
			require.Equal(t, "not_ready", status)
			require.Equal(t, "unavailable", components[failing])
			for _, name := range names {
				if name == failing {
					continue
				}
				require.Equalf(t, "ok", components[name],
					"a failing %s must not change %s's reported status", failing, name)
			}
		})
	}
}

// /metrics serves valid Prometheus exposition format, parsed by a real parser
// rather than string-matched.
func TestHealth_MetricsIsValidExpositionFormat(t *testing.T) {
	m := metrics.New("taskforge-reconciler")
	server := newHealthServer(":0", discard(), m, okCheck("postgres"))

	rec := serve(t, server, "/metrics")
	require.Equal(t, http.StatusOK, rec.Code)

	// NewTextParser, not a zero TextParser: the zero value leaves its
	// validation scheme unset and panics on the first metric name. This is a
	// property of the parser's own API, not of the endpoint under test.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(rec.Body)
	require.NoError(t, err, "the endpoint must emit parseable exposition format")
	require.NotEmpty(t, families, "at least one metric family must be exposed")

	// Every metric carries this process's service label, so a scrape from one
	// binary is distinguishable from another's.
	for name, family := range families {
		for _, metric := range family.GetMetric() {
			var found bool
			for _, label := range metric.GetLabel() {
				if label.GetName() == "service" {
					require.Equal(t, "taskforge-reconciler", label.GetValue())
					found = true
				}
			}
			require.Truef(t, found, "%s carries no service label", name)
		}
	}
}

// A server built without metrics simply has no /metrics route, and health
// still works -- nothing about the probes depends on instrumentation.
func TestHealth_MetricsIsAbsentWithoutInstrumentation(t *testing.T) {
	server := newHealthServer(":0", discard(), nil, okCheck("postgres"))

	require.Equal(t, http.StatusNotFound, serve(t, server, "/metrics").Code)
	require.Equal(t, http.StatusOK, serve(t, server, "/healthz").Code)
	require.Equal(t, http.StatusOK, serve(t, server, "/readyz").Code)
}
