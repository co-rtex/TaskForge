package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every metric this system defines.
const Namespace = "taskforge"

// Metrics holds every collector TaskForge defines, plus the registry they are
// registered in.
//
// One struct rather than package-level globals: a test builds its own and
// throws it away, which is what makes the enforcement tests independent of
// each other and of whatever a previous test registered.
type Metrics struct {
	registry *prometheus.Registry
	service  string

	// manifest records the declared label names of every collector built
	// through the helpers below. It is what the enforcement tests check, and
	// it cannot miss a collector: a collector not built through a helper is
	// never registered and therefore emits nothing.
	manifest map[string][]string

	// In-process counters and histograms.
	//
	// The job-lifecycle totals are deliberately NOT here -- they are derived
	// from durable state at scrape time by StateCollector. See its comment for
	// why, and docs/CURRENT_STATE.md for the two families that are recorded
	// nowhere yet and why.

	// Execution is worker-only, which is what makes its job_type label
	// boundable: only a worker has the handler registry to bound it against.
	Execution    *prometheus.HistogramVec // queue, job_type
	HTTPDuration *prometheus.HistogramVec // method, route
	HTTPRequests *prometheus.CounterVec   // method, route, code

	// StaleRejections counts fenced transitions the control plane refused
	// because the caller's authority was stale. It carries no queue label: the
	// rejection is recorded at the HTTP boundary, where the request carries a
	// fence (job, attempt, lease ids) and no queue, and resolving one would
	// mean a database read on a path whose entire purpose is to refuse work.
	StaleRejections prometheus.Counter

	// SchedulerPromotions carries no queue label either: scheduler.Result
	// reports an aggregate count per pass, not a per-queue breakdown, and
	// inventing one would mean changing what the store returns.
	SchedulerPromotions prometheus.Counter
	OutboxPublishFailed prometheus.Counter
	ReconcileRepairs    *prometheus.CounterVec // repair
}

// New builds and registers every collector for one process.
//
// service is attached as a constant label on every metric, so a scrape from
// one binary is distinguishable from another's without the caller having to
// pass it at each emission point.
//
// It panics on a label-rule violation. That is deliberate and is the same
// posture config.Validate already takes toward a bad exporter name: a metric
// with an unbounded label is a production incident waiting to happen, and
// failing at startup is strictly better than discovering it when a scrape
// melts the time-series database.
func New(service string) *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		service:  service,
		manifest: map[string][]string{},
	}

	m.Execution = m.histogram("execution_duration_seconds",
		"Trusted handler execution time, measured by the worker.",
		prometheus.DefBuckets, "queue", "job_type")
	m.HTTPDuration = m.histogram("http_request_duration_seconds",
		"HTTP request duration, by matched route pattern.",
		prometheus.DefBuckets, "method", "route")
	m.HTTPRequests = m.counter("http_requests_total",
		"HTTP requests, by matched route pattern and status.", "method", "route", "code")

	m.StaleRejections = m.plainCounter("stale_completion_rejections_total",
		"Fenced transitions refused because the caller's authority was stale.")
	m.SchedulerPromotions = m.plainCounter("scheduler_promotions_total",
		"Jobs the scheduler promoted to eligible.")
	m.OutboxPublishFailed = m.plainCounter("outbox_publish_failures_total",
		"Outbox events whose publication failed and was rescheduled.")
	m.ReconcileRepairs = m.counter("reconciliation_repairs_total",
		"Durable repairs reconciliation committed, by kind.", "repair")

	return m
}

// Registry exposes the underlying registry, for a Collector a caller registers
// itself (see collectors.go) and for the enforcement tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Manifest returns metric name -> declared label names, sorted.
//
// This is the complete set by construction rather than by observation: it is
// populated by the same helpers that build and register the collectors, so
// unlike a Gather() walk it reports a metric that has not yet emitted a
// single sample.
func (m *Metrics) Manifest() map[string][]string {
	out := make(map[string][]string, len(m.manifest))
	for name, labels := range m.manifest {
		copied := append([]string(nil), labels...)
		sort.Strings(copied)
		out[name] = copied
	}
	return out
}

// Handler serves this registry in Prometheus text exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// A collector that fails at scrape time reports its own absence rather
		// than failing the whole endpoint: a database outage must not also
		// take away the metrics an operator is using to diagnose it.
		ErrorHandling: promhttp.ContinueOnError,
	})
}

// --- collector constructors -------------------------------------------------
//
// Every collector goes through one of these. They register it, record its
// declared labels in the manifest, and refuse a label the rules forbid.

func (m *Metrics) record(name string, labels []string) {
	if problems := checkLabels(name, labels); len(problems) > 0 {
		panic(fmt.Sprintf("metrics: label rule violated: %s", strings.Join(problems, "; ")))
	}
	m.manifest[Namespace+"_"+name] = labels
}

func (m *Metrics) counter(name, help string, labels ...string) *prometheus.CounterVec {
	m.record(name, labels)
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   Namespace,
		Name:        name,
		Help:        help,
		ConstLabels: prometheus.Labels{"service": m.service},
	}, labels)
	m.registry.MustRegister(c)
	return c
}

func (m *Metrics) plainCounter(name, help string) prometheus.Counter {
	m.record(name, nil)
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   Namespace,
		Name:        name,
		Help:        help,
		ConstLabels: prometheus.Labels{"service": m.service},
	})
	m.registry.MustRegister(c)
	return c
}

func (m *Metrics) histogram(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	m.record(name, labels)
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   Namespace,
		Name:        name,
		Help:        help,
		Buckets:     buckets,
		ConstLabels: prometheus.Labels{"service": m.service},
	}, labels)
	m.registry.MustRegister(h)
	return h
}
