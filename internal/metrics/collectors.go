package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// scrapeTimeout bounds every query a scrape issues, matching the 2-second
// budget every readiness check already uses. A hung database must not hang a
// scrape any more than it may hang a probe.
const scrapeTimeout = 2 * time.Second

// StateCollector reports the gauges docs/ARCHITECTURE.md §14 names, read from
// PostgreSQL at scrape time rather than counted in process.
//
// That choice is forced by what these numbers mean. "Jobs queued" is a fact
// about the whole system, not about one replica: every component here is safe
// to run with N replicas, so an in-process counter would report one replica's
// share and an operator summing across replicas would still not get the
// answer, because a job queued by replica A and claimed by replica B was never
// counted by either. PostgreSQL is authoritative for this state (ADR-0001), so
// the scrape asks it.
//
// The cost is that a scrape does real queries. They are bounded, they are
// covered by indexes that already exist, and a failure degrades the metric's
// absence rather than the endpoint: promhttp is configured with
// ContinueOnError, so a database outage still serves every other metric —
// including the ones an operator would be reading to diagnose that outage.
type StateCollector struct {
	pool *pgxpool.Pool
	log  *slog.Logger

	jobsQueued     *prometheus.Desc
	jobsRunning    *prometheus.Desc
	leasesActive   *prometheus.Desc
	outboxPending  *prometheus.Desc
	workersByState *prometheus.Desc

	// Durable counters. Monotonic because the rows they count are never
	// deleted and never leave the state being counted -- see collectTotals.
	jobsSubmitted  *prometheus.Desc
	jobsCompleted  *prometheus.Desc
	jobsRetried    *prometheus.Desc
	jobsDeadLetter *prometheus.Desc
	claims         *prometheus.Desc
}

// NewStateCollector builds the collector and registers it on m's registry.
func NewStateCollector(m *Metrics, pool *pgxpool.Pool, log *slog.Logger) *StateCollector {
	constant := prometheus.Labels{"service": m.service}
	c := &StateCollector{
		pool: pool,
		log:  log,
		jobsQueued: prometheus.NewDesc(
			Namespace+"_jobs_queued",
			"Jobs currently eligible to be claimed.",
			[]string{"queue"}, constant),
		jobsRunning: prometheus.NewDesc(
			Namespace+"_jobs_running",
			"Jobs currently held by an attempt (LEASED or RUNNING).",
			[]string{"queue"}, constant),
		leasesActive: prometheus.NewDesc(
			Namespace+"_leases_active",
			"Leases currently holding authority.",
			[]string{"queue"}, constant),
		outboxPending: prometheus.NewDesc(
			Namespace+"_outbox_events_pending",
			"Outbox events awaiting publication. A rising value is the primary signal that delivery is broken.",
			nil, constant),
		workersByState: prometheus.NewDesc(
			Namespace+"_workers",
			"Logical workers by the status of their most recent session.",
			[]string{"worker_group", "status"}, constant),
		jobsSubmitted: prometheus.NewDesc(
			Namespace+"_jobs_submitted_total",
			"Jobs durably accepted, cumulative.",
			[]string{"queue"}, constant),
		jobsCompleted: prometheus.NewDesc(
			Namespace+"_jobs_completed_total",
			"Jobs that reached a terminal status, cumulative.",
			[]string{"queue", "status"}, constant),
		jobsRetried: prometheus.NewDesc(
			Namespace+"_jobs_retried_total",
			"Attempts beyond the first, cumulative. An attempt beyond the first exists only because an earlier one did not succeed.",
			[]string{"queue"}, constant),
		jobsDeadLetter: prometheus.NewDesc(
			Namespace+"_jobs_dead_lettered_total",
			"Logical dead-letter entries, cumulative.",
			[]string{"queue", "dlq_reason"}, constant),
		claims: prometheus.NewDesc(
			Namespace+"_claims_total",
			"Committed claims, cumulative. Every attempt row is one committed claim.",
			[]string{"queue"}, constant),
	}

	// The same rule every other metric obeys, applied to a hand-built
	// Collector: these Descs do not go through Metrics.counter/gauge, so the
	// check is made explicitly rather than skipped.
	for name, labels := range map[string][]string{
		Namespace + "_jobs_queued":              {"queue"},
		Namespace + "_jobs_running":             {"queue"},
		Namespace + "_leases_active":            {"queue"},
		Namespace + "_outbox_events_pending":    nil,
		Namespace + "_workers":                  {"worker_group", "status"},
		Namespace + "_jobs_submitted_total":     {"queue"},
		Namespace + "_jobs_completed_total":     {"queue", "status"},
		Namespace + "_jobs_retried_total":       {"queue"},
		Namespace + "_jobs_dead_lettered_total": {"queue", "dlq_reason"},
		Namespace + "_claims_total":             {"queue"},
	} {
		if problems := checkLabels(name, labels); len(problems) > 0 {
			panic("metrics: label rule violated by StateCollector: " + problems[0])
		}
		m.manifest[name] = labels
	}

	m.registry.MustRegister(c)
	return c
}

// Describe implements prometheus.Collector.
func (c *StateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobsQueued
	ch <- c.jobsRunning
	ch <- c.leasesActive
	ch <- c.outboxPending
	ch <- c.workersByState
	ch <- c.jobsSubmitted
	ch <- c.jobsCompleted
	ch <- c.jobsRetried
	ch <- c.jobsDeadLetter
	ch <- c.claims
}

// Collect implements prometheus.Collector.
//
// Each query is independent: one failing reports only its own absence, because
// a broken worker-status query must not also hide the queue depth an operator
// is trying to read.
func (c *StateCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()

	// Queue depth, split into the two gauges §14 names. Served by
	// jobs_scope_queue_depth_idx's leading columns; this aggregates across
	// scopes deliberately, because scope is forbidden as a label.
	c.collectByQueue(ctx, ch, c.jobsQueued, "jobs queued", `
		SELECT queue, count(*) FROM jobs WHERE status = 'QUEUED' GROUP BY queue`)
	c.collectByQueue(ctx, ch, c.jobsRunning, "jobs running", `
		SELECT queue, count(*) FROM jobs WHERE status IN ('LEASED', 'RUNNING') GROUP BY queue`)
	c.collectByQueue(ctx, ch, c.leasesActive, "active leases", `
		SELECT queue, count(*) FROM leases WHERE status = 'ACTIVE' GROUP BY queue`)

	var pending float64
	if err := c.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE status = 'PENDING'`).Scan(&pending); err != nil {
		c.warn("pending outbox events", err)
	} else {
		ch <- prometheus.MustNewConstMetric(c.outboxPending, prometheus.GaugeValue, pending)
	}

	c.collectWorkers(ctx, ch)
	c.collectTotals(ctx, ch)
}

// collectTotals reports the cumulative job-lifecycle counters.
//
// These are counters read from durable state rather than incremented in
// process, for two reasons that both matter.
//
// They are correct across replicas. Every component here is safe to run with N
// replicas, so an in-process counter would report one replica's share, and an
// operator summing across replicas would still get the wrong answer because a
// job submitted by replica A and completed by replica B was counted by neither
// for the transition it did not see.
//
// And they touch no fenced-transition code. Incrementing at the moment of a
// terminal transition would mean editing internal/workers' outcome paths --
// the code that owns fencing and the transition matrix -- to add a side effect
// that has nothing to do with correctness.
//
// Every one is genuinely monotonic, which is what a Prometheus counter
// requires. Jobs, attempts and dlq_entries are never deleted (every foreign
// key to them is ON DELETE RESTRICT), and a terminal job never returns to a
// non-terminal state (reliability invariant 2), so none of these counts can
// decrease. A counter that could decrease would be silently wrong in every
// rate() over it.
func (c *StateCollector) collectTotals(ctx context.Context, ch chan<- prometheus.Metric) {
	c.collectCounterByQueue(ctx, ch, c.jobsSubmitted, "jobs submitted", `
		SELECT queue, count(*) FROM jobs GROUP BY queue`)
	c.collectCounterByQueue(ctx, ch, c.jobsRetried, "jobs retried", `
		SELECT queue, count(*) FROM job_attempts WHERE attempt_number > 1 GROUP BY queue`)
	c.collectCounterByQueue(ctx, ch, c.claims, "claims", `
		SELECT queue, count(*) FROM job_attempts GROUP BY queue`)

	c.collectCounterByTwo(ctx, ch, c.jobsCompleted, "jobs completed", `
		SELECT queue, status, count(*) FROM jobs
		WHERE status IN ('SUCCEEDED', 'CANCELED', 'DEAD_LETTERED')
		GROUP BY queue, status`)
	c.collectCounterByTwo(ctx, ch, c.jobsDeadLetter, "jobs dead-lettered", `
		SELECT queue, reason, count(*) FROM dlq_entries GROUP BY queue, reason`)
}

func (c *StateCollector) collectCounterByQueue(
	ctx context.Context, ch chan<- prometheus.Metric, desc *prometheus.Desc, what, query string,
) {
	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		c.warn(what, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var queue string
		var count float64
		if err := rows.Scan(&queue, &count); err != nil {
			c.warn(what, err)
			return
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, count, queue)
	}
	if err := rows.Err(); err != nil {
		c.warn(what, err)
	}
}

func (c *StateCollector) collectCounterByTwo(
	ctx context.Context, ch chan<- prometheus.Metric, desc *prometheus.Desc, what, query string,
) {
	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		c.warn(what, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var first, second string
		var count float64
		if err := rows.Scan(&first, &second, &count); err != nil {
			c.warn(what, err)
			return
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, count, first, second)
	}
	if err := rows.Err(); err != nil {
		c.warn(what, err)
	}
}

func (c *StateCollector) collectByQueue(
	ctx context.Context, ch chan<- prometheus.Metric, desc *prometheus.Desc, what, query string,
) {
	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		c.warn(what, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var queue string
		var count float64
		if err := rows.Scan(&queue, &count); err != nil {
			c.warn(what, err)
			return
		}
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, count, queue)
	}
	if err := rows.Err(); err != nil {
		c.warn(what, err)
	}
}

// collectWorkers counts logical workers by their MOST RECENT session's status,
// whatever that status is.
//
// Same shape, and the same reasoning, as M6A's ListWorkers: a session that
// crashed is UNHEALTHY and a replaced one is OFFLINE, and both fall outside
// worker_sessions_one_current_per_worker_idx's predicate. Counting only
// current sessions would report the crashed workers an operator is looking for
// as simply absent. Served by worker_sessions_latest_per_worker_idx.
func (c *StateCollector) collectWorkers(ctx context.Context, ch chan<- prometheus.Metric) {
	rows, err := c.pool.Query(ctx, `
		SELECT s.worker_group, s.status, count(*)
		FROM workers w
		CROSS JOIN LATERAL (
			SELECT worker_group, status
			FROM worker_sessions ws
			WHERE ws.worker_id = w.id
			ORDER BY ws.registered_at DESC, ws.id DESC
			LIMIT 1
		) s
		GROUP BY s.worker_group, s.status`)
	if err != nil {
		c.warn("workers by state", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var group, status string
		var count float64
		if err := rows.Scan(&group, &status, &count); err != nil {
			c.warn("workers by state", err)
			return
		}
		ch <- prometheus.MustNewConstMetric(c.workersByState, prometheus.GaugeValue, count, group, status)
	}
	if err := rows.Err(); err != nil {
		c.warn("workers by state", err)
	}
}

func (c *StateCollector) warn(what string, err error) {
	if c.log == nil {
		return
	}
	// Warn, not Error: a scrape that could not read one gauge is a degraded
	// observation, not a failure of the process being observed.
	c.log.Warn("metrics scrape query failed",
		slog.String("metric", what), slog.String("error", err.Error()))
}
