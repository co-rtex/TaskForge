// Package metrics implements TaskForge's Prometheus instrumentation.
//
// The load-bearing property here is not which metrics exist — it is that no
// metric can acquire an unbounded label. docs/ARCHITECTURE.md §14 states the
// rule in prose ("Unbounded values — job ids, attempt ids, request ids — are
// never used as metric labels"); this package makes it a property a reviewer
// can check by running a test rather than by reading every call site.
//
// It is enforced structurally, in two independent directions:
//
//   - Every collector is built through the newXxx helpers in metrics.go, which
//     record the metric's declared label names in a manifest. A collector that
//     skipped them would not be registered and would emit nothing, so the
//     manifest cannot silently miss one.
//   - AllowedLabels below is an explicit, justified allowlist, and
//     ForbiddenLabels is an independent denylist of the names that must never
//     appear. Checking both means an allowlist edited carelessly still cannot
//     let a forbidden name back in.
package metrics

import "strings"

// AllowedLabels is every label name any TaskForge metric may carry, with the
// bound that makes each one safe.
//
// Adding a name here is a deliberate act that a reviewer sees in the diff. The
// justification is the point of the map: a label whose bound cannot be stated
// in one line does not belong on a metric.
var AllowedLabels = map[string]string{
	"service": "one of five binaries; set by the process, never by input",

	"queue": "foreign key to queues(name). No API creates a queue; the only " +
		"row is migration 0001's seed. Operator-provisioned, not caller-minted.",

	"status": "jobs.status CHECK constraint: 9 values (migrations/0001).",

	"attempt_status": "job_attempts.status CHECK constraint: 7 values (migrations/0002).",

	"failure_class": "lifecycle.FailureClass: 5 values.",

	"dlq_reason": "lifecycle.DLQReason: 2 values.",

	"worker_group": "worker_sessions.worker_group, regex-bounded. The regex is " +
		"the same SHAPE as job_type's, but registration is gated by " +
		"requireWorkerKey, so an operator names a handful of pools at deploy " +
		"time rather than a public caller minting one per request. That " +
		"distinction, not the regex, is why this is safe.",

	"job_type": "bounded by BoundJobType against the worker's own handler " +
		"registry, ceiling len(Registry.Types())+1. Permitted ONLY on " +
		"worker-emitted metrics; see BoundJobType.",

	"route": "the mux's matched route pattern, reused from the value M6B's " +
		"span naming already computes. Never the raw path.",

	"method": "HTTP verbs.",

	"code": "HTTP status codes.",

	"repair": "the reconciler's own fixed set of repair kinds.",
}

// ForbiddenLabels are names that must never appear on any metric, checked
// independently of AllowedLabels.
//
// Most are unbounded identifiers. `scope` is the exception and is forbidden for
// a different reason: it is bounded by the number of minted credentials, but it
// is a tenancy identifier and /metrics is unauthenticated, so a scrape would
// become a tenant-enumeration oracle — the same disclosure the indistinguishable
// 401 in api/openapi.yaml exists to prevent.
var ForbiddenLabels = []string{
	"job_id",
	"attempt_id",
	"request_id",
	"lease_id",
	"worker_session_id",
	"session_id",
	"outcome_request_id",
	"claim_request_id",
	"event_id",
	"idempotency_key",
	"worker_name",
	"worker_id",
	"hostname",
	"path",
	"url",
	"scope",
	"traceparent",
	"trace_id",
	"error_message",
}

// UnknownJobType is what BoundJobType reports for anything the emitting
// process does not have a registered handler for.
const UnknownJobType = "other"

// BoundJobType caps the one genuinely caller-reachable label.
//
// jobs.job_type has no foreign key and no allowlist — only the regex
// `^[a-z0-9][a-z0-9._-]{0,127}$`, enforced identically in the schema and at
// submission validation. Any authenticated caller can therefore mint a new
// value per request, which is cardinality explosion from ordinary use rather
// than from abuse.
//
// The bound is the emitting worker's OWN handler registry rather than a
// configured allowlist. A configured list would be a second copy of the
// registry that has to be kept in sync by hand, and the registry is already
// authoritative, already bounded, and already the thing that decides whether
// this process can execute a job type at all.
//
// This is also why job_type appears only on worker-emitted metrics. The API
// has no registry — it accepts any well-formed job_type — so its own metrics
// carry no job_type label at all rather than carrying a poorly-bounded one.
func BoundJobType(registered []string, jobType string) string {
	for _, known := range registered {
		if known == jobType {
			return jobType
		}
	}
	return UnknownJobType
}

// checkLabels reports every label name that is not allowed, and every
// forbidden name present. Used by the enforcement tests and by New, so a
// violation fails at process startup rather than only under test.
func checkLabels(metric string, labels []string) []string {
	var problems []string
	for _, label := range labels {
		if _, ok := AllowedLabels[label]; !ok {
			problems = append(problems,
				metric+": label "+label+" is not in AllowedLabels")
		}
		for _, forbidden := range ForbiddenLabels {
			if strings.EqualFold(label, forbidden) {
				problems = append(problems,
					metric+": label "+label+" is forbidden (unbounded or tenant-identifying)")
			}
		}
	}
	return problems
}
