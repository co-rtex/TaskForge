package metrics

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNoUnboundedLabels is the milestone's load-bearing assertion.
//
// It checks the MANIFEST rather than a Gather() walk, and that distinction is
// the whole point. Gather() reports only metric families that have emitted at
// least one sample, so a collector nobody has incremented yet would be
// invisible to it — and a brand-new metric with a bad label is exactly the
// case that has emitted nothing. The manifest is populated by the same
// constructors that register the collectors, so it is complete by construction.
func TestNoUnboundedLabels(t *testing.T) {
	manifest := New("taskforge-test").Manifest()
	require.NotEmpty(t, manifest, "the manifest must not be vacuously empty")

	for metric, labels := range manifest {
		for _, label := range labels {
			justification, allowed := AllowedLabels[label]
			require.Truef(t, allowed,
				"%s carries label %q, which is not in AllowedLabels. Adding it there "+
					"requires stating in one line why it is bounded.", metric, label)
			require.NotEmptyf(t, justification,
				"label %q is allowed but carries no justification", label)
		}
	}
}

// TestForbiddenLabelNames checks the same property from the other direction.
//
// AllowedLabels and ForbiddenLabels are independent lists, so an allowlist
// edited carelessly still cannot let a forbidden name back in silently.
func TestForbiddenLabelNames(t *testing.T) {
	manifest := New("taskforge-test").Manifest()

	for metric, labels := range manifest {
		for _, label := range labels {
			for _, forbidden := range ForbiddenLabels {
				require.NotEqualf(t, strings.ToLower(forbidden), strings.ToLower(label),
					"%s carries forbidden label %q", metric, label)
			}
		}
	}
}

// The two lists must not contradict each other. A name in both would make the
// two tests above disagree about the same metric, and whichever ran first
// would decide the answer.
func TestAllowedAndForbiddenDoNotOverlap(t *testing.T) {
	for _, forbidden := range ForbiddenLabels {
		_, allowed := AllowedLabels[forbidden]
		require.Falsef(t, allowed,
			"%q appears in both AllowedLabels and ForbiddenLabels", forbidden)
	}
}

// New refuses a bad label at construction, so a violation is a startup failure
// rather than something only a test would find.
//
// This also proves the enforcement tests above are not vacuous: the rule they
// check is the same one the constructor applies.
func TestNew_PanicsOnAForbiddenLabel(t *testing.T) {
	m := &Metrics{manifest: map[string][]string{}}

	require.PanicsWithValue(t,
		"metrics: label rule violated: bad_metric: label job_id is not in AllowedLabels; "+
			"bad_metric: label job_id is forbidden (unbounded or tenant-identifying)",
		func() { m.record("bad_metric", []string{"job_id"}) },
		"a forbidden label must fail loudly at construction")

	require.Panics(t, func() { m.record("bad_metric", []string{"invented_label"}) },
		"a label absent from the allowlist must fail even if it is not explicitly forbidden")
}

// --- the job_type bound -----------------------------------------------------

// The one caller-reachable label is capped by the worker's own registry.
//
// jobs.job_type has no foreign key and no allowlist, so an authenticated caller
// mints a new value per request. Ten thousand of them must not become ten
// thousand series.
func TestBoundJobType_CapsCardinalityAtTheRegistrySize(t *testing.T) {
	registered := []string{"demo.echo"}

	series := map[string]struct{}{}
	for i := 0; i < 10_000; i++ {
		// The shape a caller can actually submit: the schema's regex permits
		// any lowercase alphanumeric string up to 128 characters.
		series[BoundJobType(registered, "attack.type."+strings.Repeat("a", i%120)+itoa(i))] = struct{}{}
	}
	// Every registered type must survive unmapped, or the metric would be
	// useless for the types that actually run.
	for _, known := range registered {
		series[BoundJobType(registered, known)] = struct{}{}
	}

	require.Len(t, series, len(registered)+1,
		"10,000 distinct submitted job types must collapse to the registry plus %q",
		UnknownJobType)

	names := make([]string, 0, len(series))
	for name := range series {
		names = append(names, name)
	}
	sort.Strings(names)
	require.Equal(t, []string{"demo.echo", UnknownJobType}, names)
}

// A registered type is reported as itself; anything else is "other".
func TestBoundJobType_PreservesRegisteredTypes(t *testing.T) {
	registered := []string{"demo.echo", "demo.sleep"}

	require.Equal(t, "demo.echo", BoundJobType(registered, "demo.echo"))
	require.Equal(t, "demo.sleep", BoundJobType(registered, "demo.sleep"))
	require.Equal(t, UnknownJobType, BoundJobType(registered, "demo.echo.evil"))
	require.Equal(t, UnknownJobType, BoundJobType(registered, ""))
	require.Equal(t, UnknownJobType, BoundJobType(nil, "demo.echo"),
		"a process with no registry bounds everything to other")
}

// job_type must appear ONLY on worker-emitted metrics.
//
// The API has no handler registry, so it cannot bound the value; a job_type
// label on an API metric would be the unbounded one this whole mechanism
// exists to prevent. This pins which metrics may carry it.
func TestJobTypeAppearsOnlyOnWorkerEmittedMetrics(t *testing.T) {
	manifest := New("taskforge-test").Manifest()

	carriers := []string{}
	for metric, labels := range manifest {
		for _, label := range labels {
			if label == "job_type" {
				carriers = append(carriers, metric)
			}
		}
	}
	sort.Strings(carriers)

	require.Equal(t, []string{Namespace + "_execution_duration_seconds"}, carriers,
		"job_type may only appear on worker-emitted metrics, which are the only "+
			"ones with a handler registry to bound it against")
}

// itoa avoids importing strconv for one call in a table.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}
