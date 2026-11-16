//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
)

// TestMultiTenantDispatch_EveryScopesJobsAreClaimedWithoutRenotification is the
// workload the audit's first defect needed and no benchmark had: several scopes'
// workers running at once through the real API, publisher, scheduler and
// workers. Every existing benchmark and demonstration uses a broker queue and a
// scope of its own, which is exactly why none of them showed the defect.
//
// Re-notification is an hour away, so every job must be discovered from its own
// submission notification. Before ADR-0029 a scope's notification could be
// deleted by another scope's worker answering QUEUE_EMPTY, or spent claiming
// that worker's own job, and the job then waited for re-notification.
//
// TASKFORGE_MULTITENANT_SCOPES and TASKFORGE_MULTITENANT_JOBS_PER_SCOPE enlarge
// it into a measurement; the defaults keep it a regression test. It logs each
// job's dispatch latency (first claim minus submission, both PostgreSQL time).
func TestMultiTenantDispatch_EveryScopesJobsAreClaimedWithoutRenotification(t *testing.T) {
	scopes := envInt(t, "TASKFORGE_MULTITENANT_SCOPES", 4)
	perScope := envInt(t, "TASKFORGE_MULTITENANT_JOBS_PER_SCOPE", 5)
	deadline := 20 * time.Second

	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	stack := startE2EStack(t, registry, time.Hour)

	names := make([]string, scopes)
	for i := range names {
		names[i] = fmt.Sprintf("integration-tenant-%02d", i)
		key := mintWorkerKey(t, names[i], "tenant-worker")
		stack.startScopedWorker(t, key.Raw, fmt.Sprintf("tenant-%02d-worker", i), []string{"cpu"})
	}
	for i := range names {
		name := fmt.Sprintf("tenant-%02d-worker", i)
		eventually(t, 10*time.Second, name+" is healthy", func() bool {
			return healthySessions(t, name, names[i]) == 1
		})
	}

	// Interleaved across scopes, the order in which notifications from
	// different tenants meet one another on a shared queue.
	for j := 0; j < perScope; j++ {
		for _, scope := range names {
			createJobInScope(t, scope, fmt.Sprintf("mt-%s-%d", scope, j), "default", "demo.echo")
		}
	}
	total := scopes * perScope

	started := time.Now()
	var claimed int
	for time.Since(started) < deadline {
		require.NoError(t, testPool.QueryRow(context.Background(),
			`SELECT count(DISTINCT job_id) FROM job_attempts`).Scan(&claimed))
		if claimed == total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	rows, err := testPool.Query(context.Background(), `
		SELECT EXTRACT(EPOCH FROM (min(l.acquired_at) - j.created_at)) * 1000
		FROM jobs j JOIN leases l ON l.job_id = j.id
		GROUP BY j.id, j.created_at`)
	require.NoError(t, err)
	var latencies []float64
	for rows.Next() {
		var ms float64
		require.NoError(t, rows.Scan(&ms))
		latencies = append(latencies, ms)
	}
	require.NoError(t, rows.Err())
	sort.Float64s(latencies)
	var crossScope int
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM leases l
		JOIN outbox_events o ON o.id = l.claim_request_id
		JOIN jobs j ON j.id = o.job_id
		WHERE j.scope <> l.scope`).Scan(&crossScope))
	t.Logf("scopes=%d jobs=%d claimed within %s: %d; leases taken under another scope's notification: %d; "+
		"dispatch latency ms: p50=%s p99=%s max=%s",
		scopes, total, deadline, claimed, crossScope,
		nearestRank(latencies, 50), nearestRank(latencies, 99), nearestRank(latencies, 100))

	require.Equal(t, total, claimed,
		"every scope's every job must be discovered from its own notification, with re-notification an hour away")
	require.Zero(t, crossScope, "no lease may be taken under another scope's notification")
	require.Equal(t, total, countPublishedOutbox(t), "one notification per job: none was re-notified")
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	require.NoErrorf(t, err, "%s must be an integer", name)
	require.Positivef(t, n, "%s must be positive", name)
	return n
}

// nearestRank is the nearest-rank percentile ADR-0020 uses, or "-" for none.
func nearestRank(sorted []float64, p int) string {
	if len(sorted) == 0 {
		return "-"
	}
	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	return strconv.FormatFloat(sorted[rank-1], 'f', 1, 64)
}
