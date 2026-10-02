//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
)

// These tests are the durable-state proof for the two handlers M7B adds to the
// production worker, demo.fail and demo.sleep. They run the real handler types
// under the same real components every other end-to-end test here does -- the
// real API over real PostgreSQL, the real publisher over real ElasticMQ, the
// real scheduler and reconciler, and the worker runtime -- and assert the rows
// those components left behind, not a status code.
//
// What they do not cover is registration in the worker binary: that is a unit
// test beside main.go, and make demo / make demo-failure drive the compiled
// binary itself.

// demoAttemptIDs returns a job's attempt ids, oldest first.
func demoAttemptIDs(t *testing.T, jobID uuid.UUID) []uuid.UUID {
	t.Helper()
	rows, err := testPool.Query(context.Background(),
		`SELECT id FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number`, jobID)
	require.NoError(t, err)
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}

func demoRegistry(t *testing.T, jobType string, handler workerruntime.Handler) *workerruntime.Registry {
	t.Helper()
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register(jobType, handler))
	return registry
}

// TestDemoFail_RetryableFailureExhaustsItsBudgetAndIsDeadLettered is the retry
// story the success demonstration shows, asserted in PostgreSQL: a retryable
// failure is retried while budget remains, and the attempt that spends the last
// of it dead-letters the job with the exhausted-budget reason.
func TestDemoFail_RetryableFailureExhaustsItsBudgetAndIsDeadLettered(t *testing.T) {
	stack := startE2EStack(t, demoRegistry(t, "demo.fail", workerruntime.DemoFail{}), time.Minute)
	stack.startWorker(t, "demo-fail-retryable-worker", 2)

	job := stack.submit(t, "demo-fail-retryable",
		`{"queue":"default","job_type":"demo.fail","payload":{"class":"retryable"},"max_attempts":2}`)
	jobID := uuid.MustParse(job.ID)

	awaitJobStatus(t, jobID, "DEAD_LETTERED", 45*time.Second)

	require.Equal(t, []string{"FAILED", "FAILED"}, attemptHistory(t, jobID),
		"two failed attempts: the retry happened, and then the budget was spent")
	attempts := demoAttemptIDs(t, jobID)
	require.Len(t, attempts, 2)

	first := readAttemptOutcome(t, attempts[0])
	require.Equal(t, "RETRYABLE", *first.failureClass)
	require.Equal(t, "demo_failure", *first.errorCode)
	require.NotNil(t, first.retryAt, "the first failure scheduled a retry, and recorded when")
	require.NotNil(t, first.retryDelayMs, "and the delay that produced that instant")
	require.NotNil(t, first.finishedAt)

	second := readAttemptOutcome(t, attempts[1])
	require.Equal(t, "RETRYABLE", *second.failureClass)
	require.Equal(t, "demo_failure", *second.errorCode)
	require.Nil(t, second.retryAt, "the last attempt scheduled nothing: there was no budget left to retry with")
	require.Nil(t, second.retryDelayMs)

	require.Equal(t, 1, countRows(t, "dlq_entries"), "exactly one dead-letter entry")
	require.Equal(t, []string{string(lifecycle.ReasonAttemptsExhausted)}, dlqRows(t, jobID))
	var terminal uuid.UUID
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT terminal_attempt_id FROM dlq_entries WHERE job_id = $1`, jobID).Scan(&terminal))
	require.Equal(t, attempts[1], terminal, "the entry names the attempt that spent the budget")

	require.Equal(t, []string{"RELEASED", "RELEASED"}, leaseHistory(t, jobID))
	require.Equal(t, 0, countActiveLeases(t))
	require.Equal(t, 0, countRows(t, "results"), "a failed job has no result")
}

// TestDemoFail_PermanentFailureDeadLettersImmediatelyWithBudgetRemaining proves
// the other class: the same job, with two attempts of budget still unspent, is
// dead-lettered after one, with no retry scheduled.
func TestDemoFail_PermanentFailureDeadLettersImmediatelyWithBudgetRemaining(t *testing.T) {
	stack := startE2EStack(t, demoRegistry(t, "demo.fail", workerruntime.DemoFail{}), time.Minute)
	stack.startWorker(t, "demo-fail-permanent-worker", 2)

	job := stack.submit(t, "demo-fail-permanent",
		`{"queue":"default","job_type":"demo.fail","payload":{"class":"permanent"},"max_attempts":3}`)
	jobID := uuid.MustParse(job.ID)

	awaitJobStatus(t, jobID, "DEAD_LETTERED", 45*time.Second)

	require.Equal(t, []string{"FAILED"}, attemptHistory(t, jobID),
		"one attempt: a permanent failure does not spend the remaining two")
	attempts := demoAttemptIDs(t, jobID)
	require.Len(t, attempts, 1)

	row := readAttemptOutcome(t, attempts[0])
	require.Equal(t, "PERMANENT", *row.failureClass)
	require.Equal(t, "demo_failure", *row.errorCode)
	require.Nil(t, row.retryAt, "nothing was scheduled")
	require.Nil(t, row.retryDelayMs)

	require.Equal(t, 1, countRows(t, "dlq_entries"))
	require.Equal(t, []string{string(lifecycle.ReasonPermanentFailure)}, dlqRows(t, jobID))
	var terminal uuid.UUID
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT terminal_attempt_id FROM dlq_entries WHERE job_id = $1`, jobID).Scan(&terminal))
	require.Equal(t, attempts[0], terminal)

	require.Equal(t, []string{"RELEASED"}, leaseHistory(t, jobID))
	require.Equal(t, 0, countActiveLeases(t))
}

// sleepProbe is the real demo.sleep handler plus a signal that it is executing,
// so a test can cancel while the sleep is genuinely underway rather than
// guessing from a status when that is. Everything else is DemoSleep's own
// behavior, which is the thing under test.
type sleepProbe struct {
	entered chan struct{}
	once    atomic.Bool
}

func (p *sleepProbe) Execute(ctx context.Context, execution workerruntime.Execution) (json.RawMessage, error) {
	if p.once.CompareAndSwap(false, true) {
		close(p.entered)
	}
	return workerruntime.DemoSleep{}.Execute(ctx, execution)
}

// TestDemoSleep_CancellationMidRunIsAcknowledgedCooperatively is the property
// demo.sleep exists to make demonstrable: a running sleep is stopped by the
// operator's cancel, and the worker reports it as a cancellation.
//
// A sleep of an hour is the point. If the handler waited for its timer instead
// of its context, nothing would end this test but its own deadline.
func TestDemoSleep_CancellationMidRunIsAcknowledgedCooperatively(t *testing.T) {
	probe := &sleepProbe{entered: make(chan struct{})}
	stack := startE2EStack(t, demoRegistry(t, "demo.sleep", probe), time.Minute)
	stack.startWorker(t, "demo-sleep-cancel-worker", 2)

	job := stack.submit(t, "demo-sleep-cancel",
		`{"queue":"default","job_type":"demo.sleep","payload":{"duration_ms":3600000},"timeout_seconds":3600}`)
	jobID := uuid.MustParse(job.ID)

	select {
	case <-probe.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the sleep handler never started executing")
	}
	awaitJobStatus(t, jobID, "RUNNING", 10*time.Second)
	require.Equal(t, []string{"RUNNING"}, attemptHistory(t, jobID), "the attempt is mid-run when cancel arrives")

	response, body := stack.post(t, "/v1/jobs/"+job.ID+"/cancel", "")
	require.Equal(t, http.StatusOK, response.StatusCode)
	var cancelBody api.CancelResponse
	require.NoError(t, json.Unmarshal(body, &cancelBody))
	require.Equal(t, "CANCEL_REQUESTED", cancelBody.Status,
		"a running job is asked to stop rather than declared canceled")

	// An hour-long sleep that ends inside this window ended because it was told
	// to stop. That it was the worker that finalized the cancellation, and not
	// the reconciler giving up on a silent one, is what the lease and outcome
	// assertions below establish: only the worker's acknowledgment releases the
	// lease and carries an outcome identity.
	awaitJobStatus(t, jobID, "CANCELED", 30*time.Second)

	require.Equal(t, []string{"CANCELED"}, attemptHistory(t, jobID))
	require.Equal(t, []string{"RELEASED"}, leaseHistory(t, jobID),
		"a cooperative acknowledgment hands authority back rather than losing it")
	require.Equal(t, 0, countActiveLeases(t))

	attempts := demoAttemptIDs(t, jobID)
	require.Len(t, attempts, 1)
	row := readAttemptOutcome(t, attempts[0])
	require.Equal(t, "CANCELED", row.status)
	require.Equal(t, "CANCELED", *row.failureClass)
	require.NotNil(t, row.outcomeID, "the worker acknowledged under a retained outcome identity")
	require.NotNil(t, row.finishedAt)

	require.Empty(t, dlqRows(t, jobID), "cancellation never creates a dead-letter entry")
	require.Equal(t, 0, countRows(t, "results"), "a canceled sleep produced no result")
}
