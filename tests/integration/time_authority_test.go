//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// hostileWorkerTime is a time no honest clock could report. A worker that could
// make the control plane believe it would own a lease or a heartbeat for the
// next seventy-odd years.
const hostileWorkerTime = "2100-01-01T00:00:00Z"

// workerControlLeaseDuration is what workerControlForAPI configures, which is
// the store behind the API these tests talk to.
const workerControlLeaseDuration = 30 * time.Second

// controlPost sends one worker-control request exactly as a worker would, plus
// whatever the caller adds to it, and returns the status and the stable error
// code. The suite's transport supplies no credential here: after registration
// the session identity is the credential (ADR-0014).
func controlPost(t *testing.T, base, path string, body map[string]any, headers map[string]string) (int, string) {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(encoded))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	require.NoError(t, err)

	var failure api.ErrorBody
	if response.StatusCode >= http.StatusBadRequest {
		require.NoError(t, json.Unmarshal(raw, &failure))
	}
	return response.StatusCode, failure.Error.Code
}

// withTime returns a copy of body with extra fields added: the worker-supplied
// time a hostile or buggy client would try to slip in.
func withTime(body map[string]any, fields ...string) map[string]any {
	hostile := maps.Clone(body)
	for _, field := range fields {
		hostile[field] = hostileWorkerTime
	}
	return hostile
}

// requireWithin asserts a stored timestamp lies inside the window PostgreSQL's
// own clock bracketed around the call that wrote it. A timestamp that came from
// anywhere else -- and every hostile value here is a century away -- cannot.
func requireWithin(t *testing.T, got, lo, hi time.Time, what string) {
	t.Helper()
	require.Falsef(t, got.Before(lo), "%s = %s precedes PostgreSQL's clock at the call (%s)", what, got, lo)
	require.Falsef(t, got.After(hi), "%s = %s follows PostgreSQL's clock after the call (%s)", what, got, hi)
}

// TestWorkerSuppliedTimeIsNeverAuthoritative proves reliability invariant 18
// for every worker-control call that writes a timestamp: heartbeat, claim,
// start, renewal, success, failure, and cancellation acknowledgment.
//
// The existing heartbeat test shows the stored time comes from PostgreSQL, but
// the request has no time field at all, so that could be mere absence. This test
// goes further and OFFERS a worker-supplied time to each call, two ways:
//
//   - in the body. api/openapi.yaml documents unknown fields on these routes as
//     a 400, so each is refused, and the job's complete durable state must be
//     byte-for-byte unchanged afterwards;
//   - in headers (Date and custom ones), which an HTTP server accepts and must
//     simply ignore, so the call succeeds and what it stores must still be
//     PostgreSQL's reading, bracketed by the database clock around the call.
//
// For every body case the same request without the time is then sent and must be
// an ordinary success, which is what shows the extra field -- and not some other
// defect in the body -- was the thing refused.
func TestWorkerSuppliedTimeIsNeverAuthoritative(t *testing.T) {
	reset(t)
	ctx := context.Background()
	server := newAPI(t)
	client := workerruntime.NewClient(server.URL, &http.Client{Timeout: 10 * time.Second}, currentWorkerKey())
	control := workerControlForAPI()

	session, err := client.Register(ctx,
		workerRegistration("time-authority", 3, nil, []string{"demo.echo"}))
	require.NoError(t, err)
	sessionBody := map[string]any{"worker_id": session.WorkerID.String()}

	fenceBody := func(f workers.Fence) map[string]any {
		return map[string]any{
			"job_id": f.JobID.String(), "lease_id": f.LeaseID.String(),
			"worker_id": f.WorkerID.String(), "worker_session_id": f.SessionID.String(),
		}
	}
	attemptRow := func(attemptID uuid.UUID) (startedAt, timeoutAt, finishedAt, retryAt *time.Time) {
		require.NoError(t, testPool.QueryRow(ctx,
			`SELECT started_at, timeout_at, finished_at, retry_at FROM job_attempts WHERE id = $1`, attemptID,
		).Scan(&startedAt, &timeoutAt, &finishedAt, &retryAt))
		return
	}

	// --- heartbeat -----------------------------------------------------------
	heartbeatPath := "/internal/v1/worker-sessions/" + session.ID.String() + "/heartbeat"
	storedHeartbeat := func() time.Time {
		var at time.Time
		require.NoError(t, testPool.QueryRow(ctx,
			`SELECT last_heartbeat_at FROM worker_sessions WHERE id = $1`, session.ID).Scan(&at))
		return at
	}

	baseline := storedHeartbeat()
	status, code := controlPost(t, server.URL, heartbeatPath, withTime(sessionBody, "last_heartbeat_at"), nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, api.CodeMalformedJSON, code)
	require.True(t, baseline.Equal(storedHeartbeat()), "a refused heartbeat records nothing")

	for name, headers := range map[string]map[string]string{
		"a far-future time": {
			"Date": "Fri, 01 Jan 2100 00:00:00 GMT", "X-Worker-Time": hostileWorkerTime, "X-Timestamp": hostileWorkerTime,
		},
		"a far-past time": {
			"Date": "Thu, 01 Jan 1970 00:00:00 GMT", "X-Worker-Time": "1970-01-01T00:00:00Z", "X-Timestamp": "1970-01-01T00:00:00Z",
		},
	} {
		lo := serverNow(t)
		status, _ = controlPost(t, server.URL, heartbeatPath, sessionBody, headers)
		hi := serverNow(t)
		require.Equal(t, http.StatusOK, status, name)
		requireWithin(t, storedHeartbeat(), lo, hi, "last_heartbeat_at with "+name+" in the headers")
	}

	// --- claim ---------------------------------------------------------------
	first := createJob(t, "time-authority-success", "demo.echo", 50, nil)
	claimBody := map[string]any{
		"worker_id": session.WorkerID.String(), "worker_session_id": session.ID.String(),
		"claim_request_id": uuid.NewString(), "queue": "default",
	}
	beforeClaim := durableSnapshot(t, first)
	status, code = controlPost(t, server.URL, "/internal/v1/claims", withTime(claimBody, "acquired_at", "expires_at"), nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, api.CodeMalformedJSON, code)
	require.Equal(t, beforeClaim, durableSnapshot(t, first), "a refused claim creates no attempt and no lease")
	require.Zero(t, countRows(t, "leases"))

	lo := serverNow(t)
	claim, err := client.Claim(ctx, claimRequest(session, "default"))
	hi := serverNow(t)
	require.NoError(t, err)
	require.Equal(t, workers.Claimed, claim.Disposition)
	fence := assignmentFence(claim.Assignment)
	var acquiredAt, renewedAt, expiresAt time.Time
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT acquired_at, renewed_at, expires_at FROM leases WHERE id = $1`, fence.LeaseID,
	).Scan(&acquiredAt, &renewedAt, &expiresAt))
	requireWithin(t, acquiredAt, lo, hi, "lease acquired_at")
	requireWithin(t, renewedAt, lo, hi, "lease renewed_at")
	requireWithin(t, expiresAt, lo.Add(workerControlLeaseDuration), hi.Add(workerControlLeaseDuration), "lease expires_at")

	// --- start ---------------------------------------------------------------
	startPath := "/internal/v1/attempts/" + fence.AttemptID.String() + "/start"
	beforeStart := durableSnapshot(t, first)
	status, code = controlPost(t, server.URL, startPath, withTime(fenceBody(fence), "started_at", "timeout_at"), nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, api.CodeMalformedJSON, code)
	require.Equal(t, beforeStart, durableSnapshot(t, first), "a refused start leaves the attempt LEASED")

	lo = serverNow(t)
	_, err = client.Start(ctx, fence)
	hi = serverNow(t)
	require.NoError(t, err)
	startedAt, timeoutAt, _, _ := attemptRow(fence.AttemptID)
	requireWithin(t, *startedAt, lo, hi, "attempt started_at")
	const jobTimeout = 300 * time.Second // createJob's default
	requireWithin(t, *timeoutAt, lo.Add(jobTimeout), hi.Add(jobTimeout), "attempt timeout_at")

	// --- renewal -------------------------------------------------------------
	renewPath := "/internal/v1/leases/" + fence.LeaseID.String() + "/renew"
	renewBody := map[string]any{
		"job_id": fence.JobID.String(), "attempt_id": fence.AttemptID.String(),
		"worker_id": fence.WorkerID.String(), "worker_session_id": fence.SessionID.String(),
		"renewal_request_id": uuid.NewString(), "expected_renewal_version": 0,
	}
	beforeRenew := durableSnapshot(t, first)
	status, code = controlPost(t, server.URL, renewPath, withTime(renewBody, "renewed_at", "expires_at"), nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, api.CodeMalformedJSON, code)
	require.Equal(t, beforeRenew, durableSnapshot(t, first), "a refused renewal moves no part of the lease")

	lo = serverNow(t)
	renewal, err := client.RenewLease(ctx, workers.RenewalRequest{
		Fence: fence, RenewalRequestID: uuid.New(), ExpectedVersion: 0,
	})
	hi = serverNow(t)
	require.NoError(t, err)
	require.Equal(t, 1, renewal.RenewalVersion)
	_, renewedExpiry, renewedAtAfter, _ := leaseRow(t, fence.LeaseID)
	requireWithin(t, renewedAtAfter, lo, hi, "lease renewed_at after renewal")
	requireWithin(t, renewedExpiry, lo.Add(workerControlLeaseDuration), hi.Add(workerControlLeaseDuration),
		"lease expires_at after renewal")

	// --- success -------------------------------------------------------------
	succeedPath := "/internal/v1/attempts/" + fence.AttemptID.String() + "/succeed"
	beforeSucceed := durableSnapshot(t, first)
	status, code = controlPost(t, server.URL, succeedPath, withTime(fenceBody(fence), "finished_at"), nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, api.CodeMalformedJSON, code)
	require.Equal(t, beforeSucceed, durableSnapshot(t, first), "a refused success leaves the attempt RUNNING")

	lo = serverNow(t)
	require.NoError(t, client.Succeed(ctx, fence, nil))
	hi = serverNow(t)
	_, _, finishedAt, _ := attemptRow(fence.AttemptID)
	requireWithin(t, *finishedAt, lo, hi, "attempt finished_at after success")
	var releasedAt time.Time
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT released_at FROM leases WHERE id = $1`, fence.LeaseID).Scan(&releasedAt))
	requireWithin(t, releasedAt, lo, hi, "lease released_at after success")

	// --- failure -------------------------------------------------------------
	failing := createJob(t, "time-authority-failure", "demo.echo", 50, nil)
	failClaim, err := client.Claim(ctx, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, failing, failClaim.Assignment.JobID)
	failFence := assignmentFence(failClaim.Assignment)
	_, err = client.Start(ctx, failFence)
	require.NoError(t, err)

	report := failureReport(failFence, lifecycle.ClassRetryable, "transient", "")
	failPath := "/internal/v1/attempts/" + failFence.AttemptID.String() + "/fail"
	failBody := fenceBody(failFence)
	failBody["outcome_request_id"] = report.OutcomeRequestID.String()
	failBody["failure_class"] = string(report.Class)
	failBody["error_code"] = report.ErrorCode
	failBody["error_message"] = report.ErrorMessage
	beforeFail := durableSnapshot(t, failing)
	status, code = controlPost(t, server.URL, failPath, withTime(failBody, "finished_at", "retry_at"), nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, api.CodeMalformedJSON, code)
	require.Equal(t, beforeFail, durableSnapshot(t, failing), "a refused failure decides nothing")

	lo = serverNow(t)
	_, err = client.Fail(ctx, report)
	hi = serverNow(t)
	require.NoError(t, err)
	_, _, failedAt, retryAt := attemptRow(failFence.AttemptID)
	requireWithin(t, *failedAt, lo, hi, "attempt finished_at after failure")
	// The retry instant is the control plane's own sample plus its own delay.
	requireWithin(t, *retryAt, lo, hi.Add(integrationRetryPolicy().Max), "attempt retry_at")

	// --- cancellation acknowledgment -----------------------------------------
	canceling := createJob(t, "time-authority-cancel", "demo.echo", 50, nil)
	cancelClaim, err := client.Claim(ctx, claimRequest(session, "default"))
	require.NoError(t, err)
	require.Equal(t, canceling, cancelClaim.Assignment.JobID)
	cancelFence := assignmentFence(cancelClaim.Assignment)
	_, err = client.Start(ctx, cancelFence)
	require.NoError(t, err)
	_, err = jobStore().RequestCancel(ctx, testScope, canceling)
	require.NoError(t, err)

	ack := cancelAck(cancelFence)
	cancelPath := "/internal/v1/attempts/" + cancelFence.AttemptID.String() + "/cancel"
	cancelBody := fenceBody(cancelFence)
	cancelBody["outcome_request_id"] = ack.OutcomeRequestID.String()
	beforeCancel := durableSnapshot(t, canceling)
	status, code = controlPost(t, server.URL, cancelPath, withTime(cancelBody, "finished_at"), nil)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, api.CodeMalformedJSON, code)
	require.Equal(t, beforeCancel, durableSnapshot(t, canceling), "a refused acknowledgment finalizes nothing")

	lo = serverNow(t)
	_, err = client.AcknowledgeCancellation(ctx, ack)
	hi = serverNow(t)
	require.NoError(t, err)
	_, _, canceledAt, _ := attemptRow(cancelFence.AttemptID)
	requireWithin(t, *canceledAt, lo, hi, "attempt finished_at after cancellation")

	// --- staleness is PostgreSQL's judgment, not the worker's ----------------
	// Every heartbeat stops. The worker's last word on the subject is a refused
	// heartbeat that claims, truthfully for its own clock, to be right now. Only
	// PostgreSQL's record of when it last heard from the session counts.
	_, err = testPool.Exec(ctx, `
		UPDATE worker_sessions
		SET registered_at = clock_timestamp() - interval '1 minute',
		    last_heartbeat_at = clock_timestamp() - interval '1 minute'
		WHERE id = $1`, session.ID)
	require.NoError(t, err)
	claimedNow := map[string]any{
		"worker_id": session.WorkerID.String(), "last_heartbeat_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	status, _ = controlPost(t, server.URL, heartbeatPath, claimedNow, nil)
	require.Equal(t, http.StatusBadRequest, status)

	marked, err := control.MarkStaleSessions(ctx, 5*time.Second, 10)
	require.NoError(t, err)
	require.Equal(t, 1, marked, "a worker's claim of being alive buys no liveness")
	require.Equal(t, "UNHEALTHY", sessionStatus(t, session.ID))
}
