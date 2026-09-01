package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
	"github.com/co-rtex/TaskForge/internal/queue"
	"github.com/co-rtex/TaskForge/internal/workers"
)

func discardWorkerLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// handlerReturning registers one trusted handler that returns err.
func handlerReturning(t *testing.T, err error) *Registry {
	t.Helper()
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo",
		HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
			return nil, err
		})))
	return registry
}

// advisoryMessage is one broker delivery carrying a well-formed work
// notification for the default queue.
func advisoryMessage(t *testing.T) queue.Message {
	t.Helper()
	return queue.Message{
		ID: uuid.NewString(), ReceiptHandle: uuid.NewString(),
		Body: notificationBody(t, "default"),
	}
}

// TestFailure_TrustedClassificationReachesTheControlPlane proves a handler's
// declared classification, stable code, and safe message are what get reported.
func TestFailure_TrustedClassificationReachesTheControlPlane(t *testing.T) {
	for name, tc := range map[string]struct {
		err       error
		wantClass lifecycle.FailureClass
		wantCode  string
		wantMsg   string
	}{
		"retryable": {
			Retryable("upstream_5xx", "upstream returned 502"),
			lifecycle.ClassRetryable, "upstream_5xx", "upstream returned 502",
		},
		"permanent": {
			Permanent("invalid_payload", "the payload names no known account"),
			lifecycle.ClassPermanent, "invalid_payload", "the payload names no known account",
		},
		"code only": {
			Retryable("transient", ""),
			lifecycle.ClassRetryable, "transient", "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			session := testSession(1)
			assignment := testAssignment(session)
			var reported workers.FailureReport
			var succeeded atomic.Bool
			control := &fakeControl{
				claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
					return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
				},
				fail: func(_ context.Context, report workers.FailureReport) (workers.OutcomeResult, error) {
					reported = report
					return workers.OutcomeResult{
						JobID: report.Fence.JobID, JobStatus: "RETRY_WAIT",
						AttemptStatus: workers.AttemptFailed,
					}, nil
				},
				succeed: func(context.Context, workers.Fence) error {
					succeeded.Store(true)
					return nil
				},
			}

			runner := testRunner(control, &fakeBroker{}, handlerReturning(t, tc.err))
			require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

			require.False(t, succeeded.Load(), "a failed handler must never be reported as success")
			require.Equal(t, tc.wantClass, reported.Class)
			require.Equal(t, tc.wantCode, reported.ErrorCode)
			require.Equal(t, tc.wantMsg, reported.ErrorMessage)
			require.Equal(t, assignment.AttemptID, reported.Fence.AttemptID)
			require.NotEqual(t, uuid.Nil, reported.OutcomeRequestID)
		})
	}
}

// TestFailure_UnknownErrorsAndPanicsNeverLeakTheirText is the safety boundary.
//
// A raw handler error is the one place payload fragments, credentials, driver
// output, and stack traces reliably appear, so an untyped failure must become a
// generic retryable one and its text must not travel — not to the database, not
// to the API, not into a log line.
func TestFailure_UnknownErrorsAndPanicsNeverLeakTheirText(t *testing.T) {
	const secret = "connection refused: postgres://taskforge:hunter2@10.0.0.5/prod"

	for name, handlerErr := range map[string]error{
		"plain error":   errors.New(secret),
		"wrapped error": fmt.Errorf("dial upstream: %w", errors.New(secret)),
	} {
		t.Run(name, func(t *testing.T) {
			session := testSession(1)
			assignment := testAssignment(session)
			var reported workers.FailureReport
			control := &fakeControl{
				claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
					return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
				},
				fail: func(_ context.Context, report workers.FailureReport) (workers.OutcomeResult, error) {
					reported = report
					return workers.OutcomeResult{
						JobID: report.Fence.JobID, JobStatus: "RETRY_WAIT",
						AttemptStatus: workers.AttemptFailed,
					}, nil
				},
			}
			runner := testRunner(control, &fakeBroker{}, handlerReturning(t, handlerErr))
			require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

			require.Equal(t, lifecycle.ClassRetryable, reported.Class,
				"an unclassified failure is retryable, not permanent: TaskForge cannot know it is hopeless")
			require.Equal(t, lifecycle.CodeHandlerError, reported.ErrorCode)
			require.Equal(t, lifecycle.MessageHandlerError, reported.ErrorMessage)
			require.NotContains(t, reported.ErrorMessage, "hunter2")
			require.NotContains(t, reported.ErrorMessage, "10.0.0.5")
			require.NoError(t, lifecycle.ValidateErrorMessage(reported.ErrorMessage))
			require.NoError(t, lifecycle.ValidateErrorCode(reported.ErrorCode))
		})
	}

	t.Run("a recovered panic", func(t *testing.T) {
		session := testSession(1)
		assignment := testAssignment(session)
		var reported workers.FailureReport
		control := &fakeControl{
			claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
				return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
			},
			fail: func(_ context.Context, report workers.FailureReport) (workers.OutcomeResult, error) {
				reported = report
				return workers.OutcomeResult{
					JobID: report.Fence.JobID, JobStatus: "RETRY_WAIT",
					AttemptStatus: workers.AttemptFailed,
				}, nil
			},
		}
		registry := NewRegistry()
		require.NoError(t, registry.Register("demo.echo",
			HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
				panic(secret)
			})))

		runner := testRunner(control, &fakeBroker{}, registry)
		require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

		require.Equal(t, lifecycle.ClassRetryable, reported.Class)
		require.Equal(t, lifecycle.CodeHandlerError, reported.ErrorCode)
		require.NotContains(t, reported.ErrorMessage, "hunter2")
	})
}

// TestFailure_HandlerMessageIsBoundedBeforeItLeavesTheProcess proves even a
// trusted message is coerced into the stored bounds rather than trusted whole.
func TestFailure_HandlerMessageIsBoundedBeforeItLeavesTheProcess(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	oversized := ""
	for i := 0; i < lifecycle.MaxErrorMessageBytes*2; i++ {
		oversized += "m"
	}

	var reported workers.FailureReport
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		fail: func(_ context.Context, report workers.FailureReport) (workers.OutcomeResult, error) {
			reported = report
			return workers.OutcomeResult{
				JobID: report.Fence.JobID, JobStatus: "RETRY_WAIT",
				AttemptStatus: workers.AttemptFailed,
			}, nil
		},
	}
	runner := testRunner(control, &fakeBroker{},
		handlerReturning(t, Retryable("transient", "line one\nline two "+oversized)))
	require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

	require.LessOrEqual(t, len(reported.ErrorMessage), lifecycle.MaxErrorMessageBytes)
	require.NotContains(t, reported.ErrorMessage, "\n",
		"a newline would let one stored message become two log lines")
	require.NoError(t, lifecycle.ValidateErrorMessage(reported.ErrorMessage))
}

// TestFailure_RetriesReuseOneOutcomeIdentity is the ambiguity contract on the
// worker's side. Generating a fresh identity per attempt is precisely the bug
// the retained identity exists to prevent: a committed-but-lost failure response
// would then consume a second place in the attempt budget and redraw jitter.
func TestFailure_RetriesReuseOneOutcomeIdentity(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)

	var mu sync.Mutex
	var reports []workers.FailureReport
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		fail: func(_ context.Context, report workers.FailureReport) (workers.OutcomeResult, error) {
			mu.Lock()
			defer mu.Unlock()
			reports = append(reports, report)
			if len(reports) < 3 {
				// Ambiguous: the request may or may not have committed.
				return workers.OutcomeResult{}, &RemoteError{Status: 503, Code: "service_unavailable"}
			}
			return workers.OutcomeResult{
				JobID: report.Fence.JobID, JobStatus: "RETRY_WAIT",
				AttemptStatus: workers.AttemptFailed,
			}, nil
		},
	}

	runner := NewRunner(control, &fakeBroker{},
		handlerReturning(t, Retryable("upstream_5xx", "upstream returned 502")),
		RunnerConfig{Queue: "default", PollWait: time.Second, RetryAttempts: 3, ShutdownTimeout: time.Second},
		discardWorkerLogger())
	require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

	require.Len(t, reports, 3, "an ambiguous 503 is retried, not abandoned")
	for i := 1; i < len(reports); i++ {
		require.Equal(t, reports[0].OutcomeRequestID, reports[i].OutcomeRequestID,
			"a fresh identity on retry would consume a second place in the attempt budget")
		require.Equal(t, reports[0].Class, reports[i].Class)
		require.Equal(t, reports[0].ErrorCode, reports[i].ErrorCode)
		require.Equal(t, reports[0].ErrorMessage, reports[i].ErrorMessage)
		require.Equal(t, reports[0].Fence, reports[i].Fence)
	}
}

// TestCancellation_DeliveredWhileExecutingStopsTheHandlerAndAcknowledges is the
// whole cooperative path, end to end inside the worker.
func TestCancellation_DeliveredWhileExecutingStopsTheHandlerAndAcknowledges(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)

	entered := make(chan struct{})
	var cancelCause atomic.Value
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo",
		HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
			close(entered)
			<-ctx.Done()
			cancelCause.Store(context.Cause(ctx).Error())
			// A cooperative handler returns; the error it returns must not be
			// what gets reported, because cancellation is not a handler failure.
			return nil, ctx.Err()
		})))

	var acks []workers.CancelAcknowledgment
	var mu sync.Mutex
	var failed, succeeded atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		cancelAck: func(_ context.Context, ack workers.CancelAcknowledgment) (workers.OutcomeResult, error) {
			mu.Lock()
			defer mu.Unlock()
			acks = append(acks, ack)
			if len(acks) < 2 {
				return workers.OutcomeResult{}, &RemoteError{Status: 503, Code: "service_unavailable"}
			}
			return workers.OutcomeResult{
				JobID: ack.Fence.JobID, JobStatus: "CANCELED",
				AttemptStatus: workers.AttemptCanceled,
			}, nil
		},
		fail: func(context.Context, workers.FailureReport) (workers.OutcomeResult, error) {
			failed.Store(true)
			return workers.OutcomeResult{}, nil
		},
		succeed: func(context.Context, workers.Fence) error {
			succeeded.Store(true)
			return nil
		},
	}

	runner := NewRunner(control, &fakeBroker{}, registry,
		RunnerConfig{Queue: "default", PollWait: time.Second, RetryAttempts: 2, ShutdownTimeout: time.Second},
		discardWorkerLogger())

	done := make(chan error, 1)
	go func() {
		done <- runner.processMessage(context.Background(), session, advisoryMessage(t))
	}()

	<-entered
	// Delivery is the heartbeat's, not the broker's: no message is involved.
	runner.deliverCancellations([]workers.CancellationDirective{{
		JobID: assignment.JobID, AttemptID: assignment.AttemptID,
		LeaseID: assignment.LeaseID, CancelRequestedAt: time.Now(),
	}})

	require.NoError(t, <-done)
	require.Equal(t, errUserCanceled.Error(), cancelCause.Load(),
		"the handler must be able to tell user cancellation from any other cause")
	require.False(t, succeeded.Load(), "a canceled attempt must never be reported as success")
	require.False(t, failed.Load(),
		"cancellation is not a failure and must not consume the attempt budget")

	require.Len(t, acks, 2, "an ambiguous acknowledgment is retried")
	require.Equal(t, acks[0].OutcomeRequestID, acks[1].OutcomeRequestID,
		"one identity is reused across retries, exactly as a failure report's is")
	require.Equal(t, assignment.AttemptID, acks[0].Fence.AttemptID)
}

// TestCancellation_ArrivingBeforeTheHandlerStartsIsNotLost is why the attempt is
// registered before Start rather than after.
//
// Cancellation can win in the window between a claim committing and the handler
// being invoked. A registry populated only once execution began would drop
// exactly those directives, and the worker would run a handler for a job it had
// already been told was canceled.
func TestCancellation_ArrivingBeforeTheHandlerStartsIsNotLost(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)

	var handlerRan atomic.Bool
	var handlerCause atomic.Value
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo",
		HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
			handlerRan.Store(true)
			if err := ctx.Err(); err != nil {
				handlerCause.Store(context.Cause(ctx).Error())
				return nil, err
			}
			return nil, nil
		})))

	var acknowledged atomic.Bool
	var succeeded atomic.Bool
	runnerRef := make(chan *Runner, 1)
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start: func(_ context.Context, fence workers.Fence) (workers.StartResult, error) {
			// The directive lands between the claim and the handler, which is
			// exactly the window the pre-Start registration covers.
			runner := <-runnerRef
			runner.deliverCancellations([]workers.CancellationDirective{{
				JobID: fence.JobID, AttemptID: fence.AttemptID,
				LeaseID: fence.LeaseID, CancelRequestedAt: time.Now(),
			}})
			return okStart(fence), nil
		},
		cancelAck: func(_ context.Context, ack workers.CancelAcknowledgment) (workers.OutcomeResult, error) {
			acknowledged.Store(true)
			return workers.OutcomeResult{
				JobID: ack.Fence.JobID, JobStatus: "CANCELED",
				AttemptStatus: workers.AttemptCanceled,
			}, nil
		},
		succeed: func(context.Context, workers.Fence) error {
			succeeded.Store(true)
			return nil
		},
	}

	runner := testRunner(control, &fakeBroker{}, registry)
	runnerRef <- runner
	require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

	require.True(t, acknowledged.Load(),
		"a directive delivered before the handler ran must still be acknowledged")
	require.False(t, succeeded.Load())
	if handlerRan.Load() {
		require.Equal(t, errUserCanceled.Error(), handlerCause.Load(),
			"if the handler was invoked at all, it must already have been canceled")
	}
}

// TestCancellation_DirectiveForAnotherLeaseIsIgnored keeps a stale directive
// from stopping the wrong work.
func TestCancellation_DirectiveForAnotherLeaseIsIgnored(t *testing.T) {
	registry := newAttemptRegistry()
	fence := workers.Fence{
		JobID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(),
		WorkerID: uuid.New(), SessionID: uuid.New(),
	}
	registry.register(fence)

	require.False(t, registry.deliver(workers.CancellationDirective{
		JobID: fence.JobID, AttemptID: fence.AttemptID, LeaseID: uuid.New(),
	}), "a directive naming a previous lease is not authority to stop the current one")
	require.False(t, registry.deliver(workers.CancellationDirective{
		JobID: uuid.New(), AttemptID: fence.AttemptID, LeaseID: fence.LeaseID,
	}))
	require.False(t, registry.deliver(workers.CancellationDirective{
		JobID: fence.JobID, AttemptID: uuid.New(), LeaseID: fence.LeaseID,
	}), "a directive for an attempt this process does not hold is ignored, not an error")
	require.False(t, registry.wasCanceled(fence.AttemptID))

	require.True(t, registry.deliver(workers.CancellationDirective{
		JobID: fence.JobID, AttemptID: fence.AttemptID, LeaseID: fence.LeaseID,
	}))
	require.True(t, registry.wasCanceled(fence.AttemptID))

	// Repeated delivery is harmless and reports that it changed nothing.
	require.False(t, registry.deliver(workers.CancellationDirective{
		JobID: fence.JobID, AttemptID: fence.AttemptID, LeaseID: fence.LeaseID,
	}))

	registry.unregister(fence.AttemptID)
	require.False(t, registry.wasCanceled(fence.AttemptID))
}

// TestTimeout_WorkerCancelsLocallyAndReportsNothing is the worker's half of the
// timeout contract. Only reconciliation may record TIMED_OUT, so a worker whose
// attempt outlived its budget must report nothing at all — reporting an ordinary
// failure would mislabel the attempt.
func TestTimeout_WorkerCancelsLocallyAndReportsNothing(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	assignment.LeaseRemaining = time.Minute
	assignment.ExecutionDeadline = time.Now().Add(time.Minute)

	var cause atomic.Value
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo",
		HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
			<-ctx.Done()
			cause.Store(context.Cause(ctx).Error())
			return nil, ctx.Err()
		})))

	var reported atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		// A tiny server-measured budget: the attempt is nearly out of time.
		start: func(_ context.Context, fence workers.Fence) (workers.StartResult, error) {
			now := time.Now()
			return workers.StartResult{
				AttemptID: fence.AttemptID, StartedAt: now,
				TimeoutAt: now.Add(20 * time.Millisecond), Remaining: 20 * time.Millisecond,
			}, nil
		},
		succeed: func(context.Context, workers.Fence) error {
			reported.Store(true)
			return nil
		},
		fail: func(context.Context, workers.FailureReport) (workers.OutcomeResult, error) {
			reported.Store(true)
			return workers.OutcomeResult{}, nil
		},
		cancelAck: func(context.Context, workers.CancelAcknowledgment) (workers.OutcomeResult, error) {
			reported.Store(true)
			return workers.OutcomeResult{}, nil
		},
	}

	runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
	require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

	require.Equal(t, errAttemptTimedOut.Error(), cause.Load(),
		"a timeout must cancel the handler with its own distinguishable cause")
	require.False(t, reported.Load(),
		"only reconciliation may record TIMED_OUT; the worker reports nothing")
}

// TestTimeout_LocalDeadlineComesFromTheServerMeasuredBudget pins the rule that
// keeps an ambiguous Start retry from restarting the clock: the deadline is
// derived from what PostgreSQL reported, not from timeout_seconds recomputed
// once the response landed.
func TestTimeout_LocalDeadlineComesFromTheServerMeasuredBudget(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	// A large job-level timeout that the worker must NOT use: the server says
	// most of the budget is already gone, because this attempt already started.
	assignment.TimeoutSeconds = 3600
	assignment.LeaseRemaining = time.Minute
	assignment.ExecutionDeadline = time.Now().Add(time.Minute)

	entered := make(chan struct{})
	var cause atomic.Value
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.echo",
		HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
			close(entered)
			<-ctx.Done()
			cause.Store(context.Cause(ctx).Error())
			return nil, ctx.Err()
		})))

	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		start: func(_ context.Context, fence workers.Fence) (workers.StartResult, error) {
			now := time.Now()
			return workers.StartResult{
				AttemptID: fence.AttemptID, StartedAt: now.Add(-time.Hour),
				TimeoutAt: now.Add(30 * time.Millisecond), Remaining: 30 * time.Millisecond,
				Replayed: true,
			}, nil
		},
		succeed: func(context.Context, workers.Fence) error { return nil },
	}

	runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
	start := time.Now()
	require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

	<-entered
	require.Equal(t, errAttemptTimedOut.Error(), cause.Load())
	require.Less(t, time.Since(start), 10*time.Second,
		"the worker must honor the server-measured remaining budget, not timeout_seconds")
}

// TestOutcome_AuthorityLossIsNeverReportedAsCancellation keeps three different
// things from being conflated. Only a durable directive means the job was
// canceled; losing the lease means recovery, and shutting down means neither.
func TestOutcome_AuthorityLossIsNeverReportedAsCancellation(t *testing.T) {
	t.Run("lease authority loss reports nothing", func(t *testing.T) {
		session := testSession(1)
		assignment := testAssignment(session)
		entered := make(chan struct{})
		var cause atomic.Value
		registry := NewRegistry()
		require.NoError(t, registry.Register("demo.echo",
			HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
				close(entered)
				<-ctx.Done()
				cause.Store(context.Cause(ctx).Error())
				return nil, ctx.Err()
			})))

		var reported atomic.Int32
		control := &fakeControl{
			claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
				return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
			},
			renew: func(context.Context, workers.RenewalRequest) (workers.RenewalResult, error) {
				return workers.RenewalResult{}, workers.ErrLeaseExpired
			},
			succeed: func(context.Context, workers.Fence) error {
				reported.Add(1)
				return nil
			},
			fail: func(context.Context, workers.FailureReport) (workers.OutcomeResult, error) {
				reported.Add(1)
				return workers.OutcomeResult{}, nil
			},
			cancelAck: func(context.Context, workers.CancelAcknowledgment) (workers.OutcomeResult, error) {
				reported.Add(1)
				return workers.OutcomeResult{}, nil
			},
		}

		runner := renewingRunner(control, &fakeBroker{}, registry, RunnerConfig{})
		require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

		require.Equal(t, errAuthorityLost.Error(), cause.Load(),
			"losing the lease is not user cancellation and must not look like it")
		require.Zero(t, reported.Load(),
			"authority loss leaves recovery to lease expiry and reconciliation")
	})

	t.Run("shutdown is not job cancellation", func(t *testing.T) {
		session := testSession(1)
		assignment := testAssignment(session)
		entered := make(chan struct{})
		registry := NewRegistry()
		require.NoError(t, registry.Register("demo.echo",
			HandlerFunc(func(ctx context.Context, _ Execution) (json.RawMessage, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			})))

		var acknowledged, failed atomic.Bool
		control := &fakeControl{
			claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
				return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
			},
			cancelAck: func(context.Context, workers.CancelAcknowledgment) (workers.OutcomeResult, error) {
				acknowledged.Store(true)
				return workers.OutcomeResult{}, nil
			},
			fail: func(context.Context, workers.FailureReport) (workers.OutcomeResult, error) {
				failed.Store(true)
				return workers.OutcomeResult{}, nil
			},
			succeed: func(context.Context, workers.Fence) error { return nil },
		}

		ctx, cancel := context.WithCancel(context.Background())
		runner := testRunner(control, &fakeBroker{}, registry)
		done := make(chan error, 1)
		go func() { done <- runner.processMessage(ctx, session, advisoryMessage(t)) }()

		<-entered
		cancel()
		require.NoError(t, <-done)

		require.False(t, acknowledged.Load(),
			"telling an operator their job was canceled when the process merely stopped is a lie")
		require.False(t, failed.Load(),
			"a handler interrupted by shutdown did not fail; its lease expires into recovery")
	})
}

// TestOutcome_SuccessIsStillReportedWhenNothingWentWrong is the control for all
// of the above: none of the new precedence may break the ordinary path.
func TestOutcome_SuccessIsStillReportedWhenNothingWentWrong(t *testing.T) {
	session := testSession(1)
	assignment := testAssignment(session)
	var succeeded, failed, acknowledged atomic.Bool
	control := &fakeControl{
		claim: func(context.Context, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.Claimed, Assignment: assignment}, nil
		},
		succeed: func(context.Context, workers.Fence) error {
			succeeded.Store(true)
			return nil
		},
		fail: func(context.Context, workers.FailureReport) (workers.OutcomeResult, error) {
			failed.Store(true)
			return workers.OutcomeResult{}, nil
		},
		cancelAck: func(context.Context, workers.CancelAcknowledgment) (workers.OutcomeResult, error) {
			acknowledged.Store(true)
			return workers.OutcomeResult{}, nil
		},
	}
	runner := testRunner(control, &fakeBroker{}, handlerReturning(t, nil))
	require.NoError(t, runner.processMessage(context.Background(), session, advisoryMessage(t)))

	require.True(t, succeeded.Load())
	require.False(t, failed.Load())
	require.False(t, acknowledged.Load())
}

// TestClassifyHandlerError_RejectsAServerAuthoritativeClaim proves a handler
// cannot smuggle a server-owned classification through the typed mechanism.
func TestClassifyHandlerError_RejectsAServerAuthoritativeClaim(t *testing.T) {
	for _, class := range []lifecycle.FailureClass{
		lifecycle.ClassTimedOut, lifecycle.ClassCanceled, lifecycle.ClassAbandoned, "INVENTED",
	} {
		gotClass, gotCode, gotMessage := classifyHandlerError(
			&FailureError{Class: class, Code: "sneaky", Message: "let me through"})
		require.Equalf(t, lifecycle.ClassRetryable, gotClass,
			"a handler must not be able to declare %s about itself", class)
		require.Equal(t, lifecycle.CodeHandlerError, gotCode)
		require.Equal(t, lifecycle.MessageHandlerError, gotMessage)
	}

	// The two it may declare come through intact.
	class, code, message := classifyHandlerError(Permanent("invalid_payload", "no such account"))
	require.Equal(t, lifecycle.ClassPermanent, class)
	require.Equal(t, "invalid_payload", code)
	require.Equal(t, "no such account", message)
}
