package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/workers"
)

// ErrHeartbeatStale means this process could not confirm its own liveness with
// the control plane before the configured staleness threshold. The session may
// already have been fenced by reconciliation, so the process must stop
// presenting itself as ready and stop accepting new work.
var ErrHeartbeatStale = errors.New("worker session liveness could not be confirmed before the stale threshold")

// runHeartbeat proves this process session is alive using PostgreSQL receipt
// time, and reports the two conditions that end that proof.
//
// It runs on the completion context rather than the intake context, so a
// graceful drain keeps proving liveness while already-running work finishes.
// Waiting for a broker delivery to discover session loss is not good enough: an
// idle worker must notice that it was replaced.
func (r *Runner) runHeartbeat(ctx context.Context, session workers.Session) error {
	request := workers.HeartbeatRequest{WorkerID: session.WorkerID, SessionID: session.ID}
	// Registration itself set last_heartbeat_at, so the staleness budget starts
	// from the moment this loop starts, not from the first successful tick.
	lastConfirmed := r.now()

	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		// The request may not outlive the liveness it is trying to prove.
		//
		// Without this bound a hung control-plane call would sit here holding the
		// loop, so neither this check nor the ticker could run, and the worker
		// would keep taking work long past the point where the reconciler may
		// already have fenced its session. TASKFORGE_WORKER_REQUEST_TIMEOUT is a
		// transport setting and can be configured longer than the staleness
		// window; safety must not depend on it.
		staleAt := lastConfirmed.Add(r.cfg.SessionStaleAfter)
		if !r.now().Before(staleAt) {
			return fmt.Errorf("%w after %s", ErrHeartbeatStale, r.cfg.SessionStaleAfter)
		}
		callCtx, cancelCall := context.WithDeadline(ctx, staleAt)
		_, err := r.control.Heartbeat(callCtx, request)
		cancelCall()

		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			// A response that arrives at or after the window it was racing proves
			// nothing: the control plane may already have marked this session stale,
			// so a late success must not restore local liveness.
			if !r.now().Before(staleAt) {
				return fmt.Errorf("%w after %s", ErrHeartbeatStale, r.cfg.SessionStaleAfter)
			}
			lastConfirmed = r.now()
			continue
		}
		// A definitive fence is fatal: this boot has been replaced or marked
		// unhealthy, and nothing it does afterwards can be accepted.
		if isSessionLost(err) {
			return fmt.Errorf("%w: %v", ErrSessionLost, err)
		}
		// Transport faults, 5xx, and this call's own deadline are ambiguous, so
		// they are retried on the next tick rather than treated as loss. What is
		// not ambiguous is the clock: once the staleness threshold has passed
		// without a confirmation, the control plane may already consider this
		// session stale.
		r.log.Warn("worker heartbeat failed",
			slog.String("worker_session_id", session.ID.String()),
			slog.String("error", err.Error()))
		if !r.now().Before(staleAt) {
			return fmt.Errorf("%w after %s", ErrHeartbeatStale, r.cfg.SessionStaleAfter)
		}
	}
}

// renewalLoop keeps one executing attempt's lease authority alive.
//
// It owns a conservative monotonic authority deadline derived from the duration
// PostgreSQL reports, never from comparing this process's wall clock with the
// server's expires_at. When it can no longer prove the lease is still this
// process's, it cancels execution so success is never reported afterwards.
type renewalLoop struct {
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	mu    sync.Mutex
	lost  bool
	fatal error
}

// Stop ends the loop and reports what it observed.
//
// lost is true when lease authority could not be maintained, whether definitively
// (the fence, session, lease, or generation was rejected) or conservatively (no
// renewal could be confirmed before the authority deadline). fatal is non-nil
// only when the whole process boot lost its session and must exit.
func (l *renewalLoop) Stop() (lost bool, fatal error) {
	l.stopOnce.Do(func() { close(l.stop) })
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lost, l.fatal
}

func (l *renewalLoop) markLost(fatal error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lost = true
	if fatal != nil && l.fatal == nil {
		l.fatal = fatal
	}
}

// pendingRenewal is one logical renewal. Its identity and expected generation
// are held across transport retries so an ambiguous request that actually
// committed is recognized as a replay instead of extending authority twice.
type pendingRenewal struct {
	id       uuid.UUID
	expected int
}

// startRenewal renews fence's lease until the handler finishes or authority is
// lost, and cancels cancelAuthority in the latter case.
//
// deadline is the conservative local deadline already derived from the claim's
// server-measured window. Each successful renewal replaces it with a new one
// derived the same way. It bounds lease authority only: the job's overall
// timeout_seconds budget is measured once by the caller and renewal never
// resets it.
func (r *Runner) startRenewal(
	ctx context.Context,
	cancelAuthority context.CancelFunc,
	fence workers.Fence,
	deadline time.Time,
) *renewalLoop {
	loop := &renewalLoop{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(loop.done)

		ticker := time.NewTicker(r.cfg.RenewInterval)
		defer ticker.Stop()
		authority := time.NewTimer(time.Until(deadline))
		defer authority.Stop()

		// lose ends the loop, cancels execution, and records why. Every exit that
		// gives up authority goes through it, so cancellation can never be skipped.
		lose := func(reason string, fatal error, attrs ...slog.Attr) {
			r.log.Warn(reason, fenceLog(fence, attrs...)...)
			loop.markLost(fatal)
			cancelAuthority()
		}

		version := 0
		var pending *pendingRenewal
		for {
			select {
			case <-loop.stop:
				return
			case <-ctx.Done():
				return
			case <-authority.C:
				// Renewal kept failing transiently and the conservative deadline
				// arrived. Execution is cancelled and no outcome is reported; durable
				// recovery is left to server-time expiry and reconciliation.
				lose("lease authority deadline reached without a confirmed renewal", nil)
				return
			case <-ticker.C:
			}

			// The request may not outlive the authority it is trying to extend.
			//
			// Without this bound a hung control-plane call would sit here holding
			// the loop, so the authority timer above could not be selected and the
			// handler would keep running with no provable lease.
			// TASKFORGE_WORKER_REQUEST_TIMEOUT is a transport setting and can be
			// configured longer than the lease window; safety must not depend on it.
			if !r.now().Before(deadline) {
				lose("lease authority deadline reached without a confirmed renewal", nil)
				return
			}
			if pending == nil {
				pending = &pendingRenewal{id: uuid.New(), expected: version}
			}
			// Captured before the request, so the local deadline can never assume
			// time that was consumed by the round trip itself.
			requestStarted := r.now()
			callCtx, cancelCall := context.WithDeadline(ctx, deadline)
			result, err := r.control.RenewLease(callCtx, workers.RenewalRequest{
				Fence: fence, RenewalRequestID: pending.id, ExpectedVersion: pending.expected,
			})
			cancelCall()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				if isAuthorityLost(err) {
					lose("lease renewal rejected", escalateSessionLoss(err), slog.String("error", err.Error()))
					return
				}
				// Ambiguous, including this call's own deadline: keep the same renewal
				// identity and expected generation so the retry is recognized as a
				// replay if the first attempt committed.
				r.log.Warn("lease renewal failed", fenceLog(fence, slog.String("error", err.Error()))...)
				if !r.now().Before(deadline) {
					lose("lease authority deadline reached without a confirmed renewal", nil)
					return
				}
				continue
			}
			// A response that arrives at or after the deadline it was racing cannot
			// restore authority that has already lapsed locally. Accepting it would
			// let a slow round trip silently un-expire a window the handler already
			// outlived.
			renewedUntil := requestStarted.Add(executionBudget(result.Remaining))
			if !r.now().Before(deadline) || !r.now().Before(renewedUntil) {
				lose("lease renewal confirmed too late to extend local authority", nil)
				return
			}
			pending = nil
			version = result.RenewalVersion
			deadline = renewedUntil
			authority.Stop()
			authority.Reset(time.Until(deadline))
		}
	}()
	return loop
}

// isAuthorityLost reports the renewal failures that are definitive rather than
// ambiguous. Each of them means some other authority already won, so retrying
// cannot help and continuing to execute cannot end in a committed success.
func isAuthorityLost(err error) bool {
	for _, sentinel := range []error{
		workers.ErrSessionUnavailable, workers.ErrFenceRejected,
		workers.ErrLeaseExpired, workers.ErrRenewalConflict, workers.ErrStateConflict,
	} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	var remote *RemoteError
	if !errors.As(err, &remote) {
		return false
	}
	switch remote.Code {
	case "worker_session_unavailable", "fence_rejected",
		"lease_expired", "renewal_conflict", "state_conflict":
		return true
	}
	return false
}

// escalateSessionLoss separates "this attempt is over" from "this process boot
// is over". Only the latter ends the whole runner.
func escalateSessionLoss(err error) error {
	if isSessionLost(err) {
		return fmt.Errorf("%w: %v", ErrSessionLost, err)
	}
	return nil
}
