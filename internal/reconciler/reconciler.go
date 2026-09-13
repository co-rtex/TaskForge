// Package reconciler repairs durable control-plane state that no live process
// will repair on its own: sessions that stopped heartbeating, and leases whose
// server-owned window has passed while the work behind them never finished.
//
// It is deliberately narrow. It marks stale sessions, records the durable
// TIMED_OUT outcome for attempts that outlived their persisted deadline,
// finalizes cancellations no worker acknowledged, expires leases, abandons their
// attempts, releases the capacity those leases held, and either returns a
// recoverable job to the queue or dead-letters it. It is not a general "repair
// everything" loop, and it deliberately owns no scheduling: promoting due work
// and re-notifying stranded work belong to taskforge-scheduler.
package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/co-rtex/TaskForge/internal/workers"
)

// Store is the durable half of reconciliation. Every operation is safe to run
// repeatedly and from several replicas at once.
type Store interface {
	MarkStaleSessions(ctx context.Context, staleAfter time.Duration, limit int) (int, error)
	ReconcileDueTimeouts(ctx context.Context, limit int) (workers.ReconcileStats, error)
	ReconcileExpiredLeases(ctx context.Context, limit int) (workers.ReconcileStats, error)
}

// Config bounds one pass and names the staleness threshold it enforces.
type Config struct {
	// StaleAfter is the server-side heartbeat threshold. It must match what
	// workers are told to race; internal/config validates that relationship.
	StaleAfter   time.Duration
	PollInterval time.Duration
	BatchSize    int
}

// Reconciler runs bounded reconciliation passes.
type Reconciler struct {
	store Store
	cfg   Config
	log   *slog.Logger
}

func New(store Store, cfg Config, log *slog.Logger) *Reconciler {
	return &Reconciler{store: store, cfg: cfg, log: log}
}

// Result is what one pass durably changed.
type Result struct {
	workers.ReconcileStats
}

// Changed reports whether this pass repaired anything, so a quiet loop can stay
// quiet in the logs.
func (r Result) Changed() bool {
	return r.StaleSessions > 0 || r.ExpiredLeases > 0 ||
		r.TimedOutAttempts > 0 || r.CanceledAttempts > 0 ||
		r.RequeuedJobs > 0 || r.RetryWaitingJobs > 0 || r.DeadLetteredJobs > 0
}

// RunOnce performs exactly one bounded pass and is the seam every test uses.
//
// The three scans are deliberately separate, and their order is deliberate too.
//
//  1. Stale sessions first, so a session that has already been fenced cannot
//     renew the leases the later scans are about to reconcile.
//  2. Due attempt timeouts next, while their leases may still be ACTIVE. Running
//     this before the expired-lease scan means an attempt that timed out and
//     whose lease has not yet lapsed is recorded as TIMED_OUT rather than
//     waiting for the lease to expire first.
//  3. Expired leases last, applying the same precedence internally for the case
//     where a lease lapsed around a deadline that had already passed:
//     CANCEL_REQUESTED finalizes cancellation, a due deadline finalizes a
//     timeout, and only otherwise does ADR-0009's abandonment path run.
//
// A session can stop heartbeating while its lease is still valid, and a lease
// can expire while its session is perfectly healthy — the worker leaves a lease
// active after a handler error and keeps heartbeating — so requiring both
// conditions would strand exactly the recovery case this exists for.
func (r *Reconciler) RunOnce(ctx context.Context) (Result, error) {
	var result Result

	stale, err := r.store.MarkStaleSessions(ctx, r.cfg.StaleAfter, r.cfg.BatchSize)
	result.StaleSessions = stale
	if err != nil {
		return result, fmt.Errorf("mark stale worker sessions: %w", err)
	}

	timeouts, err := r.store.ReconcileDueTimeouts(ctx, r.cfg.BatchSize)
	result.Add(timeouts)
	if err != nil {
		return result, fmt.Errorf("reconcile due attempt timeouts: %w", err)
	}

	expired, err := r.store.ReconcileExpiredLeases(ctx, r.cfg.BatchSize)
	result.Add(expired)
	if err != nil {
		return result, fmt.Errorf("reconcile expired leases: %w", err)
	}
	return result, nil
}

// Run repeats RunOnce on a fixed interval until ctx is cancelled.
//
// A failed pass is logged and retried on the next tick rather than ending the
// process: reconciliation is idempotent, so the repair a failed pass did not
// finish is simply picked up again. Nothing here holds state between passes, so
// N replicas are safe by construction.
func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	r.log.Info("reconciler started",
		slog.Duration("poll_interval", r.cfg.PollInterval),
		slog.Duration("stale_after", r.cfg.StaleAfter),
		slog.Int("batch_size", r.cfg.BatchSize))

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reconciler stopped")
			return ctx.Err()
		case <-ticker.C:
		}

		result, err := r.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				r.log.Info("reconciler stopped")
				return ctx.Err()
			}
			r.log.Error("reconciliation pass failed", slog.String("error", err.Error()))
			continue
		}
		if result.Changed() {
			r.log.Info("reconciliation repaired durable state",
				slog.Int("stale_sessions", result.StaleSessions),
				slog.Int("expired_leases", result.ExpiredLeases),
				slog.Int("timed_out_attempts", result.TimedOutAttempts),
				slog.Int("canceled_attempts", result.CanceledAttempts),
				slog.Int("requeued_jobs", result.RequeuedJobs),
				slog.Int("retry_waiting_jobs", result.RetryWaitingJobs),
				slog.Int("dead_lettered_jobs", result.DeadLetteredJobs),
				slog.Int("skipped", result.Skipped))
		}
	}
}
