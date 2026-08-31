// Package reconciler repairs durable control-plane state that no live process
// will repair on its own: sessions that stopped heartbeating, and leases whose
// server-owned window has passed while the work behind them never finished.
//
// It is deliberately narrow. It expires leases, abandons their attempts,
// releases the capacity those leases held, and either returns a recoverable job
// to the queue with a fresh notification or applies the minimal terminal
// consequence when its attempt budget is gone. It is not a general "repair
// everything" loop; retry policy, failure classification, cancellation
// finalization, and delayed-job promotion belong to later milestones.
package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/co-rtex/TaskForge/internal/workers"
)

// Store is the durable half of reconciliation. Both operations are safe to run
// repeatedly and from several replicas at once.
type Store interface {
	MarkStaleSessions(ctx context.Context, staleAfter time.Duration, limit int) (int, error)
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
		r.RequeuedJobs > 0 || r.DeadLetteredJobs > 0
}

// RunOnce performs exactly one bounded pass and is the seam every test uses.
//
// The two scans are deliberately separate. A session can stop heartbeating while
// its lease is still valid, and a lease can expire while its session is perfectly
// healthy — the worker leaves a lease active after a handler error and keeps
// heartbeating — so requiring both conditions would strand exactly the recovery
// case this exists for.
//
// Staleness is marked first so that a session which has already been fenced
// cannot renew the leases the second scan is about to reconcile.
func (r *Reconciler) RunOnce(ctx context.Context) (Result, error) {
	var result Result

	stale, err := r.store.MarkStaleSessions(ctx, r.cfg.StaleAfter, r.cfg.BatchSize)
	result.StaleSessions = stale
	if err != nil {
		return result, fmt.Errorf("mark stale worker sessions: %w", err)
	}

	stats, err := r.store.ReconcileExpiredLeases(ctx, r.cfg.BatchSize)
	result.ExpiredLeases = stats.ExpiredLeases
	result.RequeuedJobs = stats.RequeuedJobs
	result.DeadLetteredJobs = stats.DeadLetteredJobs
	result.Skipped = stats.Skipped
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
				slog.Int("requeued_jobs", result.RequeuedJobs),
				slog.Int("dead_lettered_jobs", result.DeadLetteredJobs),
				slog.Int("skipped", result.Skipped))
		}
	}
}
