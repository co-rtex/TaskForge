// Package scheduler promotes durable work that has become eligible and repairs
// work whose notification was lost.
//
// It is deliberately narrow, and it deliberately holds no broker connection.
// Its whole job is to write authoritative PostgreSQL state — a job becoming
// QUEUED, and the outbox event that advertises it — in one transaction. Broker
// I/O belongs to taskforge-outbox, and keeping the split means a scheduler
// crash, a broker outage, and a publisher restart are three independent
// failures rather than one entangled one.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/co-rtex/TaskForge/internal/jobs"
)

// Store is the durable half of scheduling. Both operations are safe to run
// repeatedly and from several replicas at once.
type Store interface {
	PromoteDueJobs(ctx context.Context, limit int) (jobs.SchedulerStats, error)
	RenotifyStrandedQueued(ctx context.Context, after time.Duration, limit int) (jobs.SchedulerStats, error)
}

// Config bounds one pass.
type Config struct {
	PollInterval time.Duration
	BatchSize    int
	// RenotifyAfter is how long a QUEUED job may go without a notification
	// before the scheduler treats its notification as lost. internal/config
	// validates that it is safely larger than the polling cadence and at least
	// the outbox claim timeout.
	RenotifyAfter time.Duration
}

// Scheduler runs bounded scheduling passes.
type Scheduler struct {
	store Store
	cfg   Config
	log   *slog.Logger
}

func New(store Store, cfg Config, log *slog.Logger) *Scheduler {
	return &Scheduler{store: store, cfg: cfg, log: log}
}

// Result is what one pass durably changed.
type Result struct {
	jobs.SchedulerStats
}

// Changed reports whether this pass did anything, so a quiet loop stays quiet in
// the logs.
func (r Result) Changed() bool {
	return r.PromotedJobs > 0 || r.Renotified > 0
}

// RunOnce performs exactly one bounded pass and is the seam every test uses.
//
// Promotion runs before re-notification, and the order is not arbitrary: a job
// promoted in this pass has just had its notification created, so it is not
// stranded and the re-notification scan must not consider it. Running them the
// other way round would be correct but would waste a scan on rows that are about
// to change.
//
// Neither scan holds a row lock across network I/O, because neither performs
// any: the only writes are PostgreSQL transactions.
func (s *Scheduler) RunOnce(ctx context.Context) (Result, error) {
	var result Result

	promoted, err := s.store.PromoteDueJobs(ctx, s.cfg.BatchSize)
	result.Add(promoted)
	if err != nil {
		return result, fmt.Errorf("promote due jobs: %w", err)
	}

	renotified, err := s.store.RenotifyStrandedQueued(ctx, s.cfg.RenotifyAfter, s.cfg.BatchSize)
	result.Add(renotified)
	if err != nil {
		return result, fmt.Errorf("re-notify stranded queued jobs: %w", err)
	}
	return result, nil
}

// Run repeats RunOnce on a fixed interval until ctx is cancelled.
//
// A failed pass is logged and retried on the next tick rather than ending the
// process: every operation is idempotent, so whatever a failed pass did not
// finish is simply picked up again. Nothing here holds state between passes, so
// N replicas are safe by construction.
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	s.log.Info("scheduler started",
		slog.Duration("poll_interval", s.cfg.PollInterval),
		slog.Duration("renotify_after", s.cfg.RenotifyAfter),
		slog.Int("batch_size", s.cfg.BatchSize))

	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopped")
			return ctx.Err()
		case <-ticker.C:
		}

		result, err := s.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				s.log.Info("scheduler stopped")
				return ctx.Err()
			}
			s.log.Error("scheduling pass failed", slog.String("error", err.Error()))
			continue
		}
		if result.Changed() {
			// Counts only. No payloads, no job ids: a scheduler that promotes a
			// large batch must not turn one pass into an unbounded log line.
			s.log.Info("scheduler advanced durable work",
				slog.Int("promoted_jobs", result.PromotedJobs),
				slog.Int("renotified_jobs", result.Renotified),
				slog.Int("skipped", result.Skipped))
		}
	}
}
