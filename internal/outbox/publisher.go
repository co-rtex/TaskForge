package outbox

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"github.com/co-rtex/TaskForge/internal/queue"
)

// PublisherConfig tunes the publish loop.
type PublisherConfig struct {
	BatchSize    int
	PollInterval time.Duration
	// ClaimTimeout is how long a claimed event stays invisible to other
	// publishers. It must exceed the time a publish realistically takes, or a
	// second publisher will duplicate work already in flight.
	ClaimTimeout time.Duration
	Backoff      BackoffPolicy
}

// Publisher drains the outbox to the broker.
//
// One Publisher runs a single loop goroutine, so its random source needs no
// locking. Multiple Publisher instances — in one process or across replicas —
// are safe because claiming is serialized by the database.
type Publisher struct {
	store  *Store
	broker queue.Publisher
	cfg    PublisherConfig
	log    *slog.Logger
	rnd    *rand.Rand
}

// Stats summarizes one pass of the loop.
type Stats struct {
	Claimed   int
	Published int
	Failed    int
	// AlreadyPublished counts events that were published by someone else between
	// this publisher claiming and marking them. It is the observable signature of
	// the documented at-least-once window.
	AlreadyPublished int
}

// NewPublisher builds a Publisher. Passing the random source in keeps jitter
// deterministic under test.
func NewPublisher(store *Store, broker queue.Publisher, cfg PublisherConfig, log *slog.Logger, rnd *rand.Rand) *Publisher {
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 50
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.ClaimTimeout <= 0 {
		cfg.ClaimTimeout = 30 * time.Second
	}
	if rnd == nil {
		rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Publisher{store: store, broker: broker, cfg: cfg, log: log, rnd: rnd}
}

// RunOnce claims one batch and publishes it, returning what happened.
//
// The order is deliberate and is the heart of the pattern:
//
//	claim (commit) -> publish (no transaction held) -> mark published (commit)
//
// A crash between publishing and marking republishes the event once its
// visibility window expires. That duplicate is accepted and documented: broker
// notifications are advisory, and the claim query — not the notification — is
// what enforces single execution.
func (p *Publisher) RunOnce(ctx context.Context) (Stats, error) {
	var stats Stats

	events, err := p.store.ClaimDue(ctx, p.cfg.BatchSize, p.cfg.ClaimTimeout)
	if err != nil {
		return stats, err
	}
	stats.Claimed = len(events)

	for _, e := range events {
		// Stop promptly on shutdown, but report what was already done.
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}

		body, err := e.Body()
		if err != nil {
			// A malformed row cannot be fixed by retrying immediately; back it
			// off like any other failure so it stays visible without spinning.
			stats.Failed++
			p.recordFailure(ctx, e, err)
			continue
		}

		if err := p.broker.Publish(ctx, body); err != nil {
			stats.Failed++
			p.recordFailure(ctx, e, err)
			continue
		}

		marked, err := p.store.MarkPublished(ctx, e.ID)
		if err != nil {
			// Published but not marked. This is exactly the documented window:
			// the event will be republished later. Nothing is lost.
			stats.Failed++
			p.log.Error("published but failed to mark; event will be republished",
				slog.String("event_id", e.ID.String()),
				slog.String("event_type", e.Type),
				slog.String("error", err.Error()))
			continue
		}
		if !marked {
			stats.AlreadyPublished++
			p.log.Warn("outbox event was already published by another publisher",
				slog.String("event_id", e.ID.String()))
			continue
		}

		stats.Published++
		p.log.Debug("outbox event published",
			slog.String("event_id", e.ID.String()),
			slog.String("event_type", e.Type),
			slog.Int("attempts", e.Attempts))
	}
	return stats, nil
}

func (p *Publisher) recordFailure(ctx context.Context, e Event, cause error) {
	delay := p.cfg.Backoff.Delay(e.Attempts, p.rnd)
	p.log.Warn("outbox publish failed; will retry",
		slog.String("event_id", e.ID.String()),
		slog.String("event_type", e.Type),
		slog.Int("attempts", e.Attempts),
		slog.Duration("retry_in", delay),
		slog.String("error", cause.Error()))

	// Use a detached context so a shutdown mid-batch still records the failure
	// and the backoff, instead of leaving the event claimed for the full
	// visibility timeout with no explanation stored.
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := p.store.RecordFailure(recordCtx, e.ID, cause.Error(), delay); err != nil {
		p.log.Error("could not record outbox failure",
			slog.String("event_id", e.ID.String()),
			slog.String("error", err.Error()))
	}
}

// Run drains the outbox until ctx is canceled.
//
// A failing pass is logged and retried on the next tick rather than killing the
// process: the database or broker being briefly unavailable is an expected
// condition, not a reason to stop delivering.
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	p.log.Info("outbox publisher started",
		slog.Int("batch_size", p.cfg.BatchSize),
		slog.Duration("poll_interval", p.cfg.PollInterval),
		slog.Duration("claim_timeout", p.cfg.ClaimTimeout))

	for {
		select {
		case <-ctx.Done():
			p.log.Info("outbox publisher stopped")
			return nil
		case <-ticker.C:
			stats, err := p.RunOnce(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				p.log.Error("outbox pass failed", slog.String("error", err.Error()))
				continue
			}
			if stats.Published > 0 || stats.Failed > 0 || stats.AlreadyPublished > 0 {
				p.log.Info("outbox pass complete",
					slog.Int("claimed", stats.Claimed),
					slog.Int("published", stats.Published),
					slog.Int("failed", stats.Failed),
					slog.Int("already_published", stats.AlreadyPublished))
			}
		}
	}
}
