//go:build integration

package integration

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/outbox"
	"github.com/co-rtex/TaskForge/internal/queue"
)

// drainingPublisher publishes in batches of ten every three seconds, so a backlog
// of several batches tells one pass per tick apart from draining within a tick.
func drainingPublisher(t *testing.T, broker queue.Publisher) *outbox.Publisher {
	t.Helper()
	cfg := publisherConfig()
	cfg.BatchSize = 10
	cfg.PollInterval = 3 * time.Second
	// Long enough that an event whose publish failed is not due again inside
	// the test, so every attempt counted below is a first attempt.
	cfg.Backoff = outbox.BackoffPolicy{Base: time.Minute, Max: time.Minute, Multiplier: 2}
	return outbox.NewPublisher(outbox.NewStore(testPool), broker, cfg,
		discardLogger(), rand.New(rand.NewSource(1)))
}

// A backlog larger than one batch, which is what a broker outage or a burst of
// submissions leaves behind, is published within one tick rather than one batch
// per poll interval. The first tick is three seconds in and the second six, so a
// loop that published one batch per tick would still hold twenty events at 5.5 s.
func TestOutbox_RunLoopPublishesABacklogOfSeveralBatchesInOneTick(t *testing.T) {
	reset(t)
	// A queue of its own, so its exact count cannot meet a delivery another
	// test left in flight on the shared one.
	broker := newBrokerForQueue(t, createIsolatedBrokerQueue(t, "taskforge-drain-"))
	submitJobs(t, 30)
	require.Equal(t, 30, countPendingOutbox(t))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- drainingPublisher(t, broker).Run(ctx) }()

	eventually(t, 5500*time.Millisecond, "the whole backlog publishes before the second tick", func() bool {
		return countPendingOutbox(t) == 0
	})
	cancel()
	require.NoError(t, <-done)
	require.Equal(t, 30, countPublishedOutbox(t))
	require.Len(t, receiveAll(t, broker, 500*time.Millisecond), 30)
}

// unreachableBroker fails every publish at once, the way a refused connection
// does. Failing instantly is the point: a failure that took seconds would hide a
// loop that spun on failures behind its own slowness.
type unreachableBroker struct{}

func (unreachableBroker) Publish(context.Context, []byte) error {
	return errors.New("broker unreachable")
}

// Draining stops at the first batch that did not publish in full. With the
// broker down, a loop that kept going on full batches would try the whole
// backlog in a tight loop on every tick; it must try one batch and wait.
func TestOutbox_RunLoopDoesNotDrainWhileTheBrokerIsDown(t *testing.T) {
	reset(t)
	publisher := drainingPublisher(t, unreachableBroker{})
	submitJobs(t, 30)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- publisher.Run(ctx) }()

	attempted := func() int {
		var n int
		require.NoError(t, testPool.QueryRow(context.Background(),
			`SELECT count(*) FROM outbox_events WHERE attempts > 0`).Scan(&n))
		return n
	}
	eventually(t, 5500*time.Millisecond, "the first tick attempts a batch", func() bool {
		return attempted() >= 10
	})
	// Absence cannot be polled for. The second tick is at six seconds; until
	// then, a loop that ignored the failures would already have tried them all,
	// since every one of them fails at once.
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, 10, attempted(), "one failed batch per tick, not the whole backlog")
	require.Equal(t, 30, countPendingOutbox(t))

	cancel()
	require.NoError(t, <-done)
}
