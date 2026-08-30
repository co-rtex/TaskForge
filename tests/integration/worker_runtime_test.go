//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/queue"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
	"github.com/co-rtex/TaskForge/internal/workers"
)

type countingBroker struct {
	queue.Broker
	deleted atomic.Int32
}

func (b *countingBroker) Delete(ctx context.Context, receiptHandle string) error {
	if err := b.Broker.Delete(ctx, receiptHandle); err != nil {
		return err
	}
	b.deleted.Add(1)
	return nil
}

func TestWorker_EndToEndAndDuplicateBrokerDeliveryCreateOneAttempt(t *testing.T) {
	reset(t)
	server := newAPI(t)
	broker := newBroker(t, "")

	response, submitted := submit(t, server.URL, "worker-e2e",
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"hello"}}`)
	require.Equal(t, http.StatusCreated, response.StatusCode)

	stats, err := newPublisher(t, broker).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Published)

	// Capture and remove the real outbox delivery, then put two byte-identical
	// copies back on the real broker to exercise duplicate-delivery semantics.
	bodies := receiveAll(t, broker, 200*time.Millisecond)
	require.Len(t, bodies, 1)
	require.NoError(t, broker.Publish(context.Background(), bodies[0]))
	require.NoError(t, broker.Publish(context.Background(), bodies[0]))

	observedBroker := &countingBroker{Broker: broker}
	control := workerruntime.NewClient(server.URL, &http.Client{Timeout: 10 * time.Second})
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	runner := workerruntime.NewRunner(control, observedBroker, registry, workerruntime.RunnerConfig{
		Registration: workers.Registration{
			SessionID: uuid.New(), Name: "e2e-worker", Hostname: "e2e.local",
			WorkerGroup: "default", ConcurrencyLimit: 2, Capabilities: []string{"cpu"},
		},
		Queue: "default", PollWait: time.Second,
		RetryAttempts: 3, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	eventually(t, 15*time.Second, "trusted demo.echo completes the submitted job", func() bool {
		var status string
		return testPool.QueryRow(context.Background(),
			`SELECT status FROM jobs WHERE id = $1`, submitted.ID).Scan(&status) == nil && status == "SUCCEEDED"
	})
	eventually(t, 15*time.Second, "both duplicate notifications are safely acknowledged", func() bool {
		return observedBroker.deleted.Load() == 2
	})

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after cancellation")
	}

	require.Equal(t, 1, countRows(t, "job_attempts"),
		"duplicate notifications must not create a second attempt for one queued job")
	require.Equal(t, 1, countRows(t, "leases"))
	require.Equal(t, 0, countActiveLeases(t))
	var attemptStatus, leaseStatus string
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT a.status, l.status
		FROM job_attempts a JOIN leases l ON l.attempt_id = a.id
		WHERE a.job_id = $1`, submitted.ID).Scan(&attemptStatus, &leaseStatus))
	require.Equal(t, "SUCCEEDED", attemptStatus)
	require.Equal(t, "COMPLETED", leaseStatus)
}
