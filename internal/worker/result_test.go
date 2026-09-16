package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/results"
	"github.com/co-rtex/TaskForge/internal/workers"
)

// fakeObjectStore is an object-store writer with no S3-compatible service
// behind it.
type fakeObjectStore struct {
	put   func(ctx context.Context, bucket, key string, body []byte, contentType string) error
	calls int
}

func (f *fakeObjectStore) Put(ctx context.Context, bucket, key string, body []byte, contentType string) error {
	f.calls++
	if f.put == nil {
		return nil
	}
	return f.put(ctx, bucket, key, body, contentType)
}

func testRunnerWithObjects(objects ObjectStore, threshold int) *Runner {
	return NewRunner(&fakeControl{}, &fakeBroker{}, NewRegistry(), objects, RunnerConfig{
		Queue: "default", PollWait: time.Second, RetryAttempts: 2,
		ShutdownTimeout: time.Second, ResultInlineThresholdBytes: threshold,
		ResultsBucket: "taskforge-results",
	}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func testFence() workers.Fence {
	return workers.Fence{
		JobID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(),
		WorkerID: uuid.New(), SessionID: uuid.New(),
	}
}

func TestPrepareResult_EmptyHandlerResultIsNoResult(t *testing.T) {
	r := testRunnerWithObjects(nil, 100)
	ref, ok := r.prepareResult(context.Background(), testFence(), "scope-a", nil)
	require.True(t, ok)
	require.Nil(t, ref)

	ref, ok = r.prepareResult(context.Background(), testFence(), "scope-a", []byte{})
	require.True(t, ok)
	require.Nil(t, ref)
}

func TestPrepareResult_BelowThresholdStaysInlineAndNeverTouchesTheObjectStore(t *testing.T) {
	objects := &fakeObjectStore{}
	r := testRunnerWithObjects(objects, 100)
	body := []byte(`{"small":true}`)

	ref, ok := r.prepareResult(context.Background(), testFence(), "scope-a", body)
	require.True(t, ok)
	require.NotNil(t, ref)
	require.Equal(t, results.LocationInline, ref.Location)
	require.Equal(t, body, []byte(ref.Inline))
	require.Equal(t, int64(len(body)), ref.SizeBytes)
	require.Empty(t, ref.Bucket)
	require.Empty(t, ref.Key)
	require.Empty(t, ref.Checksum)
	require.Zero(t, objects.calls, "an inline result must never reach the object store")
}

// TestPrepareResult_AtOrAboveThresholdUploadsBeforeReturning is the worker-side
// half of ROADMAP.md's M5C acceptance criterion: the threshold is tested on
// both sides of the boundary, and here specifically that reaching it drives
// a real upload rather than only changing which branch Classify takes.
func TestPrepareResult_AtOrAboveThresholdUploadsBeforeReturning(t *testing.T) {
	const threshold = 10
	body := []byte(`{"this body is well over the ten byte threshold":true}`)
	require.GreaterOrEqual(t, len(body), threshold)

	var gotBucket, gotKey, gotContentType string
	var gotBody []byte
	objects := &fakeObjectStore{put: func(_ context.Context, bucket, key string, b []byte, contentType string) error {
		gotBucket, gotKey, gotContentType, gotBody = bucket, key, contentType, b
		return nil
	}}
	r := testRunnerWithObjects(objects, threshold)
	fence := testFence()

	ref, ok := r.prepareResult(context.Background(), fence, "scope-a", body)
	require.True(t, ok)
	require.NotNil(t, ref)
	require.Equal(t, results.LocationObject, ref.Location)
	require.Nil(t, ref.Inline)
	require.Equal(t, "taskforge-results", ref.Bucket)
	require.Equal(t, int64(len(body)), ref.SizeBytes)
	require.Equal(t, results.ChecksumSHA256(body), ref.Checksum)

	require.Equal(t, 1, objects.calls, "the upload must happen exactly once")
	require.Equal(t, "taskforge-results", gotBucket)
	require.Equal(t, fmt.Sprintf("results/scope-a/%s/%s", fence.JobID, fence.AttemptID), gotKey)
	require.Equal(t, ref.Key, gotKey)
	require.Equal(t, "application/json", gotContentType)
	require.Equal(t, body, gotBody)
}

func TestPrepareResult_UploadFailureAfterRetriesIsNotOK(t *testing.T) {
	objects := &fakeObjectStore{put: func(context.Context, string, string, []byte, string) error {
		return errors.New("connection refused")
	}}
	r := testRunnerWithObjects(objects, 1)

	ref, ok := r.prepareResult(context.Background(), testFence(), "scope-a", []byte(`{"large":true}`))
	require.False(t, ok)
	require.Nil(t, ref)
	require.Equal(t, 2, objects.calls, "RetryAttempts=2 must be honored, not just one bare attempt")
}

func TestPrepareResult_LargeResultWithNoObjectStoreConfiguredIsNotOK(t *testing.T) {
	r := testRunnerWithObjects(nil, 1)
	ref, ok := r.prepareResult(context.Background(), testFence(), "scope-a", []byte(`{"large":true}`))
	require.False(t, ok)
	require.Nil(t, ref)
}
