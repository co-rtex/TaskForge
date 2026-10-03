package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type seenRequest struct {
	method, path, auth, contentType, key string
	body                                 map[string]any
}

// recordingServer answers each request with the next status in statuses (the
// last one repeats) and records what it was sent.
func recordingServer(t *testing.T, statuses ...int) (*httptest.Server, *[]seenRequest) {
	t.Helper()
	var mu sync.Mutex
	var seen []seenRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		mu.Lock()
		n := len(seen)
		seen = append(seen, seenRequest{r.Method, r.URL.Path, r.Header.Get("Authorization"),
			r.Header.Get("Content-Type"), r.Header.Get("Idempotency-Key"), body})
		mu.Unlock()
		w.WriteHeader(statuses[min(n, len(statuses)-1)])
		_, _ = w.Write([]byte(`{"id":"00000000-0000-0000-0000-000000000000"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &seen
}

func TestSubmit_SendsTheWorkloadWithTheBearerKeyAndAnIdempotencyKey(t *testing.T) {
	srv, seen := recordingServer(t, http.StatusCreated)
	s := newSubmitter(srv.URL, "tf_test_key", "run1")

	require.NoError(t, s.submit(context.Background(), 7))

	require.Len(t, *seen, 1)
	got := (*seen)[0]
	require.Equal(t, http.MethodPost, got.method)
	require.Equal(t, "/v1/jobs", got.path)
	require.Equal(t, "Bearer tf_test_key", got.auth)
	require.Equal(t, "application/json", got.contentType)
	require.Equal(t, "bench-run1-7", got.key)
	require.Equal(t, map[string]any{
		"queue": "default", "job_type": "demo.sleep",
		"payload":      map[string]any{"duration_ms": float64(50)},
		"max_attempts": float64(3), "timeout_seconds": float64(30),
	}, got.body, "demo.sleep for 50ms, exactly as the owner fixed it")
}

func TestSubmit_EachJobGetsItsOwnKeyAndTheSameJobAlwaysGetsTheSame(t *testing.T) {
	srv, seen := recordingServer(t, http.StatusCreated)
	s := newSubmitter(srv.URL, "k", "run1")

	for _, i := range []int{0, 1, 0} {
		require.NoError(t, s.submit(context.Background(), i))
	}
	require.Equal(t, []string{"bench-run1-0", "bench-run1-1", "bench-run1-0"},
		[]string{(*seen)[0].key, (*seen)[1].key, (*seen)[2].key})
}

func TestSubmit_AReplayedSubmissionIsASuccess(t *testing.T) {
	srv, _ := recordingServer(t, http.StatusOK)
	require.NoError(t, newSubmitter(srv.URL, "k", "r").submit(context.Background(), 1))
}

// A retry after a server error reuses the idempotency key, which is what makes
// it safe: if the first request did create the job, the second is a replay and
// not a second job.
func TestSubmit_RetriesAServerErrorWithTheSameKeyAndCountsIt(t *testing.T) {
	srv, seen := recordingServer(t, http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusCreated)
	s := newSubmitter(srv.URL, "k", "run1")
	s.backoff = []time.Duration{time.Millisecond, time.Millisecond}

	require.NoError(t, s.submit(context.Background(), 3))

	require.Len(t, *seen, 3)
	for _, r := range *seen {
		require.Equal(t, "bench-run1-3", r.key)
	}
	require.Equal(t, submitStats{OK: 1, Retries: 2}, s.stats())
}

func TestSubmit_GivesUpAfterItsRetriesAndSaysSo(t *testing.T) {
	srv, seen := recordingServer(t, http.StatusInternalServerError)
	s := newSubmitter(srv.URL, "k", "r")
	s.backoff = []time.Duration{time.Millisecond, time.Millisecond}

	err := s.submit(context.Background(), 1)

	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
	require.Len(t, *seen, 3, "the first attempt and two retries")
	require.Equal(t, submitStats{Failed: 1, Retries: 2}, s.stats())
}

func TestSubmit_ARejectionIsNotRetried(t *testing.T) {
	srv, seen := recordingServer(t, http.StatusUnprocessableEntity)
	s := newSubmitter(srv.URL, "k", "r")

	err := s.submit(context.Background(), 1)

	require.Error(t, err)
	require.Contains(t, err.Error(), "422")
	require.Len(t, *seen, 1, "a validation error will not change on a retry")
	require.Equal(t, submitStats{Failed: 1}, s.stats())
}

func TestSubmit_AnUnreachableServerIsAFailureNotAPanic(t *testing.T) {
	srv, _ := recordingServer(t, http.StatusCreated)
	url := srv.URL
	srv.Close()
	s := newSubmitter(url, "k", "r")
	s.backoff = []time.Duration{time.Millisecond, time.Millisecond}

	require.Error(t, s.submit(context.Background(), 1))
	require.Equal(t, 1, s.stats().Failed)
}

func TestSubmit_StopsWhenTheContextEnds(t *testing.T) {
	srv, _ := recordingServer(t, http.StatusServiceUnavailable)
	s := newSubmitter(srv.URL, "k", "r")
	s.backoff = []time.Duration{time.Hour, time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	require.Error(t, s.submit(ctx, 1))
	require.Less(t, time.Since(started), 5*time.Second)
}
