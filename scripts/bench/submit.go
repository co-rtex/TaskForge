package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// submitter posts the benchmark's workload to POST /v1/jobs over net/http. It
// does not run taskforge-cli once per job: a process spawn per submission would
// make the load generator's own cost a large part of what it measures.
type submitter struct {
	client *http.Client
	url    string
	key    string
	runID  string
	body   []byte
	// backoff is the wait before each retry; its length is the number of retries.
	backoff []time.Duration

	ok, retries, failed atomic.Int64
}

// submitStats is how submission went.
type submitStats struct {
	OK      int
	Retries int
	Failed  int
}

func newSubmitter(apiURL, apiKey, runID string) *submitter {
	return newSubmitterWithDuration(apiURL, apiKey, runID, jobDurationMS*time.Millisecond)
}

// newSubmitterWithDuration is newSubmitter for jobs that sleep for jobDuration. The
// throughput run and every recorded run call newSubmitter, which is the fixed
// workload; only the smoke's fault run asks for another duration (see
// options.faultJobDuration).
func newSubmitterWithDuration(apiURL, apiKey, runID string, jobDuration time.Duration) *submitter {
	return newSubmitterFor(apiURL, apiKey, runID, jobDuration, jobTimeoutSecs)
}

// newSubmitterFor is newSubmitterWithDuration with the jobs' timeout_seconds
// given too. Only the recovery probe's idle condition passes anything but
// jobTimeoutSecs: its one job sleeps longer than that.
func newSubmitterFor(apiURL, apiKey, runID string, jobDuration time.Duration, timeoutSecs int) *submitter {
	// The workload is demo.sleep for jobDuration, which is jobDurationMS unless the
	// smoke's fault run says otherwise. max_attempts and timeout_seconds are sent
	// rather than left to the API's defaults, so the record can say what they were.
	body, err := json.Marshal(struct {
		Queue          string         `json:"queue"`
		JobType        string         `json:"job_type"`
		Payload        map[string]int `json:"payload"`
		MaxAttempts    int            `json:"max_attempts"`
		TimeoutSeconds int            `json:"timeout_seconds"`
	}{"default", jobType, map[string]int{"duration_ms": int(jobDuration / time.Millisecond)}, jobMaxAttempts, timeoutSecs})
	if err != nil {
		panic(fmt.Sprintf("marshal a constant workload: %v", err)) // cannot happen: no input
	}
	return &submitter{
		client: &http.Client{
			Timeout: 15 * time.Second,
			// Many submissions are in flight at once; reuse their connections
			// instead of opening one per request.
			Transport: &http.Transport{MaxIdleConnsPerHost: 128, MaxIdleConns: 128, IdleConnTimeout: 30 * time.Second},
		},
		url: apiURL, key: apiKey, runID: runID, body: body,
		backoff: []time.Duration{100 * time.Millisecond, 400 * time.Millisecond},
	}
}

// submit creates job number i. The Idempotency-Key is derived from the run and
// from i, so asking again for the same i can never create a second job: that is
// what makes the retry below safe.
//
// Only a transport error, a 5xx or a 429 is retried. Anything else the API
// refuses it would refuse again.
func (s *submitter) submit(ctx context.Context, i int) error {
	var last error
	for attempt := 0; attempt <= len(s.backoff); attempt++ {
		if attempt > 0 {
			s.retries.Add(1)
			select {
			case <-ctx.Done():
				s.failed.Add(1)
				return ctx.Err()
			case <-time.After(s.backoff[attempt-1]):
			}
		}
		retryable, err := s.once(ctx, i)
		if err == nil {
			s.ok.Add(1)
			return nil
		}
		last = err
		if !retryable || ctx.Err() != nil {
			break
		}
	}
	s.failed.Add(1)
	return last
}

func (s *submitter) once(ctx context.Context, i int) (retryable bool, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url+"/v1/jobs", bytes.NewReader(s.body))
	if err != nil {
		return false, err
	}
	request.Header.Set("Authorization", "Bearer "+s.key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", fmt.Sprintf("bench-%s-%d", s.runID, i))

	response, err := s.client.Do(request)
	if err != nil {
		return true, fmt.Errorf("submit job %d: %w", i, err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))

	switch {
	case response.StatusCode == http.StatusCreated, response.StatusCode == http.StatusOK:
		return false, nil
	case response.StatusCode >= 500, response.StatusCode == http.StatusTooManyRequests:
		return true, fmt.Errorf("submit job %d: HTTP %d", i, response.StatusCode)
	default:
		return false, fmt.Errorf("submit job %d: HTTP %d", i, response.StatusCode)
	}
}

func (s *submitter) stats() submitStats {
	return submitStats{OK: int(s.ok.Load()), Retries: int(s.retries.Load()), Failed: int(s.failed.Load())}
}
