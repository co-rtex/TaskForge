package main

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
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

// The duration is the one thing newSubmitterWithDuration changes. newSubmitter,
// which the throughput run and every recorded run use, is the fixed workload.
func TestSubmit_TheDurationIsTheOnlyThingAnotherWorkloadChanges(t *testing.T) {
	srv, seen := recordingServer(t, http.StatusCreated)

	require.NoError(t, newSubmitterWithDuration(srv.URL, "k", "run1", 3*time.Second).submit(context.Background(), 1))
	require.NoError(t, newSubmitter(srv.URL, "k", "run1").submit(context.Background(), 2))

	require.Len(t, *seen, 2)
	long, fixed := (*seen)[0].body, (*seen)[1].body
	require.Equal(t, float64(3000), long["payload"].(map[string]any)["duration_ms"], "3 s is 3000 ms")
	require.Equal(t, float64(50), fixed["payload"].(map[string]any)["duration_ms"], "newSubmitter is demo.sleep for 50 ms")

	delete(long, "payload")
	delete(fixed, "payload")
	require.Equal(t, fixed, long, "queue, job type, max_attempts and timeout are the same for both")
}

// requireCallsIn parses a source file of this package and returns, for each call to
// one of the named functions inside the named function, the source text of its
// arguments. It lets a test pin WHICH constructor a run calls and with what, which
// a unit test cannot observe by running them: both runs need a real stack.
func requireCallsIn(t *testing.T, file, inFunc string, callee ...string) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, 0)
	require.NoError(t, err)

	found := map[string][]string{}
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != inFunc {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			ident, ok := call.Fun.(*ast.Ident)
			if !ok || !slices.Contains(callee, ident.Name) {
				return true
			}
			var args []string
			for _, arg := range call.Args {
				args = append(args, types.ExprString(arg))
			}
			found[ident.Name] = append(found[ident.Name], strings.Join(args, ", "))
			return true
		})
		return found
	}
	require.Failf(t, "function not found", "%s is not declared in %s", inFunc, file)
	return nil
}

// TestRunThroughput_UsesTheFixedWorkload pins that the throughput run, which the
// smoke runs too, builds its submitter with newSubmitter, the fixed 50 ms workload,
// and never with a duration of its own.
func TestRunThroughput_UsesTheFixedWorkload(t *testing.T) {
	calls := requireCallsIn(t, "run_throughput.go", "runThroughput", "newSubmitter", "newSubmitterWithDuration")
	require.Len(t, calls["newSubmitter"], 1, "runThroughput builds its submitter with newSubmitter")
	require.Empty(t, calls["newSubmitterWithDuration"], "and never with a duration of its own")
}

// TestRunFaults_ThreadsTheFaultJobDurationIntoItsSubmitter pins that the fault run
// builds its submitter from the options' fault-job duration, which is 50 ms unless
// the smoke lengthens it, and not from the jobDurationMS constant.
func TestRunFaults_ThreadsTheFaultJobDurationIntoItsSubmitter(t *testing.T) {
	calls := requireCallsIn(t, "run_faults.go", "runFaults", "newSubmitter", "newSubmitterWithDuration")
	require.Empty(t, calls["newSubmitter"], "runFaults must not fall back to the fixed workload")
	require.Len(t, calls["newSubmitterWithDuration"], 1)
	require.True(t, strings.HasSuffix(calls["newSubmitterWithDuration"][0], "o.faultJobDuration"),
		"the duration is the option's, got: %s", calls["newSubmitterWithDuration"][0])
}
