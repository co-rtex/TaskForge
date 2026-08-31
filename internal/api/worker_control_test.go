package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/workers"
)

type fakeWorkerControl struct {
	register func(context.Context, string, workers.Registration) (workers.Session, error)
	claim    func(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error)
	start    func(context.Context, string, workers.Fence) error
	succeed  func(context.Context, string, workers.Fence) error
}

func (f *fakeWorkerControl) Register(ctx context.Context, scope string, req workers.Registration) (workers.Session, error) {
	return f.register(ctx, scope, req)
}
func (f *fakeWorkerControl) Claim(ctx context.Context, scope string, req workers.ClaimRequest) (workers.ClaimResult, error) {
	return f.claim(ctx, scope, req)
}
func (f *fakeWorkerControl) Start(ctx context.Context, scope string, fence workers.Fence) error {
	return f.start(ctx, scope, fence)
}
func (f *fakeWorkerControl) Succeed(ctx context.Context, scope string, fence workers.Fence) error {
	return f.succeed(ctx, scope, fence)
}

func newWorkerControlHandler(control WorkerControl) http.Handler {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewServer(nil, Config{MaxRequestBytes: 2048, DevScope: "test"}, log).
		WithWorkerControl(control).Handler()
}

func TestWorkerControl_ClaimResponseCarriesAckDecision(t *testing.T) {
	control := &fakeWorkerControl{
		claim: func(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{Disposition: workers.CapacityExhausted}, nil
		},
	}
	body := `{"worker_id":"` + uuid.NewString() + `","worker_session_id":"` + uuid.NewString() +
		`","claim_request_id":"` + uuid.NewString() + `","queue":"default"}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(body))
	newWorkerControlHandler(control).ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)

	var response ClaimResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, string(workers.CapacityExhausted), response.Outcome)
	require.False(t, response.SafeToAcknowledge)
}

func TestWorkerControl_RejectsUnknownFieldsAndBadIdentifiers(t *testing.T) {
	control := &fakeWorkerControl{}
	handler := newWorkerControlHandler(control)

	t.Run("unknown field", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/internal/v1/claims",
			strings.NewReader(`{"unexpected":true}`))
		handler.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusBadRequest, recorder.Code)
		require.Equal(t, CodeMalformedJSON, decodeError(t, recorder).Error.Code)
	})

	t.Run("bad session id", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPut, "/internal/v1/worker-sessions/not-a-uuid",
			strings.NewReader(`{}`))
		handler.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
		require.Equal(t, CodeValidationFailed, decodeError(t, recorder).Error.Code)
	})

	t.Run("claim reports every semantic field problem", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(
			`{"worker_id":"bad","worker_session_id":"bad","claim_request_id":"bad","queue":"Bad Queue"}`))
		handler.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
		body := decodeError(t, recorder)
		require.Equal(t, CodeValidationFailed, body.Error.Code)
		require.Len(t, body.Error.Details, 4)
	})
}

func TestWorkerControl_MapsSessionAndFenceConflictsToStableCodes(t *testing.T) {
	control := &fakeWorkerControl{
		claim: func(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error) {
			return workers.ClaimResult{}, workers.ErrSessionUnavailable
		},
		start: func(context.Context, string, workers.Fence) error {
			return workers.ErrFenceRejected
		},
	}
	handler := newWorkerControlHandler(control)

	claimBody := `{"worker_id":"` + uuid.NewString() + `","worker_session_id":"` + uuid.NewString() +
		`","claim_request_id":"` + uuid.NewString() + `","queue":"default"}`
	claimRecorder := httptest.NewRecorder()
	handler.ServeHTTP(claimRecorder,
		httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(claimBody)))
	require.Equal(t, http.StatusConflict, claimRecorder.Code)
	require.Equal(t, CodeSessionUnavailable, decodeError(t, claimRecorder).Error.Code)

	attemptID := uuid.NewString()
	fenceBody := `{"job_id":"` + uuid.NewString() + `","lease_id":"` + uuid.NewString() +
		`","worker_id":"` + uuid.NewString() + `","worker_session_id":"` + uuid.NewString() + `"}`
	fenceRecorder := httptest.NewRecorder()
	handler.ServeHTTP(fenceRecorder, httptest.NewRequest(http.MethodPost,
		"/internal/v1/attempts/"+attemptID+"/start", strings.NewReader(fenceBody)))
	require.Equal(t, http.StatusConflict, fenceRecorder.Code)
	require.Equal(t, CodeFenceRejected, decodeError(t, fenceRecorder).Error.Code)
}

func TestWorkerControl_TransitionUsesThePathAttemptID(t *testing.T) {
	attemptID := uuid.New()
	var got workers.Fence
	control := &fakeWorkerControl{
		start: func(_ context.Context, _ string, fence workers.Fence) error {
			got = fence
			return nil
		},
	}
	body := `{"job_id":"` + uuid.NewString() + `","lease_id":"` + uuid.NewString() +
		`","worker_id":"` + uuid.NewString() + `","worker_session_id":"` + uuid.NewString() + `"}`
	recorder := httptest.NewRecorder()
	newWorkerControlHandler(control).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost,
		"/internal/v1/attempts/"+attemptID.String()+"/start", strings.NewReader(body)))
	require.Equal(t, http.StatusNoContent, recorder.Code)
	require.Equal(t, attemptID, got.AttemptID)
}

var _ WorkerControl = (*fakeWorkerControl)(nil)

// TestIsDeadlineExhausted_ClassifiesOnlyTheReturnedError is the guard against
// laundering unrelated failures into a retryable 503. Classification reads the
// error and nothing else, so an expired context can never promote a constraint
// violation, a driver fault, or a bug into "deadline exhausted".
func TestIsDeadlineExhausted_ClassifiesOnlyTheReturnedError(t *testing.T) {
	tests := map[string]struct {
		err  error
		want bool
	}{
		"nil error":                      {nil, false},
		"raw deadline":                   {context.DeadlineExceeded, true},
		"wrapped deadline":               {fmt.Errorf("lock queue capacity: %w", context.DeadlineExceeded), true},
		"store deadline sentinel":        {workers.ErrDeadlineExceeded, true},
		"wrapped store sentinel":         {fmt.Errorf("claim job: %w", workers.ErrDeadlineExceeded), true},
		"cancellation is not a deadline": {context.Canceled, false},
		"wrapped cancellation":           {fmt.Errorf("lock queue capacity: %w", context.Canceled), false},
		"unrelated driver error":         {errors.New("SQLSTATE 23505 unique violation"), false},
		"unrelated domain error":         {workers.ErrStateConflict, false},
		"deadline-shaped message only":   {errors.New("context deadline exceeded"), false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.want, isDeadlineExhausted(test.err))
		})
	}
}

// TestWorkerControl_ExpiredContextDoesNotMaskUnrelatedFailures drives the real
// handler with an already-expired request context. Only an error that itself
// carries the deadline may become a 503; everything else keeps its own mapping.
func TestWorkerControl_ExpiredContextDoesNotMaskUnrelatedFailures(t *testing.T) {
	tests := map[string]struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		"unrelated internal failure under an expired context": {
			errors.New("insert job attempt: SQLSTATE 23505"), http.StatusInternalServerError, CodeInternal,
		},
		"client cancellation under an expired context": {
			fmt.Errorf("lock queue capacity: %w", context.Canceled), http.StatusInternalServerError, CodeInternal,
		},
		"domain conflict under an expired context": {
			workers.ErrSessionUnavailable, http.StatusConflict, CodeSessionUnavailable,
		},
		"genuine deadline under an expired context": {
			fmt.Errorf("lock queue capacity: %w", workers.ErrDeadlineExceeded),
			http.StatusServiceUnavailable, CodeServiceUnavailable,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			control := &fakeWorkerControl{
				claim: func(context.Context, string, workers.ClaimRequest) (workers.ClaimResult, error) {
					return workers.ClaimResult{}, test.err
				},
			}
			body := fmt.Sprintf(
				`{"worker_id":%q,"worker_session_id":%q,"claim_request_id":%q,"queue":"default"}`,
				uuid.NewString(), uuid.NewString(), uuid.NewString())
			request := httptest.NewRequest(http.MethodPost, "/internal/v1/claims", strings.NewReader(body))

			// The request context is already past its deadline before the handler
			// runs, which is exactly the state that used to force a 503.
			expired, cancel := context.WithDeadline(request.Context(), time.Now().Add(-time.Second))
			defer cancel()
			require.ErrorIs(t, expired.Err(), context.DeadlineExceeded, "the test context must be expired")
			request = request.WithContext(expired)

			recorder := httptest.NewRecorder()
			newWorkerControlHandler(control).ServeHTTP(recorder, request)

			require.Equal(t, test.wantStatus, recorder.Code)
			var envelope ErrorBody
			require.NoError(t, json.NewDecoder(recorder.Body).Decode(&envelope))
			require.Equal(t, test.wantCode, envelope.Error.Code)
		})
	}
}
