package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/results"
)

// fakeResults is a results reader with no database behind it.
type fakeResults struct {
	get func(context.Context, string, uuid.UUID) (*results.Result, error)
}

func (f *fakeResults) Get(ctx context.Context, scope string, jobID uuid.UUID) (*results.Result, error) {
	if f.get == nil {
		return nil, results.ErrResultNotFound
	}
	return f.get(ctx, scope, jobID)
}

// acceptingResults answers ErrResultNotFound for everything unless a test
// overrides get, mirroring acceptingKeys' role for APIKeys: a safe, non-nil
// default so a route that reaches it does something well-defined rather than
// panic, while a genuinely wrong call (wrong scope, wrong id) is still
// exactly as visible as it would be against a real store.
func acceptingResults() *fakeResults { return &fakeResults{} }

// fakeObjectStore is an object-store reader with no S3-compatible service
// behind it.
type fakeObjectStore struct {
	get func(context.Context, string, string) ([]byte, error)
}

func (f *fakeObjectStore) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	if f.get == nil {
		return nil, results.ErrResultNotFound
	}
	return f.get(ctx, bucket, key)
}

func newTestServerWithResults(t *testing.T, store Results, objects ObjectStore) http.Handler {
	t.Helper()
	return NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
		WithAuth(acceptingKeys(testScope)).
		WithResults(store, objects).
		Handler()
}

func getResult(t *testing.T, h http.Handler, jobID string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authorize(httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID+"/result", nil)))
	return rec
}

func TestGetJobResult_ServesAnInlineResultAsItsExactBytes(t *testing.T) {
	jobID := uuid.New()
	store := &fakeResults{get: func(_ context.Context, scope string, id uuid.UUID) (*results.Result, error) {
		require.Equal(t, testScope, scope)
		require.Equal(t, jobID, id)
		return &results.Result{
			JobID: jobID, Location: results.LocationInline,
			InlineBody: json.RawMessage(`{"answer":42}`),
		}, nil
	}}

	rec := getResult(t, newTestServerWithResults(t, store, nil), jobID.String())
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	require.JSONEq(t, `{"answer":42}`, rec.Body.String())
}

func TestGetJobResult_ProxiesAnObjectLocatedResult(t *testing.T) {
	jobID := uuid.New()
	store := &fakeResults{get: func(context.Context, string, uuid.UUID) (*results.Result, error) {
		return &results.Result{
			JobID: jobID, Location: results.LocationObject,
			ObjectBucket: "taskforge-results", ObjectKey: "results/scope/job/attempt",
		}, nil
	}}
	objects := &fakeObjectStore{get: func(_ context.Context, bucket, key string) ([]byte, error) {
		require.Equal(t, "taskforge-results", bucket)
		require.Equal(t, "results/scope/job/attempt", key)
		return []byte(`{"large":true}`), nil
	}}

	rec := getResult(t, newTestServerWithResults(t, store, objects), jobID.String())
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"large":true}`, rec.Body.String())
}

// An object-located result with no object store configured is an operator
// misconfiguration -- a sanitized 500, not a panic and not a silently empty
// body.
func TestGetJobResult_ObjectLocatedWithNoObjectStoreConfiguredIsInternalError(t *testing.T) {
	jobID := uuid.New()
	store := &fakeResults{get: func(context.Context, string, uuid.UUID) (*results.Result, error) {
		return &results.Result{
			JobID: jobID, Location: results.LocationObject,
			ObjectBucket: "taskforge-results", ObjectKey: "results/scope/job/attempt",
		}, nil
	}}

	rec := getResult(t, newTestServerWithResults(t, store, nil), jobID.String())
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, CodeInternal, decodeError(t, rec).Error.Code)
}

// No result recorded and a malformed id must be indistinguishable, for the
// identical anti-oracle reason GET /v1/jobs/{job_id} treats them the same.
func TestGetJobResult_NotFoundAndMalformedIDAreIndistinguishable(t *testing.T) {
	notFound := acceptingResults() // get is nil, so every lookup answers ErrResultNotFound
	handler := newTestServerWithResults(t, notFound, nil)

	for name, jobID := range map[string]string{
		"no result recorded": uuid.New().String(),
		"malformed id":       "not-a-uuid",
	} {
		t.Run(name, func(t *testing.T) {
			rec := getResult(t, handler, jobID)
			require.Equal(t, http.StatusNotFound, rec.Code)
			require.Equal(t, CodeNotFound, decodeError(t, rec).Error.Code)
		})
	}
}

func TestGetJobResult_RequiresAuthentication(t *testing.T) {
	handler := newTestServerWithResults(t, acceptingResults(), nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/v1/jobs/"+uuid.New().String()+"/result", nil))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, CodeUnauthorized, decodeError(t, rec).Error.Code)
}

// Unlike WithWorkerControl, WithAuth, and WithWorkerAuth, omitting
// WithResults does not remove the route: PROJECT_SPEC.md item 13 makes
// result retrieval core public surface, so an authenticated caller gets a
// sanitized 500 for a real operator misconfiguration, never a 404 that
// would make a real route look like it does not exist.
func TestGetJobResult_MissingResultsStoreIsInternalErrorNotNotFound(t *testing.T) {
	handler := NewServer(nil, Config{MaxRequestBytes: 1024}, discardLogger()).
		WithAuth(acceptingKeys(testScope)).
		Handler()
	rec := getResult(t, handler, uuid.New().String())
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, CodeInternal, decodeError(t, rec).Error.Code)
}

func TestGetJobResult_MethodNotAllowed(t *testing.T) {
	handler := newTestServerWithResults(t, acceptingResults(), nil)
	rec := httptest.NewRecorder()
	req := authorize(httptest.NewRequest(http.MethodPost,
		"/v1/jobs/"+uuid.New().String()+"/result", nil))
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	require.Equal(t, http.MethodGet, rec.Header().Get("Allow"))
}
