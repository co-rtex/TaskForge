package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClient_SetsAuthorizationHeaderWhenAPIKeySet(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, "tfk_test.secret")
	_, err := c.GetJob(context.Background(), "job-1")
	require.NoError(t, err)
	require.Equal(t, "Bearer tfk_test.secret", gotAuth)
}

func TestClient_OmitsAuthorizationHeaderWhenAPIKeyEmpty(t *testing.T) {
	var sawHeader bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHeader = r.Header.Get("Authorization") != ""
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, "")
	_, err := c.CreateAPIKey(context.Background(), []byte(`{"scope":"s","name":"n"}`))
	require.NoError(t, err)
	require.False(t, sawHeader, "the loopback-only key-management routes are unauthenticated; the client must not send a credential it wasn't given")
}

func TestClient_SendsIdempotencyKeyHeader(t *testing.T) {
	var gotKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, "")
	_, err := c.SubmitJob(context.Background(), "my-key-123", []byte(`{}`))
	require.NoError(t, err)
	require.Equal(t, "my-key-123", gotKey)
}

// TestClient_PathEscapesIDs proves a job id containing characters special
// to a URL (space, '?', '=') is carried as one literal path segment rather
// than being parsed as a query string -- job and key ids are documented as
// UUIDs (api/openapi.yaml: `format: uuid`), so this is a defensive
// guarantee against a malformed id, not a case the server is expected to
// accept.
func TestClient_PathEscapesIDs(t *testing.T) {
	var gotPath, gotRawQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotRawQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, "")
	const weirdID = "weird id?with=chars"
	_, err := c.GetJob(context.Background(), weirdID)
	require.NoError(t, err)
	require.Equal(t, "/v1/jobs/"+weirdID, gotPath, "the escaped id must decode back to exactly the id given, as one path segment")
	require.Empty(t, gotRawQuery, "a '?' inside the id must never be parsed as a query-string separator")
}

func TestClient_ListDLQ_OmitsEmptyQueryParams(t *testing.T) {
	var gotURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, "")
	_, err := c.ListDLQ(context.Background(), 0, "")
	require.NoError(t, err)
	require.Equal(t, "/v1/dlq", gotURL)

	_, err = c.ListDLQ(context.Background(), 10, "cursor-abc")
	require.NoError(t, err)
	require.Equal(t, "/v1/dlq?cursor=cursor-abc&limit=10", gotURL)
}

func TestClient_TransportErrorOnUnreachableHost(t *testing.T) {
	c := NewClient("http://127.0.0.1:1", "")
	_, err := c.GetJob(context.Background(), "job-1")
	require.Error(t, err)
	var transportErr *TransportError
	require.True(t, errors.As(err, &transportErr), "expected a *TransportError, got %T: %v", err, err)
}

func TestClient_ReturnsResponseForHTTPLevelFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"boom"}}`))
	}))
	defer server.Close()

	c := NewClient(server.URL, "")
	resp, err := c.GetJob(context.Background(), "job-1")
	require.NoError(t, err, "an HTTP-level failure is a Response, not a Client error")
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Contains(t, string(resp.Body), "internal_error")
}
