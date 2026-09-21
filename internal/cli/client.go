package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultRequestTimeout bounds a single taskforge-cli HTTP call to the API.
// It is deliberately independent of TASKFORGE_API_REQUEST_TIMEOUT (the
// server's own per-request budget, see .env.example): taskforge-cli is a
// client outside the server process and must bound its own wait
// regardless of what the server it happens to be talking to is configured
// with.
const DefaultRequestTimeout = 30 * time.Second

// Client is a thin HTTP client for TaskForge's public ("/v1") and
// loopback-only internal key-management ("/internal/v1/api-keys",
// "/internal/v1/worker-keys") surfaces. It contains no retry logic and no
// credential material handling beyond carrying a caller-supplied bearer
// token: TaskForge's credential model is implemented entirely server-side
// (internal/auth, internal/workerauth), and this client only ever presents
// a token it was given -- it never generates, parses, or verifies one. See
// docs/adr/0013-database-backed-api-key-authentication.md and
// docs/adr/0014-worker-control-authentication.md.
type Client struct {
	// BaseURL is the API's address, scheme included, with no trailing
	// slash -- e.g. "http://127.0.0.1:8080". See ResolveBaseURL for how it
	// is derived from --api-url / TASKFORGE_CLI_API_URL.
	BaseURL string
	// APIKey, when set, is sent as "Authorization: Bearer <APIKey>" on
	// every request. Callers of the loopback-only key-management routes
	// leave it empty: those routes are themselves unauthenticated by
	// design (ADR-0013), and the API ignores an Authorization header they
	// do not check.
	APIKey string
	// HTTPClient is the underlying transport. NewClient sets one bounded
	// by DefaultRequestTimeout; tests substitute their own.
	HTTPClient *http.Client
}

// NewClient builds a Client bound by DefaultRequestTimeout.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		HTTPClient: &http.Client{
			Timeout: DefaultRequestTimeout,
		},
	}
}

// TransportError reports that a request never produced an HTTP response:
// DNS failure, connection refused, TLS failure, or a client-side timeout.
// Callers map it to ExitTransportError, never to ExitServiceUnavailable --
// the latter means the API itself was reached and reported that ITS OWN
// deadline elapsed.
type TransportError struct {
	Err error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("could not reach the API: %v", e.Err)
}

func (e *TransportError) Unwrap() error { return e.Err }

// Response is a raw API response: a status code and body, not yet
// interpreted as success or failure. Callers classify it because which
// status codes mean success differs per operation -- e.g. job submission
// treats both 200 (idempotent replay) and 201 (created) as success, while
// DLQ replay treats 200 and 201 the same way for the identical reason.
type Response struct {
	StatusCode int
	Body       []byte
}

// do issues one HTTP request against a public ("/v1") route, presenting
// APIKey when set, and returns its raw response, or a *TransportError if
// the request never produced one. It never returns a non-nil error for an
// HTTP-level failure (4xx/5xx) -- that is a Response like any other, for
// the caller to classify with exitCodeForAPIError.
func (c *Client) do(ctx context.Context, method, path string, headers map[string]string, body []byte) (*Response, error) {
	return c.doWithAuth(ctx, method, path, headers, body, true)
}

// doUnauthenticated is do's counterpart for the loopback-only
// "/internal/v1/api-keys" and "/internal/v1/worker-keys" routes, which are
// themselves unauthenticated by design (ADR-0013, ADR-0014): they never
// receive an Authorization header, even when a caller configured one via
// --api-key / TASKFORGE_CLI_API_KEY for the public routes in the same
// invocation. Presenting an unrelated credential to a route that does not
// check it would be silently ignored by the server, but it is still the
// wrong behavior for a client to send a secret nothing asked for.
func (c *Client) doUnauthenticated(ctx context.Context, method, path string, body []byte) (*Response, error) {
	return c.doWithAuth(ctx, method, path, nil, body, false)
}

func (c *Client) doWithAuth(ctx context.Context, method, path string, headers map[string]string, body []byte, useAuth bool) (*Response, error) {
	target := c.BaseURL + path
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, &TransportError{Err: err}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if useAuth && c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, &TransportError{Err: err}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &TransportError{Err: fmt.Errorf("read response body: %w", err)}
	}
	return &Response{StatusCode: resp.StatusCode, Body: respBody}, nil
}

// SubmitJob calls POST /v1/jobs.
func (c *Client) SubmitJob(ctx context.Context, idempotencyKey string, body []byte) (*Response, error) {
	headers := map[string]string{"Idempotency-Key": idempotencyKey}
	return c.do(ctx, http.MethodPost, "/v1/jobs", headers, body)
}

// GetJob calls GET /v1/jobs/{job_id}.
func (c *Client) GetJob(ctx context.Context, jobID string) (*Response, error) {
	return c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(jobID), nil, nil)
}

// GetResult calls GET /v1/jobs/{job_id}/result.
func (c *Client) GetResult(ctx context.Context, jobID string) (*Response, error) {
	return c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(jobID)+"/result", nil, nil)
}

// CancelJob calls POST /v1/jobs/{job_id}/cancel. The route takes no
// request body.
func (c *Client) CancelJob(ctx context.Context, jobID string) (*Response, error) {
	return c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/cancel", nil, nil)
}

// RetryJob calls POST /v1/jobs/{job_id}/retry.
func (c *Client) RetryJob(ctx context.Context, jobID, idempotencyKey string) (*Response, error) {
	headers := map[string]string{"Idempotency-Key": idempotencyKey}
	return c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/retry", headers, nil)
}

// ListDLQ calls GET /v1/dlq. limit <= 0 and an empty cursor are both
// omitted, letting the server apply its own documented defaults.
func (c *Client) ListDLQ(ctx context.Context, limit int, cursor string) (*Response, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	path := "/v1/dlq"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return c.do(ctx, http.MethodGet, path, nil, nil)
}

// ReplayDLQ calls POST /v1/dlq/{job_id}/replay.
func (c *Client) ReplayDLQ(ctx context.Context, jobID, idempotencyKey string) (*Response, error) {
	headers := map[string]string{"Idempotency-Key": idempotencyKey}
	return c.do(ctx, http.MethodPost, "/v1/dlq/"+url.PathEscape(jobID)+"/replay", headers, nil)
}

// CreateAPIKey calls POST /internal/v1/api-keys. This route is itself
// unauthenticated (ADR-0013): no Authorization header is ever sent,
// regardless of whether --api-key / TASKFORGE_CLI_API_KEY is set.
func (c *Client) CreateAPIKey(ctx context.Context, body []byte) (*Response, error) {
	return c.doUnauthenticated(ctx, http.MethodPost, "/internal/v1/api-keys", body)
}

// ListAPIKeys calls GET /internal/v1/api-keys.
func (c *Client) ListAPIKeys(ctx context.Context, limit int) (*Response, error) {
	path := "/internal/v1/api-keys"
	if limit > 0 {
		path += "?limit=" + strconv.Itoa(limit)
	}
	return c.doUnauthenticated(ctx, http.MethodGet, path, nil)
}

// RevokeAPIKey calls POST /internal/v1/api-keys/{key_id}/revoke.
func (c *Client) RevokeAPIKey(ctx context.Context, keyID string) (*Response, error) {
	return c.doUnauthenticated(ctx, http.MethodPost, "/internal/v1/api-keys/"+url.PathEscape(keyID)+"/revoke", nil)
}

// CreateWorkerKey calls POST /internal/v1/worker-keys. Unauthenticated for
// the identical reason CreateAPIKey is (ADR-0014).
func (c *Client) CreateWorkerKey(ctx context.Context, body []byte) (*Response, error) {
	return c.doUnauthenticated(ctx, http.MethodPost, "/internal/v1/worker-keys", body)
}

// ListWorkerKeys calls GET /internal/v1/worker-keys.
func (c *Client) ListWorkerKeys(ctx context.Context, limit int) (*Response, error) {
	path := "/internal/v1/worker-keys"
	if limit > 0 {
		path += "?limit=" + strconv.Itoa(limit)
	}
	return c.doUnauthenticated(ctx, http.MethodGet, path, nil)
}

// RevokeWorkerKey calls POST /internal/v1/worker-keys/{key_id}/revoke.
func (c *Client) RevokeWorkerKey(ctx context.Context, keyID string) (*Response, error) {
	return c.doUnauthenticated(ctx, http.MethodPost, "/internal/v1/worker-keys/"+url.PathEscape(keyID)+"/revoke", nil)
}
