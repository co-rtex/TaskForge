package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/auth"
)

func newKeyAdminServer(t *testing.T, keys *fakeKeys, log *slog.Logger) http.Handler {
	t.Helper()
	if log == nil {
		log = discardLogger()
	}
	return NewServer(nil, Config{MaxRequestBytes: 1024, DevScope: "test"}, log).
		WithAuth(keys).Handler()
}

func doJSON(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return recorder
}

// The key-management routes are loopback operator plumbing and are deliberately
// NOT behind requireAPIKey in this milestone: they are how the first credential
// comes into existence, so requiring one would make the system unbootstrappable.
//
// That is a real trust boundary, and this test states it rather than leaving it
// to be inferred from the absence of a wrapper. If a later milestone
// authenticates this surface, this test is the one that must be changed
// deliberately.
func TestAPIKeyAdmin_IsReachableWithoutACredential(t *testing.T) {
	created := auth.Created{
		Key: auth.Key{
			ID: uuid.New(), Scope: "tenant-a", Name: "ops",
			Prefix: strings.Repeat("a", auth.LookupLen), CreatedAt: time.Now(),
		},
		Raw: testRawKey,
	}
	handler := newKeyAdminServer(t, &fakeKeys{
		create: func(context.Context, string, string) (auth.Created, error) { return created, nil },
	}, nil)

	recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys",
		`{"scope":"tenant-a","name":"ops"}`)
	require.Equal(t, http.StatusCreated, recorder.Code)
}

// Creation returns the raw credential exactly once, and the response is the only
// place it ever appears.
func TestCreateAPIKey_ReturnsTheCredentialOnceAndLogsOnlyItsPrefix(t *testing.T) {
	var logged bytes.Buffer
	id := uuid.New()
	prefix := strings.Repeat("c", auth.LookupLen)
	handler := newKeyAdminServer(t, &fakeKeys{
		create: func(_ context.Context, scope, name string) (auth.Created, error) {
			require.Equal(t, "tenant-a", scope)
			require.Equal(t, "ops", name)
			return auth.Created{
				Key: auth.Key{ID: id, Scope: scope, Name: name, Prefix: prefix, CreatedAt: time.Now()},
				Raw: testRawKey,
			}, nil
		},
	}, slog.New(slog.NewJSONHandler(&logged, nil)))

	recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys",
		`{"scope":"tenant-a","name":"ops"}`)
	require.Equal(t, http.StatusCreated, recorder.Code)

	var body APIKeyCreatedResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, id.String(), body.ID)
	require.Equal(t, "tenant-a", body.Scope)
	require.Equal(t, prefix, body.Prefix)
	require.Equal(t, testRawKey, body.Key)

	// The log must be able to identify the key for revocation without being able
	// to present it.
	require.Contains(t, logged.String(), prefix)
	require.Contains(t, logged.String(), id.String())
	require.NotContains(t, logged.String(), testRawKey)
	require.NotContains(t, logged.String(), strings.Repeat("b", auth.SecretLen))
}

// There is no Idempotency-Key on this route and there must not be one. An
// idempotent create would have to return an existing secret to a repeat, which
// is the one thing a write-only credential must never do.
func TestCreateAPIKey_HasNoIdempotencyIdentity(t *testing.T) {
	var calls int
	handler := newKeyAdminServer(t, &fakeKeys{
		create: func(context.Context, string, string) (auth.Created, error) {
			calls++
			return auth.Created{
				Key: auth.Key{ID: uuid.New(), Scope: "tenant-a", Name: "ops",
					Prefix: strings.Repeat("a", auth.LookupLen), CreatedAt: time.Now()},
				Raw: testRawKey,
			}, nil
		},
	}, nil)

	for i := 0; i < 2; i++ {
		request := httptest.NewRequest(http.MethodPost, "/internal/v1/api-keys",
			strings.NewReader(`{"scope":"tenant-a","name":"ops"}`))
		// Sent deliberately: an Idempotency-Key must be ignored here, not honored.
		request.Header.Set("Idempotency-Key", "the-same-key-twice")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusCreated, recorder.Code)
	}
	require.Equal(t, 2, calls, "every call must mint a new credential")
}

func TestCreateAPIKey_RejectsMalformedAndInvalidRequests(t *testing.T) {
	handler := newKeyAdminServer(t, &fakeKeys{
		create: func(_ context.Context, scope, name string) (auth.Created, error) {
			return auth.Created{}, &auth.ValidationError{
				Fields: []auth.FieldError{{Field: "scope", Message: "must contain between 1 and 128 printable characters"}},
			}
		},
	}, nil)

	t.Run("malformed JSON", func(t *testing.T) {
		recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys", `{`)
		require.Equal(t, http.StatusBadRequest, recorder.Code)
		require.Equal(t, CodeMalformedJSON, decodeError(t, recorder).Error.Code)
	})

	t.Run("unknown field", func(t *testing.T) {
		recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys",
			`{"scope":"tenant-a","name":"ops","expires_in":"30d"}`)
		require.Equal(t, http.StatusBadRequest, recorder.Code,
			"silently dropping a field a caller believed in would promise behavior that does not exist")
		require.Equal(t, CodeMalformedJSON, decodeError(t, recorder).Error.Code)
	})

	t.Run("two JSON documents", func(t *testing.T) {
		recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys",
			`{"scope":"a","name":"b"} {"scope":"c","name":"d"}`)
		require.Equal(t, http.StatusBadRequest, recorder.Code)
	})

	t.Run("invalid field reaches the caller as a field error", func(t *testing.T) {
		recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys",
			`{"scope":"","name":"ops"}`)
		require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
		body := decodeError(t, recorder)
		require.Equal(t, CodeValidationFailed, body.Error.Code)
		require.Equal(t, "scope", body.Error.Details[0].Field)
	})
}

// An ambiguous create is the one 503 in this API whose guidance is "do NOT
// retry": with no request identity, a repeat mints a second credential rather
// than returning the first.
func TestCreateAPIKey_ADeadlineTellsTheCallerNotToRepeatBlindly(t *testing.T) {
	handler := newKeyAdminServer(t, &fakeKeys{
		create: func(context.Context, string, string) (auth.Created, error) {
			return auth.Created{}, auth.ErrDeadlineExceeded
		},
	}, nil)

	recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys",
		`{"scope":"tenant-a","name":"ops"}`)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)

	message := strings.ToLower(decodeError(t, recorder).Error.Message)
	require.Contains(t, message, "second credential")
	require.Contains(t, message, "revoke")
	require.Contains(t, message, "/internal/v1/api-keys")
}

func TestRevokeAPIKey(t *testing.T) {
	id := uuid.New()
	revokedAt := time.Now().UTC()
	key := auth.Key{
		ID: id, Scope: "tenant-a", Name: "ops",
		Prefix: strings.Repeat("a", auth.LookupLen), CreatedAt: revokedAt.Add(-time.Hour),
		RevokedAt: &revokedAt,
	}

	t.Run("revokes a live key", func(t *testing.T) {
		handler := newKeyAdminServer(t, &fakeKeys{
			revoke: func(_ context.Context, got uuid.UUID) (auth.RevokeResult, error) {
				require.Equal(t, id, got)
				return auth.RevokeResult{Key: key}, nil
			},
		}, nil)

		recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys/"+id.String()+"/revoke", "")
		require.Equal(t, http.StatusOK, recorder.Code)

		var body APIKeyRevokedResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
		require.Equal(t, id.String(), body.ID)
		require.False(t, body.AlreadyRevoked)
		require.NotNil(t, body.RevokedAt)
	})

	// Revocation is idempotent, and a repeat reports the ORIGINAL instant. A
	// repeat that moved it would rewrite when a credential stopped being valid,
	// which is exactly the fact an incident review needs.
	t.Run("an already-revoked key is an idempotent repeat", func(t *testing.T) {
		handler := newKeyAdminServer(t, &fakeKeys{
			revoke: func(context.Context, uuid.UUID) (auth.RevokeResult, error) {
				return auth.RevokeResult{Key: key, AlreadyRevoked: true}, nil
			},
		}, nil)

		recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys/"+id.String()+"/revoke", "")
		require.Equal(t, http.StatusOK, recorder.Code)

		var body APIKeyRevokedResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
		require.True(t, body.AlreadyRevoked)
		require.Equal(t, revokedAt.Truncate(time.Millisecond), body.RevokedAt.Truncate(time.Millisecond))
	})

	t.Run("an unknown key is 404", func(t *testing.T) {
		handler := newKeyAdminServer(t, &fakeKeys{
			revoke: func(context.Context, uuid.UUID) (auth.RevokeResult, error) {
				return auth.RevokeResult{}, auth.ErrKeyNotFound
			},
		}, nil)

		recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys/"+uuid.NewString()+"/revoke", "")
		require.Equal(t, http.StatusNotFound, recorder.Code)
		require.Equal(t, CodeNotFound, decodeError(t, recorder).Error.Code)
	})

	// 422, not the 404 the public job routes answer for a malformed id. There is
	// no tenant to hide ids from on this surface, so a caller is better served by
	// being told their id is malformed than by a not-found that sends them
	// looking for a key they think they deleted.
	t.Run("a malformed key id is a field error", func(t *testing.T) {
		handler := newKeyAdminServer(t, &fakeKeys{}, nil)
		recorder := doJSON(t, handler, http.MethodPost, "/internal/v1/api-keys/not-a-uuid/revoke", "")
		require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)

		body := decodeError(t, recorder)
		require.Equal(t, CodeValidationFailed, body.Error.Code)
		require.Equal(t, "key_id", body.Error.Details[0].Field)
	})
}

func TestListAPIKeys(t *testing.T) {
	revokedAt := time.Now().UTC()
	keys := []auth.Key{
		{ID: uuid.New(), Scope: "tenant-a", Name: "live", Prefix: strings.Repeat("a", auth.LookupLen), CreatedAt: revokedAt},
		{ID: uuid.New(), Scope: "tenant-b", Name: "dead", Prefix: strings.Repeat("d", auth.LookupLen), CreatedAt: revokedAt.Add(-time.Hour), RevokedAt: &revokedAt},
	}

	t.Run("returns metadata and never a secret", func(t *testing.T) {
		var gotLimit int
		handler := newKeyAdminServer(t, &fakeKeys{
			list: func(_ context.Context, limit int) ([]auth.Key, error) {
				gotLimit = limit
				return keys, nil
			},
		}, nil)

		recorder := doJSON(t, handler, http.MethodGet, "/internal/v1/api-keys", "")
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Equal(t, auth.DefaultListLimit, gotLimit)

		var body APIKeyListResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
		require.Len(t, body.Keys, 2)
		require.Nil(t, body.Keys[0].RevokedAt, "a live key reports no revocation")
		require.NotNil(t, body.Keys[1].RevokedAt, "a revoked key stays listed and says so")

		// The response shape has nowhere to put a secret, and the serialized
		// bytes must confirm it.
		require.NotContains(t, recorder.Body.String(), "secret")
		require.NotContains(t, recorder.Body.String(), "hash")
		require.NotContains(t, recorder.Body.String(), testRawKey)
	})

	t.Run("an empty listing is an empty array, never null", func(t *testing.T) {
		handler := newKeyAdminServer(t, &fakeKeys{
			list: func(context.Context, int) ([]auth.Key, error) { return nil, nil },
		}, nil)

		recorder := doJSON(t, handler, http.MethodGet, "/internal/v1/api-keys", "")
		require.Equal(t, http.StatusOK, recorder.Code)
		require.JSONEq(t, `{"keys":[]}`, recorder.Body.String())
	})

	t.Run("an out-of-range limit is a field error", func(t *testing.T) {
		handler := newKeyAdminServer(t, &fakeKeys{}, nil)
		for _, limit := range []string{"0", "-1", "abc", strings.Repeat("9", 20)} {
			recorder := doJSON(t, handler, http.MethodGet, "/internal/v1/api-keys?limit="+limit, "")
			require.Equalf(t, http.StatusUnprocessableEntity, recorder.Code, "limit=%s", limit)
			require.Equal(t, "limit", decodeError(t, recorder).Error.Details[0].Field)
		}
	})
}

// The key-management routes must answer an unrouted method in the same
// structured shape as everything else, not ServeMux's plain-text default.
func TestAPIKeyAdmin_MethodNotAllowedIsStructured(t *testing.T) {
	handler := newKeyAdminServer(t, &fakeKeys{}, nil)

	for _, tc := range []struct{ method, path, allow string }{
		{http.MethodDelete, "/internal/v1/api-keys", "GET, POST"},
		{http.MethodGet, "/internal/v1/api-keys/" + uuid.NewString() + "/revoke", "POST"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			recorder := doJSON(t, handler, tc.method, tc.path, "")
			require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
			require.Equal(t, CodeMethodNotAllowed, decodeError(t, recorder).Error.Code)
			require.Equal(t, tc.allow, recorder.Header().Get("Allow"))
		})
	}
}
