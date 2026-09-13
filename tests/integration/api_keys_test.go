//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
	"github.com/co-rtex/TaskForge/internal/auth"
	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
)

func authStore() *auth.Store { return auth.NewStore(testPool) }

// mintKey creates a real credential against real PostgreSQL.
func mintKey(t *testing.T, scope, name string) auth.Created {
	t.Helper()
	created, err := authStore().Create(context.Background(), scope, name)
	require.NoError(t, err)
	return created
}

// --- schema ---------------------------------------------------------------

// Constraints, not application code, are the last line of defence. Each
// rejection below is paired with a positive control, so a CHECK that rejected
// everything would fail here rather than look like a passing guard.
func TestSchema_APIKeyConstraintsRejectInvalidRows(t *testing.T) {
	reset(t)
	ctx := context.Background()

	insert := func(t *testing.T, overrides map[string]string) error {
		t.Helper()
		cols := map[string]string{
			"scope":       "'tenant-a'",
			"name":        "'ops'",
			"prefix":      "'" + uuid.NewString()[:22] + "'",
			"secret_hash": "'" + strings.Repeat("a", 64) + "'",
			"revoked_at":  "NULL",
			"created_at":  "now()",
		}
		for k, v := range overrides {
			cols[k] = v
		}
		_, err := testPool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO api_keys (id, scope, name, prefix, secret_hash, created_at, revoked_at)
			VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s)`,
			cols["scope"], cols["name"], cols["prefix"], cols["secret_hash"],
			cols["created_at"], cols["revoked_at"]))
		return err
	}

	for name, overrides := range map[string]map[string]string{
		"empty scope":            {"scope": "''"},
		"scope beyond 128":       {"scope": "'" + strings.Repeat("s", 129) + "'"},
		"empty name":             {"name": "''"},
		"name beyond 128":        {"name": "'" + strings.Repeat("n", 129) + "'"},
		"prefix shorter than 8":  {"prefix": "'short'"},
		"prefix longer than 32":  {"prefix": "'" + strings.Repeat("p", 33) + "'"},
		"hash is uppercase hex":  {"secret_hash": "'" + strings.Repeat("A", 64) + "'"},
		"hash is too short":      {"secret_hash": "'" + strings.Repeat("a", 63) + "'"},
		"hash is too long":       {"secret_hash": "'" + strings.Repeat("a", 65) + "'"},
		"hash is not hex":        {"secret_hash": "'" + strings.Repeat("z", 64) + "'"},
		"hash is a raw key":      {"secret_hash": "'tfk_" + strings.Repeat("a", 60) + "'"},
		"revoked before created": {"created_at": "now()", "revoked_at": "now() - interval '1 hour'"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, insert(t, overrides))
		})
	}

	t.Run("a valid live row is accepted", func(t *testing.T) {
		require.NoError(t, insert(t, nil))
	})
	t.Run("a valid revoked row is accepted", func(t *testing.T) {
		require.NoError(t, insert(t, map[string]string{"revoked_at": "now()"}))
	})
	t.Run("boundary lengths are accepted", func(t *testing.T) {
		require.NoError(t, insert(t, map[string]string{
			"scope":  "'" + strings.Repeat("s", 128) + "'",
			"name":   "'" + strings.Repeat("n", 128) + "'",
			"prefix": "'" + strings.Repeat("p", 8) + "'",
		}))
		require.NoError(t, insert(t, map[string]string{"prefix": "'" + strings.Repeat("q", 32) + "'"}))
	})
}

// A revoked key's prefix must stay taken. If it could be reused, a log line or
// an incident record naming that prefix would become ambiguous the moment a new
// key claimed it.
func TestSchema_APIKeyPrefixIsUniqueAcrossRevocation(t *testing.T) {
	reset(t)
	ctx := context.Background()

	created := mintKey(t, testScope, "first")
	_, err := authStore().Revoke(ctx, created.Key.ID)
	require.NoError(t, err)

	_, err = testPool.Exec(ctx, `
		INSERT INTO api_keys (id, scope, name, prefix, secret_hash)
		VALUES (gen_random_uuid(), $1, 'second', $2, $3)`,
		testScope, created.Key.Prefix, strings.Repeat("a", 64))
	require.Error(t, err, "a revoked key's prefix must never be reusable")

	// The constraint NAME matters, not just that an error happened: it is what
	// auth.Store matches on to decide a collision is retryable, so a rename
	// would silently turn a retry into a leaked unique violation.
	var postgresError *pgconn.PgError
	require.ErrorAs(t, err, &postgresError)
	require.Equal(t, "23505", postgresError.Code)
	require.Equal(t, "api_keys_prefix_key", postgresError.ConstraintName)
}

// The listing index must match the query that justifies it (AGENTS.md section 6).
func TestSchema_APIKeyListingIndexMatchesItsQuery(t *testing.T) {
	ctx := context.Background()
	var definition string
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes WHERE indexname = 'api_keys_listing_idx'`).Scan(&definition))
	require.Contains(t, definition, "created_at DESC")
	require.Contains(t, definition, "id DESC",
		"the id tiebreak makes the ordering total, so a bounded page is deterministic")

	var comment string
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT obj_description(c.oid) FROM pg_class c WHERE c.relname = 'api_keys'`).Scan(&comment))
	require.Contains(t, comment, "returned once")
}

// --- store ----------------------------------------------------------------

func TestAPIKeyStore_CreateRevokeAndListRoundTrip(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := authStore()

	created := mintKey(t, "tenant-a", "ops-laptop")
	require.Equal(t, "tenant-a", created.Key.Scope)
	require.Equal(t, "ops-laptop", created.Key.Name)
	require.False(t, created.Key.Revoked())
	require.True(t, strings.HasPrefix(created.Raw, auth.KeyPrefix))

	// The raw credential must NOT be recoverable from the row, in any column.
	var storedHash, storedPrefix string
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT prefix, secret_hash FROM api_keys WHERE id = $1`, created.Key.ID).
		Scan(&storedPrefix, &storedHash))
	require.Equal(t, created.Key.Prefix, storedPrefix)
	require.NotContains(t, storedHash, created.Raw)
	require.Equal(t, auth.HashSecret(strings.Split(created.Raw, ".")[1]), storedHash)

	// Nothing anywhere in the table holds the secret.
	var rows int
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT count(*) FROM api_keys WHERE prefix = $1 OR secret_hash = $1`, created.Raw).Scan(&rows))
	require.Zero(t, rows)

	t.Run("authenticates before revocation", func(t *testing.T) {
		principal, err := store.Authenticate(ctx, created.Raw)
		require.NoError(t, err)
		require.Equal(t, created.Key.ID, principal.KeyID)
		require.Equal(t, "tenant-a", principal.Scope)
	})

	t.Run("revocation is idempotent and never moves the instant", func(t *testing.T) {
		first, err := store.Revoke(ctx, created.Key.ID)
		require.NoError(t, err)
		require.False(t, first.AlreadyRevoked)
		require.NotNil(t, first.Key.RevokedAt)

		time.Sleep(10 * time.Millisecond)

		second, err := store.Revoke(ctx, created.Key.ID)
		require.NoError(t, err)
		require.True(t, second.AlreadyRevoked)
		require.True(t, first.Key.RevokedAt.Equal(*second.Key.RevokedAt),
			"a repeat must report the original instant, which is the fact an incident review needs")
	})

	t.Run("a revoked key stops authenticating", func(t *testing.T) {
		_, err := store.Authenticate(ctx, created.Raw)
		require.ErrorIs(t, err, auth.ErrUnauthorized)
	})

	t.Run("an unknown id cannot be revoked", func(t *testing.T) {
		_, err := store.Revoke(ctx, uuid.New())
		require.ErrorIs(t, err, auth.ErrKeyNotFound)
	})

	t.Run("listing returns metadata newest first and no secret", func(t *testing.T) {
		second := mintKey(t, "tenant-b", "ci")
		keys, err := store.List(ctx, 50)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(keys), 2)
		require.Equal(t, second.Key.ID, keys[0].ID, "newest first")

		byID := map[uuid.UUID]auth.Key{}
		for _, key := range keys {
			byID[key.ID] = key
		}
		require.True(t, byID[created.Key.ID].Revoked(), "a revoked key stays listed")
		require.False(t, byID[second.Key.ID].Revoked())

		serialized, err := json.Marshal(keys)
		require.NoError(t, err)
		require.NotContains(t, string(serialized), created.Raw)
		require.NotContains(t, string(serialized), second.Raw)
		require.NotContains(t, string(serialized), storedHash)
	})

	t.Run("listing is bounded", func(t *testing.T) {
		keys, err := store.List(ctx, 1)
		require.NoError(t, err)
		require.Len(t, keys, 1)
	})
}

// Every authentication failure must be the same error, so nothing about which
// check failed can reach a caller.
func TestAPIKeyStore_EveryFailureModeAnswersOneError(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := authStore()

	good := mintKey(t, testScope, "good")
	revoked := mintKey(t, testScope, "revoked")
	_, err := store.Revoke(ctx, revoked.Key.ID)
	require.NoError(t, err)

	prefix, secret, _ := strings.Cut(strings.TrimPrefix(good.Raw, auth.KeyPrefix), ".")
	unknown, err := auth.GenerateMaterial(nil)
	require.NoError(t, err)

	// A real prefix with a wrong secret is the case that matters most: it is the
	// probe an attacker who has seen a prefix in a log would actually send.
	wrongSecret := auth.KeyPrefix + prefix + "." + unknown.Secret
	require.NotEqual(t, good.Raw, wrongSecret)

	for name, credential := range map[string]string{
		"unknown prefix":            unknown.Raw(),
		"known prefix wrong secret": wrongSecret,
		"revoked key":               revoked.Raw,
		"malformed":                 "not-a-key",
		"empty":                     "",
		"secret only":               secret,
	} {
		t.Run(name, func(t *testing.T) {
			principal, err := store.Authenticate(ctx, credential)
			require.Equal(t, auth.ErrUnauthorized, err,
				"every failure must be the same error value, not merely some error")
			require.Equal(t, auth.Principal{}, principal)
		})
	}

	t.Run("the good key still works", func(t *testing.T) {
		principal, err := store.Authenticate(ctx, good.Raw)
		require.NoError(t, err)
		require.Equal(t, testScope, principal.Scope)
	})
}

// Concurrent creation on separate connections must produce distinct,
// independently usable credentials. One shared connection would serialize the
// inserts and prove nothing (AGENTS.md section 7).
func TestAPIKeyStore_ConcurrentCreationProducesDistinctUsableKeys(t *testing.T) {
	reset(t)
	ctx := context.Background()

	const workers = 12
	type result struct {
		created auth.Created
		err     error
	}
	results := make([]result, workers)

	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for i := 0; i < workers; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			// Each goroutine takes its own connection from the pool.
			store := auth.NewStore(testPool)
			start.Wait()
			created, err := store.Create(ctx, fmt.Sprintf("tenant-%02d", i), "concurrent")
			results[i] = result{created: created, err: err}
		}(i)
	}
	start.Done()
	done.Wait()

	prefixes := map[string]struct{}{}
	raws := map[string]struct{}{}
	for i, r := range results {
		require.NoErrorf(t, r.err, "worker %d", i)
		_, duplicatePrefix := prefixes[r.created.Key.Prefix]
		require.Falsef(t, duplicatePrefix, "worker %d reused a lookup prefix", i)
		prefixes[r.created.Key.Prefix] = struct{}{}
		_, duplicateRaw := raws[r.created.Raw]
		require.False(t, duplicateRaw)
		raws[r.created.Raw] = struct{}{}
	}

	// Each must authenticate to its OWN scope. A create that raced into the
	// wrong row would show up here rather than as a duplicate above.
	for i, r := range results {
		principal, err := authStore().Authenticate(ctx, r.created.Raw)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("tenant-%02d", i), principal.Scope)
		require.Equal(t, r.created.Key.ID, principal.KeyID)
	}

	require.Equal(t, workers, countRows(t, "api_keys")-1,
		"exactly one row per creation, plus the suite's own key")
}

// Concurrent revocation of one key must have exactly one winner, and every
// caller must see the same instant.
func TestAPIKeyStore_ConcurrentRevocationHasOneWinner(t *testing.T) {
	reset(t)
	ctx := context.Background()
	created := mintKey(t, testScope, "contended")

	const callers = 8
	instants := make([]time.Time, callers)
	alreadyRevoked := make([]bool, callers)

	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for i := 0; i < callers; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			store := auth.NewStore(testPool)
			start.Wait()
			result, err := store.Revoke(ctx, created.Key.ID)
			require.NoError(t, err)
			require.NotNil(t, result.Key.RevokedAt)
			instants[i] = *result.Key.RevokedAt
			alreadyRevoked[i] = result.AlreadyRevoked
		}(i)
	}
	start.Done()
	done.Wait()

	winners := 0
	for _, already := range alreadyRevoked {
		if !already {
			winners++
		}
	}
	require.Equal(t, 1, winners, "exactly one revocation may stamp the row")
	for i, instant := range instants {
		require.Truef(t, instant.Equal(instants[0]),
			"caller %d saw a different revocation instant", i)
	}
}

// --- end to end through the real HTTP server ------------------------------

// TestAPIKeys_ScopeIsolationIsEnforcedByTheKey is the milestone's headline
// behavior.
//
// It reproduces what TestGetJob_IsScopedToTheCaller proves at the store level,
// but driven by two real minted credentials over real HTTP rather than by a
// scope string written straight into the database. That difference is the whole
// point: it is the credential, not a configured constant, that decides what a
// caller can see.
func TestAPIKeys_ScopeIsolationIsEnforcedByTheKey(t *testing.T) {
	reset(t)
	server := newAPI(t)

	keyA := mintKey(t, "tenant-a", "a")
	keyB := mintKey(t, "tenant-b", "b")
	clientA, clientB := clientWithKey(keyA.Raw), clientWithKey(keyB.Raw)

	// A owns a queue-bound job, so both scopes address the same queue row.
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs", strings.NewReader(jobBody))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "scoped-to-a")
	response, err := clientA.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusCreated, response.StatusCode)

	var job api.JobResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&job))

	read := func(t *testing.T, client *http.Client) int {
		t.Helper()
		request, err := http.NewRequest(http.MethodGet, server.URL+"/v1/jobs/"+job.ID, nil)
		require.NoError(t, err)
		response, err := client.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		return response.StatusCode
	}

	require.Equal(t, http.StatusOK, read(t, clientA), "the minting scope can read its own job")
	require.Equal(t, http.StatusNotFound, read(t, clientB),
		"another scope's key must not see this job, and must not be able to tell it exists")

	// The durable row is attributed to the KEY's scope, not to the configured
	// development scope the process still uses for worker control.
	var storedScope string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT scope FROM jobs WHERE id = $1`, job.ID).Scan(&storedScope))
	require.Equal(t, "tenant-a", storedScope)
	require.NotEqual(t, testScope, storedScope,
		"the scope must come from the credential, not from TASKFORGE_DEV_SCOPE")

	t.Run("B cannot cancel or replay A's job either", func(t *testing.T) {
		for _, path := range []string{"/v1/jobs/" + job.ID + "/cancel", "/v1/jobs/" + job.ID + "/retry"} {
			request, err := http.NewRequest(http.MethodPost, server.URL+path, nil)
			require.NoError(t, err)
			request.Header.Set("Idempotency-Key", "b-tries-"+uuid.NewString())
			response, err := clientB.Do(request)
			require.NoError(t, err)
			response.Body.Close()
			require.Equalf(t, http.StatusNotFound, response.StatusCode, "path %s", path)
		}
	})
}

// Every public route must refuse an anonymous caller over real HTTP, and the
// refusal must leave no durable trace.
func TestAPIKeys_EveryPublicRouteRefusesAnAnonymousCaller(t *testing.T) {
	reset(t)
	server := newAPI(t)
	anonymous := unauthenticatedClient()
	id := uuid.NewString()

	for _, route := range []struct{ method, path string }{
		{http.MethodPost, "/v1/jobs"},
		{http.MethodGet, "/v1/jobs/" + id},
		{http.MethodPost, "/v1/jobs/" + id + "/cancel"},
		{http.MethodPost, "/v1/jobs/" + id + "/retry"},
		{http.MethodGet, "/v1/dlq"},
		{http.MethodPost, "/v1/dlq/" + id + "/replay"},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			request, err := http.NewRequest(route.method, server.URL+route.path, strings.NewReader(jobBody))
			require.NoError(t, err)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", "anonymous-"+uuid.NewString())
			response, err := anonymous.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()

			require.Equal(t, http.StatusUnauthorized, response.StatusCode)
			require.Equal(t, "Bearer", response.Header.Get("WWW-Authenticate"))

			var envelope api.ErrorBody
			require.NoError(t, json.NewDecoder(response.Body).Decode(&envelope))
			require.Equal(t, api.CodeUnauthorized, envelope.Error.Code)
		})
	}

	require.Zero(t, countRows(t, "jobs"), "a refused request must write nothing")
	require.Zero(t, countRows(t, "idempotency_records"))
	require.Zero(t, countRows(t, "outbox_events"))
}

// Revocation takes effect on the next request. The in-flight boundary is
// documented as expected behavior, and this pins the half that is actually a
// guarantee.
func TestAPIKeys_RevocationTakesEffectOnTheNextRequest(t *testing.T) {
	reset(t)
	server := newAPI(t)

	created := mintKey(t, "tenant-revoke", "short-lived")
	client := clientWithKey(created.Raw)

	list := func(t *testing.T) int {
		t.Helper()
		request, err := http.NewRequest(http.MethodGet, server.URL+"/v1/dlq", nil)
		require.NoError(t, err)
		response, err := client.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		return response.StatusCode
	}

	require.Equal(t, http.StatusOK, list(t), "a live key works")

	_, err := authStore().Revoke(context.Background(), created.Key.ID)
	require.NoError(t, err)

	require.Equal(t, http.StatusUnauthorized, list(t), "the very next request is refused")
	require.Equal(t, http.StatusUnauthorized, list(t), "and stays refused")
}

// The internal worker-control surface must be provably untouched by this
// milestone: it still answers with no Authorization header at all.
//
// This is the regression that would be easiest to introduce and hardest to
// notice, because a worker would simply stop being able to register and the
// failure would look like a worker bug.
func TestAPIKeys_TheWorkerControlSurfaceStillNeedsNoCredential(t *testing.T) {
	reset(t)
	server := newAPI(t)
	anonymous := unauthenticatedClient()

	sessionID := uuid.New()
	body := `{"worker_name":"auth-probe","hostname":"auth.local","worker_group":"default",
		"concurrency_limit":1,"capabilities":["cpu"],"supported_job_types":["demo.echo"]}`
	request, err := http.NewRequest(http.MethodPut,
		server.URL+"/internal/v1/worker-sessions/"+sessionID.String(), strings.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")

	response, err := anonymous.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode,
		"worker control is deliberately unauthenticated in this milestone")

	var session api.WorkerSessionResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&session))
	require.Equal(t, sessionID.String(), session.WorkerSessionID)

	// And it is still attributed to the configured development scope, not to any
	// key's scope.
	var scope string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT scope FROM worker_sessions WHERE id = $1`, sessionID).Scan(&scope))
	require.Equal(t, testScope, scope)

	// The key-management routes are reachable anonymously too, which is what
	// makes the system bootstrappable and is exactly why nothing here may leave
	// loopback.
	mintRequest, err := http.NewRequest(http.MethodPost, server.URL+"/internal/v1/api-keys",
		strings.NewReader(`{"scope":"bootstrapped","name":"first-key"}`))
	require.NoError(t, err)
	mintRequest.Header.Set("Content-Type", "application/json")
	mintResponse, err := anonymous.Do(mintRequest)
	require.NoError(t, err)
	defer mintResponse.Body.Close()
	require.Equal(t, http.StatusCreated, mintResponse.StatusCode)
}

// The full key lifecycle over HTTP: mint, use, list, revoke, refuse.
func TestAPIKeys_ManagementLifecycleOverHTTP(t *testing.T) {
	reset(t)
	server := newAPI(t)
	anonymous := unauthenticatedClient()

	post := func(t *testing.T, path, body string) (*http.Response, []byte) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := anonymous.Do(request)
		require.NoError(t, err)
		t.Cleanup(func() { _ = response.Body.Close() })
		raw, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		return response, raw
	}

	response, raw := post(t, "/internal/v1/api-keys", `{"scope":"tenant-http","name":"minted-over-http"}`)
	require.Equal(t, http.StatusCreated, response.StatusCode)

	var created api.APIKeyCreatedResponse
	require.NoError(t, json.Unmarshal(raw, &created))
	require.NotEmpty(t, created.Key)
	require.True(t, strings.HasPrefix(created.Key, auth.KeyPrefix))
	require.Contains(t, created.Key, created.Prefix)

	// The minted key really works against the public surface.
	client := clientWithKey(created.Key)
	submitRequest, err := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs", strings.NewReader(jobBody))
	require.NoError(t, err)
	submitRequest.Header.Set("Content-Type", "application/json")
	submitRequest.Header.Set("Idempotency-Key", "minted-over-http")
	submitResponse, err := client.Do(submitRequest)
	require.NoError(t, err)
	defer submitResponse.Body.Close()
	require.Equal(t, http.StatusCreated, submitResponse.StatusCode)

	// Listing shows it, with no credential in the payload.
	listRequest, err := http.NewRequest(http.MethodGet, server.URL+"/internal/v1/api-keys", nil)
	require.NoError(t, err)
	listResponse, err := anonymous.Do(listRequest)
	require.NoError(t, err)
	defer listResponse.Body.Close()
	listRaw, err := io.ReadAll(listResponse.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode)
	require.NotContains(t, string(listRaw), created.Key,
		"a listing must never carry a credential")
	require.Contains(t, string(listRaw), created.Prefix)

	// Revoke, twice.
	revokeResponse, revokeRaw := post(t, "/internal/v1/api-keys/"+created.ID+"/revoke", "")
	require.Equal(t, http.StatusOK, revokeResponse.StatusCode)
	var revoked api.APIKeyRevokedResponse
	require.NoError(t, json.Unmarshal(revokeRaw, &revoked))
	require.False(t, revoked.AlreadyRevoked)
	require.NotNil(t, revoked.RevokedAt)

	repeatResponse, repeatRaw := post(t, "/internal/v1/api-keys/"+created.ID+"/revoke", "")
	require.Equal(t, http.StatusOK, repeatResponse.StatusCode)
	var repeat api.APIKeyRevokedResponse
	require.NoError(t, json.Unmarshal(repeatRaw, &repeat))
	require.True(t, repeat.AlreadyRevoked)
	require.True(t, revoked.RevokedAt.Equal(*repeat.RevokedAt))

	// And the credential is dead.
	deadRequest, err := http.NewRequest(http.MethodGet, server.URL+"/v1/dlq", nil)
	require.NoError(t, err)
	deadResponse, err := client.Do(deadRequest)
	require.NoError(t, err)
	defer deadResponse.Body.Close()
	require.Equal(t, http.StatusUnauthorized, deadResponse.StatusCode)
}

// TestE2E_APIKeyDrivesTheWholeLifecycle proves authentication did not break the
// M1-M4 lifecycle: a job submitted with a minted credential is claimed by a real
// worker, over a real broker, and reaches SUCCEEDED.
//
// The key's scope is the worker-control scope, which is the ONLY scope whose
// jobs can execute today — see the second half of this test for why.
func TestE2E_APIKeyDrivesTheWholeLifecycle(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	stack := startE2EStack(t, registry, time.Hour)

	created := mintKey(t, testScope, "e2e-executor")
	client := clientWithKey(created.Raw)

	request, err := http.NewRequest(http.MethodPost, stack.baseURL+"/v1/jobs", strings.NewReader(
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"authenticated"}}`))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "e2e-authenticated")

	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusCreated, response.StatusCode)

	var job api.JobResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&job))
	jobID := uuid.MustParse(job.ID)

	stack.startWorker(t, "e2e-auth-worker", 2)
	awaitJobStatus(t, jobID, "SUCCEEDED", 30*time.Second)

	// Durable state, not just a status code: one attempt, and it is the
	// credential's scope that owns the job.
	var attempts int
	var scope string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, jobID).Scan(&attempts))
	require.Equal(t, 1, attempts)
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT scope FROM jobs WHERE id = $1`, jobID).Scan(&scope))
	require.Equal(t, testScope, scope)

	// And the job is readable through the same credential that created it.
	readRequest, err := http.NewRequest(http.MethodGet, stack.baseURL+"/v1/jobs/"+job.ID, nil)
	require.NoError(t, err)
	readResponse, err := client.Do(readRequest)
	require.NoError(t, err)
	defer readResponse.Body.Close()
	require.Equal(t, http.StatusOK, readResponse.StatusCode)

	var fetched api.JobResponse
	require.NoError(t, json.NewDecoder(readResponse.Body).Decode(&fetched))
	require.Equal(t, "SUCCEEDED", fetched.Status)
}

// TestE2E_AKeyOutsideTheWorkerControlScopeSubmitsButNeverExecutes pins the one
// real limitation this milestone creates, so it is a known boundary rather than
// a bug someone finds later.
//
// Worker claims filter on the worker-control scope (internal/workers.Store.Claim,
// `WHERE j.scope = $1`), and that surface is still attributed to the single
// configured development scope. A key minted for any OTHER scope therefore
// submits jobs that are durable, readable and cancelable — and that no worker
// will ever claim.
//
// The failure is silent by nature: the job simply stays QUEUED. It is asserted
// here, warned about at mint time, and stated in docs/CURRENT_STATE.md. It goes
// away when worker/control scopes land, not before.
func TestE2E_AKeyOutsideTheWorkerControlScopeSubmitsButNeverExecutes(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	stack := startE2EStack(t, registry, time.Hour)

	// Two jobs, identical but for the scope of the key that submitted them.
	executable := mintKey(t, testScope, "executable")
	stranded := mintKey(t, "tenant-without-workers", "stranded")

	submitWith := func(t *testing.T, created auth.Created, key string) uuid.UUID {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, stack.baseURL+"/v1/jobs", strings.NewReader(
			`{"queue":"default","job_type":"demo.echo","payload":{"message":"scoped"}}`))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", key)
		response, err := clientWithKey(created.Raw).Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusCreated, response.StatusCode,
			"submission succeeds for both: the job is durable either way")

		var job api.JobResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&job))
		return uuid.MustParse(job.ID)
	}

	strandedJob := submitWith(t, stranded, "stranded-job")
	executableJob := submitWith(t, executable, "executable-job")

	stack.startWorker(t, "scope-boundary-worker", 2)

	// The in-scope job completes, which is what makes the other one's failure a
	// scope boundary rather than a broken stack.
	awaitJobStatus(t, executableJob, "SUCCEEDED", 30*time.Second)

	var status string
	var attempts int
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM jobs WHERE id = $1`, strandedJob).Scan(&status))
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, strandedJob).Scan(&attempts))

	require.Equal(t, "QUEUED", status,
		"a job outside the worker-control scope is never claimed, and stays QUEUED")
	require.Zero(t, attempts, "no attempt is ever created for it")

	// It is nonetheless a real, durable, fully readable job in its own scope.
	readRequest, err := http.NewRequest(http.MethodGet, stack.baseURL+"/v1/jobs/"+strandedJob.String(), nil)
	require.NoError(t, err)
	readResponse, err := clientWithKey(stranded.Raw).Do(readRequest)
	require.NoError(t, err)
	defer readResponse.Body.Close()
	require.Equal(t, http.StatusOK, readResponse.StatusCode)
}

// Concurrent authentication against one shared key must be correct under the
// race detector. Every goroutine takes its own connection from the pool.
func TestAPIKeys_ConcurrentAuthenticationIsRaceFree(t *testing.T) {
	reset(t)
	created := mintKey(t, "tenant-concurrent", "shared")

	const callers = 16
	scopes := make([]string, callers)
	errs := make([]error, callers)

	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for i := 0; i < callers; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			store := auth.NewStore(testPool)
			start.Wait()
			principal, err := store.Authenticate(context.Background(), created.Raw)
			scopes[i], errs[i] = principal.Scope, err
		}(i)
	}
	start.Done()
	done.Wait()

	for i := range scopes {
		require.NoErrorf(t, errs[i], "caller %d", i)
		require.Equal(t, "tenant-concurrent", scopes[i])
	}
}
