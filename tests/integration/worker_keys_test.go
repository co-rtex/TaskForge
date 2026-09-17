//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/co-rtex/TaskForge/internal/workerauth"
)

func workerAuthStore() *workerauth.Store { return workerauth.NewStore(testPool) }

// mintWorkerKey creates a real worker credential against real PostgreSQL.
func mintWorkerKey(t *testing.T, scope, name string) workerauth.Created {
	t.Helper()
	created, err := workerAuthStore().Create(context.Background(), scope, name)
	require.NoError(t, err)
	return created
}

// --- schema ---------------------------------------------------------------

// Constraints, not application code, are the last line of defence. Each
// rejection is paired with a positive control, so a CHECK that rejected
// everything would fail here rather than look like a passing guard.
func TestSchema_WorkerKeyConstraintsRejectInvalidRows(t *testing.T) {
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
			INSERT INTO worker_keys (id, scope, name, prefix, secret_hash, created_at, revoked_at)
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

// A revoked worker key's prefix must stay taken, for the identical reason an
// API key's does: a log line or incident record naming it must never become
// ambiguous after a new key claims the same prefix.
func TestSchema_WorkerKeyPrefixIsUniqueAcrossRevocation(t *testing.T) {
	reset(t)
	ctx := context.Background()

	created := mintWorkerKey(t, testScope, "first")
	_, err := workerAuthStore().Revoke(ctx, created.Key.ID)
	require.NoError(t, err)

	_, err = testPool.Exec(ctx, `
		INSERT INTO worker_keys (id, scope, name, prefix, secret_hash)
		VALUES (gen_random_uuid(), $1, 'second', $2, $3)`,
		testScope, created.Key.Prefix, strings.Repeat("a", 64))
	require.Error(t, err, "a revoked worker key's prefix must never be reusable")

	var postgresError *pgconn.PgError
	require.ErrorAs(t, err, &postgresError)
	require.Equal(t, "23505", postgresError.Code)
	require.Equal(t, "worker_keys_prefix_key", postgresError.ConstraintName)
}

func TestSchema_WorkerKeyListingIndexMatchesItsQuery(t *testing.T) {
	ctx := context.Background()
	var definition string
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes WHERE indexname = 'worker_keys_listing_idx'`).Scan(&definition))
	require.Contains(t, definition, "created_at DESC")
	require.Contains(t, definition, "id DESC",
		"the id tiebreak makes the ordering total, so a bounded page is deterministic")
}

// The additive FK this milestone adds must be exactly what
// internal/workers.Store.SessionScope and migrations/0015's own comment
// describe: nullable, and indexed for the reverse "which sessions did this
// key create" direction.
func TestSchema_WorkerSessionsWorkerKeyIDIsNullableAndIndexed(t *testing.T) {
	ctx := context.Background()
	var nullable string
	require.NoError(t, testPool.QueryRow(ctx, `
		SELECT is_nullable FROM information_schema.columns
		WHERE table_name = 'worker_sessions' AND column_name = 'worker_key_id'`).Scan(&nullable))
	require.Equal(t, "YES", nullable)

	var definition string
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes WHERE indexname = 'worker_sessions_worker_key_id_idx'`).Scan(&definition))
	require.Contains(t, definition, "worker_key_id")
}

// --- store ------------------------------------------------------------------

func TestWorkerKeyStore_CreateRevokeAndListRoundTrip(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := workerAuthStore()

	created := mintWorkerKey(t, "tenant-a", "worker-fleet")
	require.Equal(t, "tenant-a", created.Key.Scope)
	require.Equal(t, "worker-fleet", created.Key.Name)
	require.False(t, created.Key.Revoked())
	require.True(t, strings.HasPrefix(created.Raw, "tfk_"))

	var storedHash, storedPrefix string
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT prefix, secret_hash FROM worker_keys WHERE id = $1`, created.Key.ID).
		Scan(&storedPrefix, &storedHash))
	require.Equal(t, created.Key.Prefix, storedPrefix)
	require.NotContains(t, storedHash, created.Raw)

	t.Run("authenticates before revocation", func(t *testing.T) {
		principal, err := store.Authenticate(ctx, created.Raw)
		require.NoError(t, err)
		require.Equal(t, created.Key.ID, principal.KeyID)
		require.Equal(t, "tenant-a", principal.Scope)
	})

	t.Run("is not revoked before revocation", func(t *testing.T) {
		revoked, err := store.IsRevoked(ctx, created.Key.ID)
		require.NoError(t, err)
		require.False(t, revoked)
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
		require.True(t, first.Key.RevokedAt.Equal(*second.Key.RevokedAt))
	})

	t.Run("a revoked key stops authenticating", func(t *testing.T) {
		_, err := store.Authenticate(ctx, created.Raw)
		require.ErrorIs(t, err, workerauth.ErrUnauthorized)
	})

	t.Run("IsRevoked now reports true", func(t *testing.T) {
		revoked, err := store.IsRevoked(ctx, created.Key.ID)
		require.NoError(t, err)
		require.True(t, revoked)
	})

	t.Run("an unknown id cannot be revoked", func(t *testing.T) {
		_, err := store.Revoke(ctx, uuid.New())
		require.ErrorIs(t, err, workerauth.ErrKeyNotFound)
	})

	t.Run("IsRevoked reports true for an unknown id, not an error", func(t *testing.T) {
		revoked, err := store.IsRevoked(ctx, uuid.New())
		require.NoError(t, err)
		require.True(t, revoked)
	})

	t.Run("listing returns metadata newest first and no secret", func(t *testing.T) {
		second := mintWorkerKey(t, "tenant-b", "ci")
		keys, err := store.List(ctx, 50)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(keys), 2)
		require.Equal(t, second.Key.ID, keys[0].ID, "newest first")

		serialized, err := json.Marshal(keys)
		require.NoError(t, err)
		require.NotContains(t, string(serialized), created.Raw)
		require.NotContains(t, string(serialized), second.Raw)
	})
}

func TestWorkerKeyStore_EveryFailureModeAnswersOneError(t *testing.T) {
	reset(t)
	ctx := context.Background()
	store := workerAuthStore()

	good := mintWorkerKey(t, testScope, "good")
	revoked := mintWorkerKey(t, testScope, "revoked")
	_, err := store.Revoke(ctx, revoked.Key.ID)
	require.NoError(t, err)

	for name, credential := range map[string]string{
		"revoked key": revoked.Raw,
		"malformed":   "not-a-key",
		"empty":       "",
	} {
		t.Run(name, func(t *testing.T) {
			principal, err := store.Authenticate(ctx, credential)
			require.Equal(t, workerauth.ErrUnauthorized, err)
			require.Equal(t, workerauth.Principal{}, principal)
		})
	}

	t.Run("the good key still works", func(t *testing.T) {
		principal, err := store.Authenticate(ctx, good.Raw)
		require.NoError(t, err)
		require.Equal(t, testScope, principal.Scope)
	})
}

// TestWorkerKeyStore_IsDistinctFromAPIKeyStore is the store-level half of the
// two-tier model: a worker key must never authenticate against auth.Store,
// and an API key must never authenticate against workerauth.Store, because
// the two guard different trust boundaries and are verified against
// different tables entirely.
func TestWorkerKeyStore_IsDistinctFromAPIKeyStore(t *testing.T) {
	reset(t)
	ctx := context.Background()

	apiKey := mintKey(t, testScope, "an-api-key")
	workerKey := mintWorkerKey(t, testScope, "a-worker-key")

	_, err := workerAuthStore().Authenticate(ctx, apiKey.Raw)
	require.ErrorIs(t, err, workerauth.ErrUnauthorized,
		"an api key must never authenticate as a worker key")

	_, err = authStore().Authenticate(ctx, workerKey.Raw)
	require.ErrorIs(t, err, auth.ErrUnauthorized,
		"a worker key must never authenticate as an api key")
}

func TestWorkerKeyStore_ConcurrentCreationProducesDistinctUsableKeys(t *testing.T) {
	reset(t)
	ctx := context.Background()

	const n = 12
	type result struct {
		created workerauth.Created
		err     error
	}
	results := make([]result, n)

	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for i := 0; i < n; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			store := workerauth.NewStore(testPool)
			start.Wait()
			created, err := store.Create(ctx, fmt.Sprintf("worker-tenant-%02d", i), "concurrent")
			results[i] = result{created: created, err: err}
		}(i)
	}
	start.Done()
	done.Wait()

	prefixes := map[string]struct{}{}
	for i, r := range results {
		require.NoErrorf(t, r.err, "worker %d", i)
		_, dup := prefixes[r.created.Key.Prefix]
		require.Falsef(t, dup, "worker %d reused a lookup prefix", i)
		prefixes[r.created.Key.Prefix] = struct{}{}
	}

	for i, r := range results {
		principal, err := workerAuthStore().Authenticate(ctx, r.created.Raw)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("worker-tenant-%02d", i), principal.Scope)
	}
}

func TestWorkerKeyStore_ConcurrentRevocationHasOneWinner(t *testing.T) {
	reset(t)
	ctx := context.Background()
	created := mintWorkerKey(t, testScope, "contended")

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
			store := workerauth.NewStore(testPool)
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
	require.Equal(t, 1, winners)
	for i, instant := range instants {
		require.Truef(t, instant.Equal(instants[0]), "caller %d saw a different revocation instant", i)
	}
}

func TestWorkerKeyStore_ConcurrentAuthenticationIsRaceFree(t *testing.T) {
	reset(t)
	created := mintWorkerKey(t, "tenant-concurrent-worker", "shared")

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
			store := workerauth.NewStore(testPool)
			start.Wait()
			principal, err := store.Authenticate(context.Background(), created.Raw)
			scopes[i], errs[i] = principal.Scope, err
		}(i)
	}
	start.Done()
	done.Wait()

	for i := range scopes {
		require.NoErrorf(t, errs[i], "caller %d", i)
		require.Equal(t, "tenant-concurrent-worker", scopes[i])
	}
}

// --- end to end through the real HTTP server --------------------------------

// TestE2E_AWorkerRegisteredUnderAMatchingWorkerKeyExecutesTheJob is the
// headline test of this milestone: it closes, for real, the limitation
// TestE2E_APIKeyDrivesTheWholeLifecycle's sibling used to record (a job
// submitted under an API key for any scope but the one fixed configured
// worker-control scope stayed QUEUED forever). Since M5B a worker key can be
// minted for ANY scope, and a job submitted under an API key for that same
// scope is now actually claimed and executed by a worker registered under a
// matching worker key -- proving the fix is real, not merely that the old
// failure mode was removed.
func TestE2E_AWorkerRegisteredUnderAMatchingWorkerKeyExecutesTheJob(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registry.Register("demo.echo", workerruntime.DemoEcho{}))
	stack := startE2EStack(t, registry, time.Hour)

	const scope = "tenant-with-its-own-workers"
	apiKey := mintKey(t, scope, "e2e-submitter")
	workerKey := mintWorkerKey(t, scope, "e2e-executor")

	request, err := http.NewRequest(http.MethodPost, stack.baseURL+"/v1/jobs", strings.NewReader(
		`{"queue":"default","job_type":"demo.echo","payload":{"message":"cross-scope"}}`))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "e2e-cross-scope")
	response, err := clientWithKey(apiKey.Raw).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusCreated, response.StatusCode)

	var job api.JobResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&job))
	jobID := uuid.MustParse(job.ID)

	// Deliberately no second, wrong-scope worker sharing this stack's broker
	// queue: scope isolation is proven elsewhere (TestAPIKeys_ScopeIsolationIsEnforcedByTheKey,
	// requireWorkerKey and resolveWorkerControlScope's own unit tests), and a
	// worker that receives this job's broker notification but is never
	// eligible to claim it would hold that message invisible for the queue's
	// visibility timeout, stalling the worker that actually can -- a race
	// against the test's own timeout that this test does not need to run.
	//
	// The worker registered under a key matching the job's own scope, driven
	// through its own control client so it presents workerKey rather than the
	// stack's suite-wide default.
	control := workerruntime.NewClient(stack.baseURL, &http.Client{Timeout: 10 * time.Second}, workerKey.Raw)
	runner := workerruntime.NewRunner(control, stack.broker, registry, nil, workerruntime.RunnerConfig{
		Registration: workerRegistration("cross-scope-worker", 1, []string{"cpu"}, nil),
		Queue:        "default", PollWait: time.Second,
		RetryAttempts: 3, RetryDelay: 10 * time.Millisecond, ErrorBackoff: 10 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond, SessionStaleAfter: 3 * time.Second,
		RenewInterval: time.Second, ShutdownTimeout: 2 * time.Second,
	}, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("cross-scope worker did not stop after cancellation")
		}
	})

	awaitJobStatus(t, jobID, "SUCCEEDED", 30*time.Second)

	var attempts int
	var storedScope string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, jobID).Scan(&attempts))
	require.Equal(t, 1, attempts, "exactly one worker -- the matching one -- ever claimed this job")
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT scope FROM jobs WHERE id = $1`, jobID).Scan(&storedScope))
	require.Equal(t, scope, storedScope)

	// The session that actually executed it was registered under workerKey.
	var registeredKeyID uuid.UUID
	require.NoError(t, testPool.QueryRow(context.Background(), `
		SELECT ws.worker_key_id FROM job_attempts a
		JOIN worker_sessions ws ON ws.id = a.worker_session_id
		WHERE a.job_id = $1`, jobID).Scan(&registeredKeyID))
	require.Equal(t, workerKey.Key.ID, registeredKeyID)
}

// TestE2E_RevokingAWorkerKeyRefusesTheNextCallItsSessionsMake pins the
// confirmed Option B revocation semantics: revoking a worker key does not
// force-expire a lease a session it registered already holds, and does not
// touch reconciliation -- but the very next control-plane call that session
// makes is refused, because SessionScope's worker_key_id is checked against
// IsRevoked on every non-register call.
func TestE2E_RevokingAWorkerKeyRefusesTheNextCallItsSessionsMake(t *testing.T) {
	reset(t)
	server := newAPI(t)

	workerKey := mintWorkerKey(t, "tenant-revoked-worker", "short-lived-worker")
	sessionID := uuid.New()
	body := `{"worker_name":"revoke-probe","hostname":"revoke.local","worker_group":"default",
		"concurrency_limit":1,"capabilities":["cpu"],"supported_job_types":["demo.echo"]}`
	registerRequest, err := http.NewRequest(http.MethodPut,
		server.URL+"/internal/v1/worker-sessions/"+sessionID.String(), strings.NewReader(body))
	require.NoError(t, err)
	registerRequest.Header.Set("Content-Type", "application/json")
	registerResponse, err := clientWithKey(workerKey.Raw).Do(registerRequest)
	require.NoError(t, err)
	defer registerResponse.Body.Close()
	require.Equal(t, http.StatusOK, registerResponse.StatusCode)

	var session api.WorkerSessionResponse
	require.NoError(t, json.NewDecoder(registerResponse.Body).Decode(&session))

	heartbeat := func(t *testing.T) int {
		t.Helper()
		heartbeatBody := `{"worker_id":"` + session.WorkerID + `"}`
		request, err := http.NewRequest(http.MethodPost,
			server.URL+"/internal/v1/worker-sessions/"+sessionID.String()+"/heartbeat",
			strings.NewReader(heartbeatBody))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := unauthenticatedClient().Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		return response.StatusCode
	}

	require.Equal(t, http.StatusOK, heartbeat(t), "the session heartbeats fine before revocation")

	_, err = workerAuthStore().Revoke(context.Background(), workerKey.Key.ID)
	require.NoError(t, err)

	require.Equal(t, http.StatusUnauthorized, heartbeat(t),
		"the next call from this session must be refused once its worker key is revoked")
	require.Equal(t, http.StatusUnauthorized, heartbeat(t), "and stays refused")

	// The session itself is untouched by revocation: still HEALTHY, not
	// force-expired. This is the "does not touch reconciliation" half of
	// Option B -- revocation is discovered, not enforced by mutating the
	// session or its lease.
	var status string
	require.NoError(t, testPool.QueryRow(context.Background(),
		`SELECT status FROM worker_sessions WHERE id = $1`, sessionID).Scan(&status))
	require.Equal(t, "HEALTHY", status,
		"revocation must not force-expire the session; it only refuses the session's next call")
}
