// Package workerauth implements TaskForge's scoped, revocable credentials for
// registering a worker process session.
//
// It is deliberately not internal/auth reused wholesale: PROJECT_SPEC.md
// section 6 keeps worker/control scopes separable from user scopes, so a
// worker key and a public API key are verified against different tables, and
// a caller holding one can never be authenticated by the other's check. See
// docs/adr/0014-worker-control-authentication.md.
//
// The one thing this package does share with internal/auth is exactly what
// the design calls for reusing "as-is": the pure, table-agnostic key-material
// functions in internal/auth/key.go -- GenerateMaterial, ParseKey, HashSecret,
// VerifySecret, and the format constants they use. None of them know about
// api_keys, and importing them here costs nothing this package would
// otherwise have to re-derive, verify, and keep in sync by hand. This package
// otherwise owns nothing about job lifecycle and has no dependency on
// internal/jobs, and owns nothing about session fencing and has no dependency
// on internal/workers beyond the uuid.UUID values internal/api composes the
// two packages' answers with.
package workerauth

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-rtex/TaskForge/internal/auth"
)

// Errors callers must distinguish.
var (
	// ErrUnauthorized is the single answer to every failed authentication:
	// malformed credential, unknown prefix, wrong secret, and revoked key all
	// return exactly this. See internal/auth.ErrUnauthorized for why one
	// sentinel is the point -- the reasoning is identical here.
	ErrUnauthorized = errors.New("worker key is not valid")

	// ErrKeyNotFound reports an administrative operation naming a key id that
	// does not exist. It is a management error, not an authentication one, and
	// is never returned by Authenticate.
	ErrKeyNotFound = errors.New("worker key not found")

	// ErrPrefixExhausted reports that two consecutive generated lookup prefixes
	// both collided with an existing row. With 128 bits per prefix this is not
	// reachable by chance; it is here so a broken randomness source surfaces as
	// a named failure instead of a leaked unique-violation.
	ErrPrefixExhausted = errors.New("could not generate a unique worker key prefix")

	// ErrDeadlineExceeded reports that a database call failed because the
	// operation's own deadline elapsed, so its outcome is unknown to the
	// caller.
	//
	// It mirrors auth.ErrDeadlineExceeded, jobs.ErrDeadlineExceeded, and
	// workers.ErrDeadlineExceeded deliberately rather than being shared with
	// any of them: this package depends on none of them for its own error
	// identity, and a shared sentinel would exist only so a value could be
	// spelled once.
	ErrDeadlineExceeded = errors.New("operation deadline exceeded")
)

// MaxNameLen and MaxScopeLen mirror the worker_keys CHECK constraints. The
// database is the enforcement; these exist so a caller gets a field-level
// message instead of a constraint violation.
const (
	MaxScopeLen = 128
	MaxNameLen  = 128

	// DefaultListLimit and MaxListLimit bound the administrative listing. An
	// unbounded listing is how a management endpoint becomes a way to pull the
	// whole table in one request.
	DefaultListLimit = 50
	MaxListLimit     = 200
)

// FieldError names one invalid input field.
type FieldError struct {
	Field   string
	Message string
}

// ValidationError carries every problem found in a request, so a caller can
// fix them all at once instead of one round trip per mistake.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for _, field := range e.Fields {
		parts = append(parts, field.Field+" "+field.Message)
	}
	return "worker key request is invalid: " + strings.Join(parts, "; ")
}

// Key is the administrative view of a credential.
//
// There is deliberately no secret and no hash field on this type. A listing
// cannot leak what it has no place to put, and that is a property of the type
// rather than of every query that builds one.
type Key struct {
	ID        uuid.UUID
	Scope     string
	Name      string
	Prefix    string
	CreatedAt time.Time
	// RevokedAt is nil while the key is live.
	RevokedAt *time.Time
}

// Revoked reports whether this key has been revoked.
func (k Key) Revoked() bool { return k.RevokedAt != nil }

// Created is the one and only time a raw credential exists outside the
// caller's own possession.
type Created struct {
	Key Key
	// Raw is the complete credential to hand to the caller. It is not stored,
	// not recoverable, and must never be logged.
	Raw string
}

// RevokeResult reports a revocation, including one that had already happened.
type RevokeResult struct {
	Key Key
	// AlreadyRevoked is true when this key was revoked before this call.
	// Revocation is keyed by key id alone, so repeating it is idempotent and
	// reports the original instant rather than moving it.
	AlreadyRevoked bool
}

// Principal is what an authenticated registration acts as.
//
// Scope is the whole authorization model, exactly as it is for a public API
// key: the same tenancy value jobs, attempts, leases, and DLQ entries filter
// by, and a worker key carries exactly one. There is no permission set here,
// because there is no implemented behavior that would read one.
type Principal struct {
	KeyID uuid.UUID
	Scope string
}

// Store persists worker keys.
type Store struct {
	pool   *pgxpool.Pool
	random io.Reader
}

// NewStore builds a Store over an existing pool, drawing key material from
// crypto/rand.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, random: rand.Reader}
}

// NewStoreWithRandom builds a Store over a caller-supplied randomness source.
//
// It exists so a test can prove that a prefix collision is retried rather
// than leaked, which requires a source that repeats. Production uses
// NewStore.
func NewStoreWithRandom(pool *pgxpool.Pool, random io.Reader) *Store {
	if random == nil {
		random = rand.Reader
	}
	return &Store{pool: pool, random: random}
}

// keySelect is the one column list every administrative read uses, so a new
// column cannot be returned by one path and silently missing from another. It
// does not name secret_hash, and no read in this package does.
const keySelect = `
	SELECT id, scope, name, prefix, created_at, revoked_at
	FROM worker_keys
`

func scanKey(row pgx.Row) (Key, error) {
	var k Key
	if err := row.Scan(&k.ID, &k.Scope, &k.Name, &k.Prefix, &k.CreatedAt, &k.RevokedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Key{}, ErrKeyNotFound
		}
		return Key{}, fmt.Errorf("read worker key: %w", err)
	}
	return k, nil
}

// Create mints one new credential and returns it exactly once.
//
// Creation is deliberately NOT idempotent and carries no request identity,
// for the identical reason internal/auth.Store.Create is not: an idempotent
// create would have to return an existing secret to a repeat, which is the
// one thing a write-only credential must never do.
func (s *Store) Create(ctx context.Context, scope, name string) (Created, error) {
	scope, name = strings.TrimSpace(scope), strings.TrimSpace(name)
	if err := validateKeyRequest(scope, name); err != nil {
		return Created{}, err
	}

	// Two attempts, not a loop: a 128-bit prefix colliding once is already
	// beyond chance, so a second collision means the randomness source is
	// broken and retrying forever would spin instead of reporting it.
	for attempt := 0; attempt < 2; attempt++ {
		material, err := auth.GenerateMaterial(s.random)
		if err != nil {
			return Created{}, fmt.Errorf("generate worker key: %w", err)
		}

		id := uuid.New()
		var key Key
		// created_at comes from the database (AGENTS.md section 6), and is
		// returned by the same statement that writes it so the caller cannot
		// be handed a second, differently-sampled instant.
		err = s.pool.QueryRow(ctx, `
			INSERT INTO worker_keys (id, scope, name, prefix, secret_hash)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id, scope, name, prefix, created_at, revoked_at`,
			id, scope, name, material.Prefix, material.SecretHash,
		).Scan(&key.ID, &key.Scope, &key.Name, &key.Prefix, &key.CreatedAt, &key.RevokedAt)

		if err == nil {
			return Created{Key: key, Raw: material.Raw()}, nil
		}
		if isPrefixCollision(err) {
			continue
		}
		return Created{}, classifyDatabaseError(fmt.Errorf("insert worker key: %w", err))
	}
	return Created{}, ErrPrefixExhausted
}

// isPrefixCollision reports the one unique violation Create can legitimately
// retry: two generated prefixes landing on the same value.
//
// It checks the constraint name, not merely the SQLSTATE, exactly as
// internal/auth.isPrefixCollision does: any other unique violation on this
// table would be a different bug, and retrying it with fresh key material
// would hide it behind a second identical failure.
func isPrefixCollision(err error) bool {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23505" {
		return false
	}
	return postgresError.ConstraintName == "worker_keys_prefix_key"
}

// Revoke withdraws a credential. It is idempotent: revoking an already-revoked
// key reports the original revocation instant rather than moving it.
//
// The UPDATE is conditional on revoked_at IS NULL, so two concurrent
// revocations of one key cannot both stamp it; the loser updates zero rows
// and reads the winner's instant back. Nothing here depends on
// check-then-write.
//
// Revocation does not touch worker_sessions. A session already registered
// under this key keeps its worker_key_id, and the next control-plane call it
// makes is what discovers the key is now revoked -- see
// internal/workers.Store.SessionScope, which reads worker_key_id, and
// IsRevoked below, which this package's own caller checks it against.
func (s *Store) Revoke(ctx context.Context, id uuid.UUID) (RevokeResult, error) {
	key, err := scanKey(s.pool.QueryRow(ctx, `
		UPDATE worker_keys
		SET revoked_at = now()
		WHERE id = $1 AND revoked_at IS NULL
		RETURNING id, scope, name, prefix, created_at, revoked_at`, id))
	switch {
	case err == nil:
		return RevokeResult{Key: key}, nil
	case errors.Is(err, ErrKeyNotFound):
		// Zero rows means either no such key, or one that was already revoked.
		// Only a read can tell those apart, and the distinction matters: the
		// first is a 404 and the second is a successful idempotent repeat.
		existing, readErr := s.Get(ctx, id)
		if readErr != nil {
			return RevokeResult{}, readErr
		}
		return RevokeResult{Key: existing, AlreadyRevoked: true}, nil
	default:
		return RevokeResult{}, classifyDatabaseError(err)
	}
}

// Get reads one key's metadata by id.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (Key, error) {
	key, err := scanKey(s.pool.QueryRow(ctx, keySelect+` WHERE id = $1`, id))
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return Key{}, err
		}
		return Key{}, classifyDatabaseError(err)
	}
	return key, nil
}

// List returns one bounded page of key metadata, newest first.
//
// Revoked keys are included: an operator auditing which credentials ever
// existed needs to see the revoked ones, and hiding them would make the
// listing useless for the question it is actually asked.
func (s *Store) List(ctx context.Context, limit int) ([]Key, error) {
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	// The ORDER BY matches worker_keys_listing_idx exactly. The id tiebreak
	// makes the ordering total, so two keys minted in the same transaction
	// cannot make a bounded page non-deterministic.
	rows, err := s.pool.Query(ctx, keySelect+`
		ORDER BY created_at DESC, id DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, classifyDatabaseError(fmt.Errorf("list worker keys: %w", err))
	}
	defer rows.Close()

	keys := make([]Key, 0, limit)
	for rows.Next() {
		var k Key
		if err := rows.Scan(&k.ID, &k.Scope, &k.Name, &k.Prefix, &k.CreatedAt, &k.RevokedAt); err != nil {
			return nil, classifyDatabaseError(fmt.Errorf("read worker key row: %w", err))
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyDatabaseError(fmt.Errorf("list worker keys: %w", err))
	}
	return keys, nil
}

// Authenticate resolves a presented credential to the scope a registering
// worker will act within.
//
// It is called exactly once per session lifetime, at
// PUT /internal/v1/worker-sessions/{id}, never on Heartbeat, Claim, or any
// other worker-control call: those resolve scope from the session's own
// persisted value (internal/workers.Store.SessionScope) and check IsRevoked
// below, which costs one indexed lookup by id rather than a secret
// verification.
//
// The order is deliberate and is the whole cost model of this function,
// identical to internal/auth.Store.Authenticate:
//
//  1. Shape is checked in memory. A value that was never a key costs no round
//     trip.
//  2. One indexed equality probe on worker_keys.prefix. Not a scan.
//  3. Constant-time secret verification, then the revocation check.
//
// Every failure returns ErrUnauthorized unchanged, so nothing about which
// check failed reaches the caller. A deadline is the one exception, because
// "I could not tell" is genuinely different from "you are not authorized".
func (s *Store) Authenticate(ctx context.Context, raw string) (Principal, error) {
	presented, err := auth.ParseKey(raw)
	if err != nil {
		return Principal{}, ErrUnauthorized
	}

	var (
		id         uuid.UUID
		scope      string
		secretHash string
		revokedAt  *time.Time
	)
	err = s.pool.QueryRow(ctx, `
		SELECT id, scope, secret_hash, revoked_at
		FROM worker_keys
		WHERE prefix = $1`, presented.Prefix,
	).Scan(&id, &scope, &secretHash, &revokedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Principal{}, ErrUnauthorized
		}
		return Principal{}, classifyDatabaseError(fmt.Errorf("look up worker key: %w", err))
	}

	// Verification happens before the revocation check, and both answer the
	// same error. Checking revocation first would let a caller holding only a
	// valid prefix learn whether that key is live, without ever knowing its
	// secret.
	if !auth.VerifySecret(secretHash, presented.Secret) {
		return Principal{}, ErrUnauthorized
	}
	if revokedAt != nil {
		return Principal{}, ErrUnauthorized
	}
	return Principal{KeyID: id, Scope: scope}, nil
}

// IsRevoked reports whether the worker key identified by id has been revoked.
//
// This is the cheap, secret-free check every other worker-control call makes
// -- Heartbeat, Claim, Start, Succeed, Fail, AcknowledgeCancellation, and
// RenewLease -- once per request, keyed by the worker_key_id
// internal/workers.Store.SessionScope already read off the session row. It
// never compares a secret and never returns ErrUnauthorized: the caller
// decides what a revoked credential means for the request in front of it.
//
// A key id that does not exist reports revoked=true rather than an error.
// The only way a caller reaches this with an unknown id is a worker_key_id
// column pointing at a row that was somehow deleted, which never happens --
// worker_keys rows are only ever revoked, never deleted -- but a caller that
// cannot find the credential it was told to trust has no business trusting
// the request either, so failing safe here costs nothing and needs no
// separate error path.
func (s *Store) IsRevoked(ctx context.Context, id uuid.UUID) (bool, error) {
	var revokedAt *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT revoked_at FROM worker_keys WHERE id = $1`, id,
	).Scan(&revokedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil
		}
		return false, classifyDatabaseError(fmt.Errorf("check worker key revocation: %w", err))
	}
	return revokedAt != nil, nil
}

func validateKeyRequest(scope, name string) error {
	var fields []FieldError
	if !isPrintableWithin(scope, MaxScopeLen) {
		fields = append(fields, FieldError{
			Field:   "scope",
			Message: fmt.Sprintf("must contain between 1 and %d printable characters", MaxScopeLen),
		})
	}
	if !isPrintableWithin(name, MaxNameLen) {
		fields = append(fields, FieldError{
			Field:   "name",
			Message: fmt.Sprintf("must contain between 1 and %d printable characters", MaxNameLen),
		})
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// isPrintableWithin rejects control characters as well as length, for the
// identical reason internal/auth.isPrintableWithin does: a scope reaches
// structured log lines and a name reaches an operator's terminal.
func isPrintableWithin(value string, maxLen int) bool {
	if len(value) < 1 || len(value) > maxLen {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// classifyDatabaseError is the single translation point between a failed
// database call and the typed deadline sentinel. It inspects only the
// returned error, never ctx.Err(), for the identical reason
// internal/auth.classifyDatabaseError does.
func classifyDatabaseError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrDeadlineExceeded) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrDeadlineExceeded, err)
	}
	return err
}
