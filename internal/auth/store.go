package auth

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
)

// Errors callers must distinguish.
var (
	// ErrUnauthorized is the single answer to every failed authentication:
	// malformed credential, unknown prefix, wrong secret, and revoked key all
	// return exactly this.
	//
	// One sentinel is the point. If a caller could tell "no such key" from
	// "revoked" from "wrong secret", a prefix would be an oracle: present a
	// guessed prefix with any secret, and the response tells you whether that
	// prefix exists. The HTTP layer therefore has nothing to accidentally leak,
	// because the distinction never crosses this boundary at all.
	ErrUnauthorized = errors.New("api key is not valid")

	// ErrKeyNotFound reports an administrative operation naming a key id that
	// does not exist. It is a management error, not an authentication one, and
	// is never returned by Authenticate.
	ErrKeyNotFound = errors.New("api key not found")

	// ErrPrefixExhausted reports that two consecutive generated lookup prefixes
	// both collided with an existing row. With 128 bits per prefix this is not
	// reachable by chance; it is here so a broken randomness source surfaces as
	// a named failure instead of a leaked unique-violation.
	ErrPrefixExhausted = errors.New("could not generate a unique api key prefix")

	// ErrDeadlineExceeded reports that a database call failed because the
	// operation's own deadline elapsed, so its outcome is unknown to the caller.
	//
	// It mirrors jobs.ErrDeadlineExceeded and workers.ErrDeadlineExceeded
	// deliberately rather than being shared with them: this package depends on
	// neither, and a shared sentinel would exist only so an error value could be
	// spelled once.
	ErrDeadlineExceeded = errors.New("operation deadline exceeded")
)

// MaxNameLen and MaxScopeLen mirror the api_keys CHECK constraints. The
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

// ValidationError carries every problem found in a request, so a caller can fix
// them all at once instead of one round trip per mistake.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for _, field := range e.Fields {
		parts = append(parts, field.Field+" "+field.Message)
	}
	return "api key request is invalid: " + strings.Join(parts, "; ")
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

// Created is the one and only time a raw credential exists outside the caller's
// own possession.
type Created struct {
	Key Key
	// Raw is the complete credential to hand to the client. It is not stored,
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

// Principal is what an authenticated request acts as.
//
// Scope is the whole authorization model in M5A: it is the same tenancy value
// jobs, attempts, leases, and DLQ entries have been filtered by since M1, and a
// key carries exactly one. There is no permission set here, because there is no
// implemented behavior that would read one (AGENTS.md section 3).
type Principal struct {
	KeyID uuid.UUID
	Scope string
}

// Store persists API keys.
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
// It exists so a test can prove that a prefix collision is retried rather than
// leaked, which requires a source that repeats. Production uses NewStore.
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
	FROM api_keys
`

func scanKey(row pgx.Row) (Key, error) {
	var k Key
	if err := row.Scan(&k.ID, &k.Scope, &k.Name, &k.Prefix, &k.CreatedAt, &k.RevokedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Key{}, ErrKeyNotFound
		}
		return Key{}, fmt.Errorf("read api key: %w", err)
	}
	return k, nil
}

// Create mints one new credential and returns it exactly once.
//
// Creation is deliberately NOT idempotent and carries no request identity. Every
// call mints a distinct credential, because that is what "mint me a key" means:
// an Idempotency-Key here would let a retry return an existing secret, which is
// the one thing a write-only credential must never do. An ambiguous response is
// therefore resolved by listing keys and revoking the one nobody received, not
// by replaying the request.
func (s *Store) Create(ctx context.Context, scope, name string) (Created, error) {
	scope, name = strings.TrimSpace(scope), strings.TrimSpace(name)
	if err := validateKeyRequest(scope, name); err != nil {
		return Created{}, err
	}

	// Two attempts, not a loop: a 128-bit prefix colliding once is already
	// beyond chance, so a second collision means the randomness source is
	// broken and retrying forever would spin instead of reporting it.
	for attempt := 0; attempt < 2; attempt++ {
		material, err := GenerateMaterial(s.random)
		if err != nil {
			return Created{}, fmt.Errorf("generate api key: %w", err)
		}

		id := uuid.New()
		var key Key
		// created_at comes from the database (AGENTS.md section 6), and is
		// returned by the same statement that writes it so the caller cannot be
		// handed a second, differently-sampled instant.
		err = s.pool.QueryRow(ctx, `
			INSERT INTO api_keys (id, scope, name, prefix, secret_hash)
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
		return Created{}, classifyDatabaseError(fmt.Errorf("insert api key: %w", err))
	}
	return Created{}, ErrPrefixExhausted
}

// isPrefixCollision reports the one unique violation Create can legitimately
// retry: two generated prefixes landing on the same value.
//
// It checks the constraint name, not merely the SQLSTATE. Any other unique
// violation on this table would be a different bug, and retrying it with fresh
// key material would hide it behind a second identical failure.
func isPrefixCollision(err error) bool {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23505" {
		return false
	}
	return postgresError.ConstraintName == "api_keys_prefix_key"
}

// Revoke withdraws a credential. It is idempotent: revoking an already-revoked
// key reports the original revocation instant rather than moving it, the same
// way jobs.RequestCancel reports an already-requested cancellation.
//
// The UPDATE is conditional on revoked_at IS NULL, so two concurrent revocations
// of one key cannot both stamp it; the loser updates zero rows and reads the
// winner's instant back. Nothing here depends on check-then-write.
func (s *Store) Revoke(ctx context.Context, id uuid.UUID) (RevokeResult, error) {
	key, err := scanKey(s.pool.QueryRow(ctx, `
		UPDATE api_keys
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
// Revoked keys are included: an operator auditing which credentials ever existed
// needs to see the revoked ones, and hiding them would make the listing useless
// for the question it is actually asked.
func (s *Store) List(ctx context.Context, limit int) ([]Key, error) {
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	// The ORDER BY matches api_keys_listing_idx exactly. The id tiebreak makes
	// the ordering total, so two keys minted in the same transaction cannot make
	// a bounded page non-deterministic.
	rows, err := s.pool.Query(ctx, keySelect+`
		ORDER BY created_at DESC, id DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, classifyDatabaseError(fmt.Errorf("list api keys: %w", err))
	}
	defer rows.Close()

	keys := make([]Key, 0, limit)
	for rows.Next() {
		var k Key
		if err := rows.Scan(&k.ID, &k.Scope, &k.Name, &k.Prefix, &k.CreatedAt, &k.RevokedAt); err != nil {
			return nil, classifyDatabaseError(fmt.Errorf("read api key row: %w", err))
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyDatabaseError(fmt.Errorf("list api keys: %w", err))
	}
	return keys, nil
}

// Authenticate resolves a presented credential to the scope it acts within.
//
// The order is deliberate and is the whole cost model of this function:
//
//  1. Shape is checked in memory. A value that was never a key costs no round
//     trip, which is what stops an unauthenticated caller from turning a header
//     into database load.
//  2. One indexed equality probe on api_keys.prefix. Not a scan; the UNIQUE
//     index on prefix is what keeps this inside the request's timeout budget.
//  3. Constant-time secret verification, then the revocation check.
//
// Every failure returns ErrUnauthorized unchanged, so nothing about which check
// failed reaches the caller. A deadline is the one exception, because "I could
// not tell" is genuinely different from "you are not authorized" and answering
// 401 to a database timeout would tell an operator their key was revoked.
func (s *Store) Authenticate(ctx context.Context, raw string) (Principal, error) {
	presented, err := ParseKey(raw)
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
		FROM api_keys
		WHERE prefix = $1`, presented.Prefix,
	).Scan(&id, &scope, &secretHash, &revokedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Principal{}, ErrUnauthorized
		}
		return Principal{}, classifyDatabaseError(fmt.Errorf("look up api key: %w", err))
	}

	// Verification happens before the revocation check, and both answer the same
	// error. Checking revocation first would let a caller holding only a valid
	// prefix learn whether that key is live, without ever knowing its secret.
	if !VerifySecret(secretHash, presented.Secret) {
		return Principal{}, ErrUnauthorized
	}
	if revokedAt != nil {
		return Principal{}, ErrUnauthorized
	}
	return Principal{KeyID: id, Scope: scope}, nil
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

// isPrintableWithin rejects control characters as well as length.
//
// A scope reaches structured log lines and a name reaches an operator's terminal,
// so a newline or an escape sequence in either is a log- and terminal-injection
// vector, exactly as it is for X-Request-Id.
//
// The bound is measured in bytes to match the api_keys CHECK, which uses
// PostgreSQL's length() on TEXT — that counts characters, so the byte bound here
// is the stricter of the two and can never admit a row the database refuses.
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
// database call and the typed deadline sentinel.
//
// It inspects only the returned error, never ctx.Err(): an unrelated constraint,
// driver, or state failure that merely finishes after the deadline elapsed must
// keep its own identity rather than be laundered into a retryable deadline.
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
