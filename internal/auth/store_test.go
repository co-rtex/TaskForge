package auth

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every case below runs against a Store with a nil connection pool.
//
// That is the strongest available assertion that these paths never reach
// PostgreSQL: if one of them did, it would panic rather than quietly pass. It
// mirrors internal/api's nil-store unit tests, and anything that must reach the
// database is an integration test instead.
func nilPoolStore() *Store { return &Store{} }

// TestAuthenticate_RejectsAMalformedCredentialWithoutTouchingTheDatabase pins
// the property that keeps an unauthenticated caller from turning a header into
// database load. Shape is decided in memory; only a well-shaped credential is
// ever worth a lookup.
func TestAuthenticate_RejectsAMalformedCredentialWithoutTouchingTheDatabase(t *testing.T) {
	store := nilPoolStore()
	for name, credential := range map[string]string{
		"empty":              "",
		"not a key at all":   "hunter2",
		"wrong marker":       "sk_live_" + strings.Repeat("a", 60),
		"right length only":  strings.Repeat("a", KeyLen),
		"oversized":          strings.Repeat("a", MaxCredentialLen*2),
		"bearer header kept": "Bearer tfk_" + strings.Repeat("a", 66),
	} {
		t.Run(name, func(t *testing.T) {
			principal, err := store.Authenticate(context.Background(), credential)
			require.ErrorIs(t, err, ErrUnauthorized)
			require.Equal(t, Principal{}, principal)
		})
	}
}

// Every authentication failure must be the SAME error value, asserted by
// identity rather than by "some error happened".
//
// A caller who can distinguish "no such prefix" from "wrong secret" can use a
// prefix as an oracle: present a guess with any secret and the response says
// whether that prefix exists. Keeping one sentinel means the HTTP layer has
// nothing to leak, because the distinction never crosses this boundary.
func TestAuthenticate_ReturnsOneIndistinguishableError(t *testing.T) {
	store := nilPoolStore()
	first, err := store.Authenticate(context.Background(), "")
	require.Equal(t, Principal{}, first)
	require.Equal(t, ErrUnauthorized, err, "the sentinel must be returned unwrapped")

	_, second := store.Authenticate(context.Background(), "tfk_"+strings.Repeat("-", 66))
	require.Equal(t, ErrUnauthorized, second)
	require.Equal(t, err, second, "every shape failure must answer identically")
}

// Creation validates before it reaches the database, so a caller gets a
// field-level message instead of a constraint violation, and an invalid request
// costs no round trip.
func TestCreate_ValidatesBeforeTouchingTheDatabase(t *testing.T) {
	store := nilPoolStore()
	tests := map[string]struct {
		scope, name string
		wantFields  []string
	}{
		"empty scope":             {"", "ops", []string{"scope"}},
		"empty name":              {"tenant-a", "", []string{"name"}},
		"whitespace-only scope":   {"   ", "ops", []string{"scope"}},
		"both empty":              {"", "", []string{"scope", "name"}},
		"scope too long":          {strings.Repeat("s", MaxScopeLen+1), "ops", []string{"scope"}},
		"name too long":           {"tenant-a", strings.Repeat("n", MaxNameLen+1), []string{"name"}},
		"newline in scope":        {"tenant\na", "ops", []string{"scope"}},
		"carriage return in name": {"tenant-a", "ops\rrole", []string{"name"}},
		"nul in name":             {"tenant-a", "ops\x00", []string{"name"}},
		"escape sequence in name": {"tenant-a", "ops\x1b[31m", []string{"name"}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := store.Create(context.Background(), tc.scope, tc.name)

			var validation *ValidationError
			require.ErrorAs(t, err, &validation)

			fields := make([]string, 0, len(validation.Fields))
			for _, field := range validation.Fields {
				fields = append(fields, field.Field)
			}
			require.ElementsMatch(t, tc.wantFields, fields)
			require.NotEmpty(t, validation.Error())
		})
	}
}

// The bound is measured in bytes while the api_keys CHECK counts characters, so
// a multi-byte value at the boundary must be refused here rather than admitted
// and then rejected by the database.
func TestCreate_ByteBoundIsNeverLooserThanTheSchemaCheck(t *testing.T) {
	// 128 two-byte characters: 128 characters to PostgreSQL's length(), 256
	// bytes to this validator. The stricter of the two must win.
	multibyte := strings.Repeat("é", MaxScopeLen)
	_, err := nilPoolStore().Create(context.Background(), multibyte, "ops")

	var validation *ValidationError
	require.ErrorAs(t, err, &validation)
	require.Equal(t, "scope", validation.Fields[0].Field)
}

// Trimming is part of the contract: a scope that differs from another only by
// surrounding whitespace would authenticate to a value no job row holds.
func TestCreate_TrimsSurroundingWhitespaceBeforeValidating(t *testing.T) {
	// A tab-only name is whitespace, not a name, and must be rejected as empty
	// rather than stored as a tab.
	_, err := nilPoolStore().Create(context.Background(), "tenant-a", "\t\t")

	var validation *ValidationError
	require.ErrorAs(t, err, &validation)
	require.Equal(t, "name", validation.Fields[0].Field)
}

// List clamps rather than refusing, so an administrative caller cannot pull the
// whole table by asking for a large enough page.
func TestListLimits_AreBounded(t *testing.T) {
	require.Less(t, DefaultListLimit, MaxListLimit)
	require.Positive(t, DefaultListLimit)
}

// The administrative view has nowhere to put a secret. That is a property of the
// type, not of every query that builds one, which is what stops a future read
// from accidentally selecting secret_hash into a response.
func TestKey_CarriesNoSecretMaterial(t *testing.T) {
	require.NotContains(t, keySelect, "secret_hash",
		"no administrative read may select the stored digest")
	require.Contains(t, keySelect, "prefix",
		"the non-secret lookup segment is what identifies a key to an operator")
}
