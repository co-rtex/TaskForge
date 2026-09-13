// Package auth implements TaskForge's scoped, revocable API-key credentials for
// the public HTTP surface.
//
// It owns nothing about job lifecycle. A key answers exactly one question —
// which scope is this caller acting within — and that scope is then handed to
// the same scope-filtered stores M1 through M4 already built. The package has no
// dependency on internal/jobs or internal/workers, and must not grow one.
//
// The internal worker-control surface does not authenticate here. It still runs
// under the single configured development scope and stays loopback-bound; see
// docs/CURRENT_STATE.md and docs/adr/0013-database-backed-api-key-authentication.md.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

// The wire format of a presented key:
//
//	tfk_<lookup>.<secret>
//
// <lookup> is the non-secret segment stored in api_keys.prefix and used as the
// indexed equality probe. <secret> is never stored; only its SHA-256 digest is.
//
// Both segments are unpadded base64url, so a key is copy-pasteable, survives a
// URL or a shell argument unescaped, and contains no character that would need
// quoting in a log line.
const (
	// KeyPrefix marks a TaskForge key on sight, so one found in a log, a
	// config file, or a paste can be recognized and revoked without guessing
	// which system issued it.
	KeyPrefix = "tfk_"

	// separator divides the lookup segment from the secret segment.
	//
	// It is '.' rather than '_' precisely because base64url's alphabet INCLUDES
	// '-' and '_': splitting on '_' would cut at whichever underscore the random
	// lookup segment happened to contain, so roughly a quarter of all generated
	// keys would fail to parse. '.' is outside the alphabet, so it can never
	// occur inside either segment and the split is unambiguous.
	separator = "."

	// LookupBytes is the entropy behind the lookup segment. 128 bits makes a
	// collision on the UNIQUE prefix column a theoretical event rather than an
	// operational one, which is what lets Store.Create treat a collision as an
	// error path it retries once rather than a case it must plan around.
	LookupBytes = 16
	// SecretBytes is the entropy behind the secret segment. 256 bits is why
	// api_keys.secret_hash is a plain SHA-256 rather than a password KDF: there
	// is no guessable structure for a work factor to protect.
	SecretBytes = 32

	// LookupLen and SecretLen are the exact encoded lengths the two segments
	// always have. They are derived here once and asserted in the tests, so a
	// change to either entropy constant cannot silently move the schema's
	// prefix length bound or the format a client validates against.
	LookupLen = (LookupBytes*8 + 5) / 6 // 22
	SecretLen = (SecretBytes*8 + 5) / 6 // 43

	// KeyLen is the exact length of a well-formed presented key: 4 + 22 + 1 + 43.
	KeyLen = len(KeyPrefix) + LookupLen + len(separator) + SecretLen

	// MaxCredentialLen bounds what the HTTP layer is willing to hand this
	// package at all. It is deliberately larger than KeyLen so that a
	// near-miss is rejected by shape with a real error rather than by a length
	// cutoff, and deliberately small so an oversized credential is discarded
	// before it reaches any comparison or allocation.
	MaxCredentialLen = 4 * KeyLen
)

// ErrMalformedKey reports a credential that is not shaped like a TaskForge key.
//
// It never reaches a client. The HTTP layer answers one indistinguishable 401
// for this, for an unknown prefix, for a wrong secret, and for a revoked key —
// a caller who can tell those apart can probe for which prefixes exist.
var ErrMalformedKey = errors.New("malformed api key")

// encoding is unpadded base64url everywhere in this package. Padding would put
// '=' in a credential for no benefit and make the encoded lengths above wrong.
var encoding = base64.RawURLEncoding

// Material is a freshly generated credential, before it is persisted.
//
// Secret exists only in memory and only until Store.Create has returned it to
// the caller. Nothing in TaskForge may write it to the database, a log, or an
// error.
type Material struct {
	// Prefix is the non-secret lookup segment, stored in api_keys.prefix.
	Prefix string
	// Secret is the secret segment. Never persisted.
	Secret string
	// SecretHash is the lowercase hex SHA-256 of Secret, stored in
	// api_keys.secret_hash.
	SecretHash string
}

// Raw reassembles the single string a caller presents as their credential. It is
// returned exactly once, by the creation endpoint, and cannot be recovered
// afterwards from anything TaskForge stores.
func (m Material) Raw() string {
	return KeyPrefix + m.Prefix + separator + m.Secret
}

// GenerateMaterial mints one credential from the supplied randomness source.
//
// The source is a parameter rather than a direct crypto/rand call so a test can
// pin the exact bytes and therefore the exact encoded shape (AGENTS.md section
// 5). Production always passes crypto/rand.Reader, which Store does.
//
// A short read is an error, never a shorter key: io.ReadFull is what makes
// "16 bytes of entropy" a fact rather than an intention.
func GenerateMaterial(random io.Reader) (Material, error) {
	if random == nil {
		random = rand.Reader
	}

	lookup := make([]byte, LookupBytes)
	if _, err := io.ReadFull(random, lookup); err != nil {
		return Material{}, fmt.Errorf("read lookup entropy: %w", err)
	}
	secret := make([]byte, SecretBytes)
	if _, err := io.ReadFull(random, secret); err != nil {
		return Material{}, fmt.Errorf("read secret entropy: %w", err)
	}

	encodedSecret := encoding.EncodeToString(secret)
	return Material{
		Prefix:     encoding.EncodeToString(lookup),
		Secret:     encodedSecret,
		SecretHash: HashSecret(encodedSecret),
	}, nil
}

// Presented is a parsed credential: the lookup segment to probe with, and the
// secret to verify against what that probe returns.
type Presented struct {
	Prefix string
	Secret string
}

// ParseKey validates a presented credential's shape without touching the
// database.
//
// Everything it can reject, it rejects here: the marker, the total length, the
// separator position, and the character set of both segments. That ordering is
// the point — a caller cannot force a database round trip with a value that was
// never a key, and the length bound means an oversized header is discarded
// before it is ever split or compared.
func ParseKey(raw string) (Presented, error) {
	if len(raw) > MaxCredentialLen {
		return Presented{}, fmt.Errorf("%w: credential is longer than %d bytes", ErrMalformedKey, MaxCredentialLen)
	}
	if len(raw) != KeyLen {
		return Presented{}, fmt.Errorf("%w: credential is not %d bytes", ErrMalformedKey, KeyLen)
	}
	rest, found := strings.CutPrefix(raw, KeyPrefix)
	if !found {
		return Presented{}, fmt.Errorf("%w: missing the %q marker", ErrMalformedKey, KeyPrefix)
	}

	prefix, secret, found := strings.Cut(rest, separator)
	if !found {
		return Presented{}, fmt.Errorf("%w: missing the segment separator", ErrMalformedKey)
	}
	// Cut splits at the FIRST separator, so a value carrying a second one leaves
	// it inside the secret segment, where decodeSegment rejects it: '.' is not a
	// base64url character. The lengths are still checked explicitly, because a
	// segment of the wrong length that decodes cleanly is still not a key.
	if len(prefix) != LookupLen || len(secret) != SecretLen {
		return Presented{}, fmt.Errorf("%w: segment lengths are %d and %d, not %d and %d",
			ErrMalformedKey, len(prefix), len(secret), LookupLen, SecretLen)
	}
	if err := decodeSegment(prefix); err != nil {
		return Presented{}, fmt.Errorf("%w: lookup segment is not base64url", ErrMalformedKey)
	}
	if err := decodeSegment(secret); err != nil {
		return Presented{}, fmt.Errorf("%w: secret segment is not base64url", ErrMalformedKey)
	}
	return Presented{Prefix: prefix, Secret: secret}, nil
}

func decodeSegment(segment string) error {
	_, err := encoding.DecodeString(segment)
	return err
}

// HashSecret is the one place a secret becomes a stored value.
//
// The digest is lowercase hex so it matches the api_keys.secret_hash CHECK
// exactly, and so a stored value is comparable as a fixed-length string rather
// than as a decoded byte slice of uncertain provenance.
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// VerifySecret reports whether secret is the one behind storedHash.
//
// subtle.ConstantTimeCompare, never ==. Both operands are fixed-length hex
// digests, so the comparison leaks nothing through length, and a constant-time
// compare closes the remaining channel: with == an attacker who knows a valid
// prefix could learn how many leading digest characters a guess got right and
// walk the digest one character at a time.
//
// A stored value of the wrong length can only come from a row that bypassed the
// schema CHECK, and is treated as no match rather than as a shorter comparison.
func VerifySecret(storedHash, secret string) bool {
	computed := HashSecret(secret)
	if len(storedHash) != len(computed) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(storedHash), []byte(computed)) == 1
}
