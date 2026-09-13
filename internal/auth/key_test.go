package auth

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// repeatingReader is a deterministic entropy source. Key generation must be
// reproducible in a test so the encoded shape can be pinned to exact bytes
// rather than to whatever base64url happened to produce (AGENTS.md section 5).
type repeatingReader struct{ b byte }

func (r repeatingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
	}
	return len(p), nil
}

// TestGenerateMaterial_HasTheDocumentedShapeAndLengths pins the wire format.
//
// The lengths are load-bearing in three separate places: the api_keys.prefix
// CHECK bound, the fixed-length rejection in ParseKey, and every client that
// validates a key before sending it. A change to either entropy constant that
// moved an encoded length would break all three, and this is what makes that
// visible as a failing assertion rather than as a migration that suddenly
// rejects live rows.
func TestGenerateMaterial_HasTheDocumentedShapeAndLengths(t *testing.T) {
	material, err := GenerateMaterial(repeatingReader{b: 0xAB})
	require.NoError(t, err)

	require.Len(t, material.Prefix, LookupLen)
	require.Equal(t, 22, LookupLen, "16 random bytes encode to 22 unpadded base64url characters")
	require.Len(t, material.Secret, SecretLen)
	require.Equal(t, 43, SecretLen, "32 random bytes encode to 43 unpadded base64url characters")
	require.Len(t, material.SecretHash, 64)
	require.Equal(t, 70, KeyLen)

	raw := material.Raw()
	require.Len(t, raw, KeyLen)
	require.True(t, strings.HasPrefix(raw, KeyPrefix))
	require.Equal(t, raw, KeyPrefix+material.Prefix+"."+material.Secret)

	// No padding and no character that would need quoting in a log line, a URL,
	// or a shell argument.
	require.NotContains(t, raw, "=")
	for _, r := range raw {
		require.Truef(t,
			r == '_' || r == '-' || r == '.' || (r >= 'a' && r <= 'z') ||
				(r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'),
			"unexpected character %q in a generated key", r)
	}

	// The separator must not be a character either segment can contain.
	// base64url's alphabet includes '-' and '_', so splitting on '_' would cut
	// at whichever underscore the random lookup segment happened to hold.
	require.Equal(t, ".", separator)
	require.NotContains(t, material.Prefix, separator)
	require.NotContains(t, material.Secret, separator)
	require.Equal(t, 1, strings.Count(raw, separator),
		"exactly one separator may appear in a key")

	// The prefix bound the schema enforces must actually admit what this
	// generator produces. A generator and a CHECK that disagree would fail at
	// the first INSERT, in production, not here.
	require.GreaterOrEqual(t, len(material.Prefix), 8)
	require.LessOrEqual(t, len(material.Prefix), 32)
}

// A generated key must be parseable by the exact function that authenticates it.
// Generation and parsing agreeing is not obvious: they are separate code paths
// that only meet at runtime.
//
// This runs many keys rather than one on purpose. The defect it exists to catch
// is a separator that collides with the segment alphabet, which fails on only
// the fraction of keys whose random segment happens to contain that character —
// a single sample passes roughly three times in four and proves nothing.
func TestGenerateMaterial_RoundTripsThroughParseKey(t *testing.T) {
	for i := 0; i < 500; i++ {
		material, err := GenerateMaterial(rand.Reader)
		require.NoError(t, err)

		presented, err := ParseKey(material.Raw())
		require.NoErrorf(t, err, "generated key %q did not parse", material.Raw())
		require.Equal(t, material.Prefix, presented.Prefix)
		require.Equal(t, material.Secret, presented.Secret)
		require.True(t, VerifySecret(material.SecretHash, presented.Secret))
	}
}

// A truncated read must be an error, never a shorter key. Silently accepting
// fewer bytes would turn "16 bytes of entropy" into an intention.
func TestGenerateMaterial_RejectsAShortEntropyRead(t *testing.T) {
	for name, source := range map[string]int{
		"no entropy at all":        0,
		"enough for lookup only":   LookupBytes,
		"one byte short of secret": LookupBytes + SecretBytes - 1,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := GenerateMaterial(bytes.NewReader(make([]byte, source)))
			require.Error(t, err)
		})
	}
}

// Two calls must not produce the same credential. This is the one property a
// deterministic test reader cannot check, so it is checked against the real
// source.
func TestGenerateMaterial_IsDistinctAcrossCalls(t *testing.T) {
	seen := make(map[string]struct{}, 64)
	for i := 0; i < 64; i++ {
		material, err := GenerateMaterial(rand.Reader)
		require.NoError(t, err)
		_, duplicate := seen[material.Prefix]
		require.False(t, duplicate, "two generated keys shared a lookup prefix")
		seen[material.Prefix] = struct{}{}
	}
}

// TestParseKey_RejectsEveryMalformedShape is the boundary that keeps an
// unauthenticated caller from turning a header into database load: everything
// listed here is rejected in memory, before any lookup.
func TestParseKey_RejectsEveryMalformedShape(t *testing.T) {
	valid, err := GenerateMaterial(rand.Reader)
	require.NoError(t, err)
	raw := valid.Raw()

	for name, candidate := range map[string]string{
		"empty":                    "",
		"whitespace":               strings.Repeat(" ", KeyLen),
		"missing marker":           valid.Prefix + "." + valid.Secret,
		"wrong marker":             "tfx_" + valid.Prefix + "." + valid.Secret,
		"marker only":              KeyPrefix,
		"no separator":             KeyPrefix + valid.Prefix + valid.Secret + "x",
		"truncated by one":         raw[:len(raw)-1],
		"extended by one":          raw + "x",
		"lookup one short":         KeyPrefix + valid.Prefix[:LookupLen-1] + "." + valid.Secret + "x",
		"secret one short":         KeyPrefix + valid.Prefix + "x." + valid.Secret[:SecretLen-1],
		"non base64url in lookup":  KeyPrefix + "!" + valid.Prefix[1:] + "." + valid.Secret,
		"non base64url in secret":  KeyPrefix + valid.Prefix + "." + "!" + valid.Secret[1:],
		"extra separator":          KeyPrefix + valid.Prefix + "." + "." + valid.Secret[1:],
		"underscore as separator":  KeyPrefix + valid.Prefix + "_" + valid.Secret,
		"padding character":        KeyPrefix + valid.Prefix + "." + "=" + valid.Secret[1:],
		"oversized":                strings.Repeat("a", MaxCredentialLen+1),
		"a bearer header verbatim": "Bearer " + raw,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseKey(candidate)
			require.ErrorIs(t, err, ErrMalformedKey)
		})
	}

	t.Run("the valid key is accepted", func(t *testing.T) {
		presented, err := ParseKey(raw)
		require.NoError(t, err)
		require.Equal(t, valid.Prefix, presented.Prefix)
	})
}

// An oversized credential must be discarded on length alone, before it is split,
// decoded, or compared. Without this bound a caller could force work proportional
// to whatever they were willing to send.
func TestParseKey_RejectsAnOversizedCredentialBeforeAnythingElse(t *testing.T) {
	_, err := ParseKey(KeyPrefix + strings.Repeat("a", MaxCredentialLen))
	require.ErrorIs(t, err, ErrMalformedKey)
	require.Contains(t, err.Error(), "longer than")
}

// The stored shape must match the api_keys.secret_hash CHECK exactly:
// '^[0-9a-f]{64}$'. A digest that is uppercase, or encoded differently, would be
// rejected by the database at INSERT time rather than here.
func TestHashSecret_IsLowercaseHexAndStable(t *testing.T) {
	const secret = "a-fixed-secret-segment"
	first := HashSecret(secret)

	require.Len(t, first, 64)
	require.Equal(t, strings.ToLower(first), first)
	_, err := hex.DecodeString(first)
	require.NoError(t, err)
	require.Equal(t, first, HashSecret(secret), "hashing must be stable across calls")
	require.NotEqual(t, first, HashSecret(secret+"x"))

	// A pinned vector, so a future change of hash function or of what exactly is
	// hashed fails here instead of silently invalidating every stored key.
	require.Equal(t,
		"746d69a7982d6992d6cd53392559be03b53f4b98f2ab2f7f3760c4bac3c8fd51",
		HashSecret("taskforge-m5a-pinned-vector"))
}

func TestVerifySecret(t *testing.T) {
	material, err := GenerateMaterial(rand.Reader)
	require.NoError(t, err)

	t.Run("accepts the secret behind the hash", func(t *testing.T) {
		require.True(t, VerifySecret(material.SecretHash, material.Secret))
	})
	t.Run("rejects a different secret", func(t *testing.T) {
		other, err := GenerateMaterial(rand.Reader)
		require.NoError(t, err)
		require.False(t, VerifySecret(material.SecretHash, other.Secret))
	})
	t.Run("rejects an empty secret", func(t *testing.T) {
		require.False(t, VerifySecret(material.SecretHash, ""))
	})
	t.Run("rejects a stored hash of the wrong length", func(t *testing.T) {
		// Only reachable from a row that bypassed the schema CHECK. It must be
		// no match, never a shorter comparison that a prefix could satisfy.
		require.False(t, VerifySecret(material.SecretHash[:32], material.Secret))
		require.False(t, VerifySecret("", material.Secret))
	})
	t.Run("rejects a hash differing in only one character", func(t *testing.T) {
		for _, position := range []int{0, 31, 63} {
			mutated := []byte(material.SecretHash)
			if mutated[position] == '0' {
				mutated[position] = '1'
			} else {
				mutated[position] = '0'
			}
			require.Falsef(t, VerifySecret(string(mutated), material.Secret),
				"a hash differing at position %d must not verify", position)
		}
	})
}

// TestVerifySecret_ComparesInConstantTime guards the one property that cannot be
// observed from behavior.
//
// A timing assertion would be flaky and would prove nothing on a loaded runner,
// so this reads the implementation instead: VerifySecret must call
// subtle.ConstantTimeCompare, and must never compare the two digests with == or
// !=. The second half is the one that matters — the refactor this is here to
// stop is someone "simplifying" the call into a string comparison, which reads
// as equivalent and hands an attacker who knows a valid prefix a way to walk the
// stored digest one character at a time.
func TestVerifySecret_ComparesInConstantTime(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "key.go", nil, 0)
	require.NoError(t, err)

	var verify *ast.FuncDecl
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == "VerifySecret" {
			verify = function
			break
		}
	}
	require.NotNil(t, verify, "VerifySecret is not declared in key.go")

	var callsConstantTime bool
	var comparesDigests bool
	ast.Inspect(verify, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			selector, ok := typed.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if ok && pkg.Name == "subtle" && selector.Sel.Name == "ConstantTimeCompare" {
				callsConstantTime = true
			}
		case *ast.BinaryExpr:
			if typed.Op != token.EQL && typed.Op != token.NEQ {
				return true
			}
			// len(a) != len(b) is fine and is not a digest comparison.
			if isDigestIdent(typed.X) || isDigestIdent(typed.Y) {
				comparesDigests = true
			}
		}
		return true
	})

	require.True(t, callsConstantTime, "VerifySecret must compare with subtle.ConstantTimeCompare")
	require.False(t, comparesDigests, "VerifySecret must never compare the digests with == or !=")
}

func isDigestIdent(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)
	if !ok {
		return false
	}
	return identifier.Name == "storedHash" || identifier.Name == "computed"
}
