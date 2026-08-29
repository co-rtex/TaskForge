package jobs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalJSON_NormalizesRepresentation(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
	}{
		{"key order", `{"b":1,"a":2}`, `{"a":2,"b":1}`},
		{"whitespace", `{"a":  1 ,  "b":2}`, `{"a":1,"b":2}`},
		{"nested key order", `{"o":{"z":1,"a":2}}`, `{"o":{"a":2,"z":1}}`},
		{"deep nesting", `{"a":[{"y":1,"x":2}]}`, `{"a":[{"x":2,"y":1}]}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := CanonicalJSON([]byte(tc.a))
			require.NoError(t, err)
			b, err := CanonicalJSON([]byte(tc.b))
			require.NoError(t, err)
			require.Equal(t, string(a), string(b))
		})
	}
}

func TestCanonicalJSON_PreservesMeaningfulDifferences(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
	}{
		{"array order is significant", `{"a":[1,2]}`, `{"a":[2,1]}`},
		{"different values", `{"a":1}`, `{"a":2}`},
		{"different keys", `{"a":1}`, `{"b":1}`},
		// 1 and 1.0 are the same number but not the same thing the caller wrote.
		// Treating them as different produces a conflict rather than a silent
		// replay of a different request, which is the safe direction.
		{"numeric spelling", `{"a":1}`, `{"a":1.0}`},
		{"null vs absent", `{"a":null}`, `{}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := CanonicalJSON([]byte(tc.a))
			require.NoError(t, err)
			b, err := CanonicalJSON([]byte(tc.b))
			require.NoError(t, err)
			require.NotEqual(t, string(a), string(b))
		})
	}
}

func TestCanonicalJSON_PreservesLargeIntegerPrecision(t *testing.T) {
	// Decoding into float64 would round this to 9007199254740993 -> ...92.
	const big = `{"n":9007199254740993}`
	out, err := CanonicalJSON([]byte(big))
	require.NoError(t, err)
	require.Equal(t, `{"n":9007199254740993}`, string(out))
}

func TestCanonicalJSON_DoesNotHTMLEscape(t *testing.T) {
	// Fingerprints must not depend on whether a payload happens to contain "<".
	out, err := CanonicalJSON([]byte(`{"html":"<a href='x'>&</a>"}`))
	require.NoError(t, err)
	require.Equal(t, `{"html":"<a href='x'>&</a>"}`, string(out))
}

func TestCanonicalJSON_RejectsInvalidInput(t *testing.T) {
	for _, in := range []string{``, `{`, `{"a":}`, `not json`, `{"a":1} {"b":2}`, `{"a":1}trailing`} {
		t.Run(in, func(t *testing.T) {
			_, err := CanonicalJSON([]byte(in))
			require.Error(t, err)
		})
	}
}
