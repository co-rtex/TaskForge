package results

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClassify_BoundaryIsInclusiveOnTheObjectSide is the one test
// PROJECT_SPEC.md item 13 and ROADMAP.md's M5C acceptance criterion actually
// ask for: the threshold is explicit and tested on both sides of the
// boundary. A body exactly at the threshold is already "at or above" it, so
// it classifies as an object, not inline -- matching migrations/0016_results.sql's
// own comment on what the threshold means.
func TestClassify_BoundaryIsInclusiveOnTheObjectSide(t *testing.T) {
	const threshold = 100

	require.Equal(t, LocationInline, Classify(make([]byte, threshold-1), threshold),
		"one byte under the threshold must stay inline")
	require.Equal(t, LocationObject, Classify(make([]byte, threshold), threshold),
		"exactly at the threshold must already be an object")
	require.Equal(t, LocationObject, Classify(make([]byte, threshold+1), threshold),
		"one byte over the threshold must be an object")
}

func TestClassify_EmptyBodyIsInline(t *testing.T) {
	require.Equal(t, LocationInline, Classify(nil, 1))
	require.Equal(t, LocationInline, Classify([]byte{}, 1))
}

// TestChecksumSHA256_MatchesAnIndependentlyComputedVector pins the hash to a
// vector verified independently against sha256sum, the same discipline
// M5A's key-generation tests used for their own hash vector: a change to
// what is hashed, or to the encoding, fails here rather than silently
// producing a checksum nothing else agrees with.
func TestChecksumSHA256_MatchesAnIndependentlyComputedVector(t *testing.T) {
	require.Equal(t,
		"b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
		ChecksumSHA256([]byte("hello world")))
}

func TestResultValidate_AcceptsExactlyTheTwoWellFormedShapes(t *testing.T) {
	require.NoError(t, Result{
		Location: LocationInline, InlineBody: json.RawMessage(`{"ok":true}`), SizeBytes: 11,
	}.Validate())

	require.NoError(t, Result{
		Location: LocationObject, ObjectBucket: "taskforge-results", ObjectKey: "results/s/j/a",
		SizeBytes: 100_000, ChecksumSHA256: ChecksumSHA256([]byte("large")),
	}.Validate())
}

// TestResultValidate_RejectsEveryMalformedShape enforces in Go the identical
// shape the results_location_shape CHECK constraint enforces in the schema
// (AGENTS.md section 6: bounds are checked twice). Each case differs from a
// valid control by exactly one field, so a passing rejection is attributable
// to that field rather than to something else being wrong.
func TestResultValidate_RejectsEveryMalformedShape(t *testing.T) {
	validChecksum := ChecksumSHA256([]byte("x"))
	cases := map[string]Result{
		"inline with no body": {
			Location: LocationInline, SizeBytes: 0,
		},
		"inline carrying an object bucket": {
			Location: LocationInline, InlineBody: json.RawMessage(`{}`),
			ObjectBucket: "b", SizeBytes: 2,
		},
		"inline carrying an object key": {
			Location: LocationInline, InlineBody: json.RawMessage(`{}`),
			ObjectKey: "k", SizeBytes: 2,
		},
		"inline carrying a checksum": {
			Location: LocationInline, InlineBody: json.RawMessage(`{}`),
			ChecksumSHA256: validChecksum, SizeBytes: 2,
		},
		"object with no bucket": {
			Location: LocationObject, ObjectKey: "k", ChecksumSHA256: validChecksum, SizeBytes: 1,
		},
		"object with no key": {
			Location: LocationObject, ObjectBucket: "b", ChecksumSHA256: validChecksum, SizeBytes: 1,
		},
		"object with no checksum": {
			Location: LocationObject, ObjectBucket: "b", ObjectKey: "k", SizeBytes: 1,
		},
		"object carrying an inline body": {
			Location: LocationObject, ObjectBucket: "b", ObjectKey: "k",
			ChecksumSHA256: validChecksum, InlineBody: json.RawMessage(`{}`), SizeBytes: 1,
		},
		"unknown location": {
			Location: "elsewhere", InlineBody: json.RawMessage(`{}`), SizeBytes: 1,
		},
		"negative size": {
			Location: LocationInline, InlineBody: json.RawMessage(`{}`), SizeBytes: -1,
		},
	}
	for name, result := range cases {
		require.Errorf(t, result.Validate(), "%s should be rejected", name)
	}
}
