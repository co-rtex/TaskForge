// Package results owns the results table: a job's stored outcome, small
// enough to sit inline in PostgreSQL or large enough to live in an
// S3-compatible object store. It knows nothing about internal/objectstore's
// client or internal/workers' fencing -- see docs/adr/0015-result-storage.md
// for the package-boundary rule this keeps.
package results

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Location is where one result's bytes actually live.
type Location string

const (
	LocationInline Location = "inline"
	LocationObject Location = "object"
)

// Classify decides a result's storage location from its size alone. A body
// at or above thresholdBytes is too large to keep inline; everything smaller
// is. It takes no dependency on PostgreSQL or the object store, so the
// threshold boundary is a plain, fast unit test.
func Classify(body []byte, thresholdBytes int) Location {
	if len(body) >= thresholdBytes {
		return LocationObject
	}
	return LocationInline
}

// ChecksumSHA256 hashes body as lowercase hex, matching the
// results.checksum_sha256 CHECK constraint's shape. It exists so
// internal/worker/runner.go and any test computing an expected value share
// one implementation of what "the checksum" means.
func ChecksumSHA256(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// Result is one job's recorded outcome -- at most one row per job, keyed by
// job id. See migrations/0016_results.sql for why.
type Result struct {
	JobID     uuid.UUID
	AttemptID uuid.UUID
	Scope     string
	Location  Location
	// InlineBody is set only when Location is LocationInline.
	InlineBody json.RawMessage
	// ObjectBucket, ObjectKey, and ChecksumSHA256 are set only when Location
	// is LocationObject.
	ObjectBucket   string
	ObjectKey      string
	SizeBytes      int64
	ChecksumSHA256 string
	ContentType    string
	CreatedAt      time.Time
}

// ErrResultNotFound reports that no result is recorded for a job in scope --
// because the job does not exist, belongs to a different scope, or has not
// (yet) succeeded with a recorded result. These are deliberately
// indistinguishable to a caller, for the same anti-oracle reason
// GET /v1/jobs/{job_id} answers 404 rather than 400 for a malformed id.
var ErrResultNotFound = errors.New("result not found")

// errInvalidResult is wrapped by every validation failure InsertResultTx can
// report, so a caller can recognize "this Result is malformed" as a class
// distinct from a database failure without matching on message text.
var errInvalidResult = errors.New("invalid result")

// Validate reports whether result's location and its populated fields agree,
// enforcing in Go the identical shape the results_location_shape CHECK
// constraint enforces in the schema (AGENTS.md section 6: bounds are checked
// twice, in Go before the write and by the schema itself).
func (result Result) Validate() error {
	switch result.Location {
	case LocationInline:
		if len(result.InlineBody) == 0 {
			return fmt.Errorf("%w: inline result requires a body", errInvalidResult)
		}
		if result.ObjectBucket != "" || result.ObjectKey != "" || result.ChecksumSHA256 != "" {
			return fmt.Errorf("%w: inline result must not carry object location fields", errInvalidResult)
		}
	case LocationObject:
		if result.InlineBody != nil {
			return fmt.Errorf("%w: object result must not carry an inline body", errInvalidResult)
		}
		if result.ObjectBucket == "" || result.ObjectKey == "" {
			return fmt.Errorf("%w: object result requires a bucket and a key", errInvalidResult)
		}
		if result.ChecksumSHA256 == "" {
			return fmt.Errorf("%w: object result requires a checksum", errInvalidResult)
		}
	default:
		return fmt.Errorf("%w: unknown location %q", errInvalidResult, result.Location)
	}
	if result.SizeBytes < 0 {
		return fmt.Errorf("%w: size must not be negative", errInvalidResult)
	}
	return nil
}
