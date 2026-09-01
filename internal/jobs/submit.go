package jobs

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Submission defaults and bounds. Every one of these is also enforced by a
// database CHECK constraint, so a bug in this file cannot corrupt the store.
const (
	DefaultPriority       = 50
	DefaultMaxAttempts    = 3
	DefaultTimeoutSeconds = 300

	MinPriority = 0
	MaxPriority = 100

	MinMaxAttempts = 1
	MaxMaxAttempts = 100

	MinTimeoutSeconds = 1
	MaxTimeoutSeconds = 86400

	MaxCapabilities      = 16
	MaxIdempotencyKeyLen = 255

	// fingerprintVersion is mixed into every fingerprint. Changing how requests
	// are canonicalized must change this, or old and new fingerprints would be
	// compared as if they meant the same thing.
	fingerprintVersion = "taskforge.fingerprint.v1"

	// scheduledAtFingerprintTag separates the delayed-submission component from
	// everything before it. See NormalizedRequest.Fingerprint for why the
	// component is appended rather than always written.
	scheduledAtFingerprintTag = "taskforge.fingerprint.scheduled_at"
)

var (
	queueNamePattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	jobTypePattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	capabilityPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
)

// SubmitRequest is the client-supplied definition of a job.
//
// Pointer fields distinguish "absent" from "explicitly zero", which matters:
// priority 0 is a legal value, and an omitted scheduled_at must stay equivalent
// to an explicit null rather than becoming a different request.
type SubmitRequest struct {
	Queue                string          `json:"queue"`
	Type                 string          `json:"job_type"`
	Payload              json.RawMessage `json:"payload"`
	Priority             *int            `json:"priority"`
	MaxAttempts          *int            `json:"max_attempts"`
	TimeoutSeconds       *int            `json:"timeout_seconds"`
	ScheduledAt          *string         `json:"scheduled_at"`
	RequiredCapabilities []string        `json:"required_capabilities"`
}

// FieldError names one thing wrong with a request.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError carries every problem found in a request, so a caller can fix
// them all at once instead of one round trip per mistake.
type ValidationError struct {
	Code   string
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for _, f := range e.Fields {
		parts = append(parts, f.Field+": "+f.Message)
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// NormalizedRequest is a validated request with defaults applied and
// capabilities reduced to a sorted, deduplicated set. It is what gets persisted
// and what gets fingerprinted, so identical intent always hashes identically.
type NormalizedRequest struct {
	Queue                string
	Type                 string
	Payload              []byte // canonical JSON
	Priority             int
	MaxAttempts          int
	TimeoutSeconds       int
	RequiredCapabilities []string
	// ScheduledAt is nil for an immediate submission and otherwise the requested
	// instant converted to UTC. Two requests naming the same instant in
	// different offsets normalize to the same value here, and therefore to the
	// same fingerprint.
	ScheduledAt *time.Time
}

// Normalize validates a submission and applies defaults.
//
// The returned error is always a *ValidationError when the request is bad, so
// the HTTP layer can render field-level detail without inspecting strings.
func (r SubmitRequest) Normalize() (NormalizedRequest, error) {
	verr := &ValidationError{Code: "validation_failed"}
	add := func(field, msg string) { verr.Fields = append(verr.Fields, FieldError{Field: field, Message: msg}) }

	out := NormalizedRequest{
		Queue: strings.TrimSpace(r.Queue),
		Type:  strings.TrimSpace(r.Type),
	}

	if out.Queue == "" {
		add("queue", "is required")
	} else if !queueNamePattern.MatchString(out.Queue) {
		add("queue", "must match ^[a-z0-9][a-z0-9._-]{0,63}$")
	}

	if out.Type == "" {
		add("job_type", "is required")
	} else if !jobTypePattern.MatchString(out.Type) {
		add("job_type", "must match ^[a-z0-9][a-z0-9._-]{0,127}$")
	}

	switch {
	case len(r.Payload) == 0:
		add("payload", "is required")
	default:
		canonical, err := CanonicalJSON(r.Payload)
		if err != nil {
			add("payload", "must be valid JSON: "+err.Error())
		} else if len(canonical) == 0 || canonical[0] != '{' {
			add("payload", "must be a JSON object")
		} else {
			out.Payload = canonical
		}
	}

	out.Priority = DefaultPriority
	if r.Priority != nil {
		out.Priority = *r.Priority
		if out.Priority < MinPriority || out.Priority > MaxPriority {
			add("priority", fmt.Sprintf("must be between %d and %d", MinPriority, MaxPriority))
		}
	}

	out.MaxAttempts = DefaultMaxAttempts
	if r.MaxAttempts != nil {
		out.MaxAttempts = *r.MaxAttempts
		if out.MaxAttempts < MinMaxAttempts || out.MaxAttempts > MaxMaxAttempts {
			add("max_attempts", fmt.Sprintf("must be between %d and %d (it counts the first attempt)", MinMaxAttempts, MaxMaxAttempts))
		}
	}

	out.TimeoutSeconds = DefaultTimeoutSeconds
	if r.TimeoutSeconds != nil {
		out.TimeoutSeconds = *r.TimeoutSeconds
		if out.TimeoutSeconds < MinTimeoutSeconds || out.TimeoutSeconds > MaxTimeoutSeconds {
			add("timeout_seconds", fmt.Sprintf("must be between %d and %d", MinTimeoutSeconds, MaxTimeoutSeconds))
		}
	}

	// Delayed execution. An omitted field and an explicit null stay equivalent,
	// so an M1-M3 client that sends `"scheduled_at": null` is unaffected.
	if r.ScheduledAt != nil {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*r.ScheduledAt))
		if err != nil {
			add("scheduled_at", "must be an RFC 3339 timestamp, for example 2026-09-01T12:00:00Z")
		} else {
			// Canonicalized to UTC immediately, so persistence, comparison
			// against PostgreSQL time, and fingerprinting all see one
			// representation of the instant rather than whichever offset the
			// caller happened to write it in.
			utc := parsed.UTC()
			out.ScheduledAt = &utc
		}
	}

	out.RequiredCapabilities = normalizeCapabilities(r.RequiredCapabilities, add)

	if len(verr.Fields) > 0 {
		return NormalizedRequest{}, verr
	}
	return out, nil
}

// normalizeCapabilities treats capabilities as a set: sorted and deduplicated so
// that ["cpu","gpu"] and ["gpu","cpu","gpu"] describe the same requirement and
// therefore produce the same fingerprint.
func normalizeCapabilities(in []string, add func(field, msg string)) []string {
	if len(in) > MaxCapabilities {
		add("required_capabilities", fmt.Sprintf("must contain at most %d entries", MaxCapabilities))
		return []string{}
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for i, c := range in {
		c = strings.TrimSpace(c)
		if !capabilityPattern.MatchString(c) {
			add(fmt.Sprintf("required_capabilities[%d]", i), "must match ^[a-z0-9][a-z0-9._-]{0,63}$")
			continue
		}
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// ValidateIdempotencyKey enforces the shape of the Idempotency-Key header.
// Control characters are rejected so a key can never corrupt a log line.
func ValidateIdempotencyKey(key string) error {
	verr := &ValidationError{Code: "validation_failed"}
	switch {
	case strings.TrimSpace(key) == "":
		verr.Fields = append(verr.Fields, FieldError{Field: "Idempotency-Key", Message: "header is required"})
	case len(key) > MaxIdempotencyKeyLen:
		verr.Fields = append(verr.Fields, FieldError{
			Field:   "Idempotency-Key",
			Message: fmt.Sprintf("must be at most %d bytes", MaxIdempotencyKeyLen),
		})
	default:
		for _, r := range key {
			if r < 0x20 || r == 0x7f {
				verr.Fields = append(verr.Fields, FieldError{
					Field:   "Idempotency-Key",
					Message: "must not contain control characters",
				})
				break
			}
		}
	}
	if len(verr.Fields) > 0 {
		return verr
	}
	return nil
}

// Fingerprint hashes every job-defining field of a normalized request.
//
// Fields are length-prefixed before hashing so that no combination of values can
// be rearranged into the same byte stream — without it, queue "a" + type "bc"
// and queue "ab" + type "c" would collide.
//
// The scheduling component is APPENDED only when a schedule was requested, and
// that is deliberate rather than tidy. An immediate submission must hash to
// exactly the byte stream M1 through M3 produced, or every idempotency key
// recorded before this milestone would answer 409 instead of replaying its
// original job the first time a client retried after the upgrade. Absent
// produces the original stream; present produces a strictly longer one carrying
// its own domain tag, so the two can never collide.
//
// Two requests naming the same instant in different offsets normalized to the
// same UTC value above, so they hash identically here.
func (n NormalizedRequest) Fingerprint() string {
	h := sha256.New()
	write := func(b []byte) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(b)))
		h.Write(length[:])
		h.Write(b)
	}

	write([]byte(fingerprintVersion))
	write([]byte(n.Queue))
	write([]byte(n.Type))
	write(n.Payload)
	write([]byte(strconv.Itoa(n.Priority)))
	write([]byte(strconv.Itoa(n.MaxAttempts)))
	write([]byte(strconv.Itoa(n.TimeoutSeconds)))
	write([]byte(strconv.Itoa(len(n.RequiredCapabilities))))
	for _, c := range n.RequiredCapabilities {
		write([]byte(c))
	}
	if n.ScheduledAt != nil {
		write([]byte(scheduledAtFingerprintTag))
		write([]byte(n.ScheduledAt.UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(h.Sum(nil))
}
