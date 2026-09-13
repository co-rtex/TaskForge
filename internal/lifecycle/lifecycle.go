// Package lifecycle owns the job-lifecycle decisions that must mean the same
// thing everywhere: how an attempt's failure is classified, how long a retry
// waits, what bounded error detail may be persisted, and why a job was
// dead-lettered.
//
// It holds policy, not SQL. The one exception is InsertDLQEntryTx, which exists
// here precisely so that every path reaching DEAD_LETTERED — permanent failure,
// exhausted retryable failure, exhausted timeout, and ADR-0009's exhausted
// abandonment — inserts through the same helper instead of four near-identical
// statements that could drift apart.
package lifecycle

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// FailureClass is how an attempt ended, in retry-policy terms.
//
// The set is closed and is mirrored by a CHECK constraint on
// job_attempts.failure_class.
type FailureClass string

const (
	// ClassRetryable means the attempt failed and may be retried if attempt
	// budget remains. Otherwise the job dead-letters as ATTEMPTS_EXHAUSTED.
	ClassRetryable FailureClass = "RETRYABLE"
	// ClassPermanent means the job dead-letters immediately, even with nominal
	// attempt budget remaining. Retrying could not change the outcome.
	ClassPermanent FailureClass = "PERMANENT"
	// ClassTimedOut is server-authoritative: the attempt outlived its persisted
	// per-attempt deadline. It retries under the same policy as ClassRetryable.
	ClassTimedOut FailureClass = "TIMED_OUT"
	// ClassCanceled means cancellation won. It never retries and never
	// dead-letters.
	ClassCanceled FailureClass = "CANCELED"
	// ClassAbandoned means the worker lost authority without reporting a durable
	// outcome. ADR-0009 governs it: immediate requeue while budget remains.
	ClassAbandoned FailureClass = "ABANDONED"
)

// Valid reports whether c is a recognized class.
func (c FailureClass) Valid() bool {
	switch c {
	case ClassRetryable, ClassPermanent, ClassTimedOut, ClassCanceled, ClassAbandoned:
		return true
	default:
		return false
	}
}

// ReportableByHandler reports whether a trusted handler may declare this class
// itself. A worker cannot decide that it timed out, was canceled, or was
// abandoned: each of those is owned by the control plane.
func (c FailureClass) ReportableByHandler() bool {
	return c == ClassRetryable || c == ClassPermanent
}

// RetriesUnderPolicy reports whether this class consults the retry policy when
// attempt budget remains.
func (c FailureClass) RetriesUnderPolicy() bool {
	return c == ClassRetryable || c == ClassTimedOut
}

func (c FailureClass) String() string { return string(c) }

// DLQReason is why a job is in the logical dead-letter queue. Mirrored by a
// CHECK constraint on dlq_entries.reason.
type DLQReason string

const (
	// ReasonPermanentFailure: a trusted handler declared the failure permanent,
	// so nominal remaining attempts were deliberately not used.
	ReasonPermanentFailure DLQReason = "PERMANENT_FAILURE"
	// ReasonAttemptsExhausted: the attempt that just ended consumed the total
	// attempt budget, whether it failed, timed out, or was abandoned.
	ReasonAttemptsExhausted DLQReason = "ATTEMPTS_EXHAUSTED"
)

// Valid reports whether r is a recognized reason.
func (r DLQReason) Valid() bool {
	return r == ReasonPermanentFailure || r == ReasonAttemptsExhausted
}

func (r DLQReason) String() string { return string(r) }

// Bounds on the failure detail TaskForge is willing to store and return.
//
// These are deliberately small. Failure detail is written by a handler and read
// by an operator, so it is a channel out of the process: an unbounded one would
// carry payload fragments, driver text, and stack traces into the database, the
// API, and the logs. Both halves are enforced — here before the write, and by
// the CHECK constraints migration 0009 added.
const (
	MaxErrorCodeBytes    = 64
	MaxErrorMessageBytes = 512
)

// errorCodePattern is a stable lowercase token: something a client can branch
// on and a dashboard can group by, not a sentence.
var errorCodePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

// Generic codes used when TaskForge itself, not a trusted handler, decides how
// an attempt ended. A handler that returns a plain error or panics gets
// CodeHandlerError: its raw text is never stored, logged, or returned.
const (
	CodeHandlerError = "handler_error"
	CodeTimeout      = "attempt_timeout"
	CodeCanceled     = "canceled"
	CodeAbandoned    = "attempt_abandoned"
)

// Generic safe messages paired with the codes above. They describe what
// happened without quoting anything the handler produced.
const (
	MessageHandlerError = "the trusted handler reported an error"
	MessageTimeout      = "the attempt exceeded its per-attempt execution deadline"
	MessageCanceled     = "the job was canceled"
	MessageAbandoned    = "the worker lost authority without reporting an outcome"
)

// ValidateErrorCode enforces the stable-token contract.
func ValidateErrorCode(code string) error {
	if code == "" {
		return fmt.Errorf("error code is required")
	}
	if len(code) > MaxErrorCodeBytes {
		return fmt.Errorf("error code must be at most %d bytes", MaxErrorCodeBytes)
	}
	if !errorCodePattern.MatchString(code) {
		return fmt.Errorf("error code must match %s", errorCodePattern.String())
	}
	return nil
}

// ValidateErrorMessage enforces the bounded, single-line, control-character-free
// contract. An empty message is allowed: the code alone is a complete answer.
func ValidateErrorMessage(message string) error {
	if len(message) > MaxErrorMessageBytes {
		return fmt.Errorf("error message must be at most %d bytes", MaxErrorMessageBytes)
	}
	for _, r := range message {
		if r == '\n' || r == '\r' {
			return fmt.Errorf("error message must not contain line breaks")
		}
		if unicode.IsControl(r) {
			return fmt.Errorf("error message must not contain control characters")
		}
	}
	return nil
}

// SafeMessage coerces an already-trusted message into the stored bounds.
//
// It never invents content and never reads a raw error: callers hand it a
// message a trusted handler declared safe, or one of the generic constants
// above. Truncation is on a rune boundary so the stored value is always valid
// UTF-8.
func SafeMessage(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	cleaned := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, message)
	cleaned = strings.Join(strings.Fields(cleaned), " ")
	if len(cleaned) <= MaxErrorMessageBytes {
		return cleaned
	}
	truncated := cleaned[:MaxErrorMessageBytes]
	for len(truncated) > 0 && !isRuneStart(cleaned, len(truncated)) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}

func isRuneStart(s string, i int) bool {
	return i >= len(s) || s[i]&0xC0 != 0x80
}
