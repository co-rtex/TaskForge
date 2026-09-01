// Package worker implements the DB-less bounded worker runtime. It treats broker
// messages as advisory wakeups, obtains authoritative assignments from the API,
// and executes only handlers compiled into its registry.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
)

// Execution supplies stable identifiers so a handler with external side
// effects can implement application-level idempotency.
type Execution struct {
	JobID     uuid.UUID
	AttemptID uuid.UUID
	Payload   json.RawMessage
}

// Handler is trusted code compiled into taskforge-worker. TaskForge never
// executes uploaded source, shell commands, containers, or dynamic plugins.
type Handler interface {
	Execute(context.Context, Execution) (json.RawMessage, error)
}

// FailureError lets a trusted handler declare how its failure should be
// classified, together with a stable code and a message it asserts is safe to
// store and return.
//
// This is the ONLY way a handler influences classification. A plain error, a
// wrapped dependency error, and a recovered panic all become a generic retryable
// failure whose raw text is neither stored, returned, nor logged — because that
// text is exactly where payload fragments, credentials, driver output, and stack
// traces reliably appear.
//
// Even a declared classification is bounded: TIMED_OUT, CANCELED, and ABANDONED
// are server-authoritative, so a handler cannot claim them, and the control
// plane rejects the attempt if one is presented.
type FailureError struct {
	// Class must be lifecycle.ClassRetryable or lifecycle.ClassPermanent.
	Class lifecycle.FailureClass
	// Code is a stable lowercase token an operator can group by.
	Code string
	// Message is optional prose the handler asserts contains no secret, no
	// payload content, and no unbounded detail. It is bounded again before it is
	// stored.
	Message string
}

func (e *FailureError) Error() string {
	if e.Message == "" {
		return "handler failure: " + e.Code
	}
	return "handler failure: " + e.Code + ": " + e.Message
}

// Retryable declares a failure worth another attempt if attempt budget remains.
func Retryable(code, message string) error {
	return &FailureError{Class: lifecycle.ClassRetryable, Code: code, Message: message}
}

// Permanent declares a failure that another attempt could not fix, so the job
// dead-letters immediately even with nominal attempt budget remaining.
func Permanent(code, message string) error {
	return &FailureError{Class: lifecycle.ClassPermanent, Code: code, Message: message}
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, Execution) (json.RawMessage, error)

func (f HandlerFunc) Execute(ctx context.Context, execution Execution) (json.RawMessage, error) {
	return f(ctx, execution)
}

// Registry is the immutable-at-runtime trusted handler catalog.
type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry { return &Registry{handlers: map[string]Handler{}} }

// Register adds one trusted handler and rejects accidental replacement.
func (r *Registry) Register(jobType string, handler Handler) error {
	if jobType == "" {
		return errors.New("job type is required")
	}
	if handler == nil {
		return fmt.Errorf("handler for %q is nil", jobType)
	}
	if _, exists := r.handlers[jobType]; exists {
		return fmt.Errorf("handler for %q is already registered", jobType)
	}
	r.handlers[jobType] = handler
	return nil
}

// Lookup returns the compiled handler for jobType.
func (r *Registry) Lookup(jobType string) (Handler, bool) {
	handler, ok := r.handlers[jobType]
	return handler, ok
}

// Types returns a deterministic declaration for session registration.
func (r *Registry) Types() []string {
	types := make([]string, 0, len(r.handlers))
	for jobType := range r.handlers {
		types = append(types, jobType)
	}
	sort.Strings(types)
	return types
}

// DemoEcho is M2's single trusted handler. It returns an exact copy of the
// authoritative payload in process. Result persistence is deliberately deferred
// to M5, and the payload/result is never logged.
type DemoEcho struct{}

func (DemoEcho) Execute(_ context.Context, execution Execution) (json.RawMessage, error) {
	result := make(json.RawMessage, len(execution.Payload))
	copy(result, execution.Payload)
	return result, nil
}
