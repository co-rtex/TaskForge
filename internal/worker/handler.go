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
