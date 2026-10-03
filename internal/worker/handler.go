// Package worker implements the DB-less bounded worker runtime. It treats broker
// messages as advisory wakeups, obtains authoritative assignments from the API,
// and executes only handlers compiled into its registry.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/jobs"
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

// DemoEcho is M2's first trusted handler. It returns an exact copy of the
// authoritative payload in process. Since M5C, runner.go classifies and
// records that returned copy as the attempt's result; the payload/result
// itself is still never logged.
type DemoEcho struct{}

func (DemoEcho) Execute(_ context.Context, execution Execution) (json.RawMessage, error) {
	result := make(json.RawMessage, len(execution.Payload))
	copy(result, execution.Payload)
	return result, nil
}

// Stable codes the demonstration handlers report. Both match the pattern
// api/openapi.yaml documents for a worker-reported error_code, which a test
// reads from the document rather than from a copy.
const (
	// demoFailureCode is the one code demo.fail ever reports, whatever its
	// payload says. The caller chooses the failure's class, never any part of its
	// code or message, so no payload text can reach the dead-letter queue.
	demoFailureCode = "demo_failure"
	// invalidPayloadCode marks a payload a demonstration handler refused to act
	// on. It is Permanent: the same payload will be refused identically on every
	// retry, so spending the rest of the attempt budget on it would only delay the
	// dead-letter entry that says so.
	invalidPayloadCode = "invalid_payload"
)

// Failure messages are fixed strings. A handler's message is stored and listed,
// so building one from the payload would turn a caller's data into text every
// reader of the dead-letter queue sees (AGENTS.md section 10).
const (
	demoFailRetryableMessage = "demo.fail was asked to fail retryably"
	demoFailPermanentMessage = "demo.fail was asked to fail permanently"
	demoFailPayloadMessage   = `the payload must be exactly {"class": "retryable"} or {"class": "permanent"}`
	demoSleepPayloadMessage  = `the payload must be exactly {"duration_ms": n}, a whole number of milliseconds within the supported range`
)

// maxDemoSleepMillis bounds demo.sleep to the longest a job may be allowed to run
// at all. It is derived from the constant that bounds a job's own
// timeout_seconds, not copied from it: a sleep longer than any attempt budget
// could never finish, so a larger value would only be a payload the system
// accepts and can never complete.
const maxDemoSleepMillis = int64(jobs.MaxTimeoutSeconds) * 1000

// decodeDemoPayload strictly decodes a demonstration handler's payload into its
// members, keyed by their exact names, and returns them undecoded. It rejects a
// payload that is not a single JSON object, one whose key set is not exactly
// wantKeys, a repeated key, and anything after the object, so the accepted
// payloads are exactly the documented shape and nothing a caller could smuggle
// alongside it. The caller then decodes each value.
//
// Strictness matters more here than for a typical handler because these run
// inside the production worker for every scope (ADR-0019): what they accept is
// the whole of what a key holder can ask of them.
//
// Why not decode into a struct, or into a map with json.Unmarshal. encoding/json
// matches a struct field to a key case-insensitively, so {"DURATION_MS": 5}
// would be read as duration_ms; and both it and a map assignment resolve a
// repeated key to its last value, so {"duration_ms": 5, "duration_ms": 6} would
// run as 6 when the first member said 5. Neither is wrong by JSON's rules, but a
// payload that means two things is not one these handlers should act on. The
// object is therefore walked token by token: each key is compared as the exact
// string JSON decodes it to (whitespace and escapes such as \u005f are not part of
// the key), and a key seen twice is an error as it arrives.
func decodeDemoPayload(payload json.RawMessage, wantKeys ...string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))

	opening, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := opening.(json.Delim); !ok || delim != '{' {
		return nil, errors.New("the payload is not a JSON object")
	}

	members := map[string]json.RawMessage{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, errors.New("an object member has no name")
		}
		// Deliberately not echoed: the key is caller data (AGENTS.md section 10).
		if _, repeated := members[key]; repeated {
			return nil, errors.New("a member name is repeated")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		members[key] = value
	}
	if _, err := decoder.Token(); err != nil { // the closing brace
		return nil, err
	}
	// Decode stops after the first value. Anything further is either a second
	// object or garbage, and neither is the documented payload.
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the payload object")
	}

	// Exactly the documented key set: none missing, none extra. Keys are exact
	// strings in the map, so a wrong-case spelling is an extra key here and its
	// correctly spelled twin, if absent, is a missing one.
	if len(members) != len(wantKeys) {
		return nil, errors.New("the payload does not have exactly the documented members")
	}
	for _, key := range wantKeys {
		if _, present := members[key]; !present {
			return nil, errors.New("the payload does not have exactly the documented members")
		}
	}
	return members, nil
}

// DemoSleep waits for the requested number of milliseconds and reports how long
// it slept. It exists so a failure demonstration has a job that is still
// RUNNING when a worker is killed or frozen; see ADR-0019 for why it is a
// production handler.
//
// It waits on a timer inside a select with the context, so cancellation, an
// attempt deadline, and loss of lease authority all end it at once rather than
// when the timer happens to fire. The runner decides what each of those means
// from the cause it recorded on the context, not from the error returned here.
type DemoSleep struct{}

func (DemoSleep) Execute(ctx context.Context, execution Execution) (json.RawMessage, error) {
	members, err := decodeDemoPayload(execution.Payload, "duration_ms")
	if err != nil {
		return nil, Permanent(invalidPayloadCode, demoSleepPayloadMessage)
	}
	// A pointer so a null is distinguishable from zero, which is itself out of
	// range. A value of the wrong type, or one past int64, fails to decode.
	var durationMS *int64
	if err := json.Unmarshal(members["duration_ms"], &durationMS); err != nil ||
		durationMS == nil || *durationMS < 1 || *durationMS > maxDemoSleepMillis {
		return nil, Permanent(invalidPayloadCode, demoSleepPayloadMessage)
	}

	timer := time.NewTimer(time.Duration(*durationMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return json.Marshal(struct {
		SleptMS int64 `json:"slept_ms"`
	}{SleptMS: *durationMS})
}

// DemoFail fails every time, in the class its payload names. It exists so the
// retry and dead-letter paths can be shown with a real worker rather than only
// asserted with a test-injected one; see ADR-0019.
//
// The payload picks one of two classes and nothing else: the code is fixed and
// the message is a fixed string per class, so no part of what the caller sent is
// stored, listed, or logged as a failure.
type DemoFail struct{}

func (DemoFail) Execute(_ context.Context, execution Execution) (json.RawMessage, error) {
	members, err := decodeDemoPayload(execution.Payload, "class")
	if err != nil {
		return nil, Permanent(invalidPayloadCode, demoFailPayloadMessage)
	}
	var class *string
	if err := json.Unmarshal(members["class"], &class); err != nil || class == nil {
		return nil, Permanent(invalidPayloadCode, demoFailPayloadMessage)
	}
	switch *class {
	case "retryable":
		return nil, Retryable(demoFailureCode, demoFailRetryableMessage)
	case "permanent":
		return nil, Permanent(demoFailureCode, demoFailPermanentMessage)
	default:
		return nil, Permanent(invalidPayloadCode, demoFailPayloadMessage)
	}
}
