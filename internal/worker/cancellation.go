package worker

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/workers"
)

// Engine-owned cancellation causes.
//
// These are separate values, not one generic "cancelled", because the worker
// makes a different durable decision for each and must never confuse them:
//
//   - errUserCanceled means an operator canceled the job. The worker reports a
//     fenced cancellation acknowledgment.
//   - errAttemptTimedOut means the attempt's own execution deadline arrived. The
//     worker reports NOTHING; only reconciliation may record TIMED_OUT.
//   - errAuthorityLost means the lease, session, or generation stopped being
//     provable. The worker reports nothing and lets recovery happen.
//   - errWorkerShutdown means this process is stopping. It is emphatically not
//     job cancellation: reporting one as the other would tell an operator their
//     job was canceled when nobody canceled it.
var (
	errUserCanceled    = errors.New("the job was canceled")
	errAttemptTimedOut = errors.New("the attempt reached its execution deadline")
	errAuthorityLost   = errors.New("lease authority was lost")
	errWorkerShutdown  = errors.New("the worker process is shutting down")
)

// attemptRegistry tracks the attempts this process is executing so a
// cancellation directive delivered on the heartbeat loop can reach the handler
// goroutine that is running one.
//
// It is registered against BEFORE Start, not after, and that ordering is the
// point. Cancellation can win in the window between a claim committing and the
// handler being invoked; a registry populated only once execution begins would
// drop exactly those directives, and the worker would run a handler for a job it
// had already been told was canceled.
type attemptRegistry struct {
	mu      sync.Mutex
	entries map[uuid.UUID]*attemptEntry
}

type attemptEntry struct {
	fence workers.Fence
	// cancel is nil until the handler context exists. A directive that arrives
	// first is remembered in canceled and applied at bind time.
	cancel   context.CancelCauseFunc
	canceled bool
}

func newAttemptRegistry() *attemptRegistry {
	return &attemptRegistry{entries: make(map[uuid.UUID]*attemptEntry)}
}

// register records an attempt this process is about to execute.
func (r *attemptRegistry) register(fence workers.Fence) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[fence.AttemptID] = &attemptEntry{fence: fence}
}

// bind attaches the handler's cancel function and applies any directive that
// already arrived, so a cancellation delivered before the handler started is not
// lost.
func (r *attemptRegistry) bind(attemptID uuid.UUID, cancel context.CancelCauseFunc) {
	r.mu.Lock()
	entry, ok := r.entries[attemptID]
	if !ok {
		r.mu.Unlock()
		return
	}
	entry.cancel = cancel
	pending := entry.canceled
	r.mu.Unlock()

	if pending {
		cancel(errUserCanceled)
	}
}

// unregister forgets an attempt once its outcome has been reported.
func (r *attemptRegistry) unregister(attemptID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, attemptID)
}

// deliver applies one cancellation directive and reports whether this process
// was actually executing that attempt.
//
// A directive for an attempt this process does not hold is ignored rather than
// treated as an error. That is normal: the control plane answers a heartbeat
// from durable state, so a directive can name an attempt whose outcome this
// worker reported a moment ago.
func (r *attemptRegistry) deliver(directive workers.CancellationDirective) bool {
	r.mu.Lock()
	entry, ok := r.entries[directive.AttemptID]
	if !ok {
		r.mu.Unlock()
		return false
	}
	// The directive must name the lease this worker actually holds. A stale
	// directive for a previous lease on the same attempt is not authority to stop
	// the current one.
	if entry.fence.LeaseID != directive.LeaseID || entry.fence.JobID != directive.JobID {
		r.mu.Unlock()
		return false
	}
	first := !entry.canceled
	entry.canceled = true
	cancel := entry.cancel
	r.mu.Unlock()

	if cancel != nil {
		cancel(errUserCanceled)
	}
	return first
}

// wasCanceled reports whether a durable cancellation directive arrived for this
// attempt.
//
// The worker consults this rather than inspecting the handler context's cause,
// because a cancellation that arrived after the handler already returned
// cooperatively still has to be acknowledged: the job is CANCEL_REQUESTED
// either way, and reporting success for it would be rejected.
func (r *attemptRegistry) wasCanceled(attemptID uuid.UUID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[attemptID]
	return ok && entry.canceled
}
