// Package api serves TaskForge's HTTP surface.
//
// The public surface implements durable job submission/read. The internal
// surface implements worker registration, session heartbeat, atomic claim,
// fenced lease renewal, and fenced start/success. Every later endpoint in
// docs/PROJECT_SPEC.md arrives only with the milestone that makes it real.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/metrics"
	"github.com/co-rtex/TaskForge/internal/results"
	"github.com/co-rtex/TaskForge/internal/telemetry"
)

// Config configures the HTTP server.
type Config struct {
	MaxRequestBytes int64
	RequestTimeout  time.Duration
}

// ReadinessCheck reports whether a dependency is usable. It must honor the
// context deadline it is given.
type ReadinessCheck struct {
	Name  string
	Check func(context.Context) error
}

// Server wires handlers to their dependencies.
type Server struct {
	jobs       *jobs.Store
	control    WorkerControl
	keys       APIKeys
	workerKeys WorkerKeys
	results    Results
	objects    ObjectStore
	// workerReads serves GET /v1/workers. Separate from control, which gates
	// the fenced internal surface -- see WithWorkerReads.
	workerReads WorkerReads
	// tracer records one server span per request. It comes from the global
	// provider, which is a delegating tracer: taking it here and using it
	// later is correct even though cmd/taskforge-api installs the real
	// provider after this point. With tracing disabled it is a no-op, so
	// there is no enabled/disabled branch anywhere in the request path.
	tracer trace.Tracer
	// metrics is optional. A nil one records nothing and changes no behavior,
	// which is what keeps every pre-M6C test unchanged.
	metrics *metrics.Metrics
	cfg     Config
	log     *slog.Logger
	checks  []ReadinessCheck
}

// Results reads a job's recorded result. See internal/results.Store.
type Results interface {
	Get(ctx context.Context, scope string, jobID uuid.UUID) (*results.Result, error)
}

// ObjectStore fetches large result bytes a worker has already uploaded. See
// internal/objectstore.Client.
type ObjectStore interface {
	Get(ctx context.Context, bucket, key string) ([]byte, error)
}

// NewServer builds a Server.
func NewServer(store *jobs.Store, cfg Config, log *slog.Logger, checks ...ReadinessCheck) *Server {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 25 * time.Second
	}
	return &Server{
		jobs:   store,
		cfg:    cfg,
		log:    log,
		checks: checks,
		tracer: otel.Tracer(telemetry.TracerName),
	}
}

// WithWorkerControl enables the implemented internal worker control surface.
// It is kept separate from NewServer so M1-focused unit tests can construct a
// validation-only server without a database-backed control store.
func (s *Server) WithWorkerControl(control WorkerControl) *Server {
	s.control = control
	return s
}

// WithAuth enables API-key authentication on the public surface and the
// loopback key-management routes that mint credentials for it.
//
// Omitting it does not open the public API -- it closes it. Every public route
// answers 401 without a credential store, because a server that can authenticate
// nobody has no authenticated caller to serve, and the key-management routes are
// not registered at all. That is why this package contains no test-only
// authentication bypass: there is nothing to bypass, and a binary that forgot to
// call this serves a closed API rather than an open one.
func (s *Server) WithAuth(keys APIKeys) *Server {
	s.keys = keys
	return s
}

// WithWorkerAuth enables worker-key authentication on registration
// (PUT /internal/v1/worker-sessions/{id}) and the loopback key-management
// routes that mint credentials for it.
//
// It is independent of WithWorkerControl: that continues to gate whether the
// eight fenced worker-control routes exist at all, exactly as it always has,
// while this gates only whether registration among them can verify a
// credential and whether the three worker-key admin routes are registered. A
// production binary calls both. Omitting this one does not open registration
// -- it closes it, for the identical reason omitting WithAuth closes the
// public surface: a server that can authenticate nobody has no authenticated
// caller to serve, and the worker-key admin routes are not registered at
// all. There is no test-only bypass for this either.
func (s *Server) WithWorkerAuth(keys WorkerKeys) *Server {
	s.workerKeys = keys
	return s
}

// WithResults supplies GET /v1/jobs/{job_id}/result its dependencies. store
// reads the recorded metadata row; objects fetches an object-located
// result's actual bytes and may be nil for a deployment that only ever
// expects small, inline results -- a request that then reaches an
// object-located row is a sanitized 500, the same as any other operator
// misconfiguration this package reports, rather than a panic or a silently
// empty body.
//
// Unlike WithWorkerControl, WithAuth, and WithWorkerAuth, the route is
// registered unconditionally: PROJECT_SPEC.md item 13 makes result
// retrieval a core public route exactly like job submission and cancellation,
// not opt-in admin plumbing, so a server built without calling this answers
// the same 401 every other public route does for an unauthenticated caller,
// and a 500 for an authenticated one -- never a 404 that would make this one
// route look like it does not exist.
func (s *Server) WithResults(store Results, objects ObjectStore) *Server {
	s.results = store
	s.objects = objects
	return s
}

// WithMetrics enables HTTP instrumentation and serves GET /metrics.
//
// The endpoint is registered on this same mux rather than on a separate admin
// listener. TASKFORGE_API_ADDR is already validated as a loopback bind (see
// internal/config.Validate), exactly like the four background services'
// addresses, so /metrics crosses no boundary that the already-unauthenticated
// /healthz and /readyz do not. docs/CURRENT_STATE.md records the one thing
// that must be revisited before this process ever sits behind a real load
// balancer.
func (s *Server) WithMetrics(m *metrics.Metrics) *Server {
	s.metrics = m
	return s
}

// WithTracer overrides the tracer this server records spans with.
//
// Production never calls it: NewServer already takes the global delegating
// tracer, which resolves to whatever cmd/taskforge-api installs. It exists so
// a test can record into its own provider without touching global state and
// without racing another test that did the same.
func (s *Server) WithTracer(tracer trace.Tracer) *Server {
	s.tracer = tracer
	return s
}

// Handler returns the fully wrapped HTTP handler.
//
// Order matters: request id is outermost so every later layer can log it,
// tracing sits just inside it so a span can carry that id and so the access log
// can carry the trace id, and recovery sits inside both so a panic is logged
// with both identities.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Every public route is wrapped in requireAPIKey at registration, so the set
	// of authenticated routes is readable here rather than in an inclusion list
	// inside a middleware that would drift from the mux. Adding a public route
	// without the wrapper is a visible omission on this screen.
	mux.HandleFunc("POST /v1/jobs", s.requireAPIKey(s.handleSubmitJob))
	mux.HandleFunc("GET /v1/jobs", s.requireAPIKey(s.handleListJobs))
	mux.HandleFunc("GET /v1/jobs/{job_id}", s.requireAPIKey(s.handleGetJob))
	mux.HandleFunc("GET /v1/jobs/{job_id}/attempts", s.requireAPIKey(s.handleListJobAttempts))
	mux.HandleFunc("GET /v1/jobs/{job_id}/result", s.requireAPIKey(s.handleGetJobResult))
	mux.HandleFunc("POST /v1/jobs/{job_id}/cancel", s.requireAPIKey(s.handleCancelJob))
	// Operator retry IS DLQ replay: same service, same idempotency namespace. Two
	// routes exist because operators reach for both names, not because there are
	// two operations.
	mux.HandleFunc("POST /v1/jobs/{job_id}/retry", s.requireAPIKey(s.handleReplayJob))
	mux.HandleFunc("GET /v1/dlq", s.requireAPIKey(s.handleListDLQ))
	mux.HandleFunc("POST /v1/dlq/{job_id}/replay", s.requireAPIKey(s.handleReplayJob))
	// The operator read surface (M6A). PROJECT_SPEC.md section 4 items 4 and 7
	// name these; the dashboard, the CLI, and the SDK are all consumers of
	// exactly these four routes and nothing else.
	mux.HandleFunc("GET /v1/workers", s.requireAPIKey(s.handleListWorkers))
	mux.HandleFunc("GET /v1/queues", s.requireAPIKey(s.handleListQueues))
	// Health probes stay unauthenticated. They reveal nothing tenant-specific,
	// and a liveness probe that needed a credential would report a healthy
	// process as dead the moment that credential was revoked.
	if s.metrics != nil {
		// Unauthenticated, like the health probes beside it, and on this same
		// already-loopback-bound listener. See WithMetrics.
		mux.Handle("GET /metrics", s.metrics.Handler())
	}
	mux.HandleFunc("GET /healthz", s.handleLiveness)
	mux.HandleFunc("GET /readyz", s.handleReadiness)
	if s.keys != nil {
		mux.HandleFunc("POST /internal/v1/api-keys", s.handleCreateAPIKey)
		mux.HandleFunc("GET /internal/v1/api-keys", s.handleListAPIKeys)
		mux.HandleFunc("POST /internal/v1/api-keys/{key_id}/revoke", s.handleRevokeAPIKey)
	}
	if s.workerKeys != nil {
		mux.HandleFunc("POST /internal/v1/worker-keys", s.handleCreateWorkerKey)
		mux.HandleFunc("GET /internal/v1/worker-keys", s.handleListWorkerKeys)
		mux.HandleFunc("POST /internal/v1/worker-keys/{key_id}/revoke", s.handleRevokeWorkerKey)
	}
	if s.control != nil {
		// Registration alone is wrapped in requireWorkerKey: every other
		// worker-control route below resolves its scope from the session
		// identity the request already carries rather than a fresh credential.
		// See requireWorkerKey's doc comment for why that asymmetry is
		// deliberate.
		mux.HandleFunc("PUT /internal/v1/worker-sessions/{worker_session_id}", s.requireWorkerKey(s.handleRegisterWorkerSession))
		mux.HandleFunc("POST /internal/v1/worker-sessions/{worker_session_id}/heartbeat", s.handleHeartbeat)
		mux.HandleFunc("POST /internal/v1/claims", s.handleClaim)
		mux.HandleFunc("POST /internal/v1/leases/{lease_id}/renew", s.handleRenewLease)
		mux.HandleFunc("POST /internal/v1/attempts/{attempt_id}/start", s.handleStartAttempt)
		mux.HandleFunc("POST /internal/v1/attempts/{attempt_id}/succeed", s.handleSucceedAttempt)
		mux.HandleFunc("POST /internal/v1/attempts/{attempt_id}/fail", s.handleFailAttempt)
		mux.HandleFunc("POST /internal/v1/attempts/{attempt_id}/cancel", s.handleCancelAttempt)
	}

	// ServeMux answers an unmatched method with a plain-text 405 and an unmatched
	// path with a plain-text 404, neither of which matches the structured error
	// shape every other response uses. Registering method-less patterns alongside
	// the real ones reclaims those cases: a pattern that names a method is more
	// specific, so it still wins for that method, and everything else falls
	// through to here.
	mux.HandleFunc("/v1/jobs", s.methodNotAllowed(http.MethodGet, http.MethodPost))
	mux.HandleFunc("/v1/jobs/{job_id}", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/v1/jobs/{job_id}/attempts", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/v1/jobs/{job_id}/result", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/v1/jobs/{job_id}/cancel", s.methodNotAllowed(http.MethodPost))
	mux.HandleFunc("/v1/jobs/{job_id}/retry", s.methodNotAllowed(http.MethodPost))
	mux.HandleFunc("/v1/dlq", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/v1/dlq/{job_id}/replay", s.methodNotAllowed(http.MethodPost))
	mux.HandleFunc("/v1/workers", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/v1/queues", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/healthz", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/readyz", s.methodNotAllowed(http.MethodGet))
	if s.keys != nil {
		// 405 is answered before authentication, deliberately. "This path does
		// not accept DELETE" is a fact about the route table, not about the
		// caller, so it discloses nothing a reader of the OpenAPI document does
		// not already have.
		mux.HandleFunc("/internal/v1/api-keys", s.methodNotAllowed(http.MethodGet, http.MethodPost))
		mux.HandleFunc("/internal/v1/api-keys/{key_id}/revoke", s.methodNotAllowed(http.MethodPost))
	}
	if s.workerKeys != nil {
		mux.HandleFunc("/internal/v1/worker-keys", s.methodNotAllowed(http.MethodGet, http.MethodPost))
		mux.HandleFunc("/internal/v1/worker-keys/{key_id}/revoke", s.methodNotAllowed(http.MethodPost))
	}
	if s.control != nil {
		mux.HandleFunc("/internal/v1/worker-sessions/{worker_session_id}", s.methodNotAllowed(http.MethodPut))
		mux.HandleFunc("/internal/v1/worker-sessions/{worker_session_id}/heartbeat", s.methodNotAllowed(http.MethodPost))
		mux.HandleFunc("/internal/v1/claims", s.methodNotAllowed(http.MethodPost))
		mux.HandleFunc("/internal/v1/leases/{lease_id}/renew", s.methodNotAllowed(http.MethodPost))
		mux.HandleFunc("/internal/v1/attempts/{attempt_id}/start", s.methodNotAllowed(http.MethodPost))
		mux.HandleFunc("/internal/v1/attempts/{attempt_id}/succeed", s.methodNotAllowed(http.MethodPost))
		mux.HandleFunc("/internal/v1/attempts/{attempt_id}/fail", s.methodNotAllowed(http.MethodPost))
		mux.HandleFunc("/internal/v1/attempts/{attempt_id}/cancel", s.methodNotAllowed(http.MethodPost))
	}
	mux.HandleFunc("/", s.handleNotFound)

	// Innermost first. withSpanRoute must sit directly on the mux so it reads
	// the pattern off the same *Request the mux wrote it to -- see its comment.
	var h http.Handler = mux
	h = withSpanRoute(h)
	h = withBodyLimit(s.cfg.MaxRequestBytes, h)
	h = withTimeout(s.cfg.RequestTimeout, h)
	h = withRecovery(s.log, h)
	// withLogging sits inside withTracing so its line can carry the trace id,
	// and outside it nothing else changes: the request id still comes from the
	// outermost layer, and both identities appear on the same line.
	h = withLogging(s.log, h)
	// Outside withRecovery, so it observes the sanitized 500 a panicking
	// handler actually returns rather than the 200 an inner recorder would
	// see. See withHTTPMetrics.
	h = withHTTPMetrics(s.metrics, h)
	h = withTracing(s.tracer, h)
	h = withRequestID(h)
	return h
}

// JobResponse is the public representation of a job. It deliberately excludes
// the internal scope.
type JobResponse struct {
	ID                   string          `json:"id"`
	Queue                string          `json:"queue"`
	JobType              string          `json:"job_type"`
	Payload              json.RawMessage `json:"payload"`
	Status               string          `json:"status"`
	Priority             int             `json:"priority"`
	MaxAttempts          int             `json:"max_attempts"`
	TimeoutSeconds       int             `json:"timeout_seconds"`
	RequiredCapabilities []string        `json:"required_capabilities"`
	// ScheduledAt is what the caller asked for; AvailableAt is what PostgreSQL
	// will actually order and filter claims by, which retry backoff moves.
	ScheduledAt *time.Time `json:"scheduled_at"`
	AvailableAt time.Time  `json:"available_at"`
	// CancelRequestedAt is set from the moment cancellation wins, including
	// while a worker is still being asked to stop cooperatively.
	CancelRequestedAt *time.Time `json:"cancel_requested_at"`
	// ReplayedFromJobID links a replacement job back to the terminal job it
	// replaces. Attempt history is deliberately not here: a rich history API
	// remains future work. A result, once recorded, is retrieved separately
	// through GET /v1/jobs/{job_id}/result rather than embedded here, so
	// reading a job's status never pulls a potentially large result body
	// along with it.
	ReplayedFromJobID *string   `json:"replayed_from_job_id"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

func toJobResponse(j *jobs.Job) JobResponse {
	caps := j.RequiredCapabilities
	if caps == nil {
		caps = []string{} // an empty JSON array, never null
	}
	response := JobResponse{
		ID:                   j.ID.String(),
		Queue:                j.Queue,
		JobType:              j.Type,
		Payload:              j.Payload,
		Status:               j.Status.String(),
		Priority:             j.Priority,
		MaxAttempts:          j.MaxAttempts,
		TimeoutSeconds:       j.TimeoutSeconds,
		RequiredCapabilities: caps,
		AvailableAt:          j.AvailableAt.UTC(),
		CreatedAt:            j.CreatedAt.UTC(),
		UpdatedAt:            j.UpdatedAt.UTC(),
	}
	if j.ScheduledAt != nil {
		utc := j.ScheduledAt.UTC()
		response.ScheduledAt = &utc
	}
	if j.CancelRequestedAt != nil {
		utc := j.CancelRequestedAt.UTC()
		response.CancelRequestedAt = &utc
	}
	if j.ReplayedFromJobID != nil {
		id := j.ReplayedFromJobID.String()
		response.ReplayedFromJobID = &id
	}
	return response
}

// handleSubmitJob durably accepts a job.
//
// Responses:
//
//	201 the job was created
//	200 an identical earlier submission already created it (replay)
//	409 the key was reused with a different request
//	422 the request was well-formed JSON but invalid
//	400 the body was not valid JSON, or a field had the wrong type
//	413 the body exceeded the configured limit
//	401 no valid API key was presented
func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.scopeOrUnauthorized(w, r)
	if !ok {
		return
	}

	key := r.Header.Get("Idempotency-Key")
	if err := jobs.ValidateIdempotencyKey(key); err != nil {
		s.writeValidationError(w, r, err)
		return
	}

	var req jobs.SubmitRequest
	dec := json.NewDecoder(r.Body)
	// Unknown fields are rejected rather than ignored: silently dropping a
	// misspelled "priorty" would give the caller a job that does not match what
	// they asked for, and would change the idempotency fingerprint invisibly.
	dec.DisallowUnknownFields()

	if err := dec.Decode(&req); err != nil {
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			writeError(w, r, s.log, http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
				fmt.Sprintf("request body exceeds the %d byte limit", s.cfg.MaxRequestBytes), nil)
			return
		}
		writeError(w, r, s.log, http.StatusBadRequest, CodeMalformedJSON,
			"request body is not valid JSON: "+sanitizeDecodeError(err), nil)
		return
	}
	// Reject `{...} {...}`, which json.Decoder would otherwise silently accept
	// by reading only the first document.
	if err := dec.Decode(new(struct{})); !errors.Is(err, io.EOF) {
		writeError(w, r, s.log, http.StatusBadRequest, CodeMalformedJSON,
			"request body must contain exactly one JSON object", nil)
		return
	}

	normalized, err := req.Normalize()
	if err != nil {
		s.writeValidationError(w, r, err)
		return
	}

	result, err := s.jobs.Submit(r.Context(), scope, key, normalized)
	switch {
	case err == nil:
		status := http.StatusCreated
		if result.Replayed {
			status = http.StatusOK
		}
		s.log.Info("job submitted",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("job_id", result.Job.ID.String()),
			slog.String("queue", result.Job.Queue),
			slog.String("job_type", result.Job.Type),
			slog.Bool("replayed", result.Replayed))
		writeJSON(w, s.log, status, toJobResponse(result.Job))

	case errors.Is(err, jobs.ErrUnknownQueue):
		writeError(w, r, s.log, http.StatusUnprocessableEntity, CodeUnknownQueue,
			fmt.Sprintf("queue %q does not exist", normalized.Queue), nil)

	case errors.Is(err, jobs.ErrIdempotencyConflict):
		writeError(w, r, s.log, http.StatusConflict, CodeIdempotencyConflict,
			"this Idempotency-Key was already used for a different request", nil)

	default:
		s.internalError(w, r, "submit job", err)
	}
}

// handleGetJob reads one job within the caller's scope.
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.scopeOrUnauthorized(w, r)
	if !ok {
		return
	}

	id, err := uuid.Parse(r.PathValue("job_id"))
	if err != nil {
		// Answering 404 rather than 400 keeps the response identical whether the
		// id is malformed or simply not the caller's, so ids cannot be probed.
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
		return
	}

	job, err := s.jobs.Get(r.Context(), scope, id)
	switch {
	case err == nil:
		writeJSON(w, s.log, http.StatusOK, toJobResponse(job))
	case errors.Is(err, jobs.ErrJobNotFound):
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
	default:
		s.internalError(w, r, "get job", err)
	}
}

// handleGetJobResult serves one job's recorded result: the exact JSON bytes
// a trusted handler produced, regardless of whether they are stored inline
// in PostgreSQL or in the object store. The caller never needs to know
// which -- both are served with the same Content-Type and the same status.
//
// This is a server-side proxy of the bytes, not a presigned-URL redirect:
// every retrieval goes through the same scope check GET /v1/jobs/{job_id}
// already does, with no separate credential a redirect target would need to
// enforce on its own.
func (s *Server) handleGetJobResult(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.scopeOrUnauthorized(w, r)
	if !ok {
		return
	}
	if s.results == nil {
		// Authenticated, but this server instance was never wired with a
		// results store -- an operator misconfiguration, reported the same
		// way s.objects == nil is reported below, never a 404 that would
		// make a real route look like it does not exist.
		s.internalError(w, r, "get job result", fmt.Errorf("no results store configured"))
		return
	}

	id, err := uuid.Parse(r.PathValue("job_id"))
	if err != nil {
		// Same anti-oracle reasoning as handleGetJob: a malformed id and an id
		// that is simply not the caller's must look identical.
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "result not found", nil)
		return
	}

	result, err := s.results.Get(r.Context(), scope, id)
	if err != nil {
		if errors.Is(err, results.ErrResultNotFound) {
			writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "result not found", nil)
			return
		}
		s.internalError(w, r, "get job result", err)
		return
	}

	body := []byte(result.InlineBody)
	if result.Location == results.LocationObject {
		if s.objects == nil {
			s.internalError(w, r, "get job result", fmt.Errorf(
				"result for job %s is object-located but no object store is configured", id))
			return
		}
		body, err = s.objects.Get(r.Context(), result.ObjectBucket, result.ObjectKey)
		if err != nil {
			s.internalError(w, r, "fetch object result", err)
			return
		}
	}
	writeJSON(w, s.log, http.StatusOK, json.RawMessage(body))
}

// handleLiveness answers whether the process is alive. It deliberately checks
// nothing else: a liveness probe that fails on a database blip would restart a
// healthy process and make an outage worse.
func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.log, http.StatusOK, map[string]string{"status": "alive"})
}

// handleReadiness reports whether this process can actually serve traffic.
//
// Unlike liveness, it checks its dependencies, under a bounded timeout so a
// hung dependency cannot hang the probe.
func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	components := make(map[string]string, len(s.checks))
	ready := true
	for _, c := range s.checks {
		if err := c.Check(ctx); err != nil {
			ready = false
			components[c.Name] = "unavailable"
			s.log.Warn("readiness check failed",
				slog.String("component", c.Name),
				slog.String("error", err.Error()))
			continue
		}
		components[c.Name] = "ok"
	}

	status := http.StatusOK
	state := "ready"
	if !ready {
		status = http.StatusServiceUnavailable
		state = "not_ready"
	}
	writeJSON(w, s.log, status, map[string]any{"status": state, "components": components})
}

// methodNotAllowed renders a structured 405 and advertises what is allowed.
func (s *Server) methodNotAllowed(allowed ...string) http.HandlerFunc {
	allow := strings.Join(allowed, ", ")
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", allow)
		writeError(w, r, s.log, http.StatusMethodNotAllowed, CodeMethodNotAllowed,
			"method "+r.Method+" is not allowed on this path", nil)
	}
}

// handleNotFound renders a structured 404 for any unrouted path.
func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "no such endpoint", nil)
}

func (s *Server) writeValidationError(w http.ResponseWriter, r *http.Request, err error) {
	var verr *jobs.ValidationError
	if errors.As(err, &verr) {
		writeError(w, r, s.log, http.StatusUnprocessableEntity, verr.Code,
			"the request was rejected by validation", verr.Fields)
		return
	}
	s.internalError(w, r, "validate request", err)
}

// internalError logs the real cause and returns a sanitized message, so a
// database error never reaches a client.
func (s *Server) internalError(w http.ResponseWriter, r *http.Request, op string, err error) {
	s.log.Error("request failed",
		slog.String("request_id", RequestIDFrom(r.Context())),
		slog.String("op", op),
		slog.String("error", err.Error()))
	writeError(w, r, s.log, http.StatusInternalServerError, CodeInternal, "internal error", nil)
}

// sanitizeDecodeError keeps a JSON decode message useful without echoing the
// body back to the caller.
func sanitizeDecodeError(err error) string {
	msg := err.Error()
	const maxLen = 200
	if len(msg) > maxLen {
		msg = msg[:maxLen]
	}
	return strings.ReplaceAll(msg, "\n", " ")
}
