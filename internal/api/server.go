// Package api serves TaskForge's HTTP surface.
//
// Milestone M1 implements durable job ingress only: submit, read, and health.
// Every other endpoint in docs/PROJECT_SPEC.md arrives with the milestone that
// makes it real — an endpoint is never exposed before it works.
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

	"github.com/co-rtex/TaskForge/internal/jobs"
)

// Config configures the HTTP server.
type Config struct {
	MaxRequestBytes int64
	// DevScope attributes every request to one scope until API keys land in
	// milestone M5. See internal/config.Config.DevScope.
	DevScope string
}

// ReadinessCheck reports whether a dependency is usable. It must honor the
// context deadline it is given.
type ReadinessCheck struct {
	Name  string
	Check func(context.Context) error
}

// Server wires handlers to their dependencies.
type Server struct {
	jobs   *jobs.Store
	cfg    Config
	log    *slog.Logger
	checks []ReadinessCheck
}

// NewServer builds a Server.
func NewServer(store *jobs.Store, cfg Config, log *slog.Logger, checks ...ReadinessCheck) *Server {
	return &Server{jobs: store, cfg: cfg, log: log, checks: checks}
}

// Handler returns the fully wrapped HTTP handler.
//
// Order matters: request id is outermost so every later layer can log it, and
// recovery sits inside it so a panic is logged with its request id.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs", s.handleSubmitJob)
	mux.HandleFunc("GET /v1/jobs/{job_id}", s.handleGetJob)
	mux.HandleFunc("GET /healthz", s.handleLiveness)
	mux.HandleFunc("GET /readyz", s.handleReadiness)

	// ServeMux answers an unmatched method with a plain-text 405 and an unmatched
	// path with a plain-text 404, neither of which matches the structured error
	// shape every other response uses. Registering method-less patterns alongside
	// the real ones reclaims those cases: a pattern that names a method is more
	// specific, so it still wins for that method, and everything else falls
	// through to here.
	mux.HandleFunc("/v1/jobs", s.methodNotAllowed(http.MethodPost))
	mux.HandleFunc("/v1/jobs/{job_id}", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/healthz", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/readyz", s.methodNotAllowed(http.MethodGet))
	mux.HandleFunc("/", s.handleNotFound)

	var h http.Handler = mux
	h = withBodyLimit(s.cfg.MaxRequestBytes, h)
	h = withRecovery(s.log, h)
	h = withLogging(s.log, h)
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
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

func toJobResponse(j *jobs.Job) JobResponse {
	caps := j.RequiredCapabilities
	if caps == nil {
		caps = []string{} // an empty JSON array, never null
	}
	return JobResponse{
		ID:                   j.ID.String(),
		Queue:                j.Queue,
		JobType:              j.Type,
		Payload:              j.Payload,
		Status:               j.Status.String(),
		Priority:             j.Priority,
		MaxAttempts:          j.MaxAttempts,
		TimeoutSeconds:       j.TimeoutSeconds,
		RequiredCapabilities: caps,
		CreatedAt:            j.CreatedAt.UTC(),
		UpdatedAt:            j.UpdatedAt.UTC(),
	}
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
func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
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

	result, err := s.jobs.Submit(r.Context(), s.cfg.DevScope, key, normalized)
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
	id, err := uuid.Parse(r.PathValue("job_id"))
	if err != nil {
		// Answering 404 rather than 400 keeps the response identical whether the
		// id is malformed or simply not the caller's, so ids cannot be probed.
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
		return
	}

	job, err := s.jobs.Get(r.Context(), s.cfg.DevScope, id)
	switch {
	case err == nil:
		writeJSON(w, s.log, http.StatusOK, toJobResponse(job))
	case errors.Is(err, jobs.ErrJobNotFound):
		writeError(w, r, s.log, http.StatusNotFound, CodeNotFound, "job not found", nil)
	default:
		s.internalError(w, r, "get job", err)
	}
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
