package workers

import (
	"errors"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/internal/lifecycle"
)

const (
	MaxCapabilities      = 64
	MaxSupportedJobTypes = 64
	MaxWorkerConcurrency = 256
)

var (
	workerNamePattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	routingNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	jobTypePattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
)

// NormalizeRegistration validates and canonicalizes set-valued registration
// fields. The store calls it even when an HTTP handler already validated, so a
// non-HTTP caller cannot persist an invalid session.
func NormalizeRegistration(in Registration) (Registration, error) {
	out := in
	out.Name = strings.TrimSpace(out.Name)
	out.Hostname = strings.TrimSpace(out.Hostname)
	out.WorkerGroup = strings.TrimSpace(out.WorkerGroup)

	var fields []FieldError
	if out.SessionID == uuid.Nil {
		fields = append(fields, FieldError{Field: "worker_session_id", Message: "must be a UUID"})
	}
	if !workerNamePattern.MatchString(out.Name) {
		fields = append(fields, FieldError{Field: "worker_name", Message: "must match ^[a-z0-9][a-z0-9._-]{0,127}$"})
	}
	if len(out.Hostname) < 1 || len(out.Hostname) > 255 {
		fields = append(fields, FieldError{Field: "hostname", Message: "must contain between 1 and 255 characters"})
	}
	if !routingNamePattern.MatchString(out.WorkerGroup) {
		fields = append(fields, FieldError{Field: "worker_group", Message: "must match ^[a-z0-9][a-z0-9._-]{0,63}$"})
	}
	if out.ConcurrencyLimit < 1 || out.ConcurrencyLimit > MaxWorkerConcurrency {
		fields = append(fields, FieldError{Field: "concurrency_limit", Message: "must be between 1 and 256"})
	}

	if len(out.Capabilities) > MaxCapabilities {
		fields = append(fields, FieldError{Field: "capabilities", Message: "must contain at most 64 values"})
	}
	for _, capability := range out.Capabilities {
		if !routingNamePattern.MatchString(strings.TrimSpace(capability)) {
			fields = append(fields, FieldError{Field: "capabilities", Message: "every value must match ^[a-z0-9][a-z0-9._-]{0,63}$"})
			break
		}
	}
	out.Capabilities = canonicalSet(out.Capabilities)

	if len(out.SupportedJobTypes) < 1 || len(out.SupportedJobTypes) > MaxSupportedJobTypes {
		fields = append(fields, FieldError{Field: "supported_job_types", Message: "must contain between 1 and 64 values"})
	}
	for _, jobType := range out.SupportedJobTypes {
		if !jobTypePattern.MatchString(strings.TrimSpace(jobType)) {
			fields = append(fields, FieldError{Field: "supported_job_types", Message: "every value must be a valid job type"})
			break
		}
	}
	out.SupportedJobTypes = canonicalSet(out.SupportedJobTypes)

	if len(fields) > 0 {
		return Registration{}, &ValidationError{Fields: fields}
	}
	return out, nil
}

// ValidateClaim validates identifiers before a transaction takes capacity locks.
func ValidateClaim(req ClaimRequest) error {
	var fields []FieldError
	if req.WorkerID == uuid.Nil {
		fields = append(fields, FieldError{Field: "worker_id", Message: "must be a UUID"})
	}
	if req.SessionID == uuid.Nil {
		fields = append(fields, FieldError{Field: "worker_session_id", Message: "must be a UUID"})
	}
	if req.ClaimRequestID == uuid.Nil {
		fields = append(fields, FieldError{Field: "claim_request_id", Message: "must be a UUID"})
	}
	if !routingNamePattern.MatchString(req.Queue) {
		fields = append(fields, FieldError{Field: "queue", Message: "must be a valid queue name"})
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// ValidateHeartbeat rejects an incomplete session identity before PostgreSQL is
// touched. There is deliberately no timestamp to validate.
func ValidateHeartbeat(req HeartbeatRequest) error {
	var fields []FieldError
	if req.WorkerID == uuid.Nil {
		fields = append(fields, FieldError{Field: "worker_id", Message: "must be a UUID"})
	}
	if req.SessionID == uuid.Nil {
		fields = append(fields, FieldError{Field: "worker_session_id", Message: "must be a UUID"})
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// ValidateRenewal reports every problem in a renewal request at once: the whole
// fence, the renewal identity, and the expected generation.
func ValidateRenewal(req RenewalRequest) error {
	var fields []FieldError
	var fenceErr *ValidationError
	if err := ValidateFence(req.Fence); errors.As(err, &fenceErr) {
		fields = append(fields, fenceErr.Fields...)
	}
	if req.RenewalRequestID == uuid.Nil {
		fields = append(fields, FieldError{Field: "renewal_request_id", Message: "must be a UUID"})
	}
	if req.ExpectedVersion < 0 {
		fields = append(fields, FieldError{
			Field: "expected_renewal_version", Message: "must not be negative"})
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// ValidateFailureReport reports every problem in a failure report at once: the
// whole fence, the outcome identity, and the bounded safe error detail.
//
// The classification check is not a formality. TIMED_OUT, CANCELED, and
// ABANDONED are server-authoritative outcomes, so a worker declaring one about
// itself is rejected here rather than being allowed to overwrite a decision only
// PostgreSQL may make.
func ValidateFailureReport(report FailureReport) error {
	var fields []FieldError
	var fenceErr *ValidationError
	if err := ValidateFence(report.Fence); errors.As(err, &fenceErr) {
		fields = append(fields, fenceErr.Fields...)
	}
	if report.OutcomeRequestID == uuid.Nil {
		fields = append(fields, FieldError{Field: "outcome_request_id", Message: "must be a UUID"})
	}
	if !report.Class.ReportableByHandler() {
		fields = append(fields, FieldError{
			Field:   "failure_class",
			Message: "must be RETRYABLE or PERMANENT; timeout, cancellation, and abandonment are server-authoritative",
		})
	}
	if err := lifecycle.ValidateErrorCode(report.ErrorCode); err != nil {
		fields = append(fields, FieldError{Field: "error_code", Message: err.Error()})
	}
	if err := lifecycle.ValidateErrorMessage(report.ErrorMessage); err != nil {
		fields = append(fields, FieldError{Field: "error_message", Message: err.Error()})
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// ValidateCancelAcknowledgment rejects an incomplete cooperative cancellation
// acknowledgment. It carries no classification or error detail: cancellation is
// not a failure, and the reason is already recorded on the job.
func ValidateCancelAcknowledgment(ack CancelAcknowledgment) error {
	var fields []FieldError
	var fenceErr *ValidationError
	if err := ValidateFence(ack.Fence); errors.As(err, &fenceErr) {
		fields = append(fields, fenceErr.Fields...)
	}
	if ack.OutcomeRequestID == uuid.Nil {
		fields = append(fields, FieldError{Field: "outcome_request_id", Message: "must be a UUID"})
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// ValidateFence rejects incomplete fencing tuples before touching PostgreSQL.
func ValidateFence(f Fence) error {
	values := []struct {
		name string
		id   uuid.UUID
	}{
		{"job_id", f.JobID},
		{"attempt_id", f.AttemptID},
		{"lease_id", f.LeaseID},
		{"worker_id", f.WorkerID},
		{"worker_session_id", f.SessionID},
	}
	var fields []FieldError
	for _, value := range values {
		if value.id == uuid.Nil {
			fields = append(fields, FieldError{Field: value.name, Message: "must be a UUID"})
		}
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

func canonicalSet(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
