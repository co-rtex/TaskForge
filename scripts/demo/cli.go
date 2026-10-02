package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
)

// cliClient runs the real taskforge-cli, which is how this program talks to the
// API wherever the CLI gives machine-readable output. Every response is JSON on
// stdout; every failure is a JSON error on stderr and a non-zero exit.
type cliClient struct {
	bin    string
	apiURL string
	dir    string
}

// exec runs one CLI command and returns its stdout. The credential travels in
// the child's environment, not its arguments, so it never appears in a process
// listing.
func (c *cliClient) exec(ctx context.Context, apiKey string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, c.bin, args...)
	cmd.Dir = c.dir
	cmd.Env = []string{"TASKFORGE_CLI_API_URL=" + c.apiURL}
	if apiKey != "" {
		cmd.Env = append(cmd.Env, "TASKFORGE_CLI_API_KEY="+apiKey)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("taskforge-cli %s: exit %d: %s", args[0]+" "+args[1], exitErr.ExitCode(), truncate(strings.TrimSpace(stderr.String()), 400))
		}
		return nil, fmt.Errorf("taskforge-cli %s: %w", args[0]+" "+args[1], err)
	}
	return stdout.Bytes(), nil
}

// json runs one CLI command and decodes its JSON output into out.
func (c *cliClient) json(ctx context.Context, apiKey string, out any, args ...string) error {
	body, err := c.exec(ctx, apiKey, args...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("taskforge-cli %s: unreadable response: %w", args[0]+" "+args[1], err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// The views below name only the fields this program reads, taken from
// api/openapi.yaml. They are deliberately not the server's own types: this is a
// client, and a field it does not read is one it cannot be broken by.

type jobView struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	JobType     string `json:"job_type"`
	MaxAttempts int    `json:"max_attempts"`
}

type attemptView struct {
	ID            string     `json:"id"`
	AttemptNumber int        `json:"attempt_number"`
	Status        string     `json:"status"`
	WorkerID      string     `json:"worker_id"`
	WorkerName    string     `json:"worker_name"`
	FailureClass  *string    `json:"failure_class"`
	ErrorCode     *string    `json:"error_code"`
	RetryDelayMS  *int64     `json:"retry_delay_ms"`
	RetryAt       *time.Time `json:"retry_at"`
}

type dlqEntryView struct {
	JobID     string  `json:"job_id"`
	Reason    string  `json:"reason"`
	ErrorCode *string `json:"error_code"`
}

type dlqPageView struct {
	Entries    []dlqEntryView `json:"entries"`
	NextCursor string         `json:"next_cursor"`
}

// submit creates a job through taskforge-cli and returns its id. The payload is
// a JSON object given as text.
func (d *demo) submit(ctx context.Context, jobType, payload string, maxAttempts, timeoutSeconds int) (string, error) {
	var job jobView
	err := d.cli.json(ctx, d.apiKey, &job, "jobs", "submit",
		"--queue", "default", "--job-type", jobType, "--payload", payload,
		"--max-attempts", fmt.Sprint(maxAttempts), "--timeout-seconds", fmt.Sprint(timeoutSeconds))
	if err != nil {
		return "", err
	}
	if _, err := uuid.Parse(job.ID); err != nil {
		return "", fmt.Errorf("submit returned an unusable job id %q", job.ID)
	}
	return job.ID, nil
}

func (d *demo) job(ctx context.Context, id string) (jobView, error) {
	var job jobView
	err := d.cli.json(ctx, d.apiKey, &job, "jobs", "get", id)
	return job, err
}

func (d *demo) attempts(ctx context.Context, id string) ([]attemptView, error) {
	var list struct {
		Attempts []attemptView `json:"attempts"`
	}
	err := d.cli.json(ctx, d.apiKey, &list, "jobs", "attempts", id)
	return list.Attempts, err
}

// result returns the exact bytes the handler produced for a job.
func (d *demo) result(ctx context.Context, id string) (json.RawMessage, error) {
	return d.cli.exec(ctx, d.apiKey, "jobs", "result", id)
}

// dlqEntry finds the dead-letter entry for one job, or nil if there is none.
// The listing is limited to this run's scope by the key it is read with, and is
// then filtered to the one job asked about.
func (d *demo) dlqEntry(ctx context.Context, jobID string) (*dlqEntryView, error) {
	cursor := ""
	for page := 0; page < 20; page++ {
		args := []string{"dlq", "list", "--limit", "100"}
		if cursor != "" {
			args = append(args, "--cursor", cursor)
		}
		var list dlqPageView
		if err := d.cli.json(ctx, d.apiKey, &list, args...); err != nil {
			return nil, err
		}
		for i := range list.Entries {
			if list.Entries[i].JobID == jobID {
				return &list.Entries[i], nil
			}
		}
		if list.NextCursor == "" {
			return nil, nil
		}
		cursor = list.NextCursor
	}
	return nil, fmt.Errorf("the dead-letter listing did not end after 20 pages")
}

// --- facts the public API deliberately does not expose --------------------------
//
// api/openapi.yaml withholds session identifiers from the attempt timeline
// ("publishing one here would hand an authenticated public caller an identifier
// that surface treats as authority"), and serves a job's result as the handler's
// bytes alone, with no attempt id. Both are facts the failure demonstration is
// about, so they are read from PostgreSQL, read-only, and only for this run's
// job ids and this run's scope.

// attemptBinding is one attempt row joined to the process session that ran it.
type attemptBinding struct {
	AttemptNumber int
	AttemptID     uuid.UUID
	SessionID     uuid.UUID
	Status        string
}

func (d *demo) attemptBindings(ctx context.Context, jobID string) ([]attemptBinding, error) {
	rows, err := d.db.Query(ctx, `
		SELECT attempt_number, id, worker_session_id, status
		FROM job_attempts
		WHERE job_id = $1 AND scope = $2
		ORDER BY attempt_number`, jobID, d.scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []attemptBinding
	for rows.Next() {
		var b attemptBinding
		if err := rows.Scan(&b.AttemptNumber, &b.AttemptID, &b.SessionID, &b.Status); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// workerSessions returns every process session recorded for one of this run's
// workers, by its run-unique name.
func (d *demo) workerSessions(ctx context.Context, workerName string) ([]uuid.UUID, error) {
	rows, err := d.db.Query(ctx, `
		SELECT s.id
		FROM worker_sessions s
		JOIN workers w ON w.id = s.worker_id AND w.scope = s.scope
		WHERE w.scope = $1 AND w.name = $2`, d.scope, workerName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// resultOwners returns the attempt id of every stored result for the job. The
// table's primary key is the job id, so there is at most one.
func (d *demo) resultOwners(ctx context.Context, jobID string) ([]uuid.UUID, error) {
	rows, err := d.db.Query(ctx,
		`SELECT attempt_id FROM results WHERE job_id = $1 AND scope = $2`, jobID, d.scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
