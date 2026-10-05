package stack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// CLI runs the real taskforge-cli, which is how a stack's program talks to the
// API wherever the CLI gives machine-readable output. Every response is JSON on
// stdout; every failure is a JSON error on stderr and a non-zero exit.
type CLI struct {
	Bin    string
	APIURL string
	Dir    string
}

// Exec runs one CLI command and returns its stdout. The credential travels in
// the child's environment, not its arguments, so it never appears in a process
// listing.
func (c *CLI) Exec(ctx context.Context, apiKey string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, c.Bin, args...)
	cmd.Dir = c.Dir
	cmd.Env = []string{"TASKFORGE_CLI_API_URL=" + c.APIURL}
	if apiKey != "" {
		cmd.Env = append(cmd.Env, "TASKFORGE_CLI_API_KEY="+apiKey)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("taskforge-cli %s: exit %d: %s", args[0]+" "+args[1], exitErr.ExitCode(), Truncate(strings.TrimSpace(stderr.String()), 400))
		}
		return nil, fmt.Errorf("taskforge-cli %s: %w", args[0]+" "+args[1], err)
	}
	return stdout.Bytes(), nil
}

// JSON runs one CLI command and decodes its JSON output into out.
func (c *CLI) JSON(ctx context.Context, apiKey string, out any, args ...string) error {
	body, err := c.Exec(ctx, apiKey, args...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("taskforge-cli %s: unreadable response: %w", args[0]+" "+args[1], err)
	}
	return nil
}

// Truncate shortens s to at most n bytes, marking the cut.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
