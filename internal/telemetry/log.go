// Package telemetry configures structured logging.
//
// Logs are JSON so they can be shipped and queried. They carry correlation
// identifiers (request id, job id, event id) and never carry secrets or
// unbounded request payloads — see AGENTS.md section 10.
package telemetry

import (
	"io"
	"log/slog"
	"strings"
)

// ParseLevel maps a configuration string to a slog level, defaulting to info
// for anything unrecognized so that a typo degrades to a sane level rather than
// silencing the process.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewLogger builds a JSON logger tagged with the emitting service name.
func NewLogger(w io.Writer, level string, service string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: ParseLevel(level)})
	return slog.New(h).With(slog.String("service", service))
}
