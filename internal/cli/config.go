package cli

import (
	"fmt"
	"net/url"
	"strings"
)

// defaultAPIAddr is the host:port taskforge-api binds to out of the box. It
// matches internal/config.Config's own default for TASKFORGE_API_ADDR
// (internal/config/config.go: `env("TASKFORGE_API_ADDR", "127.0.0.1:8080")`),
// so a developer who has changed nothing runs taskforge-cli against the
// address taskforge-api is actually listening on. Only the VALUE is shared;
// see ResolveBaseURL for why the environment variable is not.
const defaultAPIAddr = "127.0.0.1:8080"

// APIURLEnv is the environment variable taskforge-cli reads for the API it
// talks to.
const APIURLEnv = "TASKFORGE_CLI_API_URL"

// ResolveBaseURL decides which API address taskforge-cli talks to.
//
// Precedence: flagValue (--api-url), then envValue (TASKFORGE_CLI_API_URL),
// then "http://" + defaultAPIAddr. Whichever is chosen must be an absolute
// http(s) URL with a host; anything else is an error, which Run reports as
// a usage error before any request is made.
//
// TASKFORGE_CLI_API_URL is a purpose-built client-target variable, and
// deliberately NOT a reuse of TASKFORGE_API_ADDR. That variable is
// taskforge-api's own BIND address: a bare "host:port" with no scheme,
// validated by internal/config.Config.Validate to be a loopback bind. A bind
// address and a reachable client target are different shapes in general
// (a server can bind 0.0.0.0 or a wildcard that no client can dial, and a
// client needs a scheme), so one name carrying both meanings would make each
// binary's reading of a shared .env line depend on which binary is reading
// it. This repository already has the established pattern for a
// client-facing address: TASKFORGE_WORKER_API_URL, a full URL read by
// taskforge-worker and validated as "an absolute http(s) URL". This variable
// follows that naming and that validation shape.
//
// It differs from the worker's rule in one deliberate way: it does not
// additionally require a loopback host. The worker's requirement is a
// permanent posture for a process that presents a worker key and whose own
// health endpoint is never exposed off-host (internal/config.go, WorkerConfig.Validate).
// For this CLI, --api-url exists precisely so a later, non-local deployment
// milestone only has to change where the CLI points, not how it talks to the
// API. Loopback is the DEFAULT, not a constraint: docs/PROJECT_SPEC.md §4
// item 1 describes V1 as "Clone TaskForge and start it with Docker Compose
// and Make" and §5's success criteria require "The full local stack starts
// from a clean clone with only Git, Go, Docker, Docker Compose, and Make
// installed" -- V1 has no non-local deployment target, and the server side
// enforces loopback itself (TASKFORGE_API_ADDR's isLoopbackBind check, and
// the loopback-only /internal/v1 routes per docs/adr/0013).
func ResolveBaseURL(flagValue, envValue string) (string, error) {
	value, source := flagValue, "--api-url"
	if value == "" {
		value, source = envValue, APIURLEnv
	}
	if value == "" {
		return "http://" + defaultAPIAddr, nil
	}
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("%s must be an absolute http(s) URL, for example http://%s", source, defaultAPIAddr)
	}
	return strings.TrimRight(value, "/"), nil
}
