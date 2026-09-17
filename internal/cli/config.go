package cli

import "strings"

// defaultAPIAddr is used only when neither --api-url nor TASKFORGE_API_ADDR
// is set. It matches internal/config.Config's own default for
// TASKFORGE_API_ADDR (internal/config/config.go: `env("TASKFORGE_API_ADDR",
// "127.0.0.1:8080")`), so a developer who has changed nothing runs
// taskforge-cli against the same address taskforge-api binds to out of the
// box.
const defaultAPIAddr = "127.0.0.1:8080"

// ResolveBaseURL decides which API address taskforge-cli talks to.
//
// Precedence: flagValue (--api-url), then envValue (TASKFORGE_API_ADDR),
// then defaultAPIAddr.
//
// TASKFORGE_API_ADDR is documented everywhere else in this repository as a
// BIND address for taskforge-api's own listener -- internal/config.go
// reads it with that name and internal/config.Config.Validate requires it
// to be loopback ("TASKFORGE_API_ADDR must bind to a loopback address
// until authentication is implemented"), and .env.example carries exactly
// one line for it, shared by whatever process reads it. taskforge-cli
// reuses that exact env-var name deliberately, rather than inventing a
// second variable naming the same address, so an operator's existing
// .env needs no new entry to point the CLI at the same server it already
// runs locally.
//
// A bind spec and a client target are different shapes, though: a bind
// address is bare "host:port" with no scheme, and a client needs a full
// URL. A value that already names a scheme (e.g. a future non-local
// deployment's "https://api.example.com") is used exactly as given; a bare
// "host:port" -- what every existing TASKFORGE_API_ADDR value in this
// repository actually looks like -- is interpreted as "http://" plus that
// value, since nothing in this project runs the API over TLS today (there
// is no TLS configuration anywhere in this codebase, and ADR-0013's
// "Alternatives considered" rejected mTLS for V1's stated experience).
//
// Loopback is safe to assume as the default -- but only as a DEFAULT, which
// is why --api-url exists as an unconditional override. docs/PROJECT_SPEC.md
// §4 item 1 describes V1 as "Clone TaskForge and start it with Docker
// Compose and Make" on a local machine, and §5's success criteria requires
// "The full local stack starts from a clean clone with only Git, Go,
// Docker, Docker Compose, and Make installed" -- V1 has no non-local
// deployment target, and TASKFORGE_API_ADDR's own server-side validation
// (internal/config.go's isLoopbackBind check) enforces that the address
// taskforge-api itself binds to is always loopback. --api-url is what lets
// a later, non-local deployment milestone change where this CLI points
// without changing how it talks to the API at all.
func ResolveBaseURL(flagValue, envValue string) string {
	value := flagValue
	if value == "" {
		value = envValue
	}
	if value == "" {
		value = defaultAPIAddr
	}
	value = strings.TrimRight(value, "/")
	if strings.Contains(value, "://") {
		return value
	}
	return "http://" + value
}
