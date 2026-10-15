// Package healthcheck is the `healthcheck` subcommand every long-running
// TaskForge binary carries: it GETs the process's own loopback liveness probe
// and turns the answer into an exit code.
//
// It exists because the images are distroless (ADR-0021): there is no shell,
// no curl and no wget for a container orchestrator's health check to run, so
// the binary checks itself. `taskforge-api healthcheck` exits 0 when the probe
// answers 200 and 1 otherwise, and prints one line saying which. See
// docs/adr/0026-listeners-by-surface.md.
package healthcheck

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// Subcommand is the argument that selects the health check instead of the
// service.
const Subcommand = "healthcheck"

// Timeout bounds the whole check, connection included, well inside the
// interval any orchestrator probes at. A probe that answers slower than this
// is reported unhealthy, which is what a health check is for.
const Timeout = 3 * time.Second

// ProbePath is the liveness probe every TaskForge process serves on its own
// loopback address. Liveness, not readiness, on purpose: an orchestrator
// replaces an unhealthy container, and a process whose database is briefly
// unreachable is not one to replace.
const ProbePath = "/healthz"

// Requested reports whether a process was started as `<binary> healthcheck`.
func Requested(args []string) bool {
	return len(args) > 1 && args[1] == Subcommand
}

// Probe GETs http://addr/healthz and returns nil only for a 200.
func Probe(ctx context.Context, addr string) error {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return fmt.Errorf("probe address %q is not host:port: %w", addr, err)
	}
	url := "http://" + addr + ProbePath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build the probe request: %w", err)
	}
	// A client of its own: no proxy from the environment (the probe is the
	// process's own loopback address) and no keep-alive to hold open.
	client := &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s answered %d", url, resp.StatusCode)
	}
	return nil
}

// Run is the subcommand: it probes addr and returns the exit code, writing one
// line to out on success and one to errOut on failure. A configuration error is
// passed in as configErr and is itself a failure: a process whose configuration
// is invalid is not running, so it is not healthy.
func Run(ctx context.Context, service, addr string, configErr error, out, errOut io.Writer) int {
	if configErr != nil {
		fmt.Fprintf(errOut, "%s healthcheck: unhealthy: configuration invalid: %v\n", service, configErr)
		return 1
	}
	if err := Probe(ctx, addr); err != nil {
		fmt.Fprintf(errOut, "%s healthcheck: unhealthy: %v\n", service, err)
		return 1
	}
	fmt.Fprintf(out, "%s healthcheck: healthy (%s%s answered 200)\n", service, addr, ProbePath)
	return 0
}
