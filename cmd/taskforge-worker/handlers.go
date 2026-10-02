package main

import (
	"fmt"

	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
)

// registerTrustedHandlers registers every handler compiled into this binary.
//
// This list is the trusted-handler boundary AGENTS.md section 10 draws: the job
// types a session declares at registration, and so the only job types this
// process will ever claim and execute. Nothing is loaded, configured, or
// uploaded at runtime, and there is no environment gate on it. ADR-0019 records
// why demo.sleep and demo.fail are on it unconditionally, and what bounds them.
//
// It is one function, rather than lines in run, so a test can pin the exact set
// without starting a process.
func registerTrustedHandlers(registry *workerruntime.Registry) error {
	for _, handler := range []struct {
		jobType string
		handler workerruntime.Handler
	}{
		{"demo.echo", workerruntime.DemoEcho{}},
		{"demo.fail", workerruntime.DemoFail{}},
		{"demo.sleep", workerruntime.DemoSleep{}},
	} {
		if err := registry.Register(handler.jobType, handler.handler); err != nil {
			return fmt.Errorf("register trusted handler %s: %w", handler.jobType, err)
		}
	}
	return nil
}
