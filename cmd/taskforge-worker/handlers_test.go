package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	workerruntime "github.com/co-rtex/TaskForge/internal/worker"
)

// TestRegisterTrustedHandlers_PinsTheExactSetTheWorkerDeclares is the guard on
// what AGENTS.md section 10 calls the trusted-handler boundary. The set this
// returns is what a session declares at registration, so it is also the whole
// of what a key holder can name as a job_type and have this binary execute.
//
// It is an exact comparison on purpose. A handler added to the worker has to be
// added here too, which puts the change in front of a reviewer as a one-line
// diff to a list of three, and a handler dropped from the worker (the demo
// targets would then leave jobs QUEUED forever, because a job type no session
// declared is never claimed) fails here instead of in a demonstration.
func TestRegisterTrustedHandlers_PinsTheExactSetTheWorkerDeclares(t *testing.T) {
	registry := workerruntime.NewRegistry()

	require.NoError(t, registerTrustedHandlers(registry))

	require.Equal(t, []string{"demo.echo", "demo.fail", "demo.sleep"}, registry.Types())
}

func TestRegisterTrustedHandlers_BindsEachJobTypeToItsOwnHandler(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registerTrustedHandlers(registry))

	for jobType, want := range map[string]workerruntime.Handler{
		"demo.echo":  workerruntime.DemoEcho{},
		"demo.fail":  workerruntime.DemoFail{},
		"demo.sleep": workerruntime.DemoSleep{},
	} {
		got, ok := registry.Lookup(jobType)
		require.Truef(t, ok, "%s must be registered", jobType)
		require.Equalf(t, want, got, "%s must run its own handler, not another's", jobType)
	}
}

// Registering the set twice is a programming error the registry already refuses
// for a single type; this pins that the function surfaces it rather than
// swallowing it, since main exits on the error it returns.
func TestRegisterTrustedHandlers_ReportsADuplicateRegistration(t *testing.T) {
	registry := workerruntime.NewRegistry()
	require.NoError(t, registerTrustedHandlers(registry))

	require.Error(t, registerTrustedHandlers(registry))
}
