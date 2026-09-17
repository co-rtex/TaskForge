// Package cli implements taskforge-cli's command dispatch, HTTP client, and
// output/exit-code contract.
//
// It is a pure consumer of the public ("/v1") and loopback-only internal
// key-management ("/internal/v1/api-keys", "/internal/v1/worker-keys")
// surfaces documented in api/openapi.yaml. It contains no domain logic, no
// credential generation or verification (that stays server-side in
// internal/auth and internal/workerauth, per docs/adr/0013 and 0014), and
// depends on no other internal/ package -- it only ever obtains and
// presents a credential it was given.
//
// cmd/taskforge-cli/main.go is wiring only (AGENTS.md section 3): it calls
// Run and exits with the code it returns. Everything else lives here so it
// can be tested without an OS process boundary.
package cli
