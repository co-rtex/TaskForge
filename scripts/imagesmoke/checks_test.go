package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A user is root if it is empty (an image with no USER runs as root), "root", or
// uid 0, with or without a group. Distroless' nonroot reports "65532".
func TestUserProblem(t *testing.T) {
	for _, ok := range []string{"65532", "65532:65532", "nonroot", "nonroot:nonroot", "1000", "1000:0"} {
		require.Emptyf(t, userProblem(ok), "%q is not root", ok)
	}
	for _, root := range []string{"", "0", "00", "0:0", "root", "root:root", "0:65532", "root:nonroot", " 0 "} {
		require.NotEmptyf(t, userProblem(root), "%q is root", root)
	}
	require.Contains(t, userProblem(""), "no USER")
}

func TestLabelProblems(t *testing.T) {
	good := map[string]string{
		"org.opencontainers.image.source":   "https://github.com/co-rtex/TaskForge",
		"org.opencontainers.image.revision": "a59bf67d17066abfefe057ae5c4dc3d638340677",
	}
	require.Empty(t, labelProblems(good, "a59bf67d17066abfefe057ae5c4dc3d638340677"))

	require.NotEmpty(t, labelProblems(map[string]string{}, "a59bf67"), "both labels missing")

	wrongRev := map[string]string{"org.opencontainers.image.source": good["org.opencontainers.image.source"], "org.opencontainers.image.revision": "unknown"}
	problems := labelProblems(wrongRev, "a59bf67d17066abfefe057ae5c4dc3d638340677")
	require.NotEmpty(t, problems)
	require.Contains(t, problems[0], "revision")

	wrongSource := map[string]string{"org.opencontainers.image.source": "https://example.invalid/x", "org.opencontainers.image.revision": good["org.opencontainers.image.revision"]}
	require.NotEmpty(t, labelProblems(wrongSource, good["org.opencontainers.image.revision"]))
}

func TestEntrypointProblem(t *testing.T) {
	require.Empty(t, entrypointProblem([]string{"/taskforge-api"}, "api"))
	require.NotEmpty(t, entrypointProblem([]string{"/taskforge-worker"}, "api"), "another service's binary")
	require.NotEmpty(t, entrypointProblem([]string{"/bin/sh", "-c", "/taskforge-api"}, "api"), "a shell in the way")
	require.NotEmpty(t, entrypointProblem(nil, "api"), "no entrypoint")
}

// The rejection test needs the process to have actually run and refused. Docker's
// own failures (125 the daemon, 126 not executable, 127 not found) are not the
// service refusing its configuration.
func TestRejectionProblem(t *testing.T) {
	const want = "TASKFORGE_API_ADDR must bind to a loopback address"
	logLine := `{"level":"ERROR","msg":"configuration invalid","error":"invalid configuration: ` + want + ` until authentication is implemented"}`

	require.Empty(t, rejectionProblem(1, logLine, want))
	require.NotEmpty(t, rejectionProblem(0, logLine, want), "it must exit non-zero")
	require.NotEmpty(t, rejectionProblem(1, `{"msg":"connect to database"}`, want), "it must say why")
	for _, dockerFailure := range []int{125, 126, 127} {
		require.NotEmptyf(t, rejectionProblem(dockerFailure, logLine, want), "exit %d is docker failing, not the service refusing", dockerFailure)
	}
	require.NotEmpty(t, rejectionProblem(2, logLine, ""), "a check with no expected message proves nothing")
}

func TestMigrationOutcome(t *testing.T) {
	applied := `{"level":"INFO","msg":"migration applied","version":17}` + "\n" +
		`{"level":"INFO","msg":"migrations complete","applied":18,"service":"taskforge-migrate"}` + "\n"
	got, err := migrationOutcome(applied)
	require.NoError(t, err)
	require.Equal(t, outcomeApplied, got.kind)
	require.Equal(t, 18, got.applied)

	current, err := migrationOutcome(`{"level":"INFO","msg":"schema already up to date","service":"taskforge-migrate"}` + "\n")
	require.NoError(t, err)
	require.Equal(t, outcomeCurrent, current.kind)

	_, err = migrationOutcome(`{"level":"ERROR","msg":"migration failed","error":"connect for migration: refused"}` + "\n")
	require.Error(t, err)
	_, err = migrationOutcome("")
	require.Error(t, err, "no output is not a migration")

	// An error line beside a success line is still a failure.
	_, err = migrationOutcome(`{"level":"INFO","msg":"migrations complete","applied":1}` + "\n" + `{"level":"ERROR","msg":"something"}` + "\n")
	require.Error(t, err)
}

func TestExpectedSchema(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"0001_a.sql", "0002_b.sql", "0017_c.sql", "embed.go", "README.md"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o600))
	}
	count, highest, err := expectedSchema(dir)
	require.NoError(t, err)
	require.Equal(t, 3, count, "only NNNN_*.sql files count")
	require.Equal(t, 17, highest)

	_, _, err = expectedSchema(t.TempDir())
	require.Error(t, err, "an empty directory would make any schema look right")

	// And against the repository's real migrations, which are what the migrate
	// image embeds.
	count, highest, err = expectedSchema(filepath.Join("..", "..", "migrations"))
	require.NoError(t, err)
	require.Positive(t, count)
	require.Equal(t, count, highest, "migrations are numbered 1..n with no gaps")
}

func TestSchemaProblem(t *testing.T) {
	require.Empty(t, schemaProblem(18, 18, 18, 18))
	require.NotEmpty(t, schemaProblem(17, 17, 18, 18), "one migration short")
	require.NotEmpty(t, schemaProblem(18, 19, 18, 18), "a version the image does not embed")
	require.NotEmpty(t, schemaProblem(0, 0, 18, 18), "nothing applied")
}

// Each image carries exactly its own service binary. The base brings a hundred
// files of its own; the check is on the taskforge-* names.
func TestBinaryProblems(t *testing.T) {
	base := []string{".dockerenv", "etc/passwd", "etc/ssl/certs/ca-certificates.crt", "usr/share/common-licenses/GPL-3"}

	require.Empty(t, binaryProblems(append([]string{"taskforge-api"}, base...), "api"))
	require.NotEmpty(t, binaryProblems(base, "api"), "its own binary is missing")
	require.NotEmpty(t, binaryProblems(append([]string{"taskforge-api", "taskforge-worker"}, base...), "api"), "another service's binary")
	require.NotEmpty(t, binaryProblems(append([]string{"taskforge-api", "taskforge-cli"}, base...), "api"), "there is no CLI image")
	require.NotEmpty(t, binaryProblems(append([]string{"taskforge-api", "app/taskforge-outbox"}, base...), "api"), "a binary under another path")
	require.NotEmpty(t, binaryProblems(append([]string{"taskforge-api", "bin/sh"}, base...), "api"), "a shell")
}

const builtIndex = `<!doctype html>
<html lang="en">
  <head>
    <meta name="taskforge-dashboard" content="built" />
    <script type="module" crossorigin src="/dashboard/assets/index-K4xx8Mbf.js"></script>
    <link rel="stylesheet" crossorigin href="/dashboard/assets/index-2BUk1Y-J.css">
  </head>
  <body><div id="root"></div></body>
</html>`

const placeholderIndex = `<!doctype html>
<html lang="en">
  <head>
    <meta name="taskforge-dashboard" content="placeholder" />
    <title>TaskForge dashboard — not built</title>
  </head>
  <body><main><h1>The TaskForge dashboard has not been built</h1></main></body>
</html>`

func TestDashboardProblem(t *testing.T) {
	require.Empty(t, dashboardProblem(builtIndex))
	require.NotEmpty(t, dashboardProblem(placeholderIndex), "the placeholder is exactly what must be refused")
	require.NotEmpty(t, dashboardProblem(""), "an empty page is not a dashboard")
	require.NotEmpty(t, dashboardProblem(`<meta name="taskforge-dashboard" content="built" />`),
		"the marker alone proves nothing: a real build also references a hashed script")
	require.NotEmpty(t, dashboardProblem(`<html><script src="/dashboard/assets/index-abc.js"></script></html>`),
		"a script without the marker is not proven to be the build")
}

func TestAssetPaths(t *testing.T) {
	scripts, styles := assetPaths(builtIndex)
	require.Equal(t, []string{"/dashboard/assets/index-K4xx8Mbf.js"}, scripts)
	require.Equal(t, []string{"/dashboard/assets/index-2BUk1Y-J.css"}, styles)

	scripts, styles = assetPaths(placeholderIndex)
	require.Empty(t, scripts)
	require.Empty(t, styles)
}

// On Docker Desktop a container cannot reach the host's loopback as 127.0.0.1, so
// the endpoints the smoke was pointed at are rewritten to host.docker.internal.
// On Linux CI the containers share the host's network and nothing is rewritten.
func TestEndpointRewrite(t *testing.T) {
	env := map[string]string{
		"TASKFORGE_DATABASE_URL":     "postgres://taskforge:taskforge@127.0.0.1:5442/taskforge?sslmode=disable",
		"TASKFORGE_RESULTS_ENDPOINT": "http://localhost:4566",
		"TASKFORGE_BROKER_ENDPOINT":  "http://[::1]:9324",
		"TASKFORGE_RESULTS_BUCKET":   "taskforge-results",
	}
	desktop := rewriteForDesktop(env)
	require.Equal(t, "postgres://taskforge:taskforge@host.docker.internal:5442/taskforge?sslmode=disable", desktop["TASKFORGE_DATABASE_URL"])
	require.Equal(t, "http://host.docker.internal:4566", desktop["TASKFORGE_RESULTS_ENDPOINT"])
	require.Equal(t, "http://host.docker.internal:9324", desktop["TASKFORGE_BROKER_ENDPOINT"])
	require.Equal(t, "taskforge-results", desktop["TASKFORGE_RESULTS_BUCKET"], "only hosts are rewritten")
	require.Equal(t, "http://localhost:4566", env["TASKFORGE_RESULTS_ENDPOINT"], "the input is not modified")

	remote := map[string]string{"TASKFORGE_DATABASE_URL": "postgres://u:p@db.example.invalid:5432/x"}
	require.Equal(t, remote, rewriteForDesktop(remote), "a host that is not loopback is left alone")
}

func TestBuilderImage(t *testing.T) {
	dockerfile := "# comment\nFROM golang:1.25.0@sha256:" + hex64 + " AS builder\nWORKDIR /src\nFROM gcr.io/distroless/static-debian12:nonroot@sha256:" + hex64 + " AS runtime\n"
	got, err := builderImage(dockerfile)
	require.NoError(t, err)
	require.Equal(t, "golang:1.25.0@sha256:"+hex64, got)

	_, err = builderImage("FROM scratch\n")
	require.Error(t, err)
}

const hex64 = "5502b0e56fca23feba76dbc5387ba59c593c02ccc2f0f7355871ea9a0852cebe"
