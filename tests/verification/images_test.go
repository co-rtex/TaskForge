package verification

// These checks read the root Dockerfile, .dockerignore, go.mod and the Makefile.
// They need no Docker daemon, so they run under `make test-unit` and in the fast
// CI job. What they prove is that the image definitions say what
// docs/adr/0021-container-images-and-supply-chain-scanning.md says they say. What
// an image actually contains and does is proved by building it and running
// scripts/imagesmoke, which is the other half of the same promise.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	dockerfilePath   = "Dockerfile"
	dockerignorePath = ".dockerignore"
	goModPath        = "go.mod"
	makefilePath     = "Makefile"
)

// imageServices are the six services that get an image, in the order the ADR lists
// them. There is deliberately no CLI image.
var imageServices = []string{"api", "outbox", "scheduler", "reconciler", "worker", "migrate"}

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot, rel))
	require.NoError(t, err, "%s must exist", rel)
	return string(raw)
}

// stage is one stage of the Dockerfile: its name, its base, and its instructions
// with comments and blank lines removed and line continuations joined.
type stage struct {
	name         string
	base         string
	instructions []string
}

var fromLine = regexp.MustCompile(`(?i)^FROM\s+(?:--platform=\S+\s+)?(\S+)(?:\s+AS\s+(\S+))?\s*$`)

func parseDockerfile(t *testing.T, text string) []stage {
	t.Helper()
	var stages []stage
	var pending string
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasSuffix(line, "\\") {
			pending += strings.TrimSuffix(line, "\\") + " "
			continue
		}
		line, pending = pending+line, ""
		if m := fromLine.FindStringSubmatch(line); m != nil {
			stages = append(stages, stage{name: m[2], base: m[1]})
			continue
		}
		require.NotEmpty(t, stages, "an instruction before the first FROM: %q", line)
		last := &stages[len(stages)-1]
		last.instructions = append(last.instructions, line)
	}
	return stages
}

func stageNamed(stages []stage, name string) (stage, bool) {
	for _, s := range stages {
		if s.name == name {
			return s, true
		}
	}
	return stage{}, false
}

// externalBases are the FROM references that name a registry image: not an
// earlier stage and not scratch.
func externalBases(stages []stage) []string {
	names := map[string]bool{}
	for _, s := range stages {
		if s.name != "" {
			names[s.name] = true
		}
	}
	var out []string
	for _, s := range stages {
		if s.base == "scratch" || names[s.base] {
			continue
		}
		out = append(out, s.base)
	}
	return out
}

var goDirective = regexp.MustCompile(`(?m)^go\s+(\d+\.\d+(?:\.\d+)?)\s*$`)

func TestDockerfile_BuilderGoVersionIsExactlyGoMods(t *testing.T) {
	want := goDirective.FindStringSubmatch(readRepoFile(t, goModPath))
	require.NotNil(t, want, "go.mod must declare a go directive")

	stages := parseDockerfile(t, readRepoFile(t, dockerfilePath))
	builder, ok := stageNamed(stages, "builder")
	require.True(t, ok, "the Dockerfile needs a stage named builder")

	// golang:<version>[-variant]@sha256:... : the version is everything between the
	// colon and the first dash or at-sign.
	m := regexp.MustCompile(`^golang:(\d+\.\d+(?:\.\d+)?)(?:-[a-z0-9.]+)?@sha256:`).FindStringSubmatch(builder.base)
	require.NotNil(t, m, "the builder must be golang:<version>@sha256:<digest>, got %q", builder.base)

	require.Equal(t, want[1], m[1],
		"the Dockerfile builds with Go %s and go.mod declares %s: the shipped binary would be compiled by a different toolchain than CI tests",
		m[1], want[1])
}

func TestDockerfile_EveryBaseImageIsPinnedByVersionAndByDigest(t *testing.T) {
	bases := externalBases(parseDockerfile(t, readRepoFile(t, dockerfilePath)))
	require.NotEmpty(t, bases)

	pinned := regexp.MustCompile(`^[a-z0-9./_-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$`)
	for _, base := range bases {
		require.Regexpf(t, pinned, base, "%q must be name:tag@sha256:<digest>; a tag alone can be re-pointed", base)
	}
}

func TestDockerfile_EachBaseImageIsPinnedInExactlyOnePlace(t *testing.T) {
	text := readRepoFile(t, dockerfilePath)
	stages := parseDockerfile(t, text)

	seen := map[string]int{}
	for _, base := range externalBases(stages) {
		repo := strings.SplitN(base, ":", 2)[0]
		seen[repo]++
	}
	require.Equal(t, map[string]int{
		"golang":                            1,
		"gcr.io/distroless/static-debian12": 1,
	}, seen, "one FROM per base image: a second pin is a second place to forget to bump")

	// And no digest is written down anywhere but a FROM line.
	digest := regexp.MustCompile(`sha256:[0-9a-f]{64}`)
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || fromLine.MatchString(trimmed) {
			continue
		}
		require.Falsef(t, digest.MatchString(trimmed), "a digest outside a FROM line: %q", trimmed)
	}
}

func TestDockerfile_HasExactlyOneTargetPerServiceAndNoCLIImage(t *testing.T) {
	stages := parseDockerfile(t, readRepoFile(t, dockerfilePath))

	var targets []string
	for _, s := range stages {
		if s.name != "builder" && s.name != "runtime" {
			targets = append(targets, s.name)
		}
	}
	require.ElementsMatch(t, imageServices, targets, "one final target per service, and nothing else")
	require.NotContains(t, targets, "cli")
}

func TestDockerfile_EachTargetCarriesOnlyItsOwnBinaryAndRunsIt(t *testing.T) {
	stages := parseDockerfile(t, readRepoFile(t, dockerfilePath))

	for _, svc := range imageServices {
		s, ok := stageNamed(stages, svc)
		require.Truef(t, ok, "target %s", svc)
		require.Equalf(t, "runtime", s.base, "target %s builds on the one runtime stage, so the final base is pinned once", svc)

		var copies, entrypoints []string
		for _, ins := range s.instructions {
			upper := strings.ToUpper(ins)
			switch {
			case strings.HasPrefix(upper, "COPY"):
				copies = append(copies, ins)
			case strings.HasPrefix(upper, "ENTRYPOINT"):
				entrypoints = append(entrypoints, ins)
			}
		}
		require.Lenf(t, copies, 1, "target %s copies exactly one thing: %v", svc, copies)
		require.Containsf(t, copies[0], "--from=builder", "target %s", svc)
		require.Containsf(t, copies[0], "/out/taskforge-"+svc, "target %s copies its own binary", svc)
		require.NotContainsf(t, copies[0], "dist", "target %s", svc)

		require.Lenf(t, entrypoints, 1, "target %s", svc)
		require.Equalf(t, `ENTRYPOINT ["/taskforge-`+svc+`"]`, entrypoints[0], "target %s uses the exec form, with no shell to put in the way", svc)
	}
}

func TestDockerfile_BuildsStaticAndTrimmed(t *testing.T) {
	stages := parseDockerfile(t, readRepoFile(t, dockerfilePath))
	builder, _ := stageNamed(stages, "builder")

	var builds []string
	for _, ins := range builder.instructions {
		if strings.Contains(ins, "go build") {
			builds = append(builds, ins)
		}
	}
	require.NotEmpty(t, builds, "the builder must run go build")
	for _, ins := range builds {
		require.Contains(t, ins, "CGO_ENABLED=0", "a distroless static image has no libc to link against")
		require.Contains(t, ins, "-trimpath", "build paths must not leak into the binary")
		for _, svc := range imageServices {
			require.Contains(t, ins, "./cmd/taskforge-"+svc)
		}
		require.NotContains(t, ins, "./cmd/taskforge-cli", "there is no CLI image")
		require.NotContains(t, ins, "./cmd/...", "name the six commands; ./cmd/... would add the CLI")
	}
}

func TestDockerfile_LabelsEveryImageWithItsSourceAndRevision(t *testing.T) {
	stages := parseDockerfile(t, readRepoFile(t, dockerfilePath))
	runtime, ok := stageNamed(stages, "runtime")
	require.True(t, ok, "the Dockerfile needs a stage named runtime")

	joined := strings.Join(runtime.instructions, "\n")
	require.Contains(t, joined, "ARG REVISION", "the revision is a build argument, declared where it is used")
	require.Regexp(t, `org\.opencontainers\.image\.source\s*=\s*"?https://github\.com/co-rtex/TaskForge`, joined)
	require.Regexp(t, `org\.opencontainers\.image\.revision\s*=\s*"?\$\{?REVISION\}?`, joined)
}

func TestDockerfile_HasNoSyntaxDirective(t *testing.T) {
	// Like dashboard/Dockerfile: a "# syntax=" line would pull a Dockerfile
	// frontend image by floating tag on every build, an unpinned dependency beside
	// the pinned ones. Nothing here needs more than the builtin frontend.
	for _, line := range strings.Split(readRepoFile(t, dockerfilePath), "\n") {
		require.NotRegexp(t, `(?i)^\s*#\s*syntax\s*=`, line)
	}
}

func TestDockerignore_IsAnAllowlistOfWhatTheGoBuildNeeds(t *testing.T) {
	var rules []string
	for _, line := range strings.Split(readRepoFile(t, dockerignorePath), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			rules = append(rules, line)
		}
	}
	require.NotEmpty(t, rules)
	require.Equal(t, "*", rules[0], "an allowlist denies everything first")

	var allowed []string
	for _, r := range rules[1:] {
		if strings.HasPrefix(r, "!") {
			allowed = append(allowed, strings.TrimPrefix(r, "!"))
		}
	}
	// internal/ carries internal/dashboard/dist, which the api embeds; it is
	// gitignored apart from .gitkeep, so Docker's ignore file is the only thing
	// that decides whether the built dashboard enters the build context.
	require.ElementsMatch(t, []string{"go.mod", "go.sum", "cmd", "internal", "migrations"}, allowed,
		"exactly what the six binaries import: nothing else may enter the build context")

	require.Contains(t, rules, "**/*_test.go", "test files are not part of an image's build")
	for _, forbidden := range []string{".git", ".env", "dashboard", "sdk", "docs", "node_modules"} {
		require.NotContains(t, allowed, forbidden)
	}
}

func TestMakefile_ImagesBuildsAfterTheDashboardIsBuilt(t *testing.T) {
	// The api embeds internal/dashboard/dist. A clean clone has only .gitkeep
	// there and serves a placeholder page, so an api image built first would ship
	// a dashboard that is not one.
	text := readRepoFile(t, makefilePath)

	rule := regexp.MustCompile(`(?m)^images:\s*(.*?)\s*(?:##.*)?$`).FindStringSubmatch(text)
	require.NotNil(t, rule, "the Makefile needs an images target")
	require.Contains(t, strings.Fields(rule[1]), "dash-build", "make images must depend on dash-build")

	require.Regexp(t, `(?m)^images:.*##`, text, "images needs a ## help line")
}
