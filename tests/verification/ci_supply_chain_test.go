package verification

// These checks read .github/workflows/ci.yml. They hold the pinning rules
// docs/adr/0021-container-images-and-supply-chain-scanning.md sets for CI (every
// action by commit SHA, every tool at an exact version, no floating "latest") and
// that the two supply-chain jobs exist and run the commands the ADR says they run.
// That the jobs pass on a hosted runner is proved by CI itself, not here.

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	yaml "go.yaml.in/yaml/v3"
)

const workflowPath = ".github/workflows/ci.yml"

type workflowStep struct {
	Name string            `yaml:"name"`
	Uses string            `yaml:"uses"`
	Run  string            `yaml:"run"`
	With map[string]any    `yaml:"with"`
	Env  map[string]string `yaml:"env"`
}

type workflowJob struct {
	Name           string         `yaml:"name"`
	RunsOn         string         `yaml:"runs-on"`
	TimeoutMinutes int            `yaml:"timeout-minutes"`
	Steps          []workflowStep `yaml:"steps"`
}

type workflow struct {
	Jobs map[string]workflowJob `yaml:"jobs"`
}

func loadWorkflow(t *testing.T) workflow {
	t.Helper()
	var wf workflow
	require.NoError(t, yaml.Unmarshal([]byte(readRepoFile(t, workflowPath)), &wf))
	require.NotEmpty(t, wf.Jobs)
	return wf
}

func (j workflowJob) runs() string {
	var all []string
	for _, s := range j.Steps {
		all = append(all, s.Run)
	}
	return strings.Join(all, "\n")
}

func TestCI_EveryActionIsPinnedToACommitSHA(t *testing.T) {
	sha := regexp.MustCompile(`^[\w.-]+/[\w.-]+(?:/[\w./-]+)?@[0-9a-f]{40}$`)
	for name, job := range loadWorkflow(t).Jobs {
		for _, step := range job.Steps {
			if step.Uses == "" {
				continue
			}
			require.Regexpf(t, sha, step.Uses, "job %s step %q: a tag can be re-pointed, a commit cannot", name, step.Name)
		}
	}
}

func TestCI_NoToolIsFetchedByAFloatingVersionOrThroughGitleaksAction(t *testing.T) {
	text := readRepoFile(t, workflowPath)

	require.NotContains(t, text, "gitleaks/gitleaks-action", "gitleaks-action needs a licence for organisations; the scan runs the pinned container")
	require.NotRegexp(t, `@latest\b`, text, "a Go tool installed @latest is a different tool tomorrow")
	require.NotContains(t, text, "pip install pip-audit", "pip-audit is installed, at an exact version, by the scan driver; CI does not install a second copy")
}

func TestCI_TheImagesJobBuildsTheSixImagesAndRunsTheSmokeAgainstItsOwnInfrastructure(t *testing.T) {
	job, ok := loadWorkflow(t).Jobs["images"]
	require.True(t, ok, "ci.yml needs an images job")
	require.Equal(t, "ubuntu-latest", job.RunsOn)
	require.Positive(t, job.TimeoutMinutes, "a hung build must not hold a runner for the default six hours")

	runs := job.runs()
	require.Contains(t, runs, "make up", "the image smoke runs the migrate and api images against PostgreSQL")
	require.Contains(t, runs, "make images", "the images are built by the target a developer runs, which builds the dashboard first")
	require.Contains(t, runs, "go run ./scripts/imagesmoke")
	require.Less(t, strings.Index(runs, "make images"), strings.Index(runs, "go run ./scripts/imagesmoke"), "build before smoke")
	require.NotContains(t, runs, "make migrate", "the smoke applies the migrations from the migrate image, to a database that has none")
	require.NotContains(t, runs, "docker push", "no registry push, no registry credentials")
	require.NotContains(t, runs, "docker login")

	// The endpoints the services themselves read, spelled out as the integration
	// job does, so what the smoke is pointed at is visible in the workflow.
	var smoke workflowStep
	for _, s := range job.Steps {
		if strings.Contains(s.Run, "scripts/imagesmoke") {
			smoke = s
		}
	}
	for _, key := range []string{"TASKFORGE_DATABASE_URL", "TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_RESULTS_ENDPOINT"} {
		require.Containsf(t, smoke.Env, key, "the smoke step must say where %s points", key)
	}

	require.Contains(t, runs, "docker compose down", "the job removes its own containers and volumes")
}

func TestCI_TheScanJobScansFullHistoryWithTheDriver(t *testing.T) {
	job, ok := loadWorkflow(t).Jobs["scan"]
	require.True(t, ok, "ci.yml needs a scan job")
	require.Equal(t, "ubuntu-latest", job.RunsOn)
	require.Positive(t, job.TimeoutMinutes)

	require.Contains(t, job.runs(), "make scan", "CI runs the command a developer runs, so the exceptions apply identically")

	var checkout *workflowStep
	var setupPython bool
	for i, s := range job.Steps {
		switch {
		case strings.HasPrefix(s.Uses, "actions/checkout@"):
			checkout = &job.Steps[i]
		case strings.HasPrefix(s.Uses, "actions/setup-python@"):
			setupPython = true
		}
	}
	require.NotNil(t, checkout, "the scan job checks out the repository")
	require.EqualValues(t, 0, checkout.With["fetch-depth"], "gitleaks scans the full history; a depth-1 checkout would scan one commit")
	require.Equal(t, false, checkout.With["persist-credentials"])
	require.True(t, setupPython, "pip-audit needs a Python the job chose, not whatever the image has")
}
