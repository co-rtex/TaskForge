package verification

// These checks hold deploy/terraform/env/<service>.json, the environment every deployed
// container gets (deploy/terraform/ecs.tf reads them with jsondecode(file(...))), to what
// the binaries actually read. They need no Terraform and no AWS: they read Go source with
// go/ast and the Terraform files as text. See docs/adr/0028-deployment-shape-on-aws.md.
//
// The set of variables a binary can read is DERIVED from internal/config/config.go, never
// hand-copied: every string literal "TASKFORGE_..." passed as the first argument to one of
// the env readers (env, envInt, envInt64, envFloat, envDuration, envSet, envList) or to
// os.LookupEnv. Literals inside LoadWorker are the worker-only set; every other function's
// are the shared set config.Load reads. A variable added to config.go is therefore known
// here the moment it is read, and a name misspelled in a JSON file is unknown.

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	configSourcePath  = "internal/config/config.go"
	terraformDir      = "deploy/terraform"
	terraformEnvDir   = "deploy/terraform/env"
	terraformVarsPath = "deploy/terraform/variables.tf"
	terraformECSPath  = "deploy/terraform/ecs.tf"
)

// deployedServices are the task definitions ecs.tf builds, each with an env file.
var deployedServices = []string{"api", "migrate", "outbox", "reconciler", "scheduler", "worker"}

// ecsServices are those that run as an ECS service (migrate is a one-off task).
var ecsServices = []string{"api", "outbox", "reconciler", "scheduler", "worker"}

// The six variables that pin a client to a local endpoint or static keys
// (internal/config's awsStaticVariables). None may appear in a deployed environment.
var staticAWSVariables = []string{
	"TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_BROKER_ACCESS_KEY_ID", "TASKFORGE_BROKER_SECRET_ACCESS_KEY",
	"TASKFORGE_RESULTS_ENDPOINT", "TASKFORGE_RESULTS_ACCESS_KEY_ID", "TASKFORGE_RESULTS_SECRET_ACCESS_KEY",
}

// secretVariables reach containers by valueFrom from Secrets Manager, never as plain
// environment.
var secretVariables = []string{"TASKFORGE_DATABASE_URL", "TASKFORGE_WORKER_API_KEY"}

// placeholder is the only templating the env files may use; ecs.tf substitutes exactly
// these two.
var placeholder = regexp.MustCompile(`\$\{([^}]*)\}`)

var allowedPlaceholders = map[string]bool{"aws_region": true, "results_bucket": true}

var envReaders = map[string]bool{
	"env": true, "envInt": true, "envInt64": true, "envFloat": true,
	"envDuration": true, "envSet": true, "envList": true,
}

// configVariables is what internal/config reads, split by the function that reads it,
// plus the Config field each shared variable populates (from Load's composite literal).
type configVariables struct {
	shared, worker map[string]bool
	fieldToEnv     map[string]string
}

func parseSource(t *testing.T, rel string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(repoRoot, rel), nil, parser.SkipObjectResolution)
	require.NoError(t, err, rel)
	return file
}

func taskforgeLiteral(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil || !strings.HasPrefix(value, "TASKFORGE_") {
		return "", false
	}
	return value, true
}

// readsVariable reports the variable a call reads, if it is one of the env readers or
// os.LookupEnv with a literal TASKFORGE_ name.
func readsVariable(call *ast.CallExpr) (string, bool) {
	if len(call.Args) == 0 {
		return "", false
	}
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		if !envReaders[fn.Name] {
			return "", false
		}
	case *ast.SelectorExpr:
		pkg, ok := fn.X.(*ast.Ident)
		if !ok || pkg.Name != "os" || fn.Sel.Name != "LookupEnv" {
			return "", false
		}
	default:
		return "", false
	}
	return taskforgeLiteral(call.Args[0])
}

func readConfigVariables(t *testing.T) configVariables {
	t.Helper()
	file := parseSource(t, configSourcePath)
	vars := configVariables{shared: map[string]bool{}, worker: map[string]bool{}, fieldToEnv: map[string]string{}}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		target := vars.shared
		if fn.Name.Name == "LoadWorker" {
			target = vars.worker
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				if name, ok := readsVariable(node); ok {
					target[name] = true
				}
			case *ast.KeyValueExpr:
				// Load's `Field: env("TASKFORGE_X", default)` entries.
				if fn.Name.Name != "Load" {
					return true
				}
				key, ok := node.Key.(*ast.Ident)
				call, isCall := node.Value.(*ast.CallExpr)
				if ok && isCall {
					if name, read := readsVariable(call); read {
						vars.fieldToEnv[key.Name] = name
					}
				}
			}
			return true
		})
	}
	require.NotEmpty(t, vars.shared, "no shared variables derived from %s; the derivation is broken", configSourcePath)
	require.NotEmpty(t, vars.worker, "no worker variables derived from LoadWorker; the derivation is broken")
	require.Contains(t, vars.shared, "TASKFORGE_AWS_DEFAULT_CHAIN", "LookupEnv reads must be derived too")
	require.Contains(t, vars.worker, "TASKFORGE_WORKER_API_URL")
	require.Equal(t, "TASKFORGE_DATABASE_URL", vars.fieldToEnv["DatabaseURL"])
	return vars
}

// buildsAWSClient reports whether a binary's main package imports either AWS client
// package, which is what makes the switch and the regions mandatory for it.
func buildsAWSClient(t *testing.T, svc string) bool {
	t.Helper()
	for _, imp := range parseSource(t, "cmd/taskforge-"+svc+"/main.go").Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		if strings.HasSuffix(path, "/internal/queue/sqsbroker") || strings.HasSuffix(path, "/internal/objectstore") {
			return true
		}
	}
	return false
}

// migrateFields is every cfg.<Field> cmd/taskforge-migrate/main.go reads.
func migrateFields(t *testing.T) []string {
	t.Helper()
	seen := map[string]bool{}
	ast.Inspect(parseSource(t, "cmd/taskforge-migrate/main.go"), func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == "cfg" {
				seen[sel.Sel.Name] = true
			}
		}
		return true
	})
	return sortedSet(seen)
}

func sortedSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func readEnvFile(t *testing.T, svc string) map[string]string {
	t.Helper()
	var env map[string]string
	require.NoError(t, json.Unmarshal([]byte(readRepoFile(t, filepath.Join(terraformEnvDir, svc+".json"))), &env),
		"%s.json must be a JSON object of string values", svc)
	return env
}

// TestTerraformEnv_EveryFileIsADeployedService proves the env directory holds exactly one
// file per task definition, and ecs.tf builds exactly those task definitions.
func TestTerraformEnv_EveryFileIsADeployedService(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join(repoRoot, terraformEnvDir, "*.json"))
	require.NoError(t, err)
	var got []string
	for _, m := range matches {
		got = append(got, strings.TrimSuffix(filepath.Base(m), ".json"))
	}
	sort.Strings(got)
	require.Equal(t, deployedServices, got)

	m := regexp.MustCompile(`(?m)^\s*services\s*=\s*toset\(\[([^\]]*)\]\)`).FindStringSubmatch(readRepoFile(t, terraformECSPath))
	require.NotNil(t, m, "ecs.tf must declare local.services")
	var declared []string
	for _, item := range strings.Split(m[1], ",") {
		if item = strings.Trim(strings.TrimSpace(item), `"`); item != "" {
			declared = append(declared, item)
		}
	}
	sort.Strings(declared)
	require.Equal(t, deployedServices, declared, "ecs.tf's services and the env files must be the same set")
}

// TestTerraformEnv_EveryVariableIsOneTheBinaryReads is the heart of the check: each name
// is one internal/config reads for that binary (the worker may also use LoadWorker's), no
// value uses a placeholder ecs.tf does not substitute, no secret is in plain environment,
// and none of the six static AWS variables appears anywhere.
func TestTerraformEnv_EveryVariableIsOneTheBinaryReads(t *testing.T) {
	vars := readConfigVariables(t)
	for _, svc := range deployedServices {
		t.Run(svc, func(t *testing.T) {
			env := readEnvFile(t, svc)
			require.NotEmpty(t, env)
			for name, value := range env {
				known := vars.shared[name] || (svc == "worker" && vars.worker[name])
				require.Truef(t, known, "%s.json sets %s, which taskforge-%s never reads", svc, name, svc)
				for _, m := range placeholder.FindAllStringSubmatch(value, -1) {
					require.Truef(t, allowedPlaceholders[m[1]], "%s.json: %s uses ${%s}, which ecs.tf does not substitute", svc, name, m[1])
				}
			}
			for _, name := range staticAWSVariables {
				_, present := env[name]
				require.Falsef(t, present, "%s.json must not set %s: with TASKFORGE_AWS_DEFAULT_CHAIN it refuses startup", svc, name)
			}
			for _, name := range secretVariables {
				_, present := env[name]
				require.Falsef(t, present, "%s.json must not carry %s in plain environment; it is a valueFrom secret", svc, name)
			}
		})
	}
}

// TestTerraformEnv_AWSModeEverywhereItIsNeeded proves every ECS service runs with
// TASKFORGE_AWS_DEFAULT_CHAIN=true and both regions set, and that the derived set of
// binaries that build an AWS client is among them.
func TestTerraformEnv_AWSModeEverywhereItIsNeeded(t *testing.T) {
	var clients []string
	for _, svc := range deployedServices {
		if buildsAWSClient(t, svc) {
			clients = append(clients, svc)
		}
	}
	require.Equal(t, []string{"api", "outbox", "worker"}, clients,
		"the binaries that import sqsbroker or objectstore changed; review their task roles in iam.tf and ADR-0028")

	for _, svc := range ecsServices {
		env := readEnvFile(t, svc)
		require.Equalf(t, "true", env["TASKFORGE_AWS_DEFAULT_CHAIN"], "%s must run with TASKFORGE_AWS_DEFAULT_CHAIN=true", svc)
		for _, region := range []string{"TASKFORGE_BROKER_REGION", "TASKFORGE_RESULTS_REGION"} {
			require.NotEmptyf(t, strings.TrimSpace(env[region]), "%s must set %s", svc, region)
		}
	}
}

// TestTerraformEnv_TheAPIsListeners pins the M8C1 topology on AWS: admin on loopback,
// public and control on every interface, and the Cloud Map name as the only control Host.
func TestTerraformEnv_TheAPIsListeners(t *testing.T) {
	env := readEnvFile(t, "api")
	require.Equal(t, "127.0.0.1:8080", env["TASKFORGE_API_ADDR"])
	require.Equal(t, "0.0.0.0:8090", env["TASKFORGE_API_PUBLIC_ADDR"])
	require.Equal(t, "0.0.0.0:8091", env["TASKFORGE_API_CONTROL_ADDR"])
	require.Equal(t, "api.taskforge.internal", env["TASKFORGE_API_CONTROL_HOSTS"])
}

// TestTerraformEnv_TheWorkerDialsTheControlListener pins the worker's side of it.
func TestTerraformEnv_TheWorkerDialsTheControlListener(t *testing.T) {
	env := readEnvFile(t, "worker")
	require.Equal(t, "true", env["TASKFORGE_WORKER_ALLOW_REMOTE_API"])
	require.Equal(t, "http://api.taskforge.internal:8091", env["TASKFORGE_WORKER_API_URL"])
}

// TestTerraformEnv_MigrateCarriesOnlyWhatMigrateReads derives migrate's needs from its
// main: the cfg fields it reads, mapped to their variables through Load's literal. The
// database URL is among them and arrives as a secret; everything else must be exactly the
// file's contents.
func TestTerraformEnv_MigrateCarriesOnlyWhatMigrateReads(t *testing.T) {
	vars := readConfigVariables(t)
	want := map[string]bool{}
	for _, field := range migrateFields(t) {
		name, ok := vars.fieldToEnv[field]
		require.Truef(t, ok, "cmd/taskforge-migrate reads cfg.%s, which Load does not populate from a variable", field)
		want[name] = true
	}
	require.Contains(t, want, "TASKFORGE_DATABASE_URL")
	delete(want, "TASKFORGE_DATABASE_URL") // a valueFrom secret, not environment

	got := map[string]bool{}
	for name := range readEnvFile(t, "migrate") {
		got[name] = true
	}
	require.Equal(t, sortedSet(want), sortedSet(got))
}

// TestTerraformEnv_ECSSubstitutesExactlyThePlaceholders proves ecs.tf replaces the two
// placeholders the files may use and no others, so a value can never reach a container
// with an unsubstituted ${...} in it, and that both secrets are wired by valueFrom.
func TestTerraformEnv_ECSSubstitutesExactlyThePlaceholders(t *testing.T) {
	ecs := readRepoFile(t, terraformECSPath)
	substituted := map[string]bool{}
	for _, m := range regexp.MustCompile(`"\$\$\{([a-z_]+)\}"`).FindAllStringSubmatch(ecs, -1) {
		substituted[m[1]] = true
	}
	require.Equal(t, allowedPlaceholders, substituted)
	for _, name := range secretVariables {
		require.Regexpf(t, `name\s*=\s*"`+name+`",\s*valueFrom\s*=`, ecs, "ecs.tf must inject %s with valueFrom", name)
	}
}

// TestTerraformVars_IngressIsClosedByDefault reads the default of allowed_ingress_cidrs
// out of variables.tf. A validation block could only constrain a value someone passes; the
// question is what an operator gets by passing nothing. The file is in `terraform fmt`'s
// canonical form (CI's fmt -check holds that), so the variable block is found by its
// header and its closing brace at column 0, and its one default line must be an empty list.
func TestTerraformVars_IngressIsClosedByDefault(t *testing.T) {
	text := readRepoFile(t, terraformVarsPath)
	block := regexp.MustCompile(`(?ms)^variable "allowed_ingress_cidrs" \{\n(.*?)^\}`).FindStringSubmatch(text)
	require.NotNil(t, block, "variables.tf must declare allowed_ingress_cidrs")

	defaults := regexp.MustCompile(`(?m)^\s*default\s*=\s*(.*)$`).FindAllStringSubmatch(block[1], -1)
	require.Len(t, defaults, 1, "allowed_ingress_cidrs must have exactly one default")
	require.Equal(t, "[]", strings.TrimSpace(defaults[0][1]),
		"allowed_ingress_cidrs must default to [] (closed): an operator who sets nothing exposes nothing")
}
