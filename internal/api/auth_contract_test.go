package api

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	yaml "go.yaml.in/yaml/v3"
)

// publicOperations is the spec-side view of the same seven routes
// publicRoutes names on the handler side. The two lists existing separately
// is the point: each is derived from a different source, and this file is
// where they have to agree.
var publicOperations = []struct{ method, path string }{
	{"post", "/v1/jobs"},
	{"get", "/v1/jobs/{job_id}"},
	{"get", "/v1/jobs/{job_id}/result"},
	{"post", "/v1/jobs/{job_id}/cancel"},
	{"post", "/v1/jobs/{job_id}/retry"},
	{"get", "/v1/dlq"},
	{"post", "/v1/dlq/{job_id}/replay"},
}

// TestOpenAPI_EveryPublicOperationRequiresAnAPIKey keeps the document from
// telling a client that a route is open when the handler refuses it.
//
// A client generated from a spec that omitted `security` on one operation would
// call it without a credential and get a 401 nothing in the document predicted.
func TestOpenAPI_EveryPublicOperationRequiresAnAPIKey(t *testing.T) {
	doc := loadOpenAPI(t)

	for _, target := range publicOperations {
		t.Run(target.method+" "+target.path, func(t *testing.T) {
			operation, ok := doc.Paths[target.path][target.method]
			require.Truef(t, ok, "%s %s is missing from the spec", target.method, target.path)

			require.Lenf(t, operation.Security, 1,
				"%s %s must declare exactly one security requirement", target.method, target.path)
			scopes, named := operation.Security[0]["ApiKeyAuth"]
			require.Truef(t, named, "%s %s must require ApiKeyAuth", target.method, target.path)
			require.Empty(t, scopes, "a key carries one scope; there are no OAuth-style scopes to request")

			response, documented := operation.Responses["401"]
			require.Truef(t, documented, "%s %s requires a key but documents no 401", target.method, target.path)
			require.Equal(t, CodeUnauthorized, response.Content["application/json"].Example.Error.Code)

			// The document must state the indistinguishability, not merely
			// practice it. A client author who does not know that a 401 cannot
			// be read as "this key was revoked" will eventually build a retry
			// loop around that misreading.
			flat := flatten(response.Description)
			require.Contains(t, flat, "indistinguishable")
			for _, cause := range []string{"missing", "malformed", "unknown", "revoked"} {
				require.Containsf(t, flat, cause,
					"the 401 must name %s as one of the cases it does not distinguish", cause)
			}
		})
	}
}

// The health probes must stay open. A liveness probe behind a credential
// reports a healthy process as dead the moment that credential is revoked,
// which turns a key rotation into an outage.
func TestOpenAPI_HealthProbesAreNotAuthenticated(t *testing.T) {
	doc := loadOpenAPI(t)
	for _, path := range []string{"/healthz", "/readyz"} {
		operation, ok := doc.Paths[path]["get"]
		require.True(t, ok)
		require.Empty(t, operation.Security, path+" must not require a credential")
		_, has401 := operation.Responses["401"]
		require.False(t, has401, path+" can never answer 401")
	}
}

// workerKeyAuthenticatedOperation is the one route under /internal/v1 that
// requires a credential as of M5B. Every test in this file that walks the
// internal surface expecting it unauthenticated has to carve this one out
// explicitly, rather than the exception silently passing because the walk
// happened to skip it.
var workerKeyAuthenticatedOperation = struct{ method, path string }{
	"put", "/internal/v1/worker-sessions/{worker_session_id}",
}

// TestOpenAPI_TheInternalSurfaceIsDocumentedAsUnauthenticated pins the trust
// boundary this milestone deliberately did NOT move for most of the surface,
// and DID move for exactly one route.
//
// Key management and every worker-control route except registration are still
// unauthenticated and loopback-only. That is a real exposure, and the document
// has to say so plainly: a reader who assumed `/internal/v1` was authenticated
// because `/v1` is would put this service behind a public load balancer.
// Registration is the one deliberate exception, and it has to be documented as
// one rather than silently exempted from this test.
func TestOpenAPI_TheInternalSurfaceIsDocumentedAsUnauthenticated(t *testing.T) {
	doc := loadOpenAPI(t)

	for path, operations := range doc.Paths {
		if !strings.HasPrefix(path, "/internal/v1/") {
			continue
		}
		for method, operation := range operations {
			if method == workerKeyAuthenticatedOperation.method && path == workerKeyAuthenticatedOperation.path {
				continue
			}
			require.Emptyf(t, operation.Security,
				"%s %s is not authenticated; declaring security would misdescribe it", method, path)
			_, has401 := operation.Responses["401"]
			require.Falsef(t, has401, "%s %s can never answer 401", method, path)
		}
	}

	document := flatten(readOpenAPI(t))
	require.Contains(t, document, "not** authenticated",
		"the document must state that most of the internal surface is unauthenticated")
	require.Contains(t, document, "anyone who can reach loopback can therefore mint a credential",
		"the document must state the actual consequence, not just the fact")
	require.Contains(t, document, "may be exposed off loopback",
		"the document must say what follows from it")
}

// TestOpenAPI_RegistrationIsTheDocumentedExceptionToTheInternalSurface proves
// the one authenticated internal route is authenticated in the document the
// exact way the handler actually authenticates it.
func TestOpenAPI_RegistrationIsTheDocumentedExceptionToTheInternalSurface(t *testing.T) {
	doc := loadOpenAPI(t)

	operation, ok := doc.Paths[workerKeyAuthenticatedOperation.path][workerKeyAuthenticatedOperation.method]
	require.True(t, ok)

	require.Lenf(t, operation.Security, 1, "registration must declare exactly one security requirement")
	scopes, named := operation.Security[0]["WorkerKeyAuth"]
	require.True(t, named, "registration must require WorkerKeyAuth, not ApiKeyAuth or nothing")
	require.Empty(t, scopes)

	response, documented := operation.Responses["401"]
	require.True(t, documented, "registration requires a credential but documents no 401")
	require.Equal(t, CodeUnauthorized, response.Content["application/json"].Example.Error.Code)

	document := flatten(readOpenAPI(t))
	require.Contains(t, document, "worker-key authentication",
		"the document must name the section a reader finds the model in")
}

// The security scheme must be the one the handler actually implements.
func TestOpenAPI_SecuritySchemeMatchesTheImplementedHeader(t *testing.T) {
	var spec struct {
		Components struct {
			SecuritySchemes map[string]struct {
				Type        string `yaml:"type"`
				Scheme      string `yaml:"scheme"`
				Description string `yaml:"description"`
			} `yaml:"securitySchemes"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(readOpenAPI(t)), &spec))

	scheme, ok := spec.Components.SecuritySchemes["ApiKeyAuth"]
	require.True(t, ok, "the document must define the ApiKeyAuth scheme")
	require.Equal(t, "http", scheme.Type)
	require.Equal(t, strings.ToLower(bearerScheme), scheme.Scheme,
		"the documented scheme must be the one bearerCredential accepts")
	require.Contains(t, strings.ToLower(scheme.Description), "authorization: bearer")

	// WorkerKeyAuth is a second, distinct scheme -- not ApiKeyAuth reused --
	// because requireWorkerKey authenticates against a different credential
	// store than requireAPIKey does, even though both parse the header with
	// the identical bearerCredential helper.
	workerScheme, ok := spec.Components.SecuritySchemes["WorkerKeyAuth"]
	require.True(t, ok, "the document must define a distinct WorkerKeyAuth scheme")
	require.Equal(t, "http", workerScheme.Type)
	require.Equal(t, strings.ToLower(bearerScheme), workerScheme.Scheme,
		"the documented scheme must be the one bearerCredential accepts")
	require.Contains(t, strings.ToLower(workerScheme.Description), "authorization: bearer")
	require.NotEqual(t, scheme.Description, workerScheme.Description,
		"the two schemes guard different trust boundaries and must not share one description")
}

// TestOpenAPI_KeyCreationDocumentsThatItIsNotIdempotent guards the single
// property most likely to be "fixed" by someone applying the rest of this API's
// conventions.
//
// Every other mutating route here takes an Idempotency-Key. This one must not,
// and the reason has to be in the document: an idempotent create would have to
// return an existing secret to a repeat.
func TestOpenAPI_KeyCreationDocumentsThatItIsNotIdempotent(t *testing.T) {
	doc := loadOpenAPI(t)
	operation, ok := doc.Paths["/internal/v1/api-keys"]["post"]
	require.True(t, ok)

	description := flatten(operation.Description)
	require.Contains(t, description, "not idempotent")
	require.Contains(t, description, "no request identity")
	require.Contains(t, description, "ignored rather than honored",
		"the contract must say what happens to an Idempotency-Key sent anyway")
	require.Contains(t, description, "exactly once",
		"the contract must say the credential is returned only once")
	require.Contains(t, description, "cannot be recovered")

	// Its 503 is the one in this API that must NOT say "retry the identical
	// request": with no request identity, a repeat mints a second credential.
	description503, message := describe503(t, doc, "post", "/internal/v1/api-keys")
	flat503 := flatten(description503 + " " + message)
	require.Contains(t, flat503, "do not repeat this request blindly")
	require.Contains(t, flat503, "second credential")
	require.Contains(t, flat503, "revoke")
	require.NotContains(t, flat503, "retry the identical request",
		"this is the one ambiguous mutation an identical retry makes worse")
}

// Revocation is idempotent and must report the ORIGINAL instant. A repeat that
// moved it would rewrite when a credential stopped being valid, which is exactly
// the fact an incident review needs.
func TestOpenAPI_RevocationDocumentsItsIdempotencyAndItsInFlightBoundary(t *testing.T) {
	doc := loadOpenAPI(t)
	operation, ok := doc.Paths["/internal/v1/api-keys/{key_id}/revoke"]["post"]
	require.True(t, ok)

	description := flatten(operation.Description)
	require.Contains(t, description, "idempotent")
	require.Contains(t, description, "original")
	require.Contains(t, description, "already_revoked")

	// The in-flight boundary is documented as expected behavior rather than left
	// for an operator to discover during an incident and read as a bug.
	require.Contains(t, description, "already in flight")
	require.Contains(t, description, "is not interrupted")
	require.Contains(t, description, "expected behavior, not a gap")
}

// TestOpenAPI_OnlyCreationEverReturnsACredential proves the document promises a
// raw key in exactly one place.
//
// A listing schema that grew a `key` or `secret_hash` field would be a leak the
// handler tests could not see, because they assert on what the handler returns
// rather than on what the contract permits.
func TestOpenAPI_OnlyCreationEverReturnsACredential(t *testing.T) {
	var spec struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]struct {
					Description string `yaml:"description"`
				} `yaml:"properties"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(readOpenAPI(t)), &spec))

	created, ok := spec.Components.Schemas["ApiKeyCreated"]
	require.True(t, ok)
	_, carriesKey := created.Properties["key"]
	require.True(t, carriesKey, "creation is where the credential is returned")
	require.Contains(t, flatten(created.Properties["key"].Description), "exactly once")

	// A worker key is a second, distinct credential with the identical
	// one-time-return property -- not a place this guard should carve an
	// exception for, but a second instance of the same rule.
	workerCreated, ok := spec.Components.Schemas["WorkerKeyCreated"]
	require.True(t, ok)
	_, workerCarriesKey := workerCreated.Properties["key"]
	require.True(t, workerCarriesKey, "worker key creation is where that credential is returned")
	require.Contains(t, flatten(workerCreated.Properties["key"].Description), "exactly once")

	for name, schema := range spec.Components.Schemas {
		if name == "ApiKeyCreated" || name == "WorkerKeyCreated" {
			continue
		}
		for property := range schema.Properties {
			require.NotEqualf(t, "key", property,
				"schema %s must not carry a credential", name)
			require.NotContainsf(t, strings.ToLower(property), "secret",
				"schema %s must not expose secret material", name)
			require.NotContainsf(t, strings.ToLower(property), "hash",
				"schema %s must not expose the stored digest", name)
		}
	}
}

// The Authentication section is where a reader looks for the key format and for
// what a 401 does and does not mean. These are the sentences that stop a client
// author from building the wrong retry behavior.
func TestOpenAPI_AuthenticationSectionStatesTheFormatAndTheBoundaries(t *testing.T) {
	document := flatten(readOpenAPI(t))

	require.Contains(t, document, "`authorization: bearer <key>`")
	require.Contains(t, document, "tfk_<lookup>.<secret>")
	require.Contains(t, document, "returned **exactly once**")

	// Revocation's timing boundary, stated once where a reader will find it.
	require.Contains(t, document, "revocation takes effect on the next request")
	require.Contains(t, document, "is not interrupted")

	// A deadline is not an authentication failure, and the document must say so
	// rather than leaving a client to infer it from a status code.
	require.Contains(t, document, "never `401`")
	require.Contains(t, document, "reads one row and writes nothing")
}

func readOpenAPI(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	require.NoError(t, err)
	return string(raw)
}
