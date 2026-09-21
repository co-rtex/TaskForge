package cli

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveBaseURL(t *testing.T) {
	cases := []struct {
		name      string
		flagValue string
		envValue  string
		want      string
	}{
		{"flag wins over env", "http://flag.example:9000", "http://env.example:8080", "http://flag.example:9000"},
		{"env used when flag empty", "", "http://127.0.0.1:9090", "http://127.0.0.1:9090"},
		{"default when both empty", "", "", "http://" + defaultAPIAddr},
		{"https accepted", "", "https://api.example.com", "https://api.example.com"},
		{"trailing slash trimmed", "http://127.0.0.1:8080/", "", "http://127.0.0.1:8080"},
		{"non-loopback host accepted, loopback is a default not a constraint", "http://10.0.0.5:8080", "", "http://10.0.0.5:8080"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveBaseURL(tc.flagValue, tc.envValue)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestResolveBaseURL_RejectsAnythingButAnAbsoluteHTTPURL pins the validation
// shape shared with TASKFORGE_WORKER_API_URL ("must be an absolute http(s)
// URL"), including the old bind-address shape ("host:port") that
// TASKFORGE_API_ADDR carries and this variable deliberately does not.
func TestResolveBaseURL_RejectsAnythingButAnAbsoluteHTTPURL(t *testing.T) {
	for _, bad := range []string{
		"127.0.0.1:8080",
		"localhost:8080",
		"api.example.com",
		"ftp://api.example.com",
		"http://",
		"//host:8080",
		"://nope",
	} {
		t.Run("env "+bad, func(t *testing.T) {
			_, err := ResolveBaseURL("", bad)
			require.Error(t, err)
			require.Contains(t, err.Error(), APIURLEnv)
			require.Contains(t, err.Error(), "absolute http(s) URL")
		})
		t.Run("flag "+bad, func(t *testing.T) {
			_, err := ResolveBaseURL(bad, "http://127.0.0.1:8080")
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), "--api-url"), "the error must name the source that was wrong")
		})
	}
}

// TestResolveBaseURL_DefaultMatchesTaskforgeAPIsOwnDefault pins that this
// CLI's fallback address is exactly internal/config.Config's own default for
// TASKFORGE_API_ADDR ("127.0.0.1:8080"): the VALUE is shared so an operator
// who has changed nothing needs no flag or env var, while the variable NAME
// is not (see ResolveBaseURL).
func TestResolveBaseURL_DefaultMatchesTaskforgeAPIsOwnDefault(t *testing.T) {
	const taskforgeAPIsOwnDefault = "127.0.0.1:8080" // internal/config/config.go
	if defaultAPIAddr != taskforgeAPIsOwnDefault {
		t.Fatalf("defaultAPIAddr = %q, want %q to match internal/config.Config's own TASKFORGE_API_ADDR default", defaultAPIAddr, taskforgeAPIsOwnDefault)
	}
}

func TestAPIURLEnv_IsNotTheServersBindAddressVariable(t *testing.T) {
	require.Equal(t, "TASKFORGE_CLI_API_URL", APIURLEnv)
	require.NotEqual(t, "TASKFORGE_API_ADDR", APIURLEnv)
}
