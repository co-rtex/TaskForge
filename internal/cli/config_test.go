package cli

import "testing"

func TestResolveBaseURL(t *testing.T) {
	cases := []struct {
		name      string
		flagValue string
		envValue  string
		want      string
	}{
		{"flag wins over env", "http://flag.example:9000", "env.example:8080", "http://flag.example:9000"},
		{"env used when flag empty", "", "127.0.0.1:9090", "http://127.0.0.1:9090"},
		{"default when both empty", "", "", "http://" + defaultAPIAddr},
		{"bare host:port gets http:// prefix", "", "10.0.0.5:8080", "http://10.0.0.5:8080"},
		{"scheme-qualified value passed through", "", "https://api.example.com", "https://api.example.com"},
		{"trailing slash trimmed", "http://127.0.0.1:8080/", "", "http://127.0.0.1:8080"},
		{"flag with scheme wins over env without", "https://flag.example", "1.2.3.4:8080", "https://flag.example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveBaseURL(tc.flagValue, tc.envValue)
			if got != tc.want {
				t.Fatalf("ResolveBaseURL(%q, %q) = %q, want %q", tc.flagValue, tc.envValue, got, tc.want)
			}
		})
	}
}

// TestResolveBaseURL_DefaultMatchesTaskforgeAPIsOwnDefault pins the
// documented fact in config.go's comment: taskforge-cli's fallback matches
// internal/config.Config's own default for TASKFORGE_API_ADDR
// ("127.0.0.1:8080"), so an operator who has changed nothing needs no
// flag or env var at all.
func TestResolveBaseURL_DefaultMatchesTaskforgeAPIsOwnDefault(t *testing.T) {
	const taskforgeAPIsOwnDefault = "127.0.0.1:8080" // internal/config/config.go
	if defaultAPIAddr != taskforgeAPIsOwnDefault {
		t.Fatalf("defaultAPIAddr = %q, want %q to match internal/config.Config's own TASKFORGE_API_ADDR default", defaultAPIAddr, taskforgeAPIsOwnDefault)
	}
}
