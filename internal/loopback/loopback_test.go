package loopback

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsLoopbackHost pins the predicate's behavior exactly as it was when it
// lived, unexported, in internal/config. The bind rule and the /internal Host
// rule both depend on it, so a change here is a change to both.
func TestIsLoopbackHost(t *testing.T) {
	accepted := []string{
		"localhost", "LOCALHOST", "LocalHost", "localhost.", "LOCALHOST.",
		"127.0.0.1", "127.0.0.2", "127.255.255.254", // all of 127.0.0.0/8
		"::1",
		"::ffff:127.0.0.1", // IPv4-mapped loopback
	}
	for _, host := range accepted {
		require.True(t, IsLoopbackHost(host), host)
	}

	refused := []string{
		"", ".", "example.com", "evil.example", "localhost.evil.example",
		"127.0.0.1.evil.example", "notlocalhost", "localhost:8080",
		"0.0.0.0", "128.0.0.1", "10.0.0.1", "192.168.1.1", "8.8.8.8",
		"::", "::2", "2001:db8::1", "::ffff:10.0.0.1",
		"[::1]", "[::1]:8080", "127.0.0.1:8080", " 127.0.0.1", "127.0.0.1 ",
	}
	for _, host := range refused {
		require.False(t, IsLoopbackHost(host), host)
	}
}
