package healthcheck

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequested_OnlyTheExactSubcommand(t *testing.T) {
	require.True(t, Requested([]string{"/taskforge-api", "healthcheck"}))
	require.False(t, Requested([]string{"/taskforge-api"}))
	require.False(t, Requested([]string{"/taskforge-api", "healthcheck-now"}))
	require.False(t, Requested([]string{"/taskforge-api", "--healthcheck"}))
}

// TestRun_ExitsZeroOnlyForA200 proves the three outcomes a container
// orchestrator sees: 0 for a probe that answers 200, 1 for one that answers
// anything else, and 1 for one that is not listening at all.
func TestRun_ExitsZeroOnlyForA200(t *testing.T) {
	var paths []string
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failing.Close()

	// An address nothing listens on: bind, note the port, close.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	down := l.Addr().String()
	require.NoError(t, l.Close())

	for name, tc := range map[string]struct {
		addr      string
		configErr error
		want      int
		says      string
	}{
		"a 200":                 {strings.TrimPrefix(healthy.URL, "http://"), nil, 0, "healthy"},
		"a 503":                 {strings.TrimPrefix(failing.URL, "http://"), nil, 1, "answered 503"},
		"nothing listening":     {down, nil, 1, "unhealthy"},
		"an address that isn't": {"127.0.0.1", nil, 1, "not host:port"},
		"invalid configuration": {strings.TrimPrefix(healthy.URL, "http://"), errors.New("bad"), 1, "configuration invalid"},
	} {
		t.Run(name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			got := Run(context.Background(), "taskforge-test", tc.addr, tc.configErr, &out, &errOut)
			require.Equal(t, tc.want, got)
			require.Contains(t, out.String()+errOut.String(), tc.says)
		})
	}
	require.Equal(t, []string{"GET /healthz"}, paths, "the check GETs the liveness probe, once")
}
