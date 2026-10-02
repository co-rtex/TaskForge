//go:build integration

package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/api"
)

// rawHTTP writes request, byte for byte, to the server's real listener and
// reads one response. It deliberately bypasses net/http's client so nothing
// normalizes, adds, or drops a header: what the browser sent is what arrives.
func rawHTTP(t *testing.T, serverURL, request string) (*http.Response, []byte) {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	require.NoError(t, err)
	conn, err := net.DialTimeout("tcp", parsed.Host, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	_, err = io.WriteString(conn, request)
	require.NoError(t, err)
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response, body
}

// TestInternalGuard_RefusesABrowserCreatingAKeyAndCreatesNothing proves the
// guard against a real listener and a real database: a request carrying an
// Origin header, which is what a script in the dashboard's own origin sends,
// is refused 403 and mints no credential.
//
// The follow-up read is the half a status-code check cannot give. A guard that
// answered 403 after the handler had already inserted the row would pass the
// first assertion and fail this one.
func TestInternalGuard_RefusesABrowserCreatingAKeyAndCreatesNothing(t *testing.T) {
	reset(t)
	server := newAPI(t)
	parsed, err := url.Parse(server.URL)
	require.NoError(t, err)

	const name = "minted-by-a-browser"
	body := `{"scope":"tenant-browser","name":"` + name + `"}`
	response, raw := rawHTTP(t, server.URL, fmt.Sprintf(
		"POST /internal/v1/api-keys HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Origin: %s\r\n"+
			"Content-Type: application/json\r\n"+
			"Content-Length: %d\r\n"+
			"Connection: close\r\n\r\n%s",
		parsed.Host, server.URL, len(body), body))

	require.Equal(t, http.StatusForbidden, response.StatusCode, string(raw))
	var refusal api.ErrorBody
	require.NoError(t, json.Unmarshal(raw, &refusal))
	require.Equal(t, api.CodeOriginRefused, refusal.Error.Code)
	require.NotEmpty(t, response.Header.Get(api.RequestIDHeader))

	// The same request without the browser header succeeds, so the refusal
	// above is the guard and not a malformed request or a broken harness. A
	// request with Sec-Fetch-Site alone and one with a foreign Host are
	// refused too.
	for _, extra := range []string{"Sec-Fetch-Site: same-origin\r\n", ""} {
		host := parsed.Host
		if extra == "" {
			host = "evil.example"
		}
		response, raw = rawHTTP(t, server.URL, fmt.Sprintf(
			"POST /internal/v1/api-keys HTTP/1.1\r\nHost: %s\r\n%sContent-Type: application/json\r\n"+
				"Content-Length: %d\r\nConnection: close\r\n\r\n%s", host, extra, len(body), body))
		require.Equal(t, http.StatusForbidden, response.StatusCode, string(raw))
	}

	// A follow-up GET with no browser headers shows nothing was created.
	listRequest, err := http.NewRequest(http.MethodGet, server.URL+"/internal/v1/api-keys", nil)
	require.NoError(t, err)
	listResponse, err := unauthenticatedClient().Do(listRequest)
	require.NoError(t, err)
	defer listResponse.Body.Close()
	listRaw, err := io.ReadAll(listResponse.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode, string(listRaw))
	require.NotContains(t, string(listRaw), name, "a refused request minted a key")
	require.NotContains(t, string(listRaw), "tenant-browser")

	// And the control: the identical request without any browser marking mints one.
	response, raw = rawHTTP(t, server.URL, fmt.Sprintf(
		"POST /internal/v1/api-keys HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\n"+
			"Content-Length: %d\r\nConnection: close\r\n\r\n%s", parsed.Host, len(body), body))
	require.Equal(t, http.StatusCreated, response.StatusCode, strings.TrimSpace(string(raw)))
}
