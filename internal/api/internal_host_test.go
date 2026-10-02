package api

import "net/http"

// testInternalHost is the Host a real TaskForge client sends: every default
// base URL (the CLI's, the SDK's, the worker's) is http://127.0.0.1:8080.
const testInternalHost = "127.0.0.1:8080"

// internalRequest gives req the loopback Host a real client of the /internal
// surface carries.
//
// httptest.NewRequest defaults Host to "example.com", which no real client of
// this surface ever sends. Every test that builds a request to an /internal
// route routes it through here, so the Host a test presents is stated in one
// place rather than inherited from a library default.
func internalRequest(req *http.Request) *http.Request {
	req.Host = testInternalHost
	return req
}
