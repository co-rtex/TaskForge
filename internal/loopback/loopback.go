// Package loopback holds the one definition of "this host name or address is
// the local machine".
//
// Two independent rules depend on it and must never disagree: the bind rule
// (internal/config refuses a non-loopback listen address, because TaskForge's
// services are unauthenticated-by-network and bind only to the local machine)
// and the Host rule (internal/api refuses a request to an /internal route
// whose Host header is not loopback, which is what defeats DNS rebinding).
// Both reduce to this predicate, so a hostname one accepts the other accepts.
package loopback

import (
	"net"
	"strings"
)

// IsLoopbackHost reports whether host names the local machine: "localhost"
// (any case, with or without a trailing dot) or an IP address for which
// net.IP.IsLoopback is true.
//
// host must be a bare host with no port and no brackets: "::1", not "[::1]"
// and not "[::1]:8080". Callers strip those first (net.SplitHostPort, or
// url.URL.Hostname); a bracketed or ported string is not a loopback host to
// this function, which is the fail-closed answer.
func IsLoopbackHost(host string) bool {
	// A trailing dot is the DNS root label: "localhost." is the same name.
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
