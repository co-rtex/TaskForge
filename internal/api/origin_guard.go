package api

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/co-rtex/TaskForge/internal/hostallow"
	"github.com/co-rtex/TaskForge/internal/loopback"
)

// CodeOriginRefused reports a request to an /internal route that the
// browser-origin guard refused. See refuseBrowserOrigin.
//
// It is distinct from unauthorized on purpose: the guard is not
// authentication, and a caller that sees it must not go looking for a
// credential. No key can fix this refusal; only sending the request from a
// non-browser client, to a loopback Host, can.
const CodeOriginRefused = "origin_refused"

// originRefusedMessage is fixed. It never names which rule fired and never
// echoes a header value: the value is attacker-chosen, and the rule is in the
// server log, tied to the response by the request id.
const originRefusedMessage = "this endpoint only accepts requests from a non-browser client addressed to a loopback host"

// hostRule is the guard's rule 3 for one listener: which request Hosts may
// reach its /internal routes. Rules 1 and 2 are the same on every listener;
// only this one is per listener (docs/adr/0026-listeners-by-surface.md amends
// ADR-0018 here and only here). The refusal is the same response on every
// listener, originRefusedMessage included, because api/openapi.yaml documents
// that one fixed message; ADR-0026 records that its wording names loopback even
// on the control listener.
type hostRule func(hostport string) bool

// loopbackHostRule is the admin listener's rule 3, unchanged from ADR-0018.
var loopbackHostRule hostRule = hostIsLoopback

// refuseEveryHost is the public listener's rule 3. That listener serves no
// /internal route (checkListenerPlacement refuses one), so nothing should ever
// consult it; if something did, it fails closed.
var refuseEveryHost hostRule = func(string) bool { return false }

// allowlistHostRule is the control listener's rule 3: the Host must be on the
// exact-match allowlist, port optional. Loopback is not implied; it passes only
// if the operator listed it.
func allowlistHostRule(list hostallow.List) hostRule {
	return func(hostport string) bool {
		host, ok := requestHost(hostport)
		return ok && list.Allows(host)
	}
}

// hostRuleFor is the rule 3 a listener's guard checks. Building the control
// listener without an allowlist is a programming error, not a configuration one
// (internal/config refuses that configuration), so it panics rather than serve
// a guard with no Host rule.
func (s *Server) hostRuleFor(l Listener) hostRule {
	switch l {
	case ListenerAdmin:
		return loopbackHostRule
	case ListenerPublic:
		return refuseEveryHost
	case ListenerControl:
		if s.controlHosts.Empty() {
			panic("api: the control listener needs a Host allowlist; call WithControlHosts before building it")
		}
		return allowlistHostRule(s.controlHosts)
	}
	panic(fmt.Sprintf("api: no listener %s", l))
}

// The three rule names are the closed set of `rule` values the guard logs, so
// a log query on them is bounded however many distinct headers are refused.
const (
	ruleSecFetchSite = "sec_fetch_site"
	ruleOrigin       = "origin"
	ruleHost         = "host"
)

const (
	secFetchSiteHeader = "Sec-Fetch-Site"
	originHeader       = "Origin"
)

// maxPort is the largest valid TCP port. A Host port above it is not one a
// real client sends, so it is refused rather than interpreted.
const maxPort = 65535

// refuseBrowserOrigin is the guard on every /internal route.
//
// /internal is unauthenticated by design (docs/adr/0014, 0017): it is
// loopback-bound operator and worker plumbing. Loopback binding keeps other
// machines out, but not a web page the operator is looking at, because a
// browser on the same machine can be made to send requests to 127.0.0.1. Two
// attacks follow, and each leaves a different fingerprint on the request:
//
//   - A script in some origin -- the dashboard's own, or any site the operator
//     visits -- calls the API. Every modern browser labels such a request with
//     Sec-Fetch-Site, and any cross-origin or same-origin fetch POST carries
//     Origin. No client of this surface sends either header: the Go and Python
//     clients and the worker are not browsers. So their mere presence, with
//     any value, means a browser is acting, and the request is refused.
//   - DNS rebinding: an attacker's page rebinds its own hostname to 127.0.0.1,
//     so the browser treats the API as same-origin with the attacker and may
//     send it requests with neither header above. What it cannot do is change
//     the Host header: that stays the attacker's name. Refusing any Host that
//     is not loopback closes this, and it is the same predicate the bind rule
//     uses (internal/loopback), so the two cannot drift apart.
//
// The rules run in a fixed order, and the first to fire names the refusal.
//
// This is NOT authentication and adds no credential. It is a stateless
// property of the request, checked before anything else: before the 405
// fallback, before authentication, and before any handler reads the body.
//
// On the control listener rule 3 is an exact-match allowlist rather than
// loopback (see hostRule): a worker in another container dials the API by a
// name, and that name, not loopback, is what DNS rebinding cannot forge.
func (s *Server) refuseBrowserOrigin(host hostRule, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if rule := browserOriginRuleFor(r, host); rule != "" {
			// The rule name and the request id are the whole log line. Never
			// a header value: Origin and Host are chosen by whoever sent
			// the request, and echoing them would let an attacker write to
			// the operator's logs.
			s.log.Warn("internal request refused",
				slog.String("request_id", RequestIDFrom(r.Context())),
				slog.String("rule", rule))
			writeError(w, r, s.log, http.StatusForbidden, CodeOriginRefused, originRefusedMessage, nil)
			return
		}
		next(w, r)
	}
}

// browserOriginRule returns the name of the first rule r breaks on the admin
// listener, or "" when it breaks none.
func browserOriginRule(r *http.Request) string {
	return browserOriginRuleFor(r, hostIsLoopback)
}

// browserOriginRuleFor returns the name of the first rule r breaks, with
// hostAllowed as rule 3, or "" when it breaks none. Rules 1 and 2 run first, in
// the same order, on every listener.
func browserOriginRuleFor(r *http.Request, hostAllowed hostRule) string {
	// Presence, not value. "none" (a user typing the URL into the address bar)
	// and an empty value are refused alike: there is no supported reason for a
	// browser to open this surface, and a rule that interpreted values would
	// be a rule an attacker could study for the value it lets through.
	if _, present := r.Header[secFetchSiteHeader]; present {
		return ruleSecFetchSite
	}
	// "null" (a sandboxed or redirected context) and the API's own origin are
	// refused with every other value, for the same reason. The dashboard is
	// served from this very origin, so allowing the API's own origin would
	// leave exactly the exposure this guard exists to close.
	if _, present := r.Header[originHeader]; present {
		return ruleOrigin
	}
	if !hostAllowed(r.Host) {
		return ruleHost
	}
	return ""
}

// hostIsLoopback reports whether a request's Host is the local machine. It
// fails closed: anything it cannot positively parse as a loopback name,
// address, or host:port is refused.
//
// A Host is "host", "host:port", or a bracketed IPv6 literal "[...]" with or
// without ":port". net.SplitHostPort handles every form with a port, but it
// also reports an error for the forms with none, so exactly two no-port forms
// are recognised by hand: a value with no ':' at all (a name or an IPv4
// literal), and one "[...]" with nothing after the ']'. Every other split
// error -- an unbracketed IPv6 literal, an unclosed bracket, text after the
// bracket, extra colons -- is a refusal, never a guess.
func hostIsLoopback(hostport string) bool {
	host, ok := requestHost(hostport)
	return ok && loopback.IsLoopbackHost(host)
}

// requestHost extracts the bare host from a request's Host, in the forms
// hostIsLoopback documents, and reports whether it could. Both listeners' Host
// rules parse through it, so they agree on what a Host is and differ only in
// which hosts they accept. It fails closed: an empty value, an invalid port, an
// unbracketed IPv6 literal, an unclosed bracket or a bracketed name is not ok.
func requestHost(hostport string) (string, bool) {
	if hostport == "" {
		return "", false
	}

	host, port, err := net.SplitHostPort(hostport)
	switch {
	case err == nil:
		// "localhost:" splits without error and yields an empty port, so the
		// port is checked explicitly.
		if !validPort(port) {
			return "", false
		}
	case !strings.Contains(hostport, ":"):
		host = hostport
	case strings.HasPrefix(hostport, "[") && strings.HasSuffix(hostport, "]"):
		host = hostport[1 : len(hostport)-1]
	default:
		return "", false
	}

	// Brackets exist to wrap an IPv6 literal, which is the only host form that
	// contains ':'. A bracketed name ("[localhost]") is not a form any client
	// sends, so it is refused rather than normalised.
	if strings.HasPrefix(hostport, "[") && !strings.Contains(host, ":") {
		return "", false
	}
	return host, true
}

// validPort reports whether port is a non-empty run of ASCII digits naming a
// TCP port. strconv.Atoi would also accept a sign, so digits are checked first.
func validPort(port string) bool {
	if port == "" {
		return false
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return false
		}
	}
	n, err := strconv.Atoi(port)
	return err == nil && n <= maxPort
}
