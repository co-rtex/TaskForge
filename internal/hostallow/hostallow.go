// Package hostallow holds the one definition of "this request Host is on the
// control listener's allowlist" (TASKFORGE_API_CONTROL_HOSTS).
//
// Two rules depend on it and must never disagree, exactly as the bind rule and
// the Host rule share internal/loopback: internal/config refuses an allowlist
// entry that is not a hostname or an IP address, and internal/api's
// browser-origin guard refuses a request to the control listener whose Host is
// not on the list. Both reduce to Canonical, so an entry the configuration
// accepts is matched by the guard in exactly the form it was accepted in.
//
// Matching is exact. There is no suffix, prefix, wildcard or substring match
// anywhere in this package: "api" allows "api" and nothing else, never
// "api.evil" and never "xapi". See docs/adr/0026-listeners-by-surface.md.
package hostallow

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// maxHostnameLength is the longest DNS name in text form (RFC 1035 section
// 2.3.4, less the root label's dot), and maxLabelLength the longest label.
const (
	maxHostnameLength = 253
	maxLabelLength    = 63
)

// List is a non-empty, exact-match allowlist of request host names and
// addresses, each held in canonical form. The zero value allows nothing.
type List struct {
	names   map[string]struct{}
	ordered []string
}

// Parse builds a List from configuration entries. It refuses an empty list,
// an empty entry, and any entry ParseEntry refuses, naming every bad entry.
func Parse(entries []string) (List, error) {
	if len(entries) == 0 {
		return List{}, errors.New("the list is empty")
	}
	list := List{names: map[string]struct{}{}}
	var problems []string
	for _, entry := range entries {
		name, err := ParseEntry(entry)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		if _, seen := list.names[name]; !seen {
			list.names[name] = struct{}{}
			list.ordered = append(list.ordered, name)
		}
	}
	if len(problems) > 0 {
		return List{}, errors.New(strings.Join(problems, "; "))
	}
	return list, nil
}

// ParseEntry returns the canonical form of one allowlist entry: a hostname, an
// IPv4 address, or an IPv6 address with or without brackets. It refuses an
// entry with a port, a scheme, a path, a wildcard, or anything else that is
// not one of those three. The value is never echoed into the error, only its
// length, because it is configuration an operator may have pasted a secret
// into by mistake.
func ParseEntry(entry string) (string, error) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return "", errors.New("an entry is empty")
	}
	host := entry
	bracketed := strings.HasPrefix(entry, "[") && strings.HasSuffix(entry, "]")
	if bracketed {
		host = entry[1 : len(entry)-1]
		// Brackets wrap an IPv6 literal and nothing else.
		if !strings.Contains(host, ":") {
			return "", fmt.Errorf("an entry of %d characters is bracketed but is not an IPv6 address", len(entry))
		}
	}
	name, ok := Canonical(host)
	if !ok {
		return "", fmt.Errorf("an entry of %d characters is not a hostname or an IP address (no port, scheme or wildcard)", len(entry))
	}
	return name, nil
}

// Canonical returns the form a bare host (no port, no brackets) is compared
// in, and whether it is a hostname or an IP address at all.
//
//   - An IP address becomes net.IP's own string form, so "::0001" and "::1"
//     are the same entry, and an IPv4-mapped IPv6 address is its IPv4 form.
//   - A hostname is lower-cased, because DNS names are case-insensitive, and
//     loses one trailing dot, because "api." is the fully qualified spelling
//     of the same name (the DNS root label), exactly as internal/loopback
//     treats "localhost.". It must then be RFC 1123 labels of letters, digits
//     and hyphens, each 1 to 63 characters, neither starting nor ending with a
//     hyphen, at most 253 characters in all.
//
// Anything else is not ok: a value with a port, a scheme, a path, an
// underscore, a wildcard, an empty label, or two trailing dots.
func Canonical(host string) (string, bool) {
	if host == "" {
		return "", false
	}
	if strings.Contains(host, ":") {
		// Only an IPv6 literal contains ':'. A zone ("fe80::1%en0") is refused:
		// net.ParseIP does not accept one, and no Host header needs one.
		ip := net.ParseIP(host)
		if ip == nil {
			return "", false
		}
		return ip.String(), true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String(), true
	}
	name := strings.ToLower(strings.TrimSuffix(host, "."))
	if name == "" || len(name) > maxHostnameLength {
		return "", false
	}
	for _, label := range strings.Split(name, ".") {
		if !validLabel(label) {
			return "", false
		}
	}
	return name, true
}

func validLabel(label string) bool {
	if label == "" || len(label) > maxLabelLength {
		return false
	}
	if label[0] == '-' || label[len(label)-1] == '-' {
		return false
	}
	for i := 0; i < len(label); i++ {
		c := label[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
		default:
			return false
		}
	}
	return true
}

// Allows reports whether a bare host (no port, no brackets) is on the list.
// It canonicalizes host exactly as Parse canonicalized every entry, and then
// looks it up: an exact match, never a suffix or substring one. A host that
// is not a hostname or an IP address is never allowed.
func (l List) Allows(host string) bool {
	name, ok := Canonical(host)
	if !ok {
		return false
	}
	_, allowed := l.names[name]
	return allowed
}

// Empty reports whether the list allows nothing, which is true of the zero
// value and of nothing Parse returns.
func (l List) Empty() bool { return len(l.names) == 0 }

// Names returns the canonical entries in the order they were first given.
func (l List) Names() []string { return append([]string(nil), l.ordered...) }
