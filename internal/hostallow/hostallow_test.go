package hostallow

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseEntry_AcceptsHostnamesAndAddressesInCanonicalForm proves the forms
// an operator writes in TASKFORGE_API_CONTROL_HOSTS are accepted and stored in
// the one form the guard compares.
func TestParseEntry_AcceptsHostnamesAndAddressesInCanonicalForm(t *testing.T) {
	for entry, want := range map[string]string{
		"api":                          "api",
		"API":                          "api",
		"api.":                         "api",
		" api ":                        "api",
		"taskforge-api.internal":       "taskforge-api.internal",
		"Control.TaskForge.Local.":     "control.taskforge.local",
		"10.0.1.17":                    "10.0.1.17",
		"::1":                          "::1",
		"[2001:db8::7]":                "2001:db8::7",
		"2001:0DB8:0000::0007":         "2001:db8::7",
		"::ffff:10.0.0.1":              "10.0.0.1",
		strings.Repeat("a", 63) + ".b": strings.Repeat("a", 63) + ".b",
	} {
		got, err := ParseEntry(entry)
		require.NoErrorf(t, err, "entry %q", entry)
		require.Equalf(t, want, got, "entry %q", entry)
	}
}

// TestParseEntry_RefusesWhatIsNotAHostnameOrAnAddress proves an entry that
// would need interpretation (a port, a scheme, a wildcard, a malformed label)
// is a configuration error, never a guess about what was meant.
func TestParseEntry_RefusesWhatIsNotAHostnameOrAnAddress(t *testing.T) {
	for _, entry := range []string{
		"",
		"   ",
		"api:8091",
		"http://api",
		"api/v1",
		"*.internal",
		"*",
		"api..internal",
		"api..",
		".api",
		"-api",
		"api-",
		"my_api",
		"[api]",
		"[10.0.0.1]",
		"fe80::1%en0",
		"[::1]:8091",
		"2001:db8::zz",
		strings.Repeat("a", 64),
		strings.Repeat("a.", 127) + "aa",
	} {
		_, err := ParseEntry(entry)
		require.Errorf(t, err, "entry %q must be refused", entry)
		if entry != "" && strings.TrimSpace(entry) != "" {
			require.NotContainsf(t, err.Error(), strings.TrimSpace(entry),
				"the error must not echo the entry %q", entry)
		}
	}
}

// TestParse_RefusesAnEmptyListAndNamesEveryBadEntry proves the list as a
// whole fails closed: no entries is an error, and one bad entry among good
// ones is an error, not a shorter list.
func TestParse_RefusesAnEmptyListAndNamesEveryBadEntry(t *testing.T) {
	_, err := Parse(nil)
	require.ErrorContains(t, err, "empty")

	_, err = Parse([]string{"api", "", "api:8091"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "an entry is empty")
	require.Contains(t, err.Error(), "not a hostname or an IP address")

	list, err := Parse([]string{"api", "API.", "10.0.0.5"})
	require.NoError(t, err)
	require.Equal(t, []string{"api", "10.0.0.5"}, list.Names(), "duplicates collapse to one canonical entry")
	require.False(t, list.Empty())
	require.True(t, List{}.Empty(), "the zero value allows nothing")
}

// TestAllows_IsAnExactMatch is the heart of the package: the canonical form of
// a request host must equal an entry. Every near-miss an attacker could
// register (a suffix, a prefix, a subdomain, a different case of a different
// name) is refused.
func TestAllows_IsAnExactMatch(t *testing.T) {
	list, err := Parse([]string{"api", "10.0.0.5", "2001:db8::7"})
	require.NoError(t, err)

	for _, host := range []string{"api", "API", "api.", "Api.", "10.0.0.5", "2001:db8::7", "2001:DB8:0::7"} {
		require.Truef(t, list.Allows(host), "%q is on the list", host)
	}
	for _, host := range []string{
		"", "api.evil", "xapi", "api.example", "apix", "evil.api", "ap", "api..",
		"127.0.0.1", "localhost", "::1", "10.0.0.50", "2001:db8::8", "api:8091", "[2001:db8::7]",
	} {
		require.Falsef(t, list.Allows(host), "%q is not on the list", host)
	}
	require.False(t, List{}.Allows("api"), "the zero value allows nothing")
}
