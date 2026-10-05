package verification

// .gitleaks.toml is the ONLY place a gitleaks finding is accepted (ADR-0021). It holds
// exactly two kinds of [[allowlists]] entry, both permanent by design because history
// is permanent:
//
//   - FIXTURE: a fake value. One rule, one anchored file, one literal value, all three
//     required together, and a comment naming the fixture.
//   - REVOKED: a real secret that has already been revoked. One rule, one anchored
//     file and exactly one commit, with no value in the entry at all, and a comment
//     saying who revoked it and when.
//
// An entry that is neither, or both, is a failure. The shape is checked here as a
// pure function over the file's text, so that it can be tested on examples that are
// not the real file and then applied to the real one.
//
// PARSER: there is no TOML library in go.mod, and adding one is a decision for the
// owner, so this is a scoped text parser. It reads the subset .gitleaks.toml uses
// (single-line `key = value` with basic or literal strings, string arrays on one
// line, booleans, and `#` comments) and REFUSES everything else: an unknown table,
// an unknown key, a multi-line array or string, a line it cannot read. A shape check
// that guessed at syntax it did not understand could pass an entry it had not read,
// so anything outside the subset is a failure and not a skip.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- the parser -------------------------------------------------------------------

type tomlValue struct {
	isList bool
	items  []string
}

type allowlistBlock struct {
	line    int
	comment string
	keys    map[string]tomlValue
}

type gitleaksFile struct {
	top       map[string]tomlValue
	extend    map[string]tomlValue
	hasExtend bool
	blocks    []allowlistBlock
}

var keyLine = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_]*)\s*=\s*(.*)$`)

// parseTOMLString reads one string starting at s and returns it and the rest.
func parseTOMLString(s string) (string, string, error) {
	switch {
	case strings.HasPrefix(s, `'''`):
		end := strings.Index(s[3:], `'''`)
		if end < 0 {
			return "", "", errors.New("a multi-line literal string is not supported")
		}
		return s[3 : 3+end], s[3+end+3:], nil
	case strings.HasPrefix(s, `"""`):
		return "", "", errors.New("a multi-line basic string is not supported")
	case strings.HasPrefix(s, `'`):
		end := strings.Index(s[1:], `'`)
		if end < 0 {
			return "", "", errors.New("an unterminated literal string")
		}
		return s[1 : 1+end], s[1+end+1:], nil
	case strings.HasPrefix(s, `"`):
		for i := 1; i < len(s); i++ {
			switch s[i] {
			case '\\':
				i++
			case '"':
				unquoted, err := strconv.Unquote(s[:i+1])
				if err != nil {
					return "", "", fmt.Errorf("an unreadable basic string: %v", err)
				}
				return unquoted, s[i+1:], nil
			}
		}
		return "", "", errors.New("an unterminated basic string")
	}
	return "", "", fmt.Errorf("expected a string, found %q", truncateTo(s, 20))
}

func truncateTo(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// parseTOMLValue reads the value part of a `key = value` line.
func parseTOMLValue(raw string) (tomlValue, error) {
	raw = strings.TrimSpace(raw)
	var value tomlValue
	rest := raw
	switch {
	case strings.HasPrefix(raw, "["):
		value.isList = true
		rest = strings.TrimSpace(raw[1:])
		for {
			if strings.HasPrefix(rest, "]") {
				rest = rest[1:]
				break
			}
			if rest == "" || strings.HasPrefix(rest, "#") {
				return tomlValue{}, errors.New("a multi-line array is not supported")
			}
			item, after, err := parseTOMLString(rest)
			if err != nil {
				return tomlValue{}, err
			}
			value.items = append(value.items, item)
			rest = strings.TrimSpace(after)
			if strings.HasPrefix(rest, ",") {
				rest = strings.TrimSpace(rest[1:])
			} else if !strings.HasPrefix(rest, "]") {
				return tomlValue{}, fmt.Errorf("expected , or ] in an array, found %q", truncateTo(rest, 20))
			}
		}
	case strings.HasPrefix(raw, "true") || strings.HasPrefix(raw, "false"):
		word := "true"
		if strings.HasPrefix(raw, "false") {
			word = "false"
		}
		value.items = []string{word}
		rest = raw[len(word):]
	default:
		item, after, err := parseTOMLString(raw)
		if err != nil {
			return tomlValue{}, err
		}
		value.items = []string{item}
		rest = after
	}
	if rest = strings.TrimSpace(rest); rest != "" && !strings.HasPrefix(rest, "#") {
		return tomlValue{}, fmt.Errorf("unexpected text after the value: %q", truncateTo(rest, 20))
	}
	return value, nil
}

func parseGitleaksFile(text string) (gitleaksFile, []string) {
	file := gitleaksFile{top: map[string]tomlValue{}, extend: map[string]tomlValue{}}
	var problems []string
	var pending []string // the run of comment lines directly above the current line
	section := "top"
	var current *allowlistBlock

	for i, raw := range strings.Split(text, "\n") {
		n := i + 1
		line := strings.TrimSpace(raw)
		switch {
		case line == "":
			pending = nil
			continue
		case strings.HasPrefix(line, "#"):
			pending = append(pending, strings.TrimSpace(strings.TrimPrefix(line, "#")))
			continue
		}

		comment := strings.Join(pending, "\n")
		pending = nil

		if strings.HasPrefix(line, "[") {
			header := line
			if hash := strings.Index(header, "#"); hash >= 0 {
				header = strings.TrimSpace(header[:hash])
			}
			switch header {
			case "[extend]":
				section, file.hasExtend = "extend", true
			case "[[allowlists]]":
				file.blocks = append(file.blocks, allowlistBlock{line: n, comment: comment, keys: map[string]tomlValue{}})
				current, section = &file.blocks[len(file.blocks)-1], "allowlist"
			default:
				section = "unsupported"
				problems = append(problems, fmt.Sprintf("line %d: the table %s is not allowed; only [extend] and [[allowlists]] are", n, header))
			}
			continue
		}

		m := keyLine.FindStringSubmatch(line)
		if m == nil {
			problems = append(problems, fmt.Sprintf("line %d: not understood: %q", n, truncateTo(line, 40)))
			continue
		}
		value, err := parseTOMLValue(m[2])
		if err != nil {
			problems = append(problems, fmt.Sprintf("line %d: %s: %v", n, m[1], err))
			continue
		}
		var target map[string]tomlValue
		switch section {
		case "top":
			target = file.top
		case "extend":
			target = file.extend
		case "allowlist":
			target = current.keys
		default:
			continue // inside a table already reported
		}
		if _, dup := target[m[1]]; dup {
			problems = append(problems, fmt.Sprintf("line %d: %s is set twice", n, m[1]))
		}
		target[m[1]] = value
	}
	return file, problems
}

// --- the shape --------------------------------------------------------------------

// fixtureRef is what a FIXTURE entry says exists: a literal value in a literal file.
type fixtureRef struct {
	line  int
	path  string
	value string
}

const regexMeta = `\.+*?()|[]{}^$`

// literalOf turns a regular expression into the literal string it matches, or says it
// is not one: any metacharacter that is not escaped, or an escape that is not of a
// metacharacter (\d, \w, \b), makes it a pattern and not a value.
func literalOf(pattern string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch {
		case c == '\\':
			if i+1 >= len(pattern) || !strings.ContainsRune(regexMeta, rune(pattern[i+1])) {
				return "", false
			}
			i++
			b.WriteByte(pattern[i])
		case strings.ContainsRune(regexMeta, rune(c)):
			return "", false
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), b.Len() > 0
}

// anchoredLiteralPath reads `^path$` as the one file it names.
func anchoredLiteralPath(pattern string) (string, bool) {
	if !strings.HasPrefix(pattern, "^") || !strings.HasSuffix(pattern, "$") || len(pattern) < 3 {
		return "", false
	}
	return literalOf(pattern[1 : len(pattern)-1])
}

var (
	fullSHA     = regexp.MustCompile(`^[0-9a-f]{40}$`)
	revokedNote = regexp.MustCompile(`Revoked \d{4}-\d{2}-\d{2} by \S`)
	fixtureKeys = map[string]bool{"description": true, "condition": true, "targetRules": true, "paths": true, "regexes": true}
	revokedKeys = map[string]bool{"condition": true, "targetRules": true, "paths": true, "commits": true}
)

const fixtureWord = "Fixture"

// exactlyOne returns the single string of a list-valued key.
func exactlyOne(b allowlistBlock, key string) (string, string) {
	v, ok := b.keys[key]
	switch {
	case !ok:
		return "", fmt.Sprintf("%s is missing", key)
	case !v.isList:
		return "", fmt.Sprintf("%s must be a list with exactly one entry", key)
	case len(v.items) != 1:
		return "", fmt.Sprintf("%s has %d entries; it must have exactly one", key, len(v.items))
	case strings.TrimSpace(v.items[0]) == "":
		return "", fmt.Sprintf("%s has an empty entry", key)
	}
	return v.items[0], ""
}

// checkAllowlistBlock judges one [[allowlists]] entry, and returns the fixture it
// names if it is a fixture entry.
func checkAllowlistBlock(b allowlistBlock) ([]string, *fixtureRef) {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf("[[allowlists]] at line %d: %s", b.line, fmt.Sprintf(format, args...)))
	}

	_, hasCommits := b.keys["commits"]
	_, hasRegexes := b.keys["regexes"]
	switch {
	case hasCommits && hasRegexes:
		add("it has both commits and regexes; an entry is a fixture (regexes) or a revoked secret (commits), never both")
		return problems, nil
	case !hasCommits && !hasRegexes:
		add("it has neither regexes nor commits; an entry is a fixture (a regexes value) or a revoked secret (a commits SHA)")
		return problems, nil
	}

	allowed, kind := fixtureKeys, "fixture"
	if hasCommits {
		allowed, kind = revokedKeys, "revoked"
	}
	for key := range b.keys {
		if !allowed[key] {
			add("a %s entry may not set %s", kind, key)
		}
	}

	if cond, ok := b.keys["condition"]; !ok || cond.isList || len(cond.items) != 1 || cond.items[0] != "AND" {
		add(`condition must be "AND": the rule, the path and the %s must all match, not any`, map[string]string{"fixture": "value", "revoked": "commit"}[kind])
	}
	if _, msg := exactlyOne(b, "targetRules"); msg != "" {
		add("%s", msg)
	}
	path, pathProblem := "", ""
	if raw, msg := exactlyOne(b, "paths"); msg != "" {
		pathProblem = msg
	} else if lit, ok := anchoredLiteralPath(raw); !ok {
		pathProblem = fmt.Sprintf("paths entry %q must be one anchored literal file path, ^path$", raw)
	} else {
		path = lit
	}
	if pathProblem != "" {
		add("%s", pathProblem)
	}

	if kind == "revoked" {
		if sha, msg := exactlyOne(b, "commits"); msg != "" {
			add("%s", msg)
		} else if !fullSHA.MatchString(sha) {
			add("commits entry %q must be a full 40-hex commit SHA", sha)
		}
		if !revokedNote.MatchString(b.comment) {
			add(`the comment above it must say "Revoked YYYY-MM-DD by <name>" and what the credential was for`)
		}
		return problems, nil
	}

	if !strings.Contains(b.comment, fixtureWord) {
		add(`the comment above it must name the fixture ("Fixture: ...")`)
	}
	var ref *fixtureRef
	if raw, msg := exactlyOne(b, "regexes"); msg != "" {
		add("%s", msg)
	} else if lit, ok := literalOf(raw); !ok {
		add("regexes entry %q must be a literal value: no regex metacharacters other than escaped ones", raw)
	} else if path != "" {
		ref = &fixtureRef{line: b.line, path: path, value: lit}
	}
	return problems, ref
}

// checkGitleaksConfig judges the whole file and returns the fixtures it names.
func checkGitleaksConfig(text string) ([]string, []fixtureRef) {
	file, problems := parseGitleaksFile(text)

	if v, ok := file.extend["useDefault"]; !file.hasExtend || !ok || v.isList || len(v.items) != 1 || v.items[0] != "true" {
		problems = append(problems, "[extend] useDefault = true is required: the default rule set stays on")
	}
	for key := range file.extend {
		if key != "useDefault" {
			problems = append(problems, fmt.Sprintf("[extend] may not set %s", key))
		}
	}
	for key := range file.top {
		if key != "title" {
			problems = append(problems, fmt.Sprintf("the top level may not set %s", key))
		}
	}
	if len(file.blocks) == 0 {
		problems = append(problems, "there is no [[allowlists]] entry; the known fake fixtures are allowlisted here")
	}

	var fixtures []fixtureRef
	for _, b := range file.blocks {
		p, ref := checkAllowlistBlock(b)
		problems = append(problems, p...)
		if ref != nil {
			fixtures = append(fixtures, *ref)
		}
	}
	return problems, fixtures
}

// fixtureValueProblems requires every fixture's value to appear verbatim in the file
// it names. An entry whose value has gone from its file is a permission for nothing,
// and a typo in a value would silently allowlist nothing and leave the real finding.
func fixtureValueProblems(fixtures []fixtureRef, read func(path string) (string, error)) []string {
	var problems []string
	for _, f := range fixtures {
		content, err := read(f.path)
		switch {
		case err != nil:
			problems = append(problems, fmt.Sprintf("[[allowlists]] at line %d: cannot read %s: %v", f.line, f.path, err))
		case !strings.Contains(content, f.value):
			problems = append(problems, fmt.Sprintf("[[allowlists]] at line %d: the value does not appear in %s", f.line, f.path))
		}
	}
	return problems
}

// --- examples that are not the real file -------------------------------------------

const configHead = "title = \"example\"\n\n[extend]\nuseDefault = true\n\n"

const validFixture = `# Fixture: the example key in example_test.go, a made-up value.
[[allowlists]]
description = "example fixture"
condition = "AND"
targetRules = ["generic-api-key"]
paths = ['''^pkg/example_test\.go$''']
regexes = ['''example_fixture_key_0123456789''']
`

const validRevoked = `# Revoked 2026-10-05 by Christian Cortez: a staging mailer token, no longer valid.
[[allowlists]]
condition = "AND"
targetRules = ["generic-api-key"]
paths = ['''^pkg/old_mailer_test\.go$''']
commits = ["0123456789abcdef0123456789abcdef01234567"]
`

// mutate changes base and fails the test if the change did not take, so that no
// variant below can pass for the wrong reason: by not being a variant at all.
func mutate(t *testing.T, base, old, replacement string) string {
	t.Helper()
	require.Contains(t, base, old, "the variant would change nothing")
	return strings.Replace(base, old, replacement, 1)
}

func TestGitleaksShape_AValidFixtureAndAValidRevokedEntryAreAccepted(t *testing.T) {
	problems, fixtures := checkGitleaksConfig(configHead + validFixture + "\n" + validRevoked)

	require.Empty(t, problems)
	require.Equal(t, []fixtureRef{{line: 7, path: "pkg/example_test.go", value: "example_fixture_key_0123456789"}}, fixtures,
		"the fixture entry names a literal file and a literal value; the revoked entry names no value at all")
}

func TestGitleaksShape_AnEscapedMetacharacterIsReadAsTheLiteralItMatches(t *testing.T) {
	// \. is an escaped metacharacter: it matches a dot, so the value is "key.with.dots_v2".
	text := mutate(t, validFixture, `example_fixture_key_0123456789`, `key\.with\.dots_v2`)
	problems, fixtures := checkGitleaksConfig(configHead + text)

	require.Empty(t, problems)
	require.Len(t, fixtures, 1)
	require.Equal(t, "key.with.dots_v2", fixtures[0].value)

	// \d is a class, not an escaped metacharacter, so it is a pattern and not a value.
	_, problems = func() ([]fixtureRef, []string) {
		p, f := checkGitleaksConfig(configHead + mutate(t, validFixture, `example_fixture_key_0123456789`, `key\d`))
		return f, p
	}()
	require.NotEmpty(t, problems)
}

func TestGitleaksShape_EveryInvalidFixtureVariantIsRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		text string
		want string
	}{
		"no condition":              {mutate(t, validFixture, "condition = \"AND\"\n", ""), "condition"},
		"condition OR":              {mutate(t, validFixture, `"AND"`, `"OR"`), "condition"},
		"no targetRules":            {mutate(t, validFixture, "targetRules = [\"generic-api-key\"]\n", ""), "targetRules"},
		"two rules":                 {mutate(t, validFixture, `["generic-api-key"]`, `["generic-api-key", "aws-access-token"]`), "targetRules"},
		"no paths":                  {mutate(t, validFixture, "paths = ['''^pkg/example_test\\.go$''']\n", ""), "paths"},
		"two paths":                 {mutate(t, validFixture, `['''^pkg/example_test\.go$''']`, `['''^pkg/a\.go$''', '''^pkg/b\.go$''']`), "paths"},
		"path not anchored":         {mutate(t, validFixture, `^pkg/example_test\.go$`, `pkg/example_test\.go`), "anchored"},
		"path is a pattern":         {mutate(t, validFixture, `^pkg/example_test\.go$`, `^pkg/.*_test\.go$`), "anchored"},
		"path is a directory glob":  {mutate(t, validFixture, `^pkg/example_test\.go$`, `^pkg/`), "anchored"},
		"no regexes and no commits": {mutate(t, validFixture, "regexes = ['''example_fixture_key_0123456789''']\n", ""), "neither"},
		"two regexes":               {mutate(t, validFixture, `['''example_fixture_key_0123456789''']`, `['''example_fixture_key_0123456789''', '''other''']`), "regexes"},
		"regex is a pattern":        {mutate(t, validFixture, `example_fixture_key_0123456789`, `example_fixture_key_[0-9]+`), "literal"},
		"regex has a wildcard":      {mutate(t, validFixture, `example_fixture_key_0123456789`, `example_.*`), "literal"},
		"regex has a class escape":  {mutate(t, validFixture, `example_fixture_key_0123456789`, `example_\d+`), "literal"},
		"empty regex":               {mutate(t, validFixture, `'''example_fixture_key_0123456789'''`, `''''''`), "regexes"},
		"commits and regexes":       {mutate(t, validFixture, "regexes =", "commits = [\"0123456789abcdef0123456789abcdef01234567\"]\nregexes ="), "both"},
		"an extra key":              {mutate(t, validFixture, "regexes =", "stopwords = [\"x\"]\nregexes ="), "stopwords"},
		"a regexTarget":             {mutate(t, validFixture, "regexes =", "regexTarget = \"line\"\nregexes ="), "regexTarget"},
		"no comment":                {mutate(t, validFixture, "# Fixture: the example key in example_test.go, a made-up value.\n", ""), "fixture"},
		"comment without Fixture":   {mutate(t, validFixture, "Fixture:", "Note:"), "fixture"},
		"comment after a blank":     {mutate(t, validFixture, "value.\n[[allowlists]]", "value.\n\n[[allowlists]]"), "fixture"},
		"a multi-line array":        {mutate(t, validFixture, "targetRules = [\"generic-api-key\"]", "targetRules = [\n  \"generic-api-key\",\n]"), "multi-line"},
	} {
		problems, _ := checkGitleaksConfig(configHead + tc.text)
		require.NotEmptyf(t, problems, "%s must be refused", name)
		require.Containsf(t, strings.Join(problems, "\n"), tc.want, "%s: the problem should say why\n%s", name, strings.Join(problems, "\n"))
	}
}

func TestGitleaksShape_EveryInvalidRevokedVariantIsRefused(t *testing.T) {
	const sha = "0123456789abcdef0123456789abcdef01234567"
	for name, tc := range map[string]struct {
		text string
		want string
	}{
		"a regexes value (the secret) in the entry": {mutate(t, validRevoked, "commits =", "regexes = ['''leaked_value_123''']\ncommits ="), "both"},
		"neither commits nor regexes":               {mutate(t, validRevoked, "commits = [\""+sha+"\"]\n", ""), "neither"},
		"a short SHA":                               {mutate(t, validRevoked, sha, "0123456"), "40-hex"},
		"an upper-case SHA":                         {mutate(t, validRevoked, sha, strings.ToUpper(sha)), "40-hex"},
		"a SHA with a non-hex digit":                {mutate(t, validRevoked, sha, "g123456789abcdef0123456789abcdef01234567"), "40-hex"},
		"two commits":                               {mutate(t, validRevoked, `["`+sha+`"]`, `["`+sha+`", "fedcba9876543210fedcba9876543210fedcba98"]`), "commits"},
		"no condition":                              {mutate(t, validRevoked, "condition = \"AND\"\n", ""), "condition"},
		"condition OR":                              {mutate(t, validRevoked, `"AND"`, `"OR"`), "condition"},
		"no targetRules":                            {mutate(t, validRevoked, "targetRules = [\"generic-api-key\"]\n", ""), "targetRules"},
		"two rules":                                 {mutate(t, validRevoked, `["generic-api-key"]`, `["generic-api-key", "aws-access-token"]`), "targetRules"},
		"no paths":                                  {mutate(t, validRevoked, "paths = ['''^pkg/old_mailer_test\\.go$''']\n", ""), "paths"},
		"path not anchored":                         {mutate(t, validRevoked, `^pkg/old_mailer_test\.go$`, `pkg/old_mailer_test\.go`), "anchored"},
		"path is a pattern":                         {mutate(t, validRevoked, `^pkg/old_mailer_test\.go$`, `^pkg/.*$`), "anchored"},
		"a description (a place to put a value)":    {mutate(t, validRevoked, "condition =", "description = \"token abc\"\ncondition ="), "description"},
		"no comment":                                {mutate(t, validRevoked, "# Revoked 2026-10-05 by Christian Cortez: a staging mailer token, no longer valid.\n", ""), "Revoked"},
		"comment without the word Revoked":          {mutate(t, validRevoked, "Revoked 2026-10-05", "Rotated 2026-10-05"), "Revoked"},
		"comment with a bad date":                   {mutate(t, validRevoked, "2026-10-05", "yesterday"), "Revoked"},
		"comment with no name":                      {mutate(t, validRevoked, " by Christian Cortez: a staging mailer token, no longer valid.", ""), "Revoked"},
		"a fixture comment instead":                 {mutate(t, validRevoked, "Revoked 2026-10-05 by Christian Cortez", "Fixture: a made-up token"), "Revoked"},
	} {
		problems, _ := checkGitleaksConfig(configHead + tc.text)
		require.NotEmptyf(t, problems, "%s must be refused", name)
		require.Containsf(t, strings.Join(problems, "\n"), tc.want, "%s: the problem should say why\n%s", name, strings.Join(problems, "\n"))
	}
}

func TestGitleaksShape_TheFileAsAWholeIsHeldToItsRules(t *testing.T) {
	for name, tc := range map[string]struct {
		text string
		want string
	}{
		"the default rules are off":   {mutate(t, configHead, "useDefault = true", "useDefault = false") + validFixture, "useDefault"},
		"no extend table":             {"title = \"x\"\n\n" + validFixture, "useDefault"},
		"a global allowlist":          {configHead + "[allowlist]\npaths = ['''^a$''']\n\n" + validFixture, "[allowlist]"},
		"a custom rule table":         {configHead + "[[rules]]\nid = \"x\"\n\n" + validFixture, "[[rules]]"},
		"a nested rule allowlist":     {configHead + "[[rules.allowlists]]\npaths = ['''^a$''']\n\n" + validFixture, "rules.allowlists"},
		"extend from another file":    {mutate(t, configHead, "useDefault = true", "useDefault = true\npath = \"other.toml\""), "path"},
		"an unknown top-level key":    {"title = \"x\"\nminVersion = \"8\"\n\n[extend]\nuseDefault = true\n\n" + validFixture, "minVersion"},
		"no allowlist entry at all":   {configHead, "[[allowlists]]"},
		"a line that is not TOML":     {configHead + validFixture + "\nthis is not toml\n", "not understood"},
		"a key set twice in an entry": {mutate(t, validFixture, "condition = \"AND\"\n", "condition = \"AND\"\ncondition = \"AND\"\n"), "twice"},
	} {
		problems, _ := checkGitleaksConfig(tc.text)
		require.NotEmptyf(t, problems, "%s must be refused", name)
		require.Containsf(t, strings.Join(problems, "\n"), tc.want, "%s\n%s", name, strings.Join(problems, "\n"))
	}
}

func TestGitleaksShape_FixtureValuesAreCheckedAgainstTheNamedFile(t *testing.T) {
	fixtures := []fixtureRef{{line: 8, path: "pkg/example_test.go", value: "example_fixture_key_0123456789"}}

	present := func(string) (string, error) { return `const key = "example_fixture_key_0123456789"`, nil }
	require.Empty(t, fixtureValueProblems(fixtures, present))

	absent := func(string) (string, error) { return `const key = "something_else"`, nil }
	problems := fixtureValueProblems(fixtures, absent)
	require.Len(t, problems, 1)
	require.Contains(t, problems[0], "pkg/example_test.go")
	require.Contains(t, problems[0], "does not appear")

	missing := func(string) (string, error) { return "", errors.New("no such file") }
	problems = fixtureValueProblems(fixtures, missing)
	require.Len(t, problems, 1)
	require.Contains(t, problems[0], "no such file")
}

// --- the real file -----------------------------------------------------------------

func TestGitleaksConfig_TheCommittedConfigIsWellFormed(t *testing.T) {
	problems, fixtures := checkGitleaksConfig(readRepoFile(t, gitleaksConfigPath))

	require.Empty(t, problems)
	require.NotEmpty(t, fixtures, "the known fake fixtures are allowlisted here")
}

// The value has to be in the named file as committed: HEAD, not whatever is in the
// working tree. An entry whose fixture has been removed from its file is a standing
// permission for nothing.
func TestGitleaksConfig_EveryFixtureValueAppearsVerbatimInItsFileAtHEAD(t *testing.T) {
	_, fixtures := checkGitleaksConfig(readRepoFile(t, gitleaksConfigPath))
	require.NotEmpty(t, fixtures)

	atHEAD := func(path string) (string, error) {
		out, err := exec.Command("git", "-C", filepath.Clean(repoRoot), "show", "HEAD:"+path).Output()
		return string(out), err
	}
	require.Empty(t, fixtureValueProblems(fixtures, atHEAD))
}

// The old scan_test.go also checked that the file existed where the driver expects it.
func TestGitleaksConfig_ExistsWhereTheDriverAndTheScanLookForIt(t *testing.T) {
	_, err := os.Stat(filepath.Join(repoRoot, gitleaksConfigPath))
	require.NoError(t, err)
	require.Contains(t, readRepoFile(t, filepath.Join(scanDriverDirectory, "sources.go")), `"`+gitleaksConfigPath+`"`)
}
