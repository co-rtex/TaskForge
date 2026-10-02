// Package verification keeps docs/VERIFICATION_MATRIX.md honest about what it
// points at.
//
// It needs no database and no broker, so it runs under `make test-unit` and in
// the fast CI job. It checks that the matrix is complete and that everything it
// names still exists. It does NOT check that a named test is load-bearing: a
// test that still exists and no longer proves its row passes this check. That
// is the reviewer's job, which is why the matrix records, for every row, the
// production-code mutation a reader can use to see the test fail.
package verification

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	repoRoot   = "../.."
	matrixFile = "docs/VERIFICATION_MATRIX.md"

	invariantCount = 18
	scenarioCount  = 12
)

// matrixHeader is the one header both tables must carry, cell for cell.
var matrixHeader = []string{"ID", "Statement", "Tests", "Durable-state assertion", "Code path", "Status"}

var validStatuses = map[string]bool{"COVERED": true, "CLOSED-M7A": true, "STOPPED": true}

var (
	idPattern        = regexp.MustCompile(`^([IS])([0-9]+)$`)
	testRefPattern   = regexp.MustCompile("`(Test[A-Za-z0-9_]+)` \\(([^()\\s]+)\\)")
	locationPattern  = regexp.MustCompile(`([A-Za-z0-9_./-]+\.go):([0-9]+)`)
	codePathPattern  = regexp.MustCompile("`([A-Za-z0-9_./-]+\\.(?:go|sql)):([A-Za-z0-9_]+)`")
	lineBreakPattern = regexp.MustCompile(`(?i)<br\s*/?>`)
)

type testRef struct{ name, path string }

type location struct {
	path string
	line int
}

type codeRef struct{ path, symbol string }

type row struct {
	table      int // 0 for the invariants table, 1 for the scenarios table
	id         string
	docLine    int
	tests      []testRef
	assertions []location
	codePaths  []codeRef
	status     string
}

// parseMatrix reads the matrix's tables. It reports every structural problem it
// finds rather than stopping at the first, so one run shows the whole picture.
func parseMatrix(markdown string) (rows []row, problems []string) {
	table := -1
	inTable := false
	for index, raw := range strings.Split(markdown, "\n") {
		docLine := index + 1
		line := strings.TrimSpace(raw)
		if !strings.HasPrefix(line, "|") {
			inTable = false
			continue
		}
		cells := splitCells(line)
		if !inTable {
			// The first line of a table is its header.
			inTable = true
			table++
			if !equalStrings(cells, matrixHeader) {
				problems = append(problems, fmt.Sprintf(
					"line %d: table %d's header is %q, want %q", docLine, table+1, cells, matrixHeader))
			}
			continue
		}
		if isSeparator(cells) {
			continue
		}
		if len(cells) != len(matrixHeader) {
			problems = append(problems, fmt.Sprintf(
				"line %d: a row has %d cells, want %d (a '|' inside a cell?)", docLine, len(cells), len(matrixHeader)))
			continue
		}
		parsed, rowProblems := parseRow(table, docLine, cells)
		problems = append(problems, rowProblems...)
		rows = append(rows, parsed)
	}
	if table != 1 {
		problems = append(problems, fmt.Sprintf("the matrix has %d tables, want exactly 2 (invariants, then scenarios)", table+1))
	}
	return rows, problems
}

func splitCells(line string) []string {
	line = strings.TrimSuffix(strings.TrimPrefix(line, "|"), "|")
	cells := strings.Split(line, "|")
	for i := range cells {
		cells[i] = strings.TrimSpace(cells[i])
	}
	return cells
}

func isSeparator(cells []string) bool {
	for _, cell := range cells {
		if strings.Trim(cell, "-: ") != "" {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func parseRow(table, docLine int, cells []string) (row, []string) {
	parsed := row{table: table, id: cells[0], docLine: docLine, status: cells[5]}
	var problems []string
	complain := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf("line %d (%s): ", docLine, cells[0])+fmt.Sprintf(format, args...))
	}

	if !idPattern.MatchString(parsed.id) {
		complain("ID %q is not I<n> or S<n>", parsed.id)
	}
	if cells[1] == "" {
		complain("the statement is empty")
	}

	for _, m := range testRefPattern.FindAllStringSubmatch(cells[2], -1) {
		parsed.tests = append(parsed.tests, testRef{name: m[1], path: m[2]})
	}
	// Everything in the Tests cell must be a recognised reference. Anything left
	// over is a reference that did not parse, and would otherwise be silently
	// unchecked.
	leftover := lineBreakPattern.ReplaceAllString(testRefPattern.ReplaceAllString(cells[2], ""), "")
	if strings.TrimSpace(leftover) != "" {
		complain("the Tests cell has text that is not a `TestName` (path) reference: %q", strings.TrimSpace(leftover))
	}

	for _, m := range locationPattern.FindAllStringSubmatch(cells[3], -1) {
		line, err := strconv.Atoi(m[2])
		if err != nil || line < 1 {
			complain("%q is not a positive line number", m[0])
			continue
		}
		parsed.assertions = append(parsed.assertions, location{path: m[1], line: line})
	}
	for _, m := range codePathPattern.FindAllStringSubmatch(cells[4], -1) {
		parsed.codePaths = append(parsed.codePaths, codeRef{path: m[1], symbol: m[2]})
	}

	if !validStatuses[parsed.status] {
		complain("status %q is not one of COVERED, CLOSED-M7A, STOPPED", parsed.status)
	}
	return parsed, problems
}

// goFile is one parsed Go file. Parsing deliberately goes through go/parser
// directly and never go/build, so a file guarded by `//go:build integration` is
// read exactly like any other: the parser ignores build constraints, which is
// what lets this check see tests that `go test ./...` does not compile.
type goFile struct {
	lines int
	funcs map[string][]funcSpan // by name; a method and a function may share one
}

type funcSpan struct {
	start, end int
	isMethod   bool
	signature  *ast.FuncType
}

func parseGoFile(path string) (*goFile, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	parsed := &goFile{
		lines: strings.Count(string(source), "\n") + 1,
		funcs: map[string][]funcSpan{},
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		parsed.funcs[fn.Name.Name] = append(parsed.funcs[fn.Name.Name], funcSpan{
			start:     fset.Position(fn.Pos()).Line,
			end:       fset.Position(fn.End()).Line,
			isMethod:  fn.Recv != nil,
			signature: fn.Type,
		})
	}
	return parsed, nil
}

// isTestFunc reports whether a declaration has the shape `go test` runs:
// a top-level function taking exactly one *testing.T.
func isTestFunc(span funcSpan) bool {
	if span.isMethod || span.signature.Results != nil || span.signature.Params == nil ||
		len(span.signature.Params.List) != 1 {
		return false
	}
	star, ok := span.signature.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "testing" && selector.Sel.Name == "T"
}

// validate checks the parsed matrix against the files under root and returns
// every problem. An empty result means the matrix is complete and every
// reference in it resolves.
func validate(rows []row, root string) []string {
	var problems []string
	files := map[string]*goFile{}
	load := func(path string) (*goFile, string) {
		if cached, ok := files[path]; ok {
			return cached, ""
		}
		parsed, err := parseGoFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			return nil, fmt.Sprintf("cannot read %s: %v", path, err)
		}
		files[path] = parsed
		return parsed, ""
	}

	// Every ID exactly once, in the right table.
	seen := map[string][]int{}
	for _, r := range rows {
		seen[r.id] = append(seen[r.id], r.docLine)
	}
	expect := func(prefix string, count, table int) {
		for n := 1; n <= count; n++ {
			id := fmt.Sprintf("%s%d", prefix, n)
			switch lines := seen[id]; len(lines) {
			case 0:
				problems = append(problems, fmt.Sprintf("%s is missing from the matrix", id))
			case 1:
			default:
				problems = append(problems, fmt.Sprintf("%s appears %d times (lines %v), want once", id, len(lines), lines))
			}
		}
		for _, r := range rows {
			m := idPattern.FindStringSubmatch(r.id)
			if m == nil || m[1] != prefix {
				continue
			}
			if n, _ := strconv.Atoi(m[2]); n < 1 || n > count {
				problems = append(problems, fmt.Sprintf("line %d: %s is outside %s1-%s%d", r.docLine, r.id, prefix, prefix, count))
			}
			if r.table != table {
				problems = append(problems, fmt.Sprintf("line %d: %s is in table %d, want table %d", r.docLine, r.id, r.table+1, table+1))
			}
		}
	}
	expect("I", invariantCount, 0)
	expect("S", scenarioCount, 1)

	for _, r := range rows {
		complain := func(format string, args ...any) {
			problems = append(problems, fmt.Sprintf("%s (line %d): ", r.id, r.docLine)+fmt.Sprintf(format, args...))
		}
		if len(r.tests) == 0 {
			complain("no test is named")
		}
		if len(r.assertions) == 0 {
			complain("no durable-state assertion location (file.go:line) is given")
		}
		if len(r.codePaths) == 0 {
			complain("no code path (`file.go:Function`) is given")
		}

		// Each named test must exist, in the file named, as a real test function.
		for _, ref := range r.tests {
			if !strings.HasSuffix(ref.path, "_test.go") {
				complain("%s is named in %s, which is not a _test.go file", ref.name, ref.path)
				continue
			}
			file, problem := load(ref.path)
			if problem != "" {
				complain("%s", problem)
				continue
			}
			spans := file.funcs[ref.name]
			found := false
			for _, span := range spans {
				found = found || isTestFunc(span)
			}
			if !found {
				complain("%s is not a test function (func %s(t *testing.T)) in %s", ref.name, ref.name, ref.path)
			}
		}

		// Each cited assertion line must sit inside one of THIS row's tests, in the
		// file cited. A line that has drifted out of every named test is a citation
		// that no longer points at anything the row claims.
		for _, at := range r.assertions {
			file, problem := load(at.path)
			if problem != "" {
				complain("%s", problem)
				continue
			}
			if at.line > file.lines {
				complain("%s:%d is past the end of the file (%d lines)", at.path, at.line, file.lines)
				continue
			}
			inside := false
			for _, ref := range r.tests {
				if ref.path != at.path {
					continue
				}
				for _, span := range file.funcs[ref.name] {
					inside = inside || (at.line >= span.start && at.line <= span.end)
				}
			}
			if !inside {
				complain("%s:%d is not inside any test this row names in that file", at.path, at.line)
			}
		}

		// Each code path must still exist.
		for _, ref := range r.codePaths {
			if strings.HasSuffix(ref.path, ".sql") {
				content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(ref.path)))
				switch {
				case err != nil:
					complain("cannot read %s: %v", ref.path, err)
				case !strings.Contains(string(content), ref.symbol):
					complain("%s no longer mentions %s", ref.path, ref.symbol)
				}
				continue
			}
			file, problem := load(ref.path)
			if problem != "" {
				complain("%s", problem)
				continue
			}
			if len(file.funcs[ref.symbol]) == 0 {
				complain("%s no longer declares %s", ref.path, ref.symbol)
			}
		}
	}

	sort.Strings(problems)
	return problems
}

// TestVerificationMatrix_EveryRowResolves is the drift check against the real
// matrix and the real tree.
func TestVerificationMatrix_EveryRowResolves(t *testing.T) {
	content, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(matrixFile)))
	require.NoError(t, err, "the matrix is canonical and must exist")

	rows, problems := parseMatrix(string(content))
	problems = append(problems, validate(rows, repoRoot)...)
	require.Empty(t, problems, "docs/VERIFICATION_MATRIX.md has drifted from the tree:\n  %s",
		strings.Join(problems, "\n  "))

	// A summary in the test's own output, so a run shows it really parsed a
	// matrix and resolved its references rather than passing on an empty one.
	distinct := map[testRef]bool{}
	for _, r := range rows {
		for _, ref := range r.tests {
			distinct[ref] = true
		}
	}
	require.Len(t, rows, invariantCount+scenarioCount)
	t.Logf("verification matrix: %d rows (I1-I%d, S1-S%d), %d distinct tests resolved to real test functions",
		len(rows), invariantCount, scenarioCount, len(distinct))
}

// --- the checker's own tests -------------------------------------------------
//
// These exercise validate against small synthetic trees, so each way the real
// check can fail is a failure the suite itself demonstrates on every run.

const fixtureTestFile = `//go:build integration

package fixture

import "testing"

func TestAlpha(t *testing.T) {
	_ = 1
	_ = 2
}

func TestBeta(t *testing.T) {
	_ = 3
}

func notATest(t *testing.T) {}

func TestNotATestEither(x int) {}
`

const fixtureCodeFile = `package fixture

func Produce() {}
`

// fixtureRows builds a complete, valid matrix over the fixture tree and returns
// it with its root. Each test then breaks exactly one thing.
func fixtureRows(t *testing.T) ([]string, string) {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "tests", "integration"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "tests", "integration", "fixture_test.go"), []byte(fixtureTestFile), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "produce.go"), []byte(fixtureCodeFile), 0o644))

	lines := []string{
		"# Matrix", "",
		"| " + strings.Join(matrixHeader, " | ") + " |",
		"| --- | --- | --- | --- | --- | --- |",
	}
	row := func(id string) string {
		return fmt.Sprintf("| %s | statement | `TestAlpha` (tests/integration/fixture_test.go) | tests/integration/fixture_test.go:7 | `produce.go:Produce` | COVERED |", id)
	}
	for n := 1; n <= invariantCount; n++ {
		lines = append(lines, row(fmt.Sprintf("I%d", n)))
	}
	lines = append(lines, "", "| "+strings.Join(matrixHeader, " | ")+" |", "| --- | --- | --- | --- | --- | --- |")
	for n := 1; n <= scenarioCount; n++ {
		lines = append(lines, row(fmt.Sprintf("S%d", n)))
	}
	return lines, root
}

func check(t *testing.T, lines []string, root string) []string {
	t.Helper()
	rows, problems := parseMatrix(strings.Join(lines, "\n"))
	return append(problems, validate(rows, root)...)
}

func TestMatrixChecker_AcceptsAValidMatrixAndReadsBuildTaggedFiles(t *testing.T) {
	lines, root := fixtureRows(t)
	// fixture_test.go carries `//go:build integration`. That it resolves at all is
	// the proof that build-tagged files are parsed.
	require.Empty(t, check(t, lines, root))
}

func TestMatrixChecker_FailsWhenARowIsDeleted(t *testing.T) {
	lines, root := fixtureRows(t)
	for i, line := range lines {
		if strings.HasPrefix(line, "| S7 |") {
			lines = append(lines[:i], lines[i+1:]...)
			break
		}
	}
	problems := check(t, lines, root)
	require.Len(t, problems, 1)
	require.Contains(t, problems[0], "S7 is missing")
}

func TestMatrixChecker_FailsOnADuplicateRow(t *testing.T) {
	lines, root := fixtureRows(t)
	for i, line := range lines {
		if strings.HasPrefix(line, "| I3 |") {
			lines = append(lines[:i+1], append([]string{line}, lines[i+1:]...)...)
			break
		}
	}
	require.Contains(t, strings.Join(check(t, lines, root), "\n"), "I3 appears 2 times")
}

func TestMatrixChecker_FailsWhenAReferencedTestDoesNotExist(t *testing.T) {
	lines, root := fixtureRows(t)
	for i, line := range lines {
		if strings.HasPrefix(line, "| I5 |") {
			lines[i] = strings.Replace(line, "`TestAlpha`", "`TestRenamedAway`", 1)
		}
	}
	problems := check(t, lines, root)
	require.Contains(t, strings.Join(problems, "\n"), "TestRenamedAway is not a test function")
}

func TestMatrixChecker_OnlyRealTestFunctionsCount(t *testing.T) {
	for _, name := range []string{"notATest", "TestNotATestEither"} {
		lines, root := fixtureRows(t)
		for i, line := range lines {
			if strings.HasPrefix(line, "| I5 |") {
				lines[i] = strings.Replace(line, "`TestAlpha`", "`"+name+"`", 1)
			}
		}
		// notATest has no Test prefix, so it does not even parse as a reference.
		require.NotEmpty(t, check(t, lines, root), name)
	}
}

func TestMatrixChecker_FailsWhenACitedLineIsOutsideTheNamedTests(t *testing.T) {
	lines, root := fixtureRows(t)
	for i, line := range lines {
		if strings.HasPrefix(line, "| I9 |") {
			// Line 12 is inside TestBeta, which this row does not name.
			lines[i] = strings.Replace(line, "fixture_test.go:7", "fixture_test.go:12", 1)
		}
		if strings.HasPrefix(line, "| I10 |") {
			lines[i] = strings.Replace(line, "fixture_test.go:7", "fixture_test.go:900", 1)
		}
	}
	problems := strings.Join(check(t, lines, root), "\n")
	require.Contains(t, problems, "fixture_test.go:12 is not inside any test this row names")
	require.Contains(t, problems, "fixture_test.go:900 is past the end of the file")
}

func TestMatrixChecker_FailsOnAMissingCodePathAndABadStatus(t *testing.T) {
	lines, root := fixtureRows(t)
	for i, line := range lines {
		if strings.HasPrefix(line, "| I11 |") {
			lines[i] = strings.Replace(line, "produce.go:Produce", "produce.go:Renamed", 1)
		}
		if strings.HasPrefix(line, "| I12 |") {
			lines[i] = strings.Replace(line, "COVERED", "PROBABLY", 1)
		}
	}
	problems := strings.Join(check(t, lines, root), "\n")
	require.Contains(t, problems, "produce.go no longer declares Renamed")
	require.Contains(t, problems, `status "PROBABLY" is not one of`)
}

func TestMatrixChecker_FailsOnAnUnparsedTestReference(t *testing.T) {
	lines, root := fixtureRows(t)
	for i, line := range lines {
		if strings.HasPrefix(line, "| I2 |") {
			// A reference missing its path would otherwise be silently unchecked.
			lines[i] = strings.Replace(line, "`TestAlpha` (tests/integration/fixture_test.go)",
				"`TestAlpha` (tests/integration/fixture_test.go)<br>`TestBeta`", 1)
		}
	}
	require.Contains(t, strings.Join(check(t, lines, root), "\n"), "has text that is not a `TestName` (path) reference")
}

func TestMatrixChecker_RequiresExactlyTwoTables(t *testing.T) {
	lines, root := fixtureRows(t)
	// Keep only the invariants table.
	var firstTableOnly []string
	for _, line := range lines {
		if strings.HasPrefix(line, "| S1 |") {
			break
		}
		firstTableOnly = append(firstTableOnly, line)
	}
	// The scenarios table's header is still there, so cut it off too.
	firstTableOnly = firstTableOnly[:len(firstTableOnly)-3]
	problems := strings.Join(check(t, firstTableOnly, root), "\n")
	require.Contains(t, problems, "the matrix has 1 tables, want exactly 2")
	require.Contains(t, problems, "S1 is missing from the matrix")
}
