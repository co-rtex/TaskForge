package verification

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The rule's own tests (see assertion_line_test.go for the rule). Each case is a
// line of a small Go fixture, found by a marker comment so editing the fixture
// cannot silently move a case, and each must be accepted or rejected for the
// reason the test says. A rule that accepted everything, or rejected everything,
// would not survive them.

// ruleFixtureTests is the file the cases are lines of. Its helpers are in a second
// file of the same package, which is what "a function in the same package" means.
const ruleFixtureTests = `package fixture

import (
	"strconv"
	"sync"
	"testing"
	"time"

	assertlib "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCases(t *testing.T) {
	rows := []int{1} // MARK:setup
	// MARK:comment: a comment line, followed by a blank one

	require.Equal(t, // MARK:multi-first
		1,
		len(rows), // MARK:multi-middle
		"the message", // MARK:multi-message
	)
	t.Run("sub", func(t *testing.T) {
		require.Len(t, rows, 1) // MARK:in-subtest
	})
	check := func() {
		assertlib.True(t, true) // MARK:in-closure
	}
	check()
	checkAgain := func(t *testing.T) {
		_ = rows // MARK:closure-setup
	}
	checkAgain(t) // MARK:closure-call
	if len(rows) == 0 {
		t.Fatalf("no rows") // MARK:fatalf
	}
	requireRows(t, rows) // MARK:helper-one-level
	require.Eventually(t, func() bool { // MARK:eventually-first
		n := count() // MARK:in-eventually
		return n == 1
	}, time.Second, time.Millisecond) // MARK:eventually-last
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.True(t, true) // MARK:in-go
	}()
	wg.Wait()
	noopHelper(t, rows) // MARK:helper-no-assertion
	chainHelper(t, rows) // MARK:helper-of-helper
}
`

const ruleFixtureHelpers = `package fixture

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func count() int { return 0 }

// requireRows asserts directly: it is a helper that counts.
func requireRows(t *testing.T, rows []int) {
	t.Helper()
	require.NotEmpty(t, rows)
}

// noopHelper takes a *testing.T and asserts nothing.
func noopHelper(t *testing.T, rows []int) {
	t.Helper()
	_ = rows
}

// chainHelper asserts only by calling another helper, which does not count.
func chainHelper(t *testing.T, rows []int) {
	requireRows(t, rows)
}
`

// ruleFixture parses the fixture package and returns what citationProblem needs.
func ruleFixture(t *testing.T) (*packageIndex, *sourceFile) {
	t.Helper()
	tests, err := parseSourceFile("fixture_test.go", []byte(ruleFixtureTests))
	require.NoError(t, err)
	helpers, err := parseSourceFile("helpers.go", []byte(ruleFixtureHelpers))
	require.NoError(t, err)
	return newPackageIndex(tests, helpers), tests
}

func ruleProblem(t *testing.T, marker string, offset int) string {
	t.Helper()
	pkg, file := ruleFixture(t)
	return citationProblem(pkg, file, "TestCases", fixtureLine(t, ruleFixtureTests, marker)+offset)
}

// TestAssertionRule_RejectsALineThatIsNotAnAssertion proves each way a cited line
// can sit inside a test and still not be an assertion is rejected, with a message
// that names the reason.
func TestAssertionRule_RejectsALineThatIsNotAnAssertion(t *testing.T) {
	for name, tc := range map[string]struct {
		marker string
		offset int
		want   string
	}{
		"a setup line":                               {"MARK:setup", 0, "is not inside an assertion call"},
		"a blank line inside the test":               {"MARK:comment", 1, "is a blank line"},
		"a comment line":                             {"MARK:comment", 0, "is a comment line"},
		"a line inside a require.Eventually closure": {"MARK:in-eventually", 0, "closure passed to require.Eventually"},
		"a line inside a go func":                    {"MARK:in-go", 0, "launched by a go statement"},
		"a call to a helper with no assertion":       {"MARK:helper-no-assertion", 0, "helper noopHelper, which takes a *testing.T but contains no assertion call"},
		"a call to a helper of a helper":             {"MARK:helper-of-helper", 0, "asserts only through the helper requireRows; only one level of helper counts"},
		"a call to a closure the test declares":      {"MARK:closure-call", 0, "closure declared inside the test, not a same-package helper"},
		"a setup line inside a closure":              {"MARK:closure-setup", 0, "is not inside an assertion call"},
	} {
		t.Run(name, func(t *testing.T) {
			got := ruleProblem(t, tc.marker, tc.offset)
			require.NotEmpty(t, got, "the line must be rejected")
			require.Contains(t, got, tc.want)
		})
	}
}

// TestAssertionRule_AcceptsALineAnAssertionCallCovers proves each shape of
// assertion the rule is meant to accept is accepted: the first line of a
// multi-line require call, a continuation and a message line of one, an assertion
// in a subtest, in a closure, a t.Fatalf, a call to a helper that asserts
// directly, and the own first and closing lines of a require.Eventually call.
func TestAssertionRule_AcceptsALineAnAssertionCallCovers(t *testing.T) {
	for name, marker := range map[string]string{
		"the first line of a multi-line require call":    "MARK:multi-first",
		"a continuation line of one":                     "MARK:multi-middle",
		"its message line":                               "MARK:multi-message",
		"an assertion inside t.Run":                      "MARK:in-subtest",
		"an assertion inside a closure (renamed import)": "MARK:in-closure",
		"a t.Fatalf": "MARK:fatalf",
		"a call to a one-level helper that asserts":     "MARK:helper-one-level",
		"the first line of a require.Eventually call":   "MARK:eventually-first",
		"the closing line of a require.Eventually call": "MARK:eventually-last",
	} {
		t.Run(name, func(t *testing.T) {
			require.Empty(t, ruleProblem(t, marker, 0))
		})
	}
}

// TestAssertionRule_ResolvesTheAssertionPackageFromTheFilesImports proves a call
// is an assertion because of the package it goes through, not because of the name
// it is called by: a renamed import is followed (the closure case above), and an
// unrelated package that happens to be called require is not an assertion.
func TestAssertionRule_ResolvesTheAssertionPackageFromTheFilesImports(t *testing.T) {
	const impostor = `package fixture

import (
	"strconv"
	"testing"

	"example.com/not/testify/require"
)

func TestImpostor(t *testing.T) {
	require.Equal(t, 1, 1) // MARK:impostor
}
`
	file, err := parseSourceFile("impostor_test.go", []byte(impostor))
	require.NoError(t, err)
	got := citationProblem(newPackageIndex(file), file, "TestImpostor", fixtureLine(t, impostor, "MARK:impostor"))
	require.Contains(t, got, "is not inside an assertion call",
		"a package named require that is not testify's is not an assertion")
}

// TestAssertionRule_ARejectionNamesItsReasonInTheMatrixCheck proves the rule is
// what the drift check reports: a row whose cited line is a setup line inside its
// own named test fails validate with a message naming the line and the reason,
// where the older check, which only asked whether the line was inside the test,
// accepted it.
func TestAssertionRule_ARejectionNamesItsReasonInTheMatrixCheck(t *testing.T) {
	lines, root := fixtureRows(t)
	for i, line := range lines {
		if strings.HasPrefix(line, "| I6 |") {
			lines[i] = strings.Replace(line, "fixture_test.go:"+itoa(fixtureAlphaAssertion(t)),
				"fixture_test.go:"+itoa(fixtureAlphaSetup(t)), 1)
		}
	}
	problems := check(t, lines, root)
	require.Len(t, problems, 1, "%v", problems)
	require.Contains(t, problems[0], "I6")
	require.Contains(t, problems[0], "fixture_test.go:"+itoa(fixtureAlphaSetup(t))+" is not an assertion line")
	require.Contains(t, problems[0], "TestAlpha")
	require.Contains(t, problems[0], "is not inside an assertion call")
}

func itoa(n int) string { return strconv.Itoa(n) }

// matrixOverFixture builds a complete matrix whose rows all name TestCases in the
// rule fixture, citing the given file:line for the one row id, and checks it. The
// tree is the rule fixture's two files plus a code file, so the check is the real
// validate over a real directory, not the rule alone.
func matrixOverFixture(t *testing.T, id string, line int) []string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "tests", "integration")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "fixture_test.go"), []byte(ruleFixtureTests), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "helpers.go"), []byte(ruleFixtureHelpers), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "produce.go"), []byte(fixtureCodeFile), 0o644))

	good := fixtureLine(t, ruleFixtureTests, "MARK:in-subtest")
	row := func(rowID string, cited int) string {
		return fmt.Sprintf("| %s | statement | `TestCases` (tests/integration/fixture_test.go) | tests/integration/fixture_test.go:%d | `produce.go:Produce` | COVERED |", rowID, cited)
	}
	table := []string{"| " + strings.Join(matrixHeader, " | ") + " |", "| --- | --- | --- | --- | --- | --- |"}
	lines := append([]string{"# Matrix", ""}, table...)
	for n := 1; n <= invariantCount; n++ {
		rowID, cited := fmt.Sprintf("I%d", n), good
		if rowID == id {
			cited = line
		}
		lines = append(lines, row(rowID, cited))
	}
	lines = append(lines, "")
	lines = append(lines, table...)
	for n := 1; n <= scenarioCount; n++ {
		rowID, cited := fmt.Sprintf("S%d", n), good
		if rowID == id {
			cited = line
		}
		lines = append(lines, row(rowID, cited))
	}
	return check(t, lines, root)
}

// TestMatrixChecker_FailsWhenACitedLineIsInAClosureThatRunsElsewhere proves the
// drift check, over a real directory, refuses a row whose citation is inside a
// require.Eventually closure or a go func, naming the row and the reason.
//
// No test the real matrix cites contains an Eventually closure, so this is the
// check's evidence for that case: the same matrix with every row valid passes.
func TestMatrixChecker_FailsWhenACitedLineIsInAClosureThatRunsElsewhere(t *testing.T) {
	require.Empty(t, matrixOverFixture(t, "", 0), "control: with every row citing a real assertion, the matrix passes")

	for name, tc := range map[string]struct{ marker, want string }{
		"inside a require.Eventually closure": {"MARK:in-eventually", "closure passed to require.Eventually"},
		"inside a go func":                    {"MARK:in-go", "launched by a go statement"},
	} {
		t.Run(name, func(t *testing.T) {
			line := fixtureLine(t, ruleFixtureTests, tc.marker)
			problems := matrixOverFixture(t, "I7", line)
			require.Len(t, problems, 1, "%v", problems)
			t.Log(problems[0])
			require.Contains(t, problems[0], "I7")
			require.Contains(t, problems[0], fmt.Sprintf("fixture_test.go:%d is not an assertion line", line))
			require.Contains(t, problems[0], tc.want)
		})
	}
}

// TestMatrixChecker_FailsWhenACitedLineCallsAHelperThatAssertsNothing proves the
// drift check refuses a row whose citation is a call to a same-package helper that
// takes a *testing.T and contains no assertion, and one that asserts only through
// another helper, naming the helper.
//
// No test the real matrix cites makes such a call, so this is the evidence for it.
func TestMatrixChecker_FailsWhenACitedLineCallsAHelperThatAssertsNothing(t *testing.T) {
	for name, tc := range map[string]struct{ marker, want string }{
		"a helper with no assertion": {"MARK:helper-no-assertion", "helper noopHelper, which takes a *testing.T but contains no assertion call"},
		"a helper of a helper":       {"MARK:helper-of-helper", "only one level of helper counts"},
	} {
		t.Run(name, func(t *testing.T) {
			line := fixtureLine(t, ruleFixtureTests, tc.marker)
			problems := matrixOverFixture(t, "S3", line)
			require.Len(t, problems, 1, "%v", problems)
			t.Log(problems[0])
			require.Contains(t, problems[0], "S3")
			require.Contains(t, problems[0], tc.want)
		})
	}
}
