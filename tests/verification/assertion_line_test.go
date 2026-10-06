package verification

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// This file is the rule that decides whether a line the matrix cites as "where a
// test asserts durable state" is an assertion at all. It replaces the weaker
// check that the line merely lies somewhere inside a named test, under which a
// setup line, a blank line or a comment passed.
//
// A cited line L in the test named T is valid only if BOTH hold:
//
//	(a) an assertion call covers it. In T's body, including nested t.Run
//	    subtests and closures defined in it, there is a call expression C whose
//	    source span (first line to last line) covers L, and C is one of
//	      - any call through package require or assert;
//	      - Fatal, Fatalf, Error, Errorf, Fail or FailNow called on a *testing.T;
//	      - a call to a function in the same package that takes a *testing.T and
//	        whose body DIRECTLY contains a call of the two kinds above. One level
//	        only: a helper that asserts only through another helper does not count.
//	(b) it is on the test's own goroutine. C is not inside a function literal
//	    launched by a go statement, and not inside one passed to Eventually,
//	    Eventuallyf, EventuallyWithT, EventuallyWithTf, Never, Neverf, Condition or
//	    Conditionf of require or assert.
//
// (b) is read to apply to the cited LINE as well as to C: L must not lie in the
// body of such a literal. Otherwise the outer require.Eventually call would itself
// satisfy (a) and (b) for every line of its closure, and a line that is only a
// setup read inside a polled closure would be accepted. The call's own first line
// and its closing line (the arguments after the closure) are not in the body, and
// are accepted like any other line of an assertion call.
//
// What this does NOT establish, and cannot syntactically: that the assertion reads
// DURABLE state (PostgreSQL) rather than an HTTP status or in-memory value. That is
// the reviewer's job, and docs/VERIFICATION_MATRIX.md says so.
//
// Calls are identified by syntax, with go/parser and go/ast, and the package a
// call goes through is resolved from the file's own imports, so a renamed import is
// followed and an unrelated package that happens to be called require is not.

const (
	testifyRequire = "github.com/stretchr/testify/require"
	testifyAssert  = "github.com/stretchr/testify/assert"
)

// failingMethods are the *testing.T methods that fail a test, which count as an
// assertion when called on one.
var failingMethods = map[string]bool{
	"Fatal": true, "Fatalf": true, "Error": true, "Errorf": true, "Fail": true, "FailNow": true,
}

// pollingMethods are the require and assert functions that run a closure repeatedly
// and off the test's goroutine.
var pollingMethods = map[string]bool{
	"Eventually": true, "Eventuallyf": true, "EventuallyWithT": true, "EventuallyWithTf": true,
	"Never": true, "Neverf": true, "Condition": true, "Conditionf": true,
}

// sourceFile is a parsed Go file with what the rule reads from it.
type sourceFile struct {
	path    string
	fset    *token.FileSet
	file    *ast.File
	lines   []string          // the source, one entry per line; lines[0] is line 1
	imports map[string]string // local package name -> import path
}

func parseSourceFile(path string, source []byte) (*sourceFile, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	imports := map[string]string{}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := importPath[strings.LastIndex(importPath, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		if name == "_" || name == "." {
			continue
		}
		imports[name] = importPath
	}
	return &sourceFile{
		path: path, fset: fset, file: file,
		lines: strings.Split(string(source), "\n"), imports: imports,
	}, nil
}

func (f *sourceFile) line(pos token.Pos) int { return f.fset.Position(pos).Line }

// helperDecl is a top-level function and the file it is declared in.
type helperDecl struct {
	decl *ast.FuncDecl
	file *sourceFile
}

// packageIndex is every file of one package and its top-level functions by name,
// which is what "a function in the same package" means for a helper.
type packageIndex struct {
	funcs map[string][]helperDecl
}

func newPackageIndex(files ...*sourceFile) *packageIndex {
	index := &packageIndex{funcs: map[string][]helperDecl{}}
	for _, f := range files {
		for _, decl := range f.file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Body != nil {
				index.funcs[fn.Name.Name] = append(index.funcs[fn.Name.Name], helperDecl{fn, f})
			}
		}
	}
	return index
}

// loadPackageIndex parses every Go file in dir that belongs to package pkgName,
// build constraints ignored, exactly as parseGoFile reads a single file: a helper
// in a `//go:build integration` file is a helper like any other.
func loadPackageIndex(dir, pkgName string) (*packageIndex, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []*sourceFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		source, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		parsed, err := parseSourceFile(path, source)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		if parsed.file.Name.Name == pkgName {
			files = append(files, parsed)
		}
	}
	return newPackageIndex(files...), nil
}

// testingTParams are the names of the parameters of fn declared *testing.T.
func testingTParams(fn *ast.FuncType, into map[string]bool) {
	if fn.Params == nil {
		return
	}
	for _, field := range fn.Params.List {
		star, ok := field.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		selector, ok := star.X.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "testing" && selector.Sel.Name == "T" {
			for _, name := range field.Names {
				into[name.Name] = true
			}
		}
	}
}

// takesTestingT reports whether fn has a *testing.T parameter.
func takesTestingT(fn *ast.FuncType) bool {
	names := map[string]bool{}
	testingTParams(fn, names)
	return len(names) > 0
}

// allTestingTParams is every *testing.T parameter name of root and of the function
// literals inside it, which is how a subtest's own t is recognised.
func allTestingTParams(root ast.Node) map[string]bool {
	names := map[string]bool{}
	ast.Inspect(root, func(n ast.Node) bool {
		switch fn := n.(type) {
		case *ast.FuncDecl:
			testingTParams(fn.Type, names)
		case *ast.FuncLit:
			testingTParams(fn.Type, names)
		}
		return true
	})
	return names
}

// testifyCall returns the package ("require" or "assert", by import path) and the
// function name when call is a call through one of them.
func testifyCall(call *ast.CallExpr, imports map[string]string) (pkg, name string, ok bool) {
	selector, isSelector := call.Fun.(*ast.SelectorExpr)
	if !isSelector {
		return "", "", false
	}
	ident, isIdent := selector.X.(*ast.Ident)
	if !isIdent {
		return "", "", false
	}
	switch imports[ident.Name] {
	case testifyRequire:
		return "require", selector.Sel.Name, true
	case testifyAssert:
		return "assert", selector.Sel.Name, true
	}
	return "", "", false
}

// isAssertionCall is the two direct kinds of assertion: any call through require or
// assert, and a failing method called on a *testing.T.
func isAssertionCall(call *ast.CallExpr, imports map[string]string, tParams map[string]bool) bool {
	if _, _, ok := testifyCall(call, imports); ok {
		return true
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !failingMethods[selector.Sel.Name] {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	return ok && tParams[ident.Name]
}

// offGoroutine explains why the node at the top of stack does not run on the test's
// own goroutine, or "" when it does: it sits inside a function literal that a go
// statement launches, or that is passed to a polling function of require or assert,
// or it is itself the call a go statement launches.
func offGoroutine(stack []ast.Node, imports map[string]string) string {
	for i := len(stack) - 1; i >= 0; i-- {
		switch node := stack[i].(type) {
		case *ast.GoStmt:
			return "is launched by a go statement"
		case *ast.FuncLit:
			if why := literalOffGoroutine(stack[:i], node, imports); why != "" {
				return why
			}
		}
	}
	return ""
}

// literalOffGoroutine reports why lit, whose ancestors are parents, runs off the
// test's goroutine, or "".
func literalOffGoroutine(parents []ast.Node, lit *ast.FuncLit, imports map[string]string) string {
	if len(parents) == 0 {
		return ""
	}
	call, ok := parents[len(parents)-1].(*ast.CallExpr)
	if !ok {
		return ""
	}
	if call.Fun == ast.Expr(lit) && len(parents) >= 2 {
		if _, launched := parents[len(parents)-2].(*ast.GoStmt); launched {
			return "is inside a function literal launched by a go statement, so it runs on another goroutine"
		}
	}
	for _, arg := range call.Args {
		if arg != ast.Expr(lit) {
			continue
		}
		if pkg, name, ok := testifyCall(call, imports); ok && pollingMethods[name] {
			return fmt.Sprintf("is inside a closure passed to %s.%s, which is polled on another goroutine", pkg, name)
		}
	}
	return ""
}

// interior is the lines strictly between a function literal's opening and closing
// braces, with why those lines are off the test's goroutine.
type interior struct {
	from, to int
	why      string
}

type callKind int

const (
	kindOther callKind = iota
	kindAssertion
	kindHelperAsserts
	kindHelperNoAssertion
	kindHelperOfHelper
	kindLocalClosure // a call to a closure declared inside the test, which is not a package-level helper
)

// classified is a call expression in the test, with the lines it spans.
type classified struct {
	first, last int
	kind        callKind
	callee      string // the helper's name, for a helper kind
	through     string // the helper that a helper of a helper asserts through
	off         string // non-empty: why it is not on the test's own goroutine
}

// helperKind classifies a call to a same-package function, by what its body does.
// A helper asserts when its body DIRECTLY contains an assertion call on its own
// goroutine; a helper that asserts only by calling another helper is a helper of a
// helper, which does not count.
func (p *packageIndex) helperKind(name string) (kind callKind, through string) {
	for _, h := range p.funcs[name] {
		if !takesTestingT(h.decl.Type) {
			continue
		}
		if p.directlyAsserts(h) {
			return kindHelperAsserts, ""
		}
		kind = kindHelperNoAssertion
		// Not asserting itself, but does it reach an assertion through a helper?
		ast.Inspect(h.decl.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name != name {
				for _, inner := range p.funcs[ident.Name] {
					if takesTestingT(inner.decl.Type) && p.directlyAsserts(inner) {
						kind, through = kindHelperOfHelper, ident.Name
					}
				}
			}
			return true
		})
		return kind, through
	}
	return kindOther, ""
}

// directlyAsserts reports whether a helper's body contains an assertion call that
// runs on its goroutine.
func (p *packageIndex) directlyAsserts(h helperDecl) bool {
	tParams := allTestingTParams(h.decl)
	found := false
	var stack []ast.Node
	ast.Inspect(h.decl.Body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		if call, ok := n.(*ast.CallExpr); ok && isAssertionCall(call, h.file.imports, tParams) &&
			offGoroutine(stack, h.file.imports) == "" {
			found = true
		}
		return !found
	})
	return found
}

// citationProblem is the rule. It returns why line is not a valid assertion
// citation inside the test named testName in f, or "" when it is valid.
func citationProblem(pkg *packageIndex, f *sourceFile, testName string, line int) string {
	var decl *ast.FuncDecl
	for _, d := range f.file.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == testName && fn.Recv == nil && fn.Body != nil {
			decl = fn
		}
	}
	if decl == nil {
		return fmt.Sprintf("there is no test function %s in the file", testName)
	}
	if line < f.line(decl.Pos()) || line > f.line(decl.End()) {
		return fmt.Sprintf("is outside %s", testName)
	}

	tParams := allTestingTParams(decl)
	// Closures the test declares for itself (name := func(...) {...}). A call to one
	// is not a call to a same-package function, so the helper rule does not apply to
	// it; the assertion inside the closure is what a citation should point at.
	closures := map[string]bool{}
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		if assign, ok := n.(*ast.AssignStmt); ok {
			for i, rhs := range assign.Rhs {
				if _, isLit := rhs.(*ast.FuncLit); !isLit || i >= len(assign.Lhs) {
					continue
				}
				if ident, ok := assign.Lhs[i].(*ast.Ident); ok {
					closures[ident.Name] = true
				}
			}
		}
		return true
	})
	var (
		calls     []classified
		interiors []interior
		stack     []ast.Node
	)
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)

		if lit, ok := n.(*ast.FuncLit); ok {
			if why := literalOffGoroutine(stack[:len(stack)-1], lit, f.imports); why != "" {
				from, to := f.line(lit.Body.Lbrace)+1, f.line(lit.Body.Rbrace)-1
				if from <= to {
					interiors = append(interiors, interior{from, to, why})
				}
			}
		}

		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		c := classified{first: f.line(call.Pos()), last: f.line(call.End()), off: offGoroutine(stack, f.imports)}
		switch {
		case isAssertionCall(call, f.imports, tParams):
			c.kind = kindAssertion
		default:
			if ident, isIdent := call.Fun.(*ast.Ident); isIdent {
				c.callee = ident.Name
				if closures[ident.Name] {
					c.kind = kindLocalClosure
				} else {
					c.kind, c.through = pkg.helperKind(ident.Name)
				}
			}
		}
		calls = append(calls, c)
		return true
	})

	var coverers []classified
	for _, c := range calls {
		if c.first <= line && line <= c.last {
			coverers = append(coverers, c)
		}
	}

	// (b), for the line itself: not in the body of a literal that runs elsewhere.
	for _, in := range interiors {
		if in.from <= line && line <= in.to {
			return in.why
		}
	}
	// (a), with (b) for the call.
	var offReason string
	for _, c := range coverers {
		if c.kind != kindAssertion && c.kind != kindHelperAsserts {
			continue
		}
		if c.off == "" {
			return ""
		}
		offReason = "is in an assertion call that " + c.off
	}
	if offReason != "" {
		return offReason
	}

	// Why not (a): name the most specific reason there is.
	for _, c := range coverers {
		switch c.kind {
		case kindHelperNoAssertion:
			return fmt.Sprintf("is a call to the helper %s, which takes a *testing.T but contains no assertion call", c.callee)
		case kindHelperOfHelper:
			return fmt.Sprintf("is a call to the helper %s, which asserts only through the helper %s; only one level of helper counts",
				c.callee, c.through)
		case kindLocalClosure:
			return fmt.Sprintf("is a call to %s, a closure declared inside the test, not a same-package helper; cite the assertion inside the closure instead",
				c.callee)
		}
	}
	text := ""
	if line >= 1 && line <= len(f.lines) {
		text = strings.TrimSpace(f.lines[line-1])
	}
	switch {
	case text == "":
		return "is a blank line"
	case strings.HasPrefix(text, "//"):
		return "is a comment line"
	}
	return "is not inside an assertion call: it is setup, not a call through require or assert, t.Fatal/Error/Fail, or a helper that asserts"
}

// citationVerdict is one cited location judged by the rule, for the inventory.
type citationVerdict struct {
	row    string
	path   string
	line   int
	test   string
	reason string // "" when valid
	text   string // the cited line's source text
}

// sortedVerdicts orders verdicts by file, then line, then row.
func sortedVerdicts(verdicts []citationVerdict) []citationVerdict {
	sort.SliceStable(verdicts, func(i, j int) bool {
		a, b := verdicts[i], verdicts[j]
		switch {
		case a.path != b.path:
			return a.path < b.path
		case a.line != b.line:
			return a.line < b.line
		}
		return a.row < b.row
	})
	return verdicts
}
