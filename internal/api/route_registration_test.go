package api

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The route table is the single source of routes only if nothing registers a
// route around it. This file holds that with a check over the syntax tree of the
// package's non-test files.
//
// A regex over source text cannot: it would match a comment, miss a registration
// split across lines, and could not tell a call on the server's mux from one on
// some other type that happens to have a method named Handle. Calls are
// identified by syntax and by receiver instead: a call to a method or function
// named Handle or HandleFunc, whose receiver is read from the declared type of
// the enclosing function's parameters.

// registrationFunctions are the only functions allowed to call Handle or
// HandleFunc. Together they are "the registration code that iterates the table":
// registerRoutes loops over the table and registers the "/" catch-all, register
// turns a chain into wrappers, and handleInternal puts the browser-origin guard
// outside whatever it is given.
var registrationFunctions = map[string]bool{
	"registerRoutes": true,
	"register":       true,
	"handleInternal": true,
}

// registrationFinding is one place the rule is broken.
type registrationFinding struct {
	pos token.Position
	msg string
}

func (f registrationFinding) String() string {
	return fmt.Sprintf("%s:%d: %s", f.pos.Filename, f.pos.Line, f.msg)
}

// checkRegistration reports every way the files break the registration rule:
//
//   - a call to Handle or HandleFunc outside registrationFunctions, including a
//     package-level http.Handle or http.HandleFunc, which would register on the
//     default mux;
//   - inside them, a call whose receiver is not that function's own
//     *http.ServeMux parameter;
//   - in registerRoutes, any registration but the single "/" catch-all outside
//     the loop; in register and handleInternal, any whose pattern is not the
//     function's own `pattern` parameter, which is what keeps a literal pattern
//     out of them;
//   - a call to register outside a loop in registerRoutes, and a call to
//     handleInternal from anywhere but register;
//   - and the absence of what a passing result depends on, so that renaming the
//     registration code cannot make the check pass by finding nothing.
func checkRegistration(fset *token.FileSet, files []*ast.File) []registrationFinding {
	var findings []registrationFinding
	report := func(n ast.Node, format string, args ...any) {
		findings = append(findings, registrationFinding{fset.Position(n.Pos()), fmt.Sprintf(format, args...)})
	}
	missing := func(format string, args ...any) {
		findings = append(findings, registrationFinding{token.Position{Filename: "(package)"}, fmt.Sprintf(format, args...)})
	}

	var catchAlls, loopedRegisters int
	var haveRegisterRoutes bool

	for _, file := range files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "registerRoutes" {
				haveRegisterRoutes = true
			}
		}

		var stack []ast.Node
		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)

			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			decl, inRange := enclosing(stack)
			name := ""
			if decl != nil {
				name = decl.Name.Name
			}

			switch sel.Sel.Name {
			case "Handle", "HandleFunc":
				if !registrationFunctions[name] {
					where := "package level"
					if decl != nil {
						where = name
					}
					report(call, "%s registers a route outside the route table's registration code (in %s)",
						types.ExprString(call.Fun), where)
					return true
				}
				if recv, ok := sel.X.(*ast.Ident); !ok || !serveMuxParams(decl)[recv.Name] {
					report(call, "%s in %s is not called on its own *http.ServeMux parameter",
						types.ExprString(call.Fun), name)
					return true
				}
				switch name {
				case "registerRoutes":
					if inRange || !isCatchAll(call) {
						report(call, `registerRoutes may register only the "/" catch-all directly, outside the loop; this is %s`,
							types.ExprString(call))
						return true
					}
					catchAlls++
				default: // register, handleInternal
					if !firstArgIsIdent(call, "pattern") {
						report(call, "%s must register its own `pattern` parameter, not %s",
							name, types.ExprString(call))
					}
				}

			case "register":
				switch {
				case name != "registerRoutes":
					report(call, "register is called from %q; only registerRoutes may call it", name)
				case !inRange:
					report(call, "register is called in registerRoutes outside the loop over the table")
				default:
					loopedRegisters++
				}

			case "handleInternal":
				if name != "register" {
					report(call, "handleInternal is called from %q; only register may call it", name)
				}
			}
			return true
		})
	}

	switch {
	case !haveRegisterRoutes:
		missing("registerRoutes was not found: the registration code moved or was renamed; update this check with it")
	case loopedRegisters == 0:
		missing("registerRoutes never calls register inside a loop, so it does not iterate the table")
	}
	if catchAlls != 1 {
		missing(`want exactly one "/" catch-all registered by registerRoutes, found %d`, catchAlls)
	}

	sort.Slice(findings, func(i, j int) bool {
		a, b := findings[i].pos, findings[j].pos
		if a.Filename != b.Filename {
			return a.Filename < b.Filename
		}
		return a.Line < b.Line
	})
	return findings
}

// enclosing finds the function declaration a node is inside, and whether a loop
// within that function is also on the path to the node.
func enclosing(stack []ast.Node) (decl *ast.FuncDecl, inRange bool) {
	for i := len(stack) - 1; i >= 0; i-- {
		switch n := stack[i].(type) {
		case *ast.RangeStmt:
			inRange = true
		case *ast.FuncDecl:
			return n, inRange
		}
	}
	return nil, false
}

// serveMuxParams names the parameters of fn declared as *http.ServeMux.
func serveMuxParams(fn *ast.FuncDecl) map[string]bool {
	names := map[string]bool{}
	if fn.Type.Params == nil {
		return names
	}
	for _, field := range fn.Type.Params.List {
		if types.ExprString(field.Type) != "*http.ServeMux" {
			continue
		}
		for _, name := range field.Names {
			names[name.Name] = true
		}
	}
	return names
}

func isCatchAll(call *ast.CallExpr) bool {
	if len(call.Args) != 2 {
		return false
	}
	lit, ok := call.Args[0].(*ast.BasicLit)
	return ok && lit.Kind == token.STRING && lit.Value == `"/"` &&
		types.ExprString(call.Args[1]) == "s.handleNotFound"
}

func firstArgIsIdent(call *ast.CallExpr, name string) bool {
	if len(call.Args) == 0 {
		return false
	}
	ident, ok := call.Args[0].(*ast.Ident)
	return ok && ident.Name == name
}

// packageNonTestFiles parses every non-test Go file in the package directory.
// Filenames are reported as they appear in the repository, so a finding is a
// clickable path.
func packageNonTestFiles(t *testing.T) (*token.FileSet, []*ast.File) {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		require.NoError(t, err)
		file, err := parser.ParseFile(fset, "internal/api/"+path, src, parser.SkipObjectResolution)
		require.NoError(t, err)
		files = append(files, file)
	}
	require.NotEmpty(t, files)
	return fset, files
}

// TestRoutes_NothingRegistersAroundTheTable proves that every Handle and
// HandleFunc call in internal/api's non-test files is in the route table's own
// registration code, and that the only registration that code makes outside the
// loop over the table is the "/" catch-all.
//
// It is what makes routeTable the single source of routes: a route registered
// any other way, however it is spelled, is reported with its file and line. It
// replaces the earlier check that counted `s.handleInternal(mux,` lines in
// server.go, which a table-driven Handler() breaks by construction and which
// could say nothing about a registration that was not on such a line.
func TestRoutes_NothingRegistersAroundTheTable(t *testing.T) {
	fset, files := packageNonTestFiles(t)
	findings := checkRegistration(fset, files)

	var lines []string
	for _, finding := range findings {
		lines = append(lines, finding.String())
	}
	require.Emptyf(t, findings,
		"a route is registered outside the route table; add it to routeTable in routes.go instead:\n%s",
		strings.Join(lines, "\n"))
}

// compliantRegistration is the smallest source the checker accepts. The
// placeholders are where the self-tests below add the registration they expect
// to be reported.
const compliantRegistration = `package api

import "net/http"

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	{{handler}}
	return mux
}

func init() {
	{{init}}
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	for _, rt := range s.enabledRoutes() {
		s.register(mux, rt.pattern(), rt.chain, rt.handler)
	}
	{{registerRoutes}}
	mux.HandleFunc("/", s.handleNotFound)
}

func (s *Server) register(mux *http.ServeMux, pattern string, c chain, handler http.HandlerFunc) {
	switch c {
	case chainNone:
		mux.HandleFunc(pattern, handler)
	case chainGuard:
		s.handleInternal(mux, pattern, handler)
	}
	{{register}}
}

func (s *Server) handleInternal(mux *http.ServeMux, pattern string, handler http.HandlerFunc) {
	mux.HandleFunc(pattern, s.refuseBrowserOrigin(handler))
}
`

func parseRegistrationSource(t *testing.T, substitutions map[string]string) []registrationFinding {
	t.Helper()
	src := compliantRegistration
	for _, placeholder := range []string{"handler", "init", "registerRoutes", "register"} {
		src = strings.ReplaceAll(src, "{{"+placeholder+"}}", substitutions[placeholder])
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
	require.NoError(t, err, src)
	return checkRegistration(fset, []*ast.File{file})
}

// TestRoutes_RegistrationCheckerAcceptsTheCompliantShapeAndReportsEveryBreach
// proves the checker itself: the shape registerRoutes, register and
// handleInternal have passes, and each way of registering around the table is
// reported, at the line it is on. Without this, a checker that found nothing
// would pass the real test above for the wrong reason.
func TestRoutes_RegistrationCheckerAcceptsTheCompliantShapeAndReportsEveryBreach(t *testing.T) {
	require.Empty(t, parseRegistrationSource(t, nil), "the compliant shape must pass")

	for name, tc := range map[string]struct {
		substitute map[string]string
		wantLine   int
		wantText   string
	}{
		"a direct HandleFunc in Handler": {
			map[string]string{"handler": `mux.HandleFunc("GET /extra", s.handleExtra)`}, 8,
			"outside the route table's registration code (in Handler)",
		},
		"a direct Handle on the mux in Handler": {
			map[string]string{"handler": `mux.Handle("GET /extra", http.NotFoundHandler())`}, 8,
			"mux.Handle registers a route outside",
		},
		"a package-level http.HandleFunc in init": {
			map[string]string{"init": `http.HandleFunc("/extra", nil)`}, 13,
			"http.HandleFunc registers a route outside the route table's registration code (in init)",
		},
		"a second registration in registerRoutes": {
			map[string]string{"registerRoutes": `mux.HandleFunc("GET /extra", s.handleExtra)`}, 20,
			`registerRoutes may register only the "/" catch-all`,
		},
		"a literal pattern in register": {
			map[string]string{"register": `mux.HandleFunc("GET /extra", handler)`}, 31,
			"register must register its own `pattern` parameter",
		},
		"register called from Handler": {
			map[string]string{"handler": `s.register(mux, "GET /extra", chainNone, nil)`}, 8,
			`register is called from "Handler"`,
		},
		"handleInternal called from Handler": {
			map[string]string{"handler": `s.handleInternal(mux, "POST /internal/v1/extra", nil)`}, 8,
			`handleInternal is called from "Handler"`,
		},
		"register called in registerRoutes outside the loop": {
			map[string]string{"registerRoutes": `s.register(mux, "GET /extra", chainNone, nil)`}, 20,
			"outside the loop over the table",
		},
	} {
		t.Run(name, func(t *testing.T) {
			findings := parseRegistrationSource(t, tc.substitute)
			require.Len(t, findings, 1, "%v", findings)
			require.Equal(t, tc.wantLine, findings[0].pos.Line, findings[0].String())
			require.Contains(t, findings[0].msg, tc.wantText)
		})
	}

	t.Run("a registerRoutes that is not found is itself a failure", func(t *testing.T) {
		src := strings.ReplaceAll(compliantRegistration, "registerRoutes", "registerEverything")
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
		require.NoError(t, err)
		findings := checkRegistration(fset, []*ast.File{file})
		require.NotEmpty(t, findings)
		require.Contains(t, findings[0].msg, "registerRoutes was not found")
	})

	t.Run("a missing catch-all is a failure", func(t *testing.T) {
		src := strings.ReplaceAll(compliantRegistration, `mux.HandleFunc("/", s.handleNotFound)`, "")
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
		require.NoError(t, err)
		findings := checkRegistration(fset, []*ast.File{file})
		require.Len(t, findings, 1)
		require.Contains(t, findings[0].msg, `exactly one "/" catch-all`)
	})
}
