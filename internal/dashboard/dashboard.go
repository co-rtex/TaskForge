// Package dashboard embeds the operator dashboard's static assets into the
// taskforge-api binary, so the dashboard is served same-origin and the running
// stack needs no Node runtime. See docs/adr/0017-dashboard-toolchain-and-serving.md.
//
// The frontend source lives in dashboard/ at the repository root and has its own
// toolchain. `make dash-build` compiles it inside a pinned Node container and
// writes the output to dist/ here. That output is not committed: dist/ holds
// only a .gitkeep in version control, which is what lets this package -- and
// therefore every binary -- compile on a clean clone before anyone has built the
// frontend. Until then, Assets serves placeholder/index.html, a page that says
// the dashboard has not been built and names the target that builds it.
package dashboard

import (
	"embed"
	"fmt"
	"io/fs"
)

// IndexFile is the entry document every dashboard route falls back to.
const IndexFile = "index.html"

// dist is embedded with the all: prefix so the committed .gitkeep matches the
// pattern. Without it, a clean clone's dist/ contains no embeddable file and
// `go build` fails with "pattern all:dist: no matching files found".
//
//go:embed all:dist
var dist embed.FS

//go:embed placeholder/index.html
var placeholder embed.FS

// Assets returns the built dashboard when `make dash-build` has produced one,
// and the placeholder page otherwise. built reports which, so the binary can log
// it rather than leave an operator guessing why the page says "not built".
func Assets() (assets fs.FS, built bool) {
	builtFS, err := fs.Sub(dist, "dist")
	if err != nil {
		// fs.Sub fails only on an invalid path, and "dist" is a constant.
		panic(fmt.Sprintf("dashboard: sub dist: %v", err))
	}
	placeholderFS, err := fs.Sub(placeholder, "placeholder")
	if err != nil {
		panic(fmt.Sprintf("dashboard: sub placeholder: %v", err))
	}
	return choose(builtFS, placeholderFS)
}

// choose prefers the built output whenever it contains an entry document. A
// dist/ with only .gitkeep in it -- the committed state -- is not a build.
func choose(builtFS, placeholderFS fs.FS) (fs.FS, bool) {
	if info, err := fs.Stat(builtFS, IndexFile); err == nil && info.Mode().IsRegular() {
		return builtFS, true
	}
	return placeholderFS, false
}
