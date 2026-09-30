package dashboard

import (
	"io/fs"
	"os"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

// requireBuiltEnv makes TestAssets_BuiltOutputIsSelfConsistent fail instead of
// skip when no build is present. The dashboard CI job sets it after
// `make dash-build`, so that job cannot pass by never having built anything.
const requireBuiltEnv = "TASKFORGE_REQUIRE_BUILT_DASHBOARD"

func TestChoose_CommittedStateServesThePlaceholder(t *testing.T) {
	// Exactly what a clean clone's dist/ contains.
	builtFS := fstest.MapFS{".gitkeep": {Data: nil}}
	placeholderFS := fstest.MapFS{IndexFile: {Data: []byte("placeholder")}}

	got, built := choose(builtFS, placeholderFS)
	require.False(t, built, "a dist/ holding only .gitkeep is not a build")
	body, err := fs.ReadFile(got, IndexFile)
	require.NoError(t, err)
	require.Equal(t, "placeholder", string(body))
}

func TestChoose_PrefersABuildWhenOneExists(t *testing.T) {
	builtFS := fstest.MapFS{
		".gitkeep":            {Data: nil},
		IndexFile:             {Data: []byte("built")},
		"assets/index-abc.js": {Data: []byte("js")},
	}
	placeholderFS := fstest.MapFS{IndexFile: {Data: []byte("placeholder")}}

	got, built := choose(builtFS, placeholderFS)
	require.True(t, built)
	body, err := fs.ReadFile(got, IndexFile)
	require.NoError(t, err)
	require.Equal(t, "built", string(body))
}

func TestChoose_ADirectoryNamedIndexIsNotABuild(t *testing.T) {
	builtFS := fstest.MapFS{IndexFile + "/x": {Data: []byte("x")}}
	placeholderFS := fstest.MapFS{IndexFile: {Data: []byte("placeholder")}}

	_, built := choose(builtFS, placeholderFS)
	require.False(t, built)
}

// The placeholder is a build-state message, never a stand-in dashboard. It must
// say it is not the dashboard, name the target that builds one, and contain
// nothing that could render data -- no script at all.
func TestPlaceholder_SaysItIsNotBuiltAndCarriesNoScript(t *testing.T) {
	body, err := fs.ReadFile(placeholder, "placeholder/"+IndexFile)
	require.NoError(t, err)
	page := string(body)

	require.Contains(t, page, "has not been built")
	require.Contains(t, page, "make dash-build")
	require.Contains(t, page, `content="placeholder"`)
	require.NotContains(t, strings.ToLower(page), "<script")
}

var assetReference = regexp.MustCompile(`(?:src|href)="([^"]+)"`)

// A real build must be internally consistent: every asset index.html references
// is under the /dashboard/ base the server mounts it at, and is actually present
// in the embedded tree. A Vite base-path mismatch fails here rather than as a
// blank page in a browser.
func TestAssets_BuiltOutputIsSelfConsistent(t *testing.T) {
	assets, built := Assets()
	if !built {
		if os.Getenv(requireBuiltEnv) != "" {
			t.Fatalf("%s is set but no dashboard build is embedded; run `make dash-build` first", requireBuiltEnv)
		}
		t.Skip("no dashboard build embedded; `make dash-build` produces one")
	}

	index, err := fs.ReadFile(assets, IndexFile)
	require.NoError(t, err)

	refs := assetReference.FindAllStringSubmatch(string(index), -1)
	require.NotEmpty(t, refs, "a built index.html references at least its entry script")
	for _, ref := range refs {
		url := ref[1]
		require.True(t, strings.HasPrefix(url, "/dashboard/"),
			"asset %q is outside the /dashboard/ mount; check vite.config.ts base", url)
		name := strings.TrimPrefix(url, "/dashboard/")
		info, err := fs.Stat(assets, name)
		require.NoError(t, err, "index.html references %q, which the build did not produce", url)
		require.True(t, info.Mode().IsRegular())
	}
}
