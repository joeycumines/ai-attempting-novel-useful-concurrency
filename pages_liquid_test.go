package bigbuff

// Regression guard for the GitHub Pages build. The optional front matter plugin
// turns every root Markdown file into a page, and Jekyll runs Liquid over a page
// whenever its text contains "{{" or "{%" (Jekyll 3.10 offers no per-page opt-out).
// A delimiter that does not parse as Liquid fails the whole build, so each one
// must sit inside a {% raw %} ... {% endraw %} block.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var pagesLiquidRawBlock = regexp.MustCompile(`(?s)\{% raw %\}.*?\{% endraw %\}`)

func TestPagesMarkdownLiquidDelimitersAreEscaped(t *testing.T) {
	paths, err := filepath.Glob("*.md")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no root Markdown files found; the guard would pass vacuously")
	}
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		unescaped := pagesLiquidRawBlock.ReplaceAllString(string(content), "")
		for _, delimiter := range []string{"{{", "{%"} {
			if n := strings.Count(unescaped, delimiter); n > 0 {
				t.Errorf("%s: %d unescaped %q outside {%% raw %%} blocks; Jekyll will parse it as Liquid and fail the Pages build", path, n, delimiter)
			}
		}
	}
}
