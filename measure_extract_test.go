package bigbuff

// bigbuff-measure extract: pull the source files embedded in a model response
// (NN-*.md) out into a scratch directory, so extraction is a reproducible,
// pure-Go step rather than hand copy/paste.
//
// Two embeddings are recognised, matching what the responses actually use:
//
//  1. txtar archives ("-- name --" marker lines), wherever they occur inside
//     a fenced block;
//  2. ordinary fenced code blocks, named after the back-quoted file names in
//     the nearest preceding heading or paragraph line. When one heading names
//     several files ("`a_fast.go` & `a_race.go`") the blocks that follow take
//     the names in order. Blocks with no discoverable name are written as
//     block_<line>.<ext> so nothing is silently dropped.
//
// The extractor never edits content: renaming identifiers and repairing
// defects is integration work, done (and recorded) by hand.

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// extractNameReach is how many lines may separate a file-naming line from the
// fence it captions.
const extractNameReach = 12

type extractedFile struct {
	Name string
	Line int // 1-based line of the first content line in the response
	Body []string
}

var (
	extractFenceRE  = regexp.MustCompile("^\\s*(`{3,}|~{3,})\\s*([A-Za-z0-9_+.-]*)\\s*$")
	extractTxtarRE  = regexp.MustCompile(`^-- (\S(?:.*\S)?) --$`)
	extractNamedRE  = regexp.MustCompile("`([A-Za-z0-9_./-]+\\.(?:go|s|c|h|mod|md|txt|sum))`")
	extractLangExts = map[string]string{
		"go": "go", "golang": "go", "assembly": "s", "asm": "s", "s": "s", "nasm": "s",
		"c": "c", "sh": "sh", "bash": "sh", "txtar": "txtar", "text": "txt", "": "txt",
	}
)

// extractResponseFiles parses one response document.
func extractResponseFiles(lines []string) []extractedFile {
	var out []extractedFile
	var pending []string // file names announced by the last naming line
	pendingLine := 0     // where they were announced; an announcement goes stale
	inFence := false
	fenceMark, fenceLang := "", ""
	var cur *extractedFile
	sawTxtar := false
	flush := func() {
		if cur != nil {
			// Trim trailing blank lines; keep exactly one final newline.
			for len(cur.Body) > 0 && strings.TrimSpace(cur.Body[len(cur.Body)-1]) == "" {
				cur.Body = cur.Body[:len(cur.Body)-1]
			}
			out = append(out, *cur)
			cur = nil
		}
	}
	for i, line := range lines {
		if !inFence {
			if m := extractFenceRE.FindStringSubmatch(line); m != nil {
				inFence, fenceMark, fenceLang = true, m[1], strings.ToLower(m[2])
				sawTxtar = false
				continue
			}
			if names := extractNamedRE.FindAllStringSubmatch(line, -1); names != nil {
				pending = pending[:0]
				pendingLine = i
				for _, n := range names {
					pending = append(pending, n[1])
				}
			}
			continue
		}
		// Inside a fence. A closing fence is the same marker (or longer) alone.
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, fenceMark) && strings.Trim(trimmed, fenceMark[:1]) == "" {
			flush()
			inFence = false
			pendingLine = i // remaining names caption the blocks that follow directly
			continue
		}
		if m := extractTxtarRE.FindStringSubmatch(line); m != nil {
			flush()
			sawTxtar = true
			cur = &extractedFile{Name: m[1], Line: i + 2}
			continue
		}
		if cur == nil {
			if sawTxtar {
				continue // preamble between txtar members cannot occur; defensive
			}
			ext, ok := extractLangExts[fenceLang]
			if !ok {
				ext = "txt"
			}
			name := fmt.Sprintf("block_%d.%s", i+1, ext)
			// Only source-like fences consume an announced file name, so a
			// shell or usage snippet between heading and source is harmless.
			// An announcement more than a dozen lines back is prose that merely
			// mentioned a file, not a caption for this block.
			if i-pendingLine > extractNameReach {
				pending = pending[:0]
			}
			if len(pending) > 0 && (ext == "go" || ext == "s" || ext == "c") && strings.HasSuffix(pending[0], "."+ext) {
				name = pending[0]
				pending = pending[1:]
			}
			cur = &extractedFile{Name: name, Line: i + 1}
		}
		cur.Body = append(cur.Body, line)
	}
	flush()
	return out
}

func cmdMeasureExtract(args []string) int {
	fs := flag.NewFlagSet("extract", flag.ContinueOnError)
	in := fs.String("in", "", "response markdown file (NN-*.md)")
	outDir := fs.String("out", "", "directory to write the embedded files into (must not be the repository root)")
	list := fs.Bool("list", false, "only list the embedded files; write nothing")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *in == "" || (!*list && *outDir == "") {
		fmt.Fprintln(os.Stderr, "bigbuff-measure extract: --in is required, and --out unless --list")
		return 2
	}
	f, err := os.Open(*in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure extract: %v\n", err)
		return 1
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<26)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure extract: %v\n", err)
		return 1
	}
	files := extractResponseFiles(lines)
	// A repeated name (a response revising its own file) keeps every version:
	// later ones get a .vN suffix so the integrator must choose deliberately.
	seen := map[string]int{}
	for i := range files {
		seen[files[i].Name]++
		if n := seen[files[i].Name]; n > 1 {
			files[i].Name = fmt.Sprintf("%s.v%d", files[i].Name, n)
		}
	}
	sort.SliceStable(files, func(i, j int) bool { return files[i].Line < files[j].Line })
	if !*list {
		abs, err := filepath.Abs(*outDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bigbuff-measure extract: %v\n", err)
			return 1
		}
		if cwd, err := os.Getwd(); err == nil && abs == cwd {
			fmt.Fprintln(os.Stderr, "bigbuff-measure extract: refusing to extract into the repository root (flat layout: integrate by hand)")
			return 2
		}
	}
	for _, x := range files {
		fmt.Printf("%6d lines  line %-6d %s\n", len(x.Body), x.Line, x.Name)
		if *list {
			continue
		}
		dst := filepath.Join(*outDir, filepath.FromSlash(x.Name))
		if rel, err := filepath.Rel(*outDir, dst); err != nil || strings.HasPrefix(rel, "..") {
			fmt.Fprintf(os.Stderr, "bigbuff-measure extract: unsafe embedded path %q\n", x.Name)
			return 1
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "bigbuff-measure extract: %v\n", err)
			return 1
		}
		if err := os.WriteFile(dst, []byte(strings.Join(x.Body, "\n")+"\n"), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "bigbuff-measure extract: %v\n", err)
			return 1
		}
	}
	if len(files) == 0 {
		fmt.Println("no embedded files found")
	}
	return 0
}
