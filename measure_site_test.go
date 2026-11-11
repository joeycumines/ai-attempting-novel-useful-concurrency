package bigbuff

// Landing page and documentation/report wire-up module for bigbuff, implemented
// natively in Go (no external dependencies, no Python).
//
// Generates and synchronizes index.html (the GitHub Pages / local landing page),
// dynamically discovers all HTML reports, and injects/updates clean bidirectional
// backlinks across all reports, linking:
//   - Landing page (index.html / https://joeycumines.github.io/ai-attempting-novel-useful-concurrency/)
//   - Gist entry point (01-readme.md)
//   - GitHub repository README (README.md / https://github.com/joeycumines/ai-attempting-novel-useful-concurrency)
//   - All measurement reports (results-*-03-report.html)
//   - All architectural/empirical reports (report-*.html)
//   - Manifests, CSVs, JSONL datasets, and every model response.

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	githubRepoURL   = "https://github.com/joeycumines/ai-attempting-novel-useful-concurrency"
	githubReadmeURL = "https://github.com/joeycumines/ai-attempting-novel-useful-concurrency/blob/main/README.md"
	githubPagesURL  = "https://joeycumines.github.io/ai-attempting-novel-useful-concurrency/"
)

type siteReportMeta struct {
	Filename string `json:"filename"`
	Title    string `json:"title"`
	Type     string `json:"type"`
	Run      string `json:"run"`
	Size     int64  `json:"size"`
}

type siteResponseMeta struct {
	Num        string `json:"num"`
	Filename   string `json:"filename"`
	Slug       string `json:"slug"`
	Provider   string `json:"provider"`
	PromptType string `json:"prompt_type"`
	Impls      string `json:"impls"`
	Files      string `json:"files"`
	// FileList is Files resolved to the files that exist (see trackerFiles).
	FileList []string `json:"file_list"`
	State    string   `json:"state"`
	ChatLink string   `json:"chat_link"`
	Size     int64    `json:"size"`
}

type siteScoreRow struct {
	Family          string `json:"family"`
	Impl            string `json:"impl"`
	Score           string `json:"score"`
	Verdict         string `json:"verdict"`
	Baseline        string `json:"baseline"`
	Integrity       string `json:"integrity"`
	Throughput      string `json:"throughput"`
	Latency         string `json:"latency"`
	Allocations     string `json:"allocations"`
	Churn           string `json:"churn"`
	Scaling         string `json:"scaling"`
	StallResilience string `json:"stall_resilience"`
	ExpectedGoodFit string `json:"expected_good_fit"`
	ExposureLimited string `json:"exposure_limited"`
	// Class is the delivery class the score is relative to (empty for a
	// family without classes, and for every run before record contract
	// 1.4.0); DeliveredAt8 is the share of published values the subscribers
	// received in the throughput battery at 8 subscribers.
	Class        string `json:"class"`
	DeliveredAt8 string `json:"delivered_at_8"`
}

type siteCatalog struct {
	Reports   []siteReportMeta   `json:"reports"`
	Responses []siteResponseMeta `json:"responses"`
	Scores    []siteScoreRow     `json:"scores"`
}

func cmdMeasureSite(args []string) int {
	fs := flag.NewFlagSet("site", flag.ContinueOnError)
	dir := fs.String("dir", ".", "repository root directory")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	reports, err := discoverSiteReports(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "site: discover reports: %v\n", err)
		return 1
	}

	if err := wireAllReportBacklinks(*dir, reports); err != nil {
		fmt.Fprintf(os.Stderr, "site: wire backlinks: %v\n", err)
		return 1
	}

	// Wiring rewrites the reports, so their sizes are read again: the landing
	// page must describe the files as they are written.
	reports, err = discoverSiteReports(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "site: rediscover reports: %v\n", err)
		return 1
	}

	// The manifests record the SHA-256 of the reports just rewritten, and the
	// landing page is read from the manifests, so the index runs between the
	// two. It is idempotent; a run that fails verification is reported on
	// the page and fails this command after the page is written.
	indexRC := cmdMeasureIndex([]string{"--dir", *dir})

	cat, err := loadSiteCatalogData(*dir, reports)
	if err != nil {
		fmt.Fprintf(os.Stderr, "site: load catalog: %v\n", err)
		return 1
	}
	facts, err := loadSiteFacts(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "site: load facts: %v\n", err)
		return 1
	}

	htmlContent := renderLandingPage(cat, facts)
	indexPath := filepath.Join(*dir, "index.html")
	if err := os.WriteFile(indexPath, []byte(htmlContent), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "site: write index.html: %v\n", err)
		return 1
	}

	fmt.Printf("bigbuff-measure site: wrote %s (%d bytes, %d reports, %d responses, %d scores)\n",
		indexPath, len(htmlContent), len(cat.Reports), len(cat.Responses), len(cat.Scores))
	return indexRC
}

func discoverSiteReports(dir string) ([]siteReportMeta, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.html"))
	if err != nil {
		return nil, err
	}
	var reports []siteReportMeta
	titleRe := regexp.MustCompile(`(?i)<title>(.*?)</title>`)
	runRe := regexp.MustCompile(`results-([0-9]+)-`)

	for _, path := range matches {
		base := filepath.Base(path)
		if base == "index.html" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		title := base
		if m := titleRe.FindSubmatch(raw[:min(len(raw), 8192)]); len(m) > 1 {
			title = strings.TrimSpace(string(m[1]))
		}

		repType := "architectural"
		runNum := ""
		if strings.HasPrefix(base, "results-") {
			repType = "measurement"
			if rm := runRe.FindStringSubmatch(base); len(rm) > 1 {
				runNum = rm[1]
			}
		} else if strings.HasPrefix(base, "report-01") {
			runNum = "01"
		}

		reports = append(reports, siteReportMeta{
			Filename: base,
			Title:    title,
			Type:     repType,
			Run:      runNum,
			Size:     info.Size(),
		})
	}
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].Filename < reports[j].Filename
	})
	return reports, nil
}

func loadSiteCatalogData(dir string, reports []siteReportMeta) (siteCatalog, error) {
	var cat siteCatalog
	cat.Reports = reports

	// 1. Extraction tracker & chat links from 01-readme.md
	extractionMap := map[string][3]string{}
	chatLinks := map[string]string{}
	if raw01, err := os.ReadFile(filepath.Join(dir, "01-readme.md")); err == nil {
		lines := strings.Split(string(raw01), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "|") && !strings.HasPrefix(line, "| Response") && !strings.HasPrefix(line, "|---") {
				parts := strings.Split(line, "|")
				if len(parts) >= 5 {
					p0 := strings.TrimSpace(parts[1])
					p1 := strings.ReplaceAll(strings.TrimSpace(parts[2]), "`", "")
					p2 := strings.ReplaceAll(strings.TrimSpace(parts[3]), "`", "")
					p3 := strings.TrimSpace(parts[4])
					if len(p0) > 0 && p0[0] >= '0' && p0[0] <= '9' {
						numInt := 0
						fmt.Sscanf(p0, "%d", &numInt)
						key := fmt.Sprintf("%02d", numInt)
						extractionMap[key] = [3]string{p1, p2, p3}
					}
				}
			}
		}

		linkRe := regexp.MustCompile(`- \[([0-9]{2})-[^\]]+\]\([^\)]+\)(?: \(\[link\]\((https?://[^\)]+)\)\))?`)
		for _, m := range linkRe.FindAllStringSubmatch(string(raw01), -1) {
			if len(m) > 2 && m[2] != "" {
				chatLinks[m[1]] = m[2]
			}
		}
	}

	// 2. Scan response markdown files (03 onwards)
	matches, _ := filepath.Glob(filepath.Join(dir, "[0-9][0-9]-*.md"))
	sort.Strings(matches)
	for _, path := range matches {
		base := filepath.Base(path)
		num := strings.Split(base, "-")[0]
		if num == "01" || num == "02" {
			continue
		}
		info, _ := os.Stat(path)
		slug := strings.TrimSuffix(base[3:], ".md")

		promptType := "Standard"
		if strings.Contains(slug, "prompt-mini") {
			promptType = "Prompt-Mini"
		} else if strings.Contains(slug, "prompt-safe") {
			promptType = "Prompt-Safe"
		}

		provider := "Other"
		if strings.Contains(slug, "chatgpt") || strings.Contains(slug, "gpt") {
			provider = "OpenAI"
		} else if strings.Contains(slug, "claude") || strings.Contains(slug, "haiku") || strings.Contains(slug, "sonnet") || strings.Contains(slug, "opus") {
			provider = "Anthropic"
		} else if strings.Contains(slug, "gemini") || strings.Contains(slug, "argon") {
			provider = "Google"
		} else if strings.Contains(slug, "deepseek") {
			provider = "DeepSeek"
		} else if strings.Contains(slug, "qwen") {
			provider = "Alibaba / Qwen"
		} else if strings.Contains(slug, "meta") || strings.Contains(slug, "muse") || strings.Contains(slug, "spark") {
			provider = "Meta / Muse"
		} else if strings.Contains(slug, "grok") {
			provider = "xAI / Grok"
		} else if strings.Contains(slug, "mistral") {
			provider = "Mistral AI"
		} else if strings.Contains(slug, "nemotron") {
			provider = "NVIDIA / Nemotron"
		} else if strings.Contains(slug, "kimi") {
			provider = "Moonshot / Kimi"
		} else if strings.Contains(slug, "xing") {
			provider = "Xing / Baichuan"
		} else if strings.Contains(slug, "crouton") || strings.Contains(slug, "flint") || strings.Contains(slug, "dots3") || strings.Contains(slug, "north") || strings.Contains(slug, "lightning") {
			provider = "OpenRouter"
		} else if strings.Contains(slug, "mai-code") || strings.Contains(slug, "github") {
			provider = "GitHub Copilot / Mai"
		} else if strings.Contains(slug, "chatjimmy") {
			provider = "ChatJimmy"
		}

		// The tracker is the only source. A response it does not list is
		// shown as exactly that; TestTrackerCoversEveryResponse fails on one.
		ext, ok := extractionMap[num]
		if !ok {
			ext = [3]string{"", "", "not listed in the tracker (01-readme.md)"}
		}

		// Only files that exist are linked; the cell's shorthand is expanded.
		fileList, _ := trackerFiles(dir, ext[1])
		if fileList == nil {
			fileList = []string{}
		}
		cat.Responses = append(cat.Responses, siteResponseMeta{
			Num:        num,
			Filename:   base,
			Slug:       slug,
			Provider:   provider,
			PromptType: promptType,
			Impls:      ext[0],
			Files:      ext[1],
			FileList:   fileList,
			State:      ext[2],
			ChatLink:   chatLinks[num],
			Size:       info.Size(),
		})
	}

	// 3. Load scores from the newest run that has a scores CSV. Run numbers
	// are zero-padded, so the lexically last match is the newest run.
	scoresPath := filepath.Join(dir, "results-01-05-scores.csv")
	if matches, _ := filepath.Glob(filepath.Join(dir, "results-[0-9][0-9]-05-scores.csv")); len(matches) > 0 {
		sort.Strings(matches)
		scoresPath = matches[len(matches)-1]
	}
	if f, err := os.Open(scoresPath); err == nil {
		defer f.Close()
		r := csv.NewReader(f)
		records, err := r.ReadAll()
		if err == nil && len(records) > 1 {
			header := records[0]
			colMap := map[string]int{}
			for i, col := range header {
				colMap[col] = i
			}
			getVal := func(row []string, col string) string {
				if idx, ok := colMap[col]; ok && idx < len(row) {
					return row[idx]
				}
				return ""
			}

			for _, row := range records[1:] {
				cat.Scores = append(cat.Scores, siteScoreRow{
					Family:          getVal(row, "family"),
					Impl:            getVal(row, "impl"),
					Score:           getVal(row, "score"),
					Verdict:         getVal(row, "verdict"),
					Baseline:        getVal(row, "baseline"),
					Integrity:       getVal(row, "integrity"),
					Throughput:      getVal(row, "throughput"),
					Latency:         getVal(row, "latency"),
					Allocations:     getVal(row, "allocations"),
					Churn:           getVal(row, "churn"),
					Scaling:         getVal(row, "scaling"),
					StallResilience: getVal(row, "stall_resilience"),
					ExpectedGoodFit: getVal(row, "expected_good_fit"),
					ExposureLimited: getVal(row, "exposure_limited"),
					Class:           getVal(row, "class"),
					DeliveredAt8:    getVal(row, "delivered_at_8"),
				})
			}
		}
	}

	return cat, nil
}

func renderLandingPage(cat siteCatalog, facts siteFacts) string {
	respJSON, _ := json.Marshal(cat.Responses)
	scoresJSON, _ := json.Marshal(cat.Scores)

	html := renderSiteFacts(landingPageTemplate, cat, facts)
	html = strings.ReplaceAll(html, "__GITHUB_REPO_URL__", githubRepoURL)
	html = strings.ReplaceAll(html, "__RESPONSES_JSON__", string(respJSON))
	html = strings.ReplaceAll(html, "__SCORES_JSON__", string(scoresJSON))
	return html
}

const landingPageHead = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<title>AI Attempting Novel Useful Concurrency in Go · bigbuff</title>
<meta name="description" content="Measurements, tests and written analysis of __N_RESPONSES__ AI model responses implementing novel synchronous Go channel concurrency primitives.">
<style>
`

const landingPageTail = `</body>
</html>
`

const landingPageTemplate = landingPageHead + landingPageStyle + landingPageBody + landingPageScript + landingPageTail
