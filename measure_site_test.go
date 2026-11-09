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
//   - Manifests, CSVs, JSONL datasets, and 58 model responses.

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"html"
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
	State      string `json:"state"`
	ChatLink   string `json:"chat_link"`
	Size       int64  `json:"size"`
}

type siteScoreRow struct {
	Family           string `json:"family"`
	Impl             string `json:"impl"`
	Score            string `json:"score"`
	Verdict          string `json:"verdict"`
	Baseline         string `json:"baseline"`
	Integrity        string `json:"integrity"`
	Throughput       string `json:"throughput"`
	Latency          string `json:"latency"`
	Allocations      string `json:"allocations"`
	Churn            string `json:"churn"`
	Scaling          string `json:"scaling"`
	StallResilience  string `json:"stall_resilience"`
	ExpectedGoodFit  string `json:"expected_good_fit"`
	ExposureLimited  string `json:"exposure_limited"`
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

	cat, err := loadSiteCatalogData(*dir, reports)
	if err != nil {
		fmt.Fprintf(os.Stderr, "site: load catalog: %v\n", err)
		return 1
	}

	htmlContent := renderLandingPage(cat)
	indexPath := filepath.Join(*dir, "index.html")
	if err := os.WriteFile(indexPath, []byte(htmlContent), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "site: write index.html: %v\n", err)
		return 1
	}

	fmt.Printf("bigbuff-measure site: wrote %s (%d bytes, %d reports, %d responses, %d scores)\n",
		indexPath, len(htmlContent), len(cat.Reports), len(cat.Responses), len(cat.Scores))
	return 0
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

func wireAllReportBacklinks(dir string, reports []siteReportMeta) error {
	for _, r := range reports {
		path := filepath.Join(dir, r.Filename)
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		content := string(raw)

		if strings.HasPrefix(r.Filename, "results-") {
			content = wireMeasurementReportContent(content, r.Filename, reports)
		} else {
			content = wireArchitecturalReportContent(content, r.Filename, reports)
		}

		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func wireMeasurementReportContent(content, currentFile string, reports []siteReportMeta) string {
	var otherReportLinks []string
	for _, rep := range reports {
		if rep.Filename != currentFile {
			label := rep.Title
			if rep.Type == "measurement" && rep.Run != "" {
				label = fmt.Sprintf("Measurement Report (Run %s)", rep.Run)
			} else if rep.Type == "architectural" {
				label = "Architectural Analysis"
			}
			otherReportLinks = append(otherReportLinks, fmt.Sprintf(
				`    <a href="%s" style="color:var(--ink2);font-size:12px" title="%s">%s</a>`,
				rep.Filename, html.EscapeString(rep.Title), html.EscapeString(label),
			))
		}
	}

	navBacklinks := fmt.Sprintf(`  <!-- START AUTOMATED BACKLINKS -->
  <div class="nav-backlinks" style="margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--line);display:flex;flex-direction:column;gap:5px">
    <a href="index.html" style="color:var(--cyan);font-weight:600;display:flex;align-items:center;gap:6px" title="Return to Landing Page">
      <span style="font-size:12px">&larr;</span> <span>Landing Page</span>
    </a>
%s
    <a href="01-readme.md" style="color:var(--ink2);font-size:12px" title="Gist Entry Point &amp; Response Index">Gist Index (01-readme.md)</a>
    <a href="%s" target="_blank" rel="noopener" style="color:var(--ink2);font-size:12px" title="GitHub Repository">GitHub Repository</a>
    <a href="README.md" style="color:var(--ink2);font-size:12px" title="Project README">README.md</a>
  </div>
  <!-- END AUTOMATED BACKLINKS -->`, strings.Join(otherReportLinks, "\n"), githubRepoURL)

	if strings.Contains(content, "<!-- START AUTOMATED BACKLINKS") {
		re := regexp.MustCompile(`(?s)<!-- START AUTOMATED BACKLINKS.*?<!-- END AUTOMATED BACKLINKS -->\s*`)
		content = re.ReplaceAllString(content, "")
	}
	brandClose := "</div>\n  </div>"
	idx := strings.Index(content, brandClose)
	if idx != -1 {
		insertPos := idx + len(brandClose)
		content = content[:insertPos] + "\n" + navBacklinks + content[insertPos:]
	}

	mobileTopbar := fmt.Sprintf(`<!-- START AUTOMATED MOBILE TOPBAR -->
<div class="mobile-topbar" style="display:none;background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px 16px;margin-bottom:24px;font-size:12.5px;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px">
  <a href="index.html" style="color:var(--cyan);text-decoration:none;font-weight:600;display:inline-flex;align-items:center;gap:6px">
    <span>&larr;</span> <span>Landing Page</span>
  </a>
  <div style="display:flex;align-items:center;gap:14px">
    <a href="01-readme.md" style="color:var(--ink2);text-decoration:none">Gist Index</a>
    <a href="%s" target="_blank" rel="noopener" style="color:var(--ink2);text-decoration:none">GitHub</a>
  </div>
</div>
<!-- END AUTOMATED MOBILE TOPBAR -->`, githubRepoURL)

	if strings.Contains(content, "<!-- START AUTOMATED MOBILE TOPBAR") {
		re := regexp.MustCompile(`(?s)<!-- START AUTOMATED MOBILE TOPBAR.*?<!-- END AUTOMATED MOBILE TOPBAR -->\s*`)
		content = re.ReplaceAllString(content, "")
	}
	mainRe := regexp.MustCompile(`(?i)(<main[^>]*>\s*)`)
	if loc := mainRe.FindStringIndex(content); loc != nil {
		content = content[:loc[1]] + "\n" + mobileTopbar + "\n" + content[loc[1]:]
	}

	if !strings.Contains(content, ".mobile-topbar{display:flex!important}") {
		content = strings.Replace(
			content,
			"@media (max-width:1080px){#shell{grid-template-columns:1fr}#nav{display:none}}",
			"@media (max-width:1080px){#shell{grid-template-columns:1fr}#nav{display:none}.mobile-topbar{display:flex!important}}",
			1,
		)
	}

	return content
}

func wireArchitecturalReportContent(content, currentFile string, reports []siteReportMeta) string {
	var reportLinks []string
	for _, rep := range reports {
		if rep.Filename != currentFile {
			label := rep.Title
			if rep.Type == "measurement" && rep.Run != "" {
				label = fmt.Sprintf("Measurement Report (Run %s)", rep.Run)
			}
			reportLinks = append(reportLinks, fmt.Sprintf(
				`    <a href="%s" style="color:#818cf8;text-decoration:none" title="%s">%s</a>`,
				rep.Filename, html.EscapeString(rep.Title), html.EscapeString(label),
			))
		}
	}

	globalNav := fmt.Sprintf(`<!-- START AUTOMATED GLOBAL NAV -->
<div class="global-report-nav" style="background:#070b14;border-bottom:1px solid #1f2a55;padding:10px 40px;font-size:13px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;position:relative;z-index:1000">
  <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap">
    <a href="index.html" style="color:#22d3ee;text-decoration:none;font-weight:600;display:inline-flex;align-items:center;gap:6px" title="Return to Landing Page">
      <span style="font-size:14px">&larr;</span> <span>Landing Page</span>
    </a>
    <span style="color:#2e3d52">|</span>
    <span style="color:#9da7b3;font-size:12px">Bigbuff Concurrency Study</span>
  </div>
  <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;font-size:12px">
%s
    <a href="01-readme.md" style="color:#9da7b3;text-decoration:none" title="Gist Entry Point &amp; Response Index">Gist Index</a>
    <a href="%s" target="_blank" rel="noopener" style="color:#9da7b3;text-decoration:none" title="View Source on GitHub">GitHub Repo</a>
    <a href="README.md" style="color:#9da7b3;text-decoration:none" title="Project README">README.md</a>
  </div>
</div>
<!-- END AUTOMATED GLOBAL NAV -->`, strings.Join(reportLinks, "\n"), githubRepoURL)

	if strings.Contains(content, "<!-- START AUTOMATED GLOBAL NAV") {
		re := regexp.MustCompile(`(?s)<!-- START AUTOMATED GLOBAL NAV.*?<!-- END AUTOMATED GLOBAL NAV -->\s*`)
		content = re.ReplaceAllString(content, "")
	}
	bodyRe := regexp.MustCompile(`(?i)(<body[^>]*>\s*)`)
	if loc := bodyRe.FindStringIndex(content); loc != nil {
		content = content[:loc[1]] + "\n" + globalNav + "\n" + content[loc[1]:]
	}

	return content
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

	earlyMap := map[string][3]string{
		"03": {"ChanLatest", "chanlatest.go", "Slab-based latest-value broadcast with ring notification"},
		"04": {"ChanFeed", "chanfeed.go", "Dynamic subscriber slice with lockless send fast-path"},
		"05": {"ChanGatherer", "changatherer.go", "Signal gathering barrier & broadcast with O(1) state"},
		"06": {"ChanCasterV2_06meta, ChanPubSubV2_06meta, GenCaster", "chancaster_06_meta.go, chanpubsub_06_meta.go, gencaster.go", "Generational barrier pub-sub & lock-free fast-path caster"},
		"07": {"ChanBeacon, ChanRotor", "chanbeacon.go, chanrotor.go", "State beacon notification & round-robin ring dispatcher"},
		"08": {"RingBeacon, ReliableBeacon, Beacon", "ring_beacon.go, reliable_beacon.go, beacon.go", "Lock-free atomic generation beacons with loss tracking"},
		"09": {"ChanCaster09Gemini, ChanPubSub09Gemini, EpochCasterGemini", "chancaster_09_gemini.go, chanpubsub_09_gemini.go, epochcaster_09_gemini.go", "Epoch-based generational caster and lock-step pub-sub"},
		"10": {"ChanPubSub10Gemini, EpochTape", "chanpubsub_10_gemini.go, epochtape.go", "Generational ring buffer tape & synchronous pub-sub"},
		"11": {"ChanCaster11Meta, EpochPubSub", "chancaster_11_meta.go, epochpubsub_11_meta.go", "Epoch-fenced atomic publication barrier & zero-allocation caster"},
		"12": {"ChanFanout, ChanRelay", "chanfanout.go, chanrelay.go", "Asynchronous ring buffer fan-out & buffered relay"},
		"13": {"EpochCaster13Crouton", "epochcaster_13_crouton.go", "Ref-counted epoch caster over shared sequence ring"},
		"14": {"ChanCaster14Xing, ChanPubSub14Xing, RingFanOut14Xing", "chancaster_14_xing.go, chanpubsub_14_xing.go, ringfanout_14_xing.go", "High-throughput ring fanout & rendezvous caster with fast-path CAS"},
		"15": {"ChanCaster15Nemotron, ChanHub", "chancaster_15_nemotron.go, chanhub.go", "Cursor-based ring hub & low-overhead registration caster"},
		"16": {"ChanPubSub16DeepSeek", "chanpubsub_16_deepseek.go", "Two-tier completion barrier pub-sub with withdrawal protocol"},
		"17": {"ChanCaster17FastPath, ChanCaster17Pumped, ChanPubSub17Gemini", "chancaster_17_gemini.go, chanpubsub_17_gemini.go", "Pumped ring caster & split-state pub-sub barrier"},
		"18": {"RingPubSub", "ringpubsub.go", "Bounded lossless/lossy ring buffer pub-sub with subscriber cursors"},
		"19": {"ChanHub, PhasePubSub, RelayPubSub", "chanhub.go, phasepubsub.go, relaypubsub.go", "Phase-alternating transfer pub-sub & lockless hub"},
		"20": {"EpochTape, ChanRelay", "epochtape.go, chanrelay.go", "Ref-counted epoch log tape with replay cursors"},
	}

	// 2. Scan response markdown files (03..58)
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

		ext, ok := extractionMap[num]
		if !ok {
			if early, okEarly := earlyMap[num]; okEarly {
				ext = early
			} else {
				ext = [3]string{"Extracted", "-", "Desk-checked / verified"}
			}
		}

		cat.Responses = append(cat.Responses, siteResponseMeta{
			Num:        num,
			Filename:   base,
			Slug:       slug,
			Provider:   provider,
			PromptType: promptType,
			Impls:      ext[0],
			Files:      ext[1],
			State:      ext[2],
			ChatLink:   chatLinks[num],
			Size:       info.Size(),
		})
	}

	// 3. Load scores from results-01-05-scores.csv (or results-02-05-scores.csv)
	scoresPath := filepath.Join(dir, "results-01-05-scores.csv")
	if _, err := os.Stat(scoresPath); os.IsNotExist(err) {
		scoresPath = filepath.Join(dir, "results-02-05-scores.csv")
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
				})
			}
		}
	}

	return cat, nil
}

func renderLandingPage(cat siteCatalog) string {
	respJSON, _ := json.Marshal(cat.Responses)
	scoresJSON, _ := json.Marshal(cat.Scores)
	reportsJSON, _ := json.Marshal(cat.Reports)

	html := landingPageTemplate
	html = strings.ReplaceAll(html, "__GITHUB_REPO_URL__", githubRepoURL)
	html = strings.ReplaceAll(html, "__RESPONSES_JSON__", string(respJSON))
	html = strings.ReplaceAll(html, "__SCORES_JSON__", string(scoresJSON))
	html = strings.ReplaceAll(html, "__REPORTS_JSON__", string(reportsJSON))
	return html
}

const landingPageTemplate = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<title>AI Attempting Novel Useful Concurrency in Go · bigbuff</title>
<meta name="description" content="Empirical benchmark, architectural analysis, and verification of 58+ AI model outputs implementing novel synchronous Go channel concurrency patterns.">
<style>
:root {
  --bg: #070b14;
  --bg2: #0b1020;
  --panel: #0e1530;
  --panel2: #121a3a;
  --panel3: #182148;
  --line: #1f2a55;
  --line2: #2b3a6e;
  --ink: #e8ecf8;
  --ink2: #a9b3d6;
  --ink3: #6b769e;
  --cyan: #22d3ee;
  --indigo: #818cf8;
  --violet: #a78bfa;
  --green: #34d399;
  --amber: #fbbf24;
  --orange: #fb923c;
  --red: #f87171;
  --teal: #2dd4bf;
  --radius: 14px;
  --shadow: 0 18px 50px rgba(0,0,0,.45);
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  --sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; }
body { background: var(--bg); color: var(--ink); font-family: var(--sans); line-height: 1.55; -webkit-font-smoothing: antialiased; }
::selection { background: rgba(129,140,248,.35); }
a { color: var(--cyan); text-decoration: none; transition: color .18s; }
a:hover { color: #a5f3fc; text-decoration: underline; }

/* Top Navbar */
header.site-nav {
  position: sticky; top: 0; z-index: 1000;
  background: rgba(11,16,32,.92); backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--line); padding: 12px 32px;
  display: flex; align-items: center; justify-content: space-between; gap: 16px;
}
.nav-brand { display: flex; align-items: center; gap: 12px; }
.nav-brand .mark {
  width: 36px; height: 36px; border-radius: 10px;
  background: conic-gradient(from 210deg, var(--cyan), var(--indigo), var(--violet), var(--cyan));
  display: grid; place-items: center; font-weight: 800; color: #060a17; font-size: 18px;
  box-shadow: 0 0 20px rgba(56,189,248,.35);
}
.nav-brand h1 { font-size: 15px; letter-spacing: .02em; line-height: 1.2; color: #fff; }
.nav-brand small { display: block; color: var(--ink3); font-size: 10px; letter-spacing: .12em; text-transform: uppercase; }

.nav-links { display: flex; align-items: center; gap: 20px; font-size: 13px; font-weight: 500; }
.nav-links a { color: var(--ink2); }
.nav-links a:hover { color: #fff; text-decoration: none; }
.nav-actions { display: flex; align-items: center; gap: 10px; }
.btn {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 7px 14px; border-radius: 8px; font-size: 12.5px; font-weight: 600;
  border: 1px solid var(--line2); background: var(--panel); color: var(--ink);
  cursor: pointer; transition: all .18s; text-decoration: none !important;
}
.btn:hover { background: var(--panel2); border-color: var(--cyan); color: #fff; transform: translateY(-1px); }
.btn-primary {
  background: linear-gradient(90deg, #0e7490, #312e81); border-color: #38bdf8; color: #f0f9ff;
}
.btn-primary:hover { background: linear-gradient(90deg, #0891b2, #3730a3); border-color: #67e8f9; color: #fff; }

/* Container */
.container { max-width: 1440px; margin: 0 auto; padding: 32px 32px 80px; }

/* Hero Section */
.hero {
  position: relative; overflow: hidden; border: 1px solid var(--line2); border-radius: 20px;
  padding: 44px 44px 38px;
  background: radial-gradient(1100px 480px at 12% -10%, rgba(99,102,241,.32), transparent 55%),
              radial-gradient(900px 420px at 95% 0%, rgba(34,211,238,.18), transparent 50%),
              linear-gradient(180deg, #101736, #0b1020);
  box-shadow: var(--shadow); margin-bottom: 48px;
}
.hero::after {
  content: ""; position: absolute; inset: 0;
  background-image: radial-gradient(rgba(148,163,216,.12) 1px, transparent 1px);
  background-size: 22px 22px; pointer-events: none;
  mask-image: linear-gradient(180deg, #000 20%, transparent 90%);
}
.kicker {
  font-size: 11px; letter-spacing: .22em; text-transform: uppercase; color: var(--cyan);
  font-weight: 700; margin-bottom: 8px; display: flex; align-items: center; gap: 8px;
}
.hero h2 {
  font-size: 38px; letter-spacing: -.02em; line-height: 1.15; max-width: 960px; position: relative; color: #fff;
}
.hero h2 em {
  font-style: normal;
  background: linear-gradient(90deg, #67e8f9, #a5b4fc 55%, #c4b5fd);
  -webkit-background-clip: text; background-clip: text; color: transparent;
}
.hero p.lead {
  color: var(--ink2); margin-top: 14px; max-width: 860px; font-size: 15.5px; position: relative; line-height: 1.6;
}
.hero-cta { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 24px; position: relative; z-index: 2; }
.hero-cta .btn { padding: 10px 18px; font-size: 13.5px; border-radius: 10px; }

.kpis {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 14px; margin-top: 28px; position: relative;
}
.kpi {
  background: rgba(9,13,30,.65); border: 1px solid var(--line2); border-radius: 14px; padding: 14px 18px;
}
.kpi .v { font-size: 28px; font-weight: 750; letter-spacing: -.01em; font-family: var(--mono); color: #fff; }
.kpi .l { font-size: 11px; color: var(--ink2); text-transform: uppercase; letter-spacing: .08em; margin-top: 2px; font-weight: 600; }
.kpi .s { font-size: 11px; color: var(--ink3); margin-top: 3px; }

/* Section Styling */
section.block { margin-bottom: 56px; scroll-margin-top: 80px; }
h3.section-title { font-size: 24px; letter-spacing: -.01em; color: #fff; margin-bottom: 8px; }
.lede { color: var(--ink2); max-width: 900px; font-size: 14px; margin-bottom: 20px; }

/* Featured Reports Grid */
.reports-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }
.report-card {
  background: linear-gradient(180deg, var(--panel), var(--bg2));
  border: 1px solid var(--line); border-radius: var(--radius); padding: 24px 26px;
  position: relative; overflow: hidden; transition: all .2s; box-shadow: var(--shadow);
  display: flex; flex-direction: column; justify-content: space-between;
}
.report-card:hover { border-color: var(--line2); transform: translateY(-2px); }
.report-card::before {
  content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 4px;
}
.report-card.meas::before { background: linear-gradient(180deg, var(--cyan), var(--indigo)); }
.report-card.arch::before { background: linear-gradient(180deg, var(--violet), var(--teal)); }
.report-badge {
  display: inline-flex; align-items: center; gap: 6px;
  font-size: 10px; font-family: var(--mono); text-transform: uppercase; letter-spacing: .12em;
  padding: 3px 10px; border-radius: 999px; margin-bottom: 12px;
}
.report-card.meas .report-badge { background: rgba(34,211,238,.12); color: var(--cyan); border: 1px solid rgba(34,211,238,.3); }
.report-card.arch .report-badge { background: rgba(167,139,250,.12); color: var(--violet); border: 1px solid rgba(167,139,250,.3); }
.report-card h4 { font-size: 18px; color: #fff; margin-bottom: 8px; line-height: 1.3; }
.report-card p { font-size: 13.5px; color: var(--ink2); line-height: 1.55; margin-bottom: 18px; }
.report-features { list-style: none; display: flex; flex-direction: column; gap: 6px; margin-bottom: 22px; font-size: 12.5px; color: var(--ink2); }
.report-features li { display: flex; align-items: center; gap: 8px; }
.report-features li span.dot { width: 5px; height: 5px; border-radius: 50%; background: var(--cyan); flex: none; }
.report-actions { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; margin-top: auto; padding-top: 14px; border-top: 1px solid var(--line); }

/* Card Panels */
.card {
  background: linear-gradient(180deg, var(--panel), var(--bg2));
  border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow);
}
.card.pad { padding: 22px 24px; }

/* Filter Toolbars */
.toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-bottom: 16px; }
.search-input {
  background: var(--panel2); border: 1px solid var(--line2); color: var(--ink);
  border-radius: 8px; padding: 8px 14px; font-size: 13px; min-width: 240px; outline: none;
}
.search-input:focus { border-color: var(--cyan); }
.filter-pills { display: flex; gap: 6px; flex-wrap: wrap; }
.pill {
  cursor: pointer; border: 1px solid var(--line2); background: var(--panel);
  color: var(--ink2); font-size: 12px; font-weight: 500; padding: 6px 12px; border-radius: 8px; transition: all .15s;
}
.pill:hover { color: var(--ink); border-color: var(--line); }
.pill.active { background: linear-gradient(90deg, #164e63, #312e81); border-color: #38bdf8; color: #fff; font-weight: 600; }

/* Tables */
.tbl-wrap { overflow: auto; border: 1px solid var(--line); border-radius: var(--radius); max-height: 580px; }
table.tbl { width: 100%; border-collapse: collapse; font-size: 12.5px; text-align: left; }
table.tbl th {
  position: sticky; top: 0; background: #131b3f; color: var(--ink2); padding: 11px 12px;
  font-size: 11px; letter-spacing: .08em; text-transform: uppercase; border-bottom: 1px solid var(--line2);
  white-space: nowrap; cursor: pointer; user-select: none;
}
table.tbl td { padding: 9px 12px; border-bottom: 1px solid rgba(31,42,85,.5); vertical-align: middle; }
table.tbl tr:hover td { background: rgba(99,102,241,.06); }

.scorebar { position: relative; height: 16px; background: #1a2350; border-radius: 4px; min-width: 110px; overflow: hidden; }
.scorebar .fill { position: absolute; left: 0; top: 0; bottom: 0; border-radius: 4px; background: linear-gradient(90deg, #0e7490, #22d3ee); }
.scorebar span { position: absolute; left: 6px; top: 1px; font-size: 10.5px; font-weight: 700; color: #04121a; }

.verdict { font-size: 11px; font-weight: 700; white-space: nowrap; }
.v-strong { color: var(--green); }
.v-situational { color: var(--amber); }
.v-niche { color: var(--orange); }
.v-avoid { color: var(--red); }

.tag {
  display: inline-block; font-size: 10px; font-family: var(--mono);
  border: 1px solid var(--line2); padding: 2px 7px; border-radius: 4px; color: var(--ink2);
}
.tag-prov { color: var(--indigo); border-color: rgba(129,140,248,.3); background: rgba(129,140,248,.08); }
.tag-prompt { color: var(--cyan); border-color: rgba(34,211,238,.3); background: rgba(34,211,238,.08); }

/* Grid Cards for Paradigms & Findings */
.cards-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 16px; }
.f-card {
  background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius);
  padding: 18px 20px; position: relative; overflow: hidden;
}
.f-card::before { content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 3px; background: var(--cyan); }
.f-card.danger::before { background: var(--red); }
.f-card.warn::before { background: var(--amber); }
.f-card.info::before { background: var(--indigo); }
.f-card h4 { font-size: 14.5px; color: #fff; margin-bottom: 6px; line-height: 1.35; }
.f-card p { font-size: 12.5px; color: var(--ink2); line-height: 1.5; }

/* Footer */
footer.site-foot {
  border-top: 1px solid var(--line); padding: 40px 32px 30px; color: var(--ink3); font-size: 12px; line-height: 1.7;
  display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 24px;
}
footer.site-foot a { color: var(--ink2); }
footer.site-foot a:hover { color: var(--cyan); }

@media (max-width: 900px) {
  header.site-nav { flex-direction: column; align-items: flex-start; padding: 14px 20px; }
  .nav-links { display: none; }
  .container { padding: 20px 16px 60px; }
  .hero { padding: 28px 22px; }
  .hero h2 { font-size: 28px; }
  .reports-grid { grid-template-columns: 1fr; }
}
</style>
</head>
<body>

<header class="site-nav">
  <div class="nav-brand">
    <div class="mark">b</div>
    <div>
      <h1>bigbuff</h1>
      <small>AI Concurrency Benchmark</small>
    </div>
  </div>
  <nav class="nav-links">
    <a href="#reports">Reports</a>
    <a href="#leaderboard">Leaderboard</a>
    <a href="#responses">58 Responses</a>
    <a href="#paradigms">Paradigms</a>
    <a href="#baselines">Baselines</a>
    <a href="#pathology">Pathology Lab</a>
    <a href="#downloads">Datasets</a>
  </nav>
  <div class="nav-actions">
    <a href="results-01-03-report.html" class="btn btn-primary" title="Launch Run 01 Measurement Report">Measurement Report (Run 01)</a>
    <a href="report-01-gemini-flash-3.8-high.html" class="btn" title="View Architectural Evolution Analysis">Architectural Analysis</a>
    <a href="__GITHUB_REPO_URL__" target="_blank" rel="noopener" class="btn" title="GitHub Repository">GitHub Repo</a>
  </div>
</header>

<main class="container">

  <!-- HERO -->
  <section class="hero">
    <div class="kicker"><span>Empirical Concurrency Research</span> &middot; <span>Deterministic Harness</span></div>
    <h2>AI Attempting <em>Novel Useful Concurrency</em> in Go</h2>
    <p class="lead">
      We challenged <strong>58 AI model configurations</strong> with a one-shot prompt requiring novel, low-allocation Go concurrency primitives for synchronous channel broadcast. We then extracted, repaired in spirit, and measured <strong>116 concurrent primitives</strong> against 10 staff-engineer baselines across 18 scenarios and 2,013 deterministic benchmark runs.
    </p>
    <div class="hero-cta">
      <a href="results-01-03-report.html" class="btn btn-primary">Measurement Report (Run 01 &middot; Standard)</a>
      <a href="results-02-03-report.html" class="btn">Measurement Report (Run 02 &middot; Quick)</a>
      <a href="report-01-gemini-flash-3.8-high.html" class="btn">Architectural &amp; Empirical Analysis</a>
      <a href="#responses" class="btn">Browse 58 Responses</a>
      <a href="__GITHUB_REPO_URL__" target="_blank" rel="noopener" class="btn">GitHub Repository</a>
    </div>
    <div class="kpis">
      <div class="kpi">
        <div class="v">58</div>
        <div class="l">Model Responses</div>
        <div class="s">OpenAI, Anthropic, Google, DeepSeek, Qwen...</div>
      </div>
      <div class="kpi">
        <div class="v">116</div>
        <div class="l">Evaluated Primitives</div>
        <div class="s">Casters, PubSubs, Rings, Epochs, Fans</div>
      </div>
      <div class="kpi">
        <div class="v">18 / 291</div>
        <div class="l">Scenarios / Attachments</div>
        <div class="s">Strict exhaustiveness invariants</div>
      </div>
      <div class="kpi">
        <div class="v">2,013</div>
        <div class="l">Measurements (Run 01)</div>
        <div class="s">Verified PASS across all batteries</div>
      </div>
      <div class="kpi">
        <div class="v">0</div>
        <div class="l">Test Flakiness</div>
        <div class="s">100% deterministic ack/ready synchronization</div>
      </div>
    </div>
  </section>

  <!-- REPORTS HUB -->
  <section class="block" id="reports">
    <div class="kicker">Interactive Analysis &amp; Data Hub</div>
    <h3 class="section-title">Comprehensive Reports &amp; Artifacts</h3>
    <p class="lede">
      Every report is completely self-contained with embedded data &mdash; no CDNs or external assets. Open them online or locally from disk via <code>file://</code>.
    </p>

    <div class="reports-grid">
      <!-- Card 1: Measurement Report Run 01 -->
      <div class="report-card meas">
        <div>
          <span class="report-badge">Interactive Report &middot; Run 01 (Standard Profile)</span>
          <h4>Measurement Report (Run 01 &middot; Standard)</h4>
          <p>
            The master decision-grade benchmark report computed from 2,013 JSONL records. Features interactive throughput scaling curves (1&rarr;64 subscribers), adoptability radar, latency percentiles (p50/p90/p99), memory allocations, churn costs, and the Pathology Lab.
          </p>
          <ul class="report-features">
            <li><span class="dot"></span> <strong>Interactive Scaling Curves:</strong> Toggle implementations, inspect log&sup2; fan-out scaling</li>
            <li><span class="dot"></span> <strong>Adoptability Leaderboard:</strong> 116 primitives scored by throughput, latency, memory &amp; stability</li>
            <li><span class="dot"></span> <strong>Pathology Lab:</strong> Concrete case studies of races, deadlocks, and silent drops</li>
            <li><span class="dot"></span> <strong>Scenario Coverage Matrix:</strong> Complete 18-scenario attachment and baseline grid</li>
          </ul>
        </div>
        <div class="report-actions">
          <a href="results-01-03-report.html" class="btn btn-primary">Open Run 01 Report &rarr;</a>
          <div style="display:flex;gap:8px">
            <a href="results-01-04-metrics_wide.csv" download class="btn" title="Download Flattened Metrics CSV">Metrics CSV</a>
            <a href="results-01-05-scores.csv" download class="btn" title="Download Scores CSV">Scores CSV</a>
          </div>
        </div>
      </div>

      <!-- Card 2: Measurement Report Run 02 -->
      <div class="report-card meas">
        <div>
          <span class="report-badge">Interactive Report &middot; Run 02 (Quick Profile)</span>
          <h4>Measurement Report (Run 02 &middot; Quick)</h4>
          <p>
            The fast smoke / CI-grade benchmark report computed from 1,479 JSONL records. Provides rapid verification across all registered primitives with identical visual tooling and metric exports.
          </p>
          <ul class="report-features">
            <li><span class="dot"></span> <strong>CI-Grade Verification:</strong> 1,479 verified records across all scenario attachments</li>
            <li><span class="dot"></span> <strong>Identical Analytics:</strong> Full scaling charts, adoptability scorecards, and data downloads</li>
            <li><span class="dot"></span> <strong>Run Comparison:</strong> Evaluates consistency across toolchain revisions</li>
          </ul>
        </div>
        <div class="report-actions">
          <a href="results-02-03-report.html" class="btn btn-primary">Open Run 02 Report &rarr;</a>
          <div style="display:flex;gap:8px">
            <a href="results-02-04-metrics_wide.csv" download class="btn" title="Download Flattened Metrics CSV">Metrics CSV</a>
            <a href="results-02-05-scores.csv" download class="btn" title="Download Scores CSV">Scores CSV</a>
          </div>
        </div>
      </div>

      <!-- Card 3: Architectural Analysis Report -->
      <div class="report-card arch">
        <div>
          <span class="report-badge">Structural Analysis &middot; Gemini 3.8 Flash</span>
          <h4>Architectural &amp; Empirical Evolution Report</h4>
          <p>
            An in-depth structural review of Go channel broadcast evolution. Unpacks protocol constructions, explicit registration/receive contracts, safe mid-flight withdrawal mechanics, O(1) control-plane state, and select compatibility.
          </p>
          <ul class="report-features">
            <li><span class="dot"></span> <strong>Protocol Taxonomy:</strong> Lock-step vs Cursor Rings vs Epoch Gates vs RCU Fanout</li>
            <li><span class="dot"></span> <strong>Zero-Allocation Analysis:</strong> Probing runtime slice escapes and interface conversions</li>
            <li><span class="dot"></span> <strong>State Machine Teardown:</strong> Safe cancellation vs deadlock on blocked receivers</li>
          </ul>
        </div>
        <div class="report-actions">
          <a href="report-01-gemini-flash-3.8-high.html" class="btn btn-primary">Read Architectural Analysis &rarr;</a>
          <a href="01-readme.md" class="btn">Extraction Tracker</a>
        </div>
      </div>
    </div>
  </section>

  <!-- LEADERBOARD -->
  <section class="block" id="leaderboard">
    <div class="kicker">Adoptability Leaderboard</div>
    <h3 class="section-title">Empirical Scorecards (116 Primitives)</h3>
    <p class="lede">
      Adoptability scores (0&ndash;100) computed from multi-dimensional statistical metrics: throughput, tail latency, allocation rate, subscriber churn resistance, fan-out scalability, and stall resilience.
    </p>

    <div class="card pad">
      <div class="toolbar">
        <input type="text" id="scoreSearch" class="search-input" placeholder="Search implementation, model, or baseline...">
        <div class="filter-pills" id="familyPills">
          <div class="pill active" data-family="all">All Families</div>
          <div class="pill" data-family="broadcast">Broadcast (Lossless/Lossy)</div>
          <div class="pill" data-family="caster">Casters (Rendezvous)</div>
          <div class="pill" data-family="scatter">Scatter (Queue)</div>
          <div class="pill" data-family="gather">Gather (Barrier)</div>
          <div class="pill" data-family="payload">Payload (Async)</div>
        </div>
        <div class="filter-pills" id="verdictPills">
          <div class="pill active" data-verdict="all">All Verdicts</div>
          <div class="pill" data-verdict="Strong candidate">Strong Candidate</div>
          <div class="pill" data-verdict="Situational">Situational</div>
          <div class="pill" data-verdict="Niche">Niche</div>
          <div class="pill" data-verdict="Avoid">Avoid</div>
        </div>
      </div>

      <div class="tbl-wrap">
        <table class="tbl" id="scoresTable">
          <thead>
            <tr>
              <th>Family</th>
              <th>Implementation / Origin</th>
              <th>Adoptability Score</th>
              <th>Verdict</th>
              <th>Throughput</th>
              <th>Latency</th>
              <th>Memory</th>
              <th>Scaling</th>
              <th>Churn</th>
            </tr>
          </thead>
          <tbody id="scoresBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- MODEL RESPONSES (58 RESPONSES) -->
  <section class="block" id="responses">
    <div class="kicker">One-Shot Prompt Archive</div>
    <h3 class="section-title">58 AI Model Responses (03 &rarr; 58)</h3>
    <p class="lede">
      Filter, search, and inspect the raw outputs, prompt variants, extracted Go code, and chat links across all tested model providers.
    </p>

    <div class="card pad">
      <div class="toolbar">
        <input type="text" id="respSearch" class="search-input" placeholder="Filter by model, provider, Go file, or mechanism...">
        <div class="filter-pills" id="providerPills">
          <div class="pill active" data-prov="all">All Providers</div>
          <div class="pill" data-prov="OpenAI">OpenAI</div>
          <div class="pill" data-prov="Anthropic">Anthropic</div>
          <div class="pill" data-prov="Google">Google</div>
          <div class="pill" data-prov="DeepSeek">DeepSeek</div>
          <div class="pill" data-prov="Alibaba / Qwen">Qwen</div>
          <div class="pill" data-prov="Meta / Muse">Meta / Muse</div>
          <div class="pill" data-prov="xAI / Grok">Grok</div>
          <div class="pill" data-prov="Mistral AI">Mistral</div>
          <div class="pill" data-prov="NVIDIA / Nemotron">Nemotron</div>
          <div class="pill" data-prov="OpenRouter">OpenRouter</div>
        </div>
        <div class="filter-pills" id="promptPills">
          <div class="pill active" data-prompt="all">All Prompts</div>
          <div class="pill" data-prompt="Standard">Standard (02-prompt.md)</div>
          <div class="pill" data-prompt="Prompt-Mini">Mini (02-prompt-mini.md)</div>
          <div class="pill" data-prompt="Prompt-Safe">Safe (02-prompt-safe.md)</div>
        </div>
      </div>

      <div class="tbl-wrap">
        <table class="tbl" id="respTable">
          <thead>
            <tr>
              <th>#</th>
              <th>Response File</th>
              <th>Provider</th>
              <th>Prompt</th>
              <th>Extracted Primitive(s)</th>
              <th>Go Source</th>
              <th>Status / Mechanism</th>
              <th>Chat Link</th>
            </tr>
          </thead>
          <tbody id="respBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- ARCHITECTURAL PARADIGMS -->
  <section class="block" id="paradigms">
    <div class="kicker">Architectural Taxonomy</div>
    <h3 class="section-title">7 Core Concurrency Paradigms</h3>
    <p class="lede">
      How AI models attempted to evolve beyond linear rendezvous and lock-step barriers.
    </p>

    <div class="cards-grid">
      <div class="f-card info">
        <h4>1. Lock-step PubSub &amp; Barrier Rendezvous</h4>
        <p>
          Zero-allocation synchronous handoff where sender blocks until all registered subscribers acknowledge. Evolved with phase-alternating generations (<code>PhasePubSub</code>, <code>GenPubSub</code>) and atomic acknowledgement planes (<code>ChanPubSub57DeepSeek</code>).
        </p>
      </div>
      <div class="f-card info">
        <h4>2. Synchronous Rendezvous Casters</h4>
        <p>
          Dynamic subscriber registries with explicit registration/receive contracts and safe mid-flight withdrawal (<code>ChanCaster</code>, <code>SeqCaster</code>, <code>SenseCaster</code>). Zero-allocation fast-paths achieve sub-microsecond latency.
        </p>
      </div>
      <div class="f-card info">
        <h4>3. Bounded Lossy &amp; Lossless Rings</h4>
        <p>
          Sequence-stamped ring buffers with atomic cursors (<code>RingPubSub</code>, <code>RingCaster55Haiku</code>, <code>PhantomCast</code>). Eliminates sender blocking while allowing subscribers to catch up or detect overruns.
        </p>
      </div>
      <div class="f-card info">
        <h4>4. Epoch Gates &amp; Generation Signals</h4>
        <p>
          Parity-flipped generation markers and channel-close fan-out (<code>EpochBroadcast</code>, <code>EpochTape</code>, <code>EpochCaster</code>). Wakes unbounded subscribers with O(1) control-plane operations.
        </p>
      </div>
      <div class="f-card info">
        <h4>5. Nonblocking Mailbox &amp; RCU Fanout</h4>
        <p>
          Copy-on-write subscriber arrays paired with per-consumer unbounded FIFO queues (<code>QueuedFanout24Meta</code>, <code>SelectFanout</code>, <code>Fanout56MaiCode</code>). Guarantees sender non-blocking at the cost of heap allocations.
        </p>
      </div>
      <div class="f-card info">
        <h4>6. Work Distribution / Competing Consumers</h4>
        <p>
          Distributes each item to exactly one receiver (<code>ChanScatter</code>, <code>BaselineScatterQueue</code>, <code>StableRing32Nano</code>). Safe mid-flight withdrawal prevents item loss when a consumer cancels.
        </p>
      </div>
      <div class="f-card info">
        <h4>7. Signal Gathering &amp; State Beacons</h4>
        <p>
          Inverted fan-in signal collection (<code>ChanGatherer</code>) and loss-tracking atomic state beacons (<code>ChanBeacon</code>, <code>RingBeacon</code>).
        </p>
      </div>
    </div>
  </section>

  <!-- BASELINES -->
  <section class="block" id="baselines">
    <div class="kicker">Textbook Standard Practice</div>
    <h3 class="section-title">10 Staff-Engineer Baselines</h3>
    <p class="lede">
      Every scenario is anchored by a textbook baseline (see <code>baseline.go</code>) &mdash; the simplest sound design an expert staff engineer would write in a vacuum.
    </p>

    <div class="card pad">
      <div class="tbl-wrap">
        <table class="tbl">
          <thead>
            <tr>
              <th>Variant / Scenario</th>
              <th>Baseline Implementation</th>
              <th>Source File</th>
              <th>Textbook Design Rationale</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>Lock-step Pub-Sub</strong></td>
              <td><code>BaselineBarrierPubSub</code></td>
              <td><code>baseline.go</code></td>
              <td>Lossless synchronous broadcast using slice of subscriber channels with mutex barrier</td>
            </tr>
            <tr>
              <td><strong>Rendezvous Caster</strong></td>
              <td><code>BaselineRendezvousCaster</code></td>
              <td><code>baseline.go</code></td>
              <td>Rendezvous fan-out with explicit registration and per-send slice snapshot</td>
            </tr>
            <tr>
              <td><strong>Latest-Value Coalescing</strong></td>
              <td><code>BaselineLatestBroadcast</code></td>
              <td><code>baseline.go</code></td>
              <td>Single-slot atomic value buffer with rotating notification channel</td>
            </tr>
            <tr>
              <td><strong>Buffered Lossless Broadcast</strong></td>
              <td><code>BaselineBufferedBroadcast</code></td>
              <td><code>baseline.go</code></td>
              <td>Bounded per-subscriber channel buffers with backpressure blocking</td>
            </tr>
            <tr>
              <td><strong>Work Distribution</strong></td>
              <td><code>BaselineScatterQueue</code></td>
              <td><code>baseline.go</code></td>
              <td>Single-item dispatch to exactly one available worker via synchronized queue</td>
            </tr>
            <tr>
              <td><strong>Signal Gathering</strong></td>
              <td><code>BaselineGatherBarrier</code></td>
              <td><code>baseline.go</code></td>
              <td>N senders coordinate to unblock a single waiter via sync.WaitGroup / barrier</td>
            </tr>
            <tr>
              <td><strong>Async Fan-Out</strong></td>
              <td><code>BaselineFanout</code></td>
              <td><code>baseline.go</code></td>
              <td>Non-blocking worker goroutine per subscriber with unbounded channel queue</td>
            </tr>
            <tr>
              <td><strong>Unbounded Queued Fan-Out</strong></td>
              <td><code>BaselineQueuedFanout</code></td>
              <td><code>baseline_queued.go</code></td>
              <td>COW subscriber registry with non-blocking per-consumer FIFO mailboxes</td>
            </tr>
            <tr>
              <td><strong>Bounded Scatter Queue</strong></td>
              <td><code>BaselineBoundedScatterQueue</code></td>
              <td><code>baseline_scatter_bounded.go</code></td>
              <td>Fixed-ring competing consumer queue with exact overflow eviction</td>
            </tr>
            <tr>
              <td><strong>Bounded Catch-up Replay</strong></td>
              <td><code>BaselineBoundedReplay</code></td>
              <td><code>baseline_replay_test.go</code></td>
              <td>Synchronized live handoff with bounded history ring for late joiners</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- PATHOLOGY LAB -->
  <section class="block" id="pathology">
    <div class="kicker">Failure Mode Analysis</div>
    <h3 class="section-title">Pathology Lab: Concurrency Traps in AI Code</h3>
    <p class="lede">
      Testing AI code under rigorous stress, race-detection, and fuzzing uncovered fundamental failure patterns.
    </p>

    <div class="cards-grid">
      <div class="f-card danger">
        <h4>1. Concurrent Mid-Flight Withdrawal Races</h4>
        <p>
          When a receiver abandons a <code>select</code> midway through a publication round, models frequently reused channel slots or completed a stale rendezvous, causing data races or double-deliveries.
        </p>
      </div>
      <div class="f-card danger">
        <h4>2. Deadlock on Unbuffered Hub Teardown</h4>
        <p>
          Calling <code>Close()</code> while a publisher or subscriber was blocked on an unbuffered rendezvous channel routinely deadlocked because the teardown routine waited for active rounds to complete.
        </p>
      </div>
      <div class="f-card warn">
        <h4>3. Hidden Lock Contention in "Non-Blocking" Paths</h4>
        <p>
          Several designs claimed lock-free non-blocking operation, but placed <code>sync.Mutex</code> inside <code>TrySend()</code>, causing catastrophic throughput collapse under 64-subscriber fan-out.
        </p>
      </div>
      <div class="f-card warn">
        <h4>4. Silent Message Loss in Ring Buffers</h4>
        <p>
          Ring buffer outputs frequently failed to stamp items with monotonic sequences, causing slow subscribers to read torn values or skip laps without ever noticing they fell behind.
        </p>
      </div>
      <div class="f-card info">
        <h4>5. Zero-Allocation Illusions</h4>
        <p>
          Over 60% of models claimed "0 allocations per send", but runtime profiling detected hidden allocations from interface boxing (<code>any</code>), slice headers escaping to heap, or ephemeral notification channels.
        </p>
      </div>
    </div>
  </section>

  <!-- RAW DATA & DOWNLOADS -->
  <section class="block" id="downloads">
    <div class="kicker">Raw Data &amp; Verification</div>
    <h3 class="section-title">Committed Runs &amp; Datasets</h3>
    <p class="lede">
      All raw data is committed directly at repository root in the zero-padded run log (<code>results-*-*-*</code>).
    </p>

    <div class="card pad">
      <div class="tbl-wrap">
        <table class="tbl">
          <thead>
            <tr>
              <th>Run</th>
              <th>Profile</th>
              <th>Artifact File</th>
              <th>Description</th>
              <th>Verification</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-03-report.html</code></td>
              <td>Interactive Concurrency Measurement Report (2,013 records)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-03-report.html" class="btn btn-primary">Open Report</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-01-results.jsonl</code></td>
              <td>Raw JSONL benchmark records (2.59 MB)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-01-results.jsonl" download class="btn">Download JSONL</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-04-metrics_wide.csv</code></td>
              <td>Flattened wide metrics CSV (294 KB)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-04-metrics_wide.csv" download class="btn">Download CSV</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-05-scores.csv</code></td>
              <td>Adoptability scores &amp; dimension verdicts CSV</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-05-scores.csv" download class="btn">Download CSV</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-02-registry.json</code></td>
              <td>Scenario/Case/Attachment registry snapshot</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-02-registry.json" download class="btn">Download JSON</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-00-manifest.json</code></td>
              <td>Run environment fingerprint &amp; SHA-256 manifest</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-00-manifest.json" download class="btn">Download JSON</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-03-report.html</code></td>
              <td>Interactive Concurrency Measurement Report (1,479 records)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-03-report.html" class="btn btn-primary">Open Report</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-01-results.jsonl</code></td>
              <td>Smoke / CI profile JSONL (1.53 MB)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-01-results.jsonl" download class="btn">Download JSONL</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-04-metrics_wide.csv</code></td>
              <td>Flattened wide metrics CSV (Run 02)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-04-metrics_wide.csv" download class="btn">Download CSV</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-05-scores.csv</code></td>
              <td>Adoptability scores CSV (Run 02)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-05-scores.csv" download class="btn">Download CSV</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-00-manifest.json</code></td>
              <td>Run 02 environment fingerprint &amp; SHA-256 manifest</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-00-manifest.json" download class="btn">Download JSON</a></td>
            </tr>
            <tr>
              <td><strong>Analysis</strong></td>
              <td><span class="tag tag-prov">empirical</span></td>
              <td><code>report-01-gemini-flash-3.8-high.html</code></td>
              <td>Architectural &amp; Empirical Evolution Analysis</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="report-01-gemini-flash-3.8-high.html" class="btn">Open Analysis</a></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>

</main>

<footer class="site-foot">
  <div>
    <div style="font-weight:700;color:var(--ink);margin-bottom:4px">bigbuff &middot; AI Concurrency Benchmark</div>
    <div>Copyright &copy; 2026 Joseph Cumines &middot; MIT License</div>
    <div style="margin-top:6px;max-width:520px;color:var(--ink3)">
      Flat layout constraint: all source files, reports, and manifests reside directly at the repository root to maintain sync with the original GitHub Gist.
    </div>
  </div>
  <div style="display:flex;gap:32px;flex-wrap:wrap">
    <div>
      <div style="font-weight:600;color:var(--ink);margin-bottom:8px">Reports &amp; Analysis</div>
      <div style="display:flex;flex-direction:column;gap:5px">
        <a href="results-01-03-report.html">Measurement Report (Run 01 &middot; Standard)</a>
        <a href="results-02-03-report.html">Measurement Report (Run 02 &middot; Quick)</a>
        <a href="report-01-gemini-flash-3.8-high.html">Architectural Evolution Report</a>
        <a href="results-00-00-readme.md">Run Log Documentation</a>
      </div>
    </div>
    <div>
      <div style="font-weight:600;color:var(--ink);margin-bottom:8px">Repository Links</div>
      <div style="display:flex;flex-direction:column;gap:5px">
        <a href="__GITHUB_REPO_URL__" target="_blank" rel="noopener">GitHub Repository</a>
        <a href="01-readme.md">Gist Index (01-readme.md)</a>
        <a href="README.md">README.md</a>
        <a href="measurement.md">Measurement Methodology</a>
        <a href="AGENTS.md">AGENTS.md</a>
      </div>
    </div>
  </div>
</footer>

<script>
(function(){
  var RESPONSES = __RESPONSES_JSON__;
  var SCORES = __SCORES_JSON__;
  var REPORTS = __REPORTS_JSON__;

  function esc(s){
    if (s == null) return '';
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // --- Render Leaderboard Scores Table ---
  var currentFamily = 'all';
  var currentVerdict = 'all';
  var scoreQuery = '';

  function renderScores(){
    var q = scoreQuery.toLowerCase();
    var filtered = SCORES.filter(function(r){
      if (currentFamily !== 'all' && r.family !== currentFamily) return false;
      if (currentVerdict !== 'all' && r.verdict.toLowerCase().indexOf(currentVerdict.toLowerCase()) === -1) return false;
      if (q) {
        var match = (r.impl||'').toLowerCase().indexOf(q) !== -1 ||
                    (r.family||'').toLowerCase().indexOf(q) !== -1 ||
                    (r.verdict||'').toLowerCase().indexOf(q) !== -1;
        if (!match) return false;
      }
      return true;
    });

    var html = '';
    filtered.forEach(function(r){
      var s = parseInt(r.score, 10) || 0;
      var vClass = 'v-avoid';
      if (r.verdict.indexOf('Strong') !== -1) vClass = 'v-strong';
      else if (r.verdict.indexOf('Situational') !== -1) vClass = 'v-situational';
      else if (r.verdict.indexOf('Niche') !== -1) vClass = 'v-niche';

      html += '<tr>' +
        '<td><span class="tag">' + esc(r.family) + '</span></td>' +
        '<td style="font-weight:600;font-family:var(--mono);font-size:12px">' + esc(r.impl) + '</td>' +
        '<td><div class="scorebar"><div class="fill" style="width:' + s + '%"></div><span>' + s + '</span></div></td>' +
        '<td><span class="verdict ' + vClass + '">' + esc(r.verdict) + '</span></td>' +
        '<td>' + (r.throughput || '-') + '</td>' +
        '<td>' + (r.latency || '-') + '</td>' +
        '<td>' + (r.allocations || '-') + '</td>' +
        '<td>' + (r.scaling || '-') + '</td>' +
        '<td>' + (r.churn || '-') + '</td>' +
        '</tr>';
    });

    if (!filtered.length) {
      html = '<tr><td colspan="9" style="text-align:center;padding:24px;color:var(--ink3)">No implementations matched your filter criteria.</td></tr>';
    }
    document.getElementById('scoresBody').innerHTML = html;
  }

  document.getElementById('scoreSearch').addEventListener('input', function(e){
    scoreQuery = e.target.value;
    renderScores();
  });

  document.getElementById('familyPills').addEventListener('click', function(e){
    var target = e.target.closest('.pill');
    if (!target) return;
    document.querySelectorAll('#familyPills .pill').forEach(function(p){ p.classList.remove('active'); });
    target.classList.add('active');
    currentFamily = target.getAttribute('data-family');
    renderScores();
  });

  document.getElementById('verdictPills').addEventListener('click', function(e){
    var target = e.target.closest('.pill');
    if (!target) return;
    document.querySelectorAll('#verdictPills .pill').forEach(function(p){ p.classList.remove('active'); });
    target.classList.add('active');
    currentVerdict = target.getAttribute('data-verdict');
    renderScores();
  });

  // --- Render Model Responses Table ---
  var currentProv = 'all';
  var currentPrompt = 'all';
  var respQuery = '';

  function renderResponses(){
    var q = respQuery.toLowerCase();
    var filtered = RESPONSES.filter(function(r){
      if (currentProv !== 'all' && r.provider !== currentProv) return false;
      if (currentPrompt !== 'all' && r.prompt_type !== currentPrompt) return false;
      if (q) {
        var match = r.num.indexOf(q) !== -1 ||
                    r.filename.toLowerCase().indexOf(q) !== -1 ||
                    r.provider.toLowerCase().indexOf(q) !== -1 ||
                    r.impls.toLowerCase().indexOf(q) !== -1 ||
                    r.files.toLowerCase().indexOf(q) !== -1 ||
                    r.state.toLowerCase().indexOf(q) !== -1;
        if (!match) return false;
      }
      return true;
    });

    var html = '';
    filtered.forEach(function(r){
      var chatBtn = r.chat_link ? '<a href="' + esc(r.chat_link) + '" target="_blank" rel="noopener" class="tag" style="color:var(--cyan)">Chat &nearr;</a>' : '<span style="color:var(--ink3)">-</span>';
      
      var fileLinks = r.files.split(',').map(function(f){
        f = f.trim();
        if (f && f !== '-') {
          return '<a href="' + esc(f) + '" style="font-family:var(--mono);font-size:11.5px">' + esc(f) + '</a>';
        }
        return esc(f);
      }).join(', ');

      html += '<tr>' +
        '<td style="font-family:var(--mono);color:var(--cyan);font-weight:700">' + esc(r.num) + '</td>' +
        '<td><a href="' + esc(r.filename) + '" style="font-weight:600">' + esc(r.filename) + '</a></td>' +
        '<td><span class="tag tag-prov">' + esc(r.provider) + '</span></td>' +
        '<td><span class="tag tag-prompt">' + esc(r.prompt_type) + '</span></td>' +
        '<td style="font-weight:600">' + esc(r.impls) + '</td>' +
        '<td>' + fileLinks + '</td>' +
        '<td style="font-size:12px;color:var(--ink2)">' + esc(r.state) + '</td>' +
        '<td>' + chatBtn + '</td>' +
        '</tr>';
    });

    if (!filtered.length) {
      html = '<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--ink3)">No model responses matched your search criteria.</td></tr>';
    }
    document.getElementById('respBody').innerHTML = html;
  }

  document.getElementById('respSearch').addEventListener('input', function(e){
    respQuery = e.target.value;
    renderResponses();
  });

  document.getElementById('providerPills').addEventListener('click', function(e){
    var target = e.target.closest('.pill');
    if (!target) return;
    document.querySelectorAll('#providerPills .pill').forEach(function(p){ p.classList.remove('active'); });
    target.classList.add('active');
    currentProv = target.getAttribute('data-prov');
    renderResponses();
  });

  document.getElementById('promptPills').addEventListener('click', function(e){
    var target = e.target.closest('.pill');
    if (!target) return;
    document.querySelectorAll('#promptPills .pill').forEach(function(p){ p.classList.remove('active'); });
    target.classList.add('active');
    currentPrompt = target.getAttribute('data-prompt');
    renderResponses();
  });

  // Initial renders
  renderScores();
  renderResponses();
})();
</script>
</body>
</html>
`
