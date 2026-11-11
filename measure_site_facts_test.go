package bigbuff

// Everything on the landing page that is a count, a run, a report or a
// baseline is read from the committed files here, so the page cannot fall
// behind them: a new run or a new report-NN-*.{html,md} appears on it the
// next time `make site` runs. The prose sections of the template (paradigms,
// pathology lab) are written by hand.

import (
	"encoding/json"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type siteRunFile struct {
	Path, Slot, Kind string
	Bytes            int64
	Records          int
}

type siteRun struct {
	Run             string // "05"
	Profile         string
	Commit, Go      string
	Generator       string
	NumCPU          int
	Records         int
	Skipped         int
	DurationS       float64
	Verified        bool
	VerifiedAgainst string
	Files           []siteRunFile
}

func (r siteRun) file(slot string) (siteRunFile, bool) {
	for _, f := range r.Files {
		if f.Slot == slot {
			return f, true
		}
	}
	return siteRunFile{}, false
}

type siteAnalysis struct {
	Filename, Title, Summary string
	Size                     int64
	// ReadsRun is the run ("07") whose results file a Markdown analysis
	// names first, or empty when it names none. A written analysis is not
	// regenerated, so the page says which run it was written from.
	ReadsRun string
}

type siteBaseline struct {
	Scenario, Title, Delivery, Baseline, Remarks string
}

type siteFacts struct {
	Runs        []siteRun // oldest first
	Analyses    []siteAnalysis
	Scenarios   int
	Attachments int
	Baselines   []siteBaseline
	RegistryRun string
}

func loadSiteFacts(dir string) (siteFacts, error) {
	var facts siteFacts
	manifests, err := filepath.Glob(filepath.Join(dir, "results-[0-9][0-9]-00-manifest.json"))
	if err != nil {
		return facts, err
	}
	sort.Strings(manifests)
	for _, path := range manifests {
		raw, err := os.ReadFile(path)
		if err != nil {
			return facts, err
		}
		var m struct {
			Run             int     `json:"run"`
			Profile         string  `json:"profile"`
			Commit          string  `json:"commit"`
			Go              string  `json:"go"`
			Generator       string  `json:"generator_version"`
			NumCPU          int     `json:"num_cpu"`
			Skipped         float64 `json:"skipped"`
			DurationS       float64 `json:"duration_s"`
			Verified        bool    `json:"verified"`
			VerifiedAgainst string  `json:"verified_against"`
			Files           []struct {
				Path    string `json:"path"`
				Slot    string `json:"slot"`
				Kind    string `json:"kind"`
				Bytes   int64  `json:"bytes"`
				Summary struct {
					Records float64 `json:"records"`
				} `json:"summary"`
			} `json:"files"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return facts, fmt.Errorf("%s: %w", path, err)
		}
		run := siteRun{
			Run: fmt.Sprintf("%02d", m.Run), Profile: m.Profile, Commit: m.Commit, Go: m.Go, Generator: m.Generator,
			NumCPU: m.NumCPU, Skipped: int(m.Skipped), DurationS: m.DurationS,
			Verified: m.Verified, VerifiedAgainst: m.VerifiedAgainst,
		}
		if info, err := os.Stat(path); err == nil {
			run.Files = append(run.Files, siteRunFile{Path: filepath.Base(path), Slot: "00", Kind: "manifest", Bytes: info.Size()})
		}
		for _, f := range m.Files {
			kind := f.Kind
			if strings.HasSuffix(f.Path, ".jsonl") {
				kind = "results"
				run.Records = int(f.Summary.Records)
			}
			run.Files = append(run.Files, siteRunFile{Path: f.Path, Slot: f.Slot, Kind: kind, Bytes: f.Bytes, Records: int(f.Summary.Records)})
		}
		facts.Runs = append(facts.Runs, run)
	}

	// The newest registry dump describes the scenarios and their baselines.
	dumps, _ := filepath.Glob(filepath.Join(dir, "results-[0-9][0-9]-02-registry.json"))
	sort.Strings(dumps)
	if len(dumps) > 0 {
		path := dumps[len(dumps)-1]
		facts.RegistryRun = strings.TrimPrefix(filepath.Base(path), "results-")[:2]
		reg, err := readRegistryDump(path)
		if err != nil {
			return facts, err
		}
		scenarios, _ := reg["scenarios"].([]any)
		attachments, _ := reg["attachments"].([]any)
		facts.Scenarios, facts.Attachments = len(scenarios), len(attachments)
		remarks := map[[2]string]string{}
		for _, a := range attachments {
			if m, ok := a.(map[string]any); ok {
				impl, _ := m["impl"].(string)
				scenario, _ := m["scenario"].(string)
				text, _ := m["remarks"].(string)
				remarks[[2]string{scenario, impl}] = text
			}
		}
		for _, s := range scenarios {
			m, ok := s.(map[string]any)
			if !ok {
				continue
			}
			id, _ := m["id"].(string)
			title, _ := m["title"].(string)
			baseline, _ := m["baseline"].(string)
			kind, _ := m["kind"].(string)
			loss, _ := m["loss"].(string)
			facts.Baselines = append(facts.Baselines, siteBaseline{
				Scenario: id, Title: title, Delivery: strings.TrimSpace(kind + ", " + loss), Baseline: baseline,
				Remarks: remarks[[2]string{id, baseline}],
			})
		}
	}

	// Written analyses: report-NN-*.html and report-NN-*.md.
	for _, pattern := range []string{"report-[0-9][0-9]-*.html", "report-[0-9][0-9]-*.md"} {
		paths, _ := filepath.Glob(filepath.Join(dir, pattern))
		for _, path := range paths {
			raw, err := os.ReadFile(path)
			if err != nil {
				return facts, err
			}
			a := siteAnalysis{Filename: filepath.Base(path), Size: int64(len(raw))}
			if strings.HasSuffix(path, ".md") {
				a.Title, a.Summary = markdownTitleAndSummary(string(raw))
				if m := siteReadsRunRE.FindSubmatch(raw); m != nil {
					a.ReadsRun = string(m[1])
				}
			} else {
				a.Title, a.Summary = htmlTitleAndSummary(string(raw[:min(len(raw), 65536)]))
			}
			if a.Title == "" {
				a.Title = a.Filename
			}
			facts.Analyses = append(facts.Analyses, a)
		}
	}
	sort.Slice(facts.Analyses, func(i, j int) bool { return facts.Analyses[i].Filename > facts.Analyses[j].Filename })
	return facts, nil
}

var (
	siteReadsRunRE  = regexp.MustCompile(`results-(\d\d)-01-results\.jsonl`)
	siteHTMLTitleRE = regexp.MustCompile(`(?is)<title>(.*?)</title>`)
	siteHTMLDescRE  = regexp.MustCompile(`(?is)<meta\s+name="description"\s+content="(.*?)"`)
	siteHTMLParaRE  = regexp.MustCompile(`(?is)<p[^>]*>(.{40,}?)</p>`)
	siteHTMLTagRE   = regexp.MustCompile(`(?s)<[^>]*>`)
	siteMDLinkRE    = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)
)

func htmlTitleAndSummary(head string) (title, summary string) {
	if m := siteHTMLTitleRE.FindStringSubmatch(head); m != nil {
		title = html.UnescapeString(strings.TrimSpace(m[1]))
	}
	if m := siteHTMLDescRE.FindStringSubmatch(head); m != nil {
		summary = html.UnescapeString(strings.TrimSpace(m[1]))
	} else if m := siteHTMLParaRE.FindStringSubmatch(head); m != nil {
		// No description: the first paragraph, as plain text.
		text := siteHTMLTagRE.ReplaceAllString(m[1], "")
		summary = siteCut(strings.Join(strings.Fields(html.UnescapeString(text)), " "))
	}
	return title, summary
}

// siteCut shortens a summary at a word boundary.
func siteCut(summary string) string {
	const limit = 340
	if len(summary) <= limit {
		return summary
	}
	cut := strings.LastIndexByte(summary[:limit], ' ')
	if cut < 0 {
		cut = limit
	}
	return strings.TrimRight(summary[:cut], " ,;:") + " …"
}

// markdownTitleAndSummary returns the first heading and the first paragraph
// of a Markdown document as plain text, the paragraph cut at a word boundary.
func markdownTitleAndSummary(doc string) (title, summary string) {
	var para []string
	for _, line := range strings.Split(doc, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case title == "" && strings.HasPrefix(line, "# "):
			title = strings.TrimSpace(line[2:])
		case title == "":
		case line == "" && len(para) > 0:
			goto done
		case line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "|") || strings.HasPrefix(line, ">"):
		default:
			para = append(para, line)
		}
	}
done:
	summary = strings.Join(para, " ")
	summary = siteMDLinkRE.ReplaceAllString(summary, "$1")
	summary = strings.NewReplacer("**", "", "`", "", "__", "").Replace(summary)
	return title, siteCut(summary)
}

func siteTitleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func siteBytes(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

// siteFileLabels: what each kind of run file is, and what its link says.
var siteFileLabels = map[string][2]string{
	"manifest":           {"Environment fingerprint and SHA-256 of every file in the run", "Download JSON"},
	"results":            {"Raw records, one JSON object per line", "Download JSONL"},
	"registry-dump":      {"The scenario, case, attachment and runnable registry the run was made from", "Download JSON"},
	"interactive-report": {"Interactive measurement report", "Open Report"},
	"csv-export":         {"CSV export", "Download CSV"},
	"gates-record":       {"What `make verify` reported, and at which commit", "Download JSON"},
	"evaluation":         {"Evaluation generated from the run's own files", "Open Evaluation"},
}

func siteFileDescription(f siteRunFile) (desc, action string) {
	label, ok := siteFileLabels[f.Kind]
	if !ok {
		label = [2]string{"Run artifact", "Open"}
	}
	desc = label[0]
	switch {
	case f.Kind == "csv-export" && f.Slot == "04":
		desc = "Every metric record, flattened to one row"
	case f.Kind == "csv-export" && f.Slot == "05":
		desc = "Scores, dimension ranks and verdicts"
	case f.Kind == "results":
		desc += " (" + thousandsSep(float64(f.Records)) + " records)"
	}
	return strings.ReplaceAll(desc, "`", "") + " · " + siteBytes(f.Bytes), label[1]
}

// renderSiteFacts substitutes every generated region of the landing page.
func renderSiteFacts(page string, cat siteCatalog, facts siteFacts) string {
	esc := html.EscapeString
	var latest siteRun
	if n := len(facts.Runs); n > 0 {
		latest = facts.Runs[n-1]
	}
	latestReport := "#reports"
	if f, ok := latest.file("03"); ok {
		latestReport = f.Path
	}
	verified := 0
	for _, r := range facts.Runs {
		if r.Verified {
			verified++
		}
	}
	ranked := 0
	for _, s := range cat.Scores {
		if s.Score != "" {
			ranked++
		}
	}
	baselineNames := map[string]bool{}
	for _, b := range facts.Baselines {
		baselineNames[b.Baseline] = true
	}
	first, last := "", ""
	if n := len(cat.Responses); n > 0 {
		first, last = cat.Responses[0].Num, cat.Responses[n-1].Num
	}

	// Report cards: runs newest first, then written analyses newest first.
	var cards, hero, footer, datasets strings.Builder
	for i := len(facts.Runs) - 1; i >= 0; i-- {
		r := facts.Runs[i]
		report, hasReport := r.file("03")
		if !hasReport {
			continue
		}
		status := `<span class="v-strong">verified</span>`
		if !r.Verified {
			status = `<span class="v-avoid">NOT verified</span>`
		}
		fmt.Fprintf(&cards, `      <div class="report-card meas">
        <div>
          <span class="report-badge">Measurement &middot; Run %s (%s profile)</span>
          <h4>Measurement Report (Run %s &middot; %s)</h4>
          <p>
            %s records taken in %.0f s at commit <code>%s</code> with %s on %d logical CPUs. Coverage %s against <code>%s</code>.
          </p>
          <ul class="report-features">
`, r.Run, esc(r.Profile), r.Run, esc(siteTitleCase(r.Profile)), thousandsSep(float64(r.Records)), r.DurationS, esc(r.Commit), esc(r.Go), r.NumCPU, status, esc(r.VerifiedAgainst))
		if i == len(facts.Runs)-1 {
			cards.WriteString(`            <li><span class="dot"></span> <strong>Newest run:</strong> the scorecards below are read from it</li>` + "\n")
		}
		if generatorBefore(r.Generator, isolationContract) {
			cards.WriteString(`            <li><span class="dot"></span> <strong>Not ranked:</strong> the run was one process, and publishers left spinning by an early stalled-consumer probe kept the CPUs busy for everything measured after it; runs under record contract ` + isolationContract + ` or later measure each battery in a process of its own</li>` + "\n")
		}
		if _, ok := r.file("06"); ok {
			cards.WriteString(`            <li><span class="dot"></span> <strong>Gates recorded:</strong> the evaluation states what <code>make verify</code> reported</li>` + "\n")
		}
		fmt.Fprintf(&cards, `            <li><span class="dot"></span> %d implementation(s) skipped a battery, each with a recorded reason</li>
          </ul>
        </div>
        <div class="report-actions">
          <a href="%s" class="btn btn-primary">Open Run %s Report &rarr;</a>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
`, r.Skipped, esc(report.Path), r.Run)
		if f, ok := r.file("07"); ok {
			fmt.Fprintf(&cards, `            <a href="%s" class="btn" title="Evaluation generated from the run's own files">Evaluation</a>`+"\n", esc(f.Path))
		}
		if f, ok := r.file("04"); ok {
			fmt.Fprintf(&cards, `            <a href="%s" download class="btn" title="Download flattened metrics CSV">Metrics CSV</a>`+"\n", esc(f.Path))
		}
		if f, ok := r.file("05"); ok {
			fmt.Fprintf(&cards, `            <a href="%s" download class="btn" title="Download scores CSV">Scores CSV</a>`+"\n", esc(f.Path))
		}
		cards.WriteString("          </div>\n        </div>\n      </div>\n\n")
		fmt.Fprintf(&footer, `        <a href="%s">Measurement Report (Run %s &middot; %s)</a>`+"\n", esc(report.Path), r.Run, esc(siteTitleCase(r.Profile)))

		for _, f := range r.Files {
			desc, action := siteFileDescription(f)
			class, download := "btn", " download"
			if f.Kind == "interactive-report" || f.Kind == "evaluation" {
				download = ""
			}
			if f.Kind == "interactive-report" {
				class = "btn btn-primary"
			}
			fmt.Fprintf(&datasets, `            <tr>
              <td><strong>Run %s</strong></td>
              <td><span class="tag tag-prompt">%s</span></td>
              <td><code>%s</code></td>
              <td>%s</td>
              <td>%s</td>
              <td><a href="%s"%s class="%s">%s</a></td>
            </tr>
`, r.Run, esc(r.Profile), esc(f.Path), esc(desc), status, esc(f.Path), download, class, esc(action))
		}
	}
	if f, ok := latest.file("03"); ok {
		fmt.Fprintf(&hero, `      <a href="%s" class="btn btn-primary">Measurement Report (Run %s &middot; %s)</a>`+"\n", esc(f.Path), latest.Run, esc(siteTitleCase(latest.Profile)))
	}
	if f, ok := latest.file("07"); ok {
		fmt.Fprintf(&hero, `      <a href="%s" class="btn">Evaluation of Run %s</a>`+"\n", esc(f.Path), latest.Run)
	}
	for _, a := range facts.Analyses {
		kind := "Written analysis"
		if strings.HasSuffix(a.Filename, ".html") {
			kind = "Written analysis &middot; HTML"
		}
		summary := a.Summary
		if summary == "" {
			summary = "No summary is recorded in the file."
		}
		reads := `Names no measurement run. It was written by its author at the time and is not regenerated; any figure in it predates run ` + latest.Run
		if a.ReadsRun != "" {
			reads = `Written from run ` + esc(a.ReadsRun) + `; it is not regenerated`
			for _, r := range facts.Runs {
				if r.Run != a.ReadsRun {
					continue
				}
				if generatorBefore(r.Generator, isolationContract) {
					reads += `, and the measurements of that run are not valid (see the run's card)`
				} else if a.ReadsRun != latest.Run {
					reads += `, and run ` + latest.Run + ` is newer`
				}
			}
		}
		fmt.Fprintf(&cards, `      <div class="report-card arch">
        <div>
          <span class="report-badge">%s</span>
          <h4>%s</h4>
          <p>
            %s
          </p>
          <ul class="report-features">
            <li><span class="dot"></span> <code>%s</code> &middot; %s</li>
            <li><span class="dot"></span> `+reads+`</li>
          </ul>
        </div>
        <div class="report-actions">
          <a href="%s" class="btn btn-primary">Read &rarr;</a>
          <a href="01-readme.md" class="btn">Extraction Tracker</a>
        </div>
      </div>

`, kind, esc(a.Title), esc(summary), esc(a.Filename), siteBytes(a.Size), esc(a.Filename))
		fmt.Fprintf(&hero, `      <a href="%s" class="btn">%s</a>`+"\n", esc(a.Filename), esc(a.Title))
		fmt.Fprintf(&footer, `        <a href="%s">%s</a>`+"\n", esc(a.Filename), esc(a.Title))
		fmt.Fprintf(&datasets, `            <tr>
              <td><strong>Analysis</strong></td>
              <td><span class="tag tag-prov">written</span></td>
              <td><code>%s</code></td>
              <td>%s · %s</td>
              <td>not applicable</td>
              <td><a href="%s" class="btn">Open</a></td>
            </tr>
`, esc(a.Filename), esc(a.Title), siteBytes(a.Size), esc(a.Filename))
	}

	var baselines strings.Builder
	for _, b := range facts.Baselines {
		fmt.Fprintf(&baselines, `            <tr>
              <td><strong>%s</strong><br><code>%s</code></td>
              <td>%s</td>
              <td><code>%s</code></td>
              <td>%s</td>
            </tr>
`, esc(b.Title), esc(b.Scenario), esc(b.Delivery), esc(b.Baseline), esc(b.Remarks))
	}

	return strings.NewReplacer(
		"__HERO_BUTTONS__", hero.String(),
		"__REPORT_CARDS__", cards.String(),
		"__DATASET_ROWS__", datasets.String(),
		"__FOOTER_REPORT_LINKS__", footer.String(),
		"__BASELINE_ROWS__", baselines.String(),
		"__LATEST_REPORT__", latestReport,
		"__LATEST_RUN__", latest.Run,
		"__LATEST_PROFILE__", esc(latest.Profile),
		"__LATEST_RECORDS__", thousandsSep(float64(latest.Records)),
		"__LATEST_CPUS__", strconv.Itoa(latest.NumCPU),
		"__REGISTRY_RUN__", facts.RegistryRun,
		"__MIN_RANKED__", strconv.Itoa(minRankedImpls),
		"__N_RESPONSES__", strconv.Itoa(len(cat.Responses)),
		"__RESP_FIRST__", first,
		"__RESP_LAST__", last,
		"__N_IMPLS__", strconv.Itoa(len(cat.Scores)),
		"__N_RANKED__", strconv.Itoa(ranked),
		"__N_SCENARIOS__", strconv.Itoa(facts.Scenarios),
		"__N_ATTACHMENTS__", strconv.Itoa(facts.Attachments),
		"__N_BASELINES__", strconv.Itoa(len(baselineNames)),
		"__N_RUNS__", strconv.Itoa(len(facts.Runs)),
		"__N_RUNS_VERIFIED__", strconv.Itoa(verified),
	).Replace(page)
}

// trackerFiles resolves the "files" cell of a tracker row against dir. The
// cell is written for people: `a_72{,_fast,_race}.go`, `b_70*.go`, and
// `ring.go, _fast.go` (a suffix of the file before it). It returns the files
// that exist, in order, and the tokens that resolve to nothing.
func trackerFiles(dir, cell string) (files, unresolved []string) {
	var tokens []string
	depth, start := 0, 0
	for i, c := range cell {
		switch c {
		case '{':
			depth++
		case '}':
			depth--
		case ',':
			if depth == 0 {
				tokens = append(tokens, cell[start:i])
				start = i + 1
			}
		}
	}
	tokens = append(tokens, cell[start:])
	seen := map[string]bool{}
	previous := ""
	for _, token := range tokens {
		token = strings.Trim(strings.TrimSpace(token), "`")
		if token == "" || token == "-" {
			continue
		}
		patterns := []string{token}
		if open, close := strings.IndexByte(token, '{'), strings.IndexByte(token, '}'); open >= 0 && close > open {
			patterns = patterns[:0]
			for _, alt := range strings.Split(token[open+1:close], ",") {
				patterns = append(patterns, token[:open]+strings.TrimSpace(alt)+token[close+1:])
			}
		}
		resolved := false
		for _, pattern := range patterns {
			if strings.HasPrefix(pattern, "_") && previous != "" {
				pattern = strings.TrimSuffix(previous, filepath.Ext(previous)) + pattern
			}
			matches, _ := filepath.Glob(filepath.Join(dir, pattern))
			sort.Strings(matches)
			for _, m := range matches {
				name := filepath.Base(m)
				// A glob names the implementation's files, not its tests.
				if strings.ContainsAny(pattern, "*?") && strings.HasSuffix(name, "_test.go") {
					continue
				}
				resolved = true
				if !seen[name] {
					seen[name] = true
					files = append(files, name)
				}
			}
		}
		if !resolved {
			unresolved = append(unresolved, token)
		} else if !strings.HasPrefix(token, "_") {
			previous = files[len(files)-1]
			if first := patterns[0]; !strings.ContainsAny(first, "*?") && seen[first] {
				previous = first
			}
		}
	}
	return files, unresolved
}
