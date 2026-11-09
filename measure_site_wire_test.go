package bigbuff

// Report wiring for the site build. Each injected block is marked, removed and
// reinserted on every run, so a second run must change nothing.

import (
	"fmt"
	"html"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

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
		re := regexp.MustCompile(`(?s)\s*<!-- START AUTOMATED BACKLINKS.*?<!-- END AUTOMATED BACKLINKS -->`)
		content = re.ReplaceAllString(content, "")
	}
	brandClose := "</div>\n  </div>"
	idx := strings.Index(content, brandClose)
	if idx != -1 {
		insertPos := idx + len(brandClose)
		content = injectBlockAfter(content, insertPos, navBacklinks)
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
		re := regexp.MustCompile(`(?s)\s*<!-- START AUTOMATED MOBILE TOPBAR.*?<!-- END AUTOMATED MOBILE TOPBAR -->`)
		content = re.ReplaceAllString(content, "")
	}
	mainRe := regexp.MustCompile(`(?i)<main[^>]*>`)
	if loc := mainRe.FindStringIndex(content); loc != nil {
		content = injectBlockAfter(content, loc[1], mobileTopbar)
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

// injectBlockAfter places block on its own line directly after content[:pos].
// The content that followed keeps its indentation, on the line after the block.
// Wiring a report again then rebuilds exactly these bytes, so the pass is
// idempotent. Whitespace before the anchor's remainder is normalised here.
func injectBlockAfter(content string, pos int, block string) string {
	rest := content[pos:]
	body := strings.TrimLeft(rest, " \t\r\n")
	lead := rest[:len(rest)-len(body)]
	indent := ""
	if i := strings.LastIndex(lead, "\n"); i >= 0 {
		indent = lead[i+1:]
	}
	return content[:pos] + "\n" + block + "\n" + indent + body
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
		re := regexp.MustCompile(`(?s)\s*<!-- START AUTOMATED GLOBAL NAV.*?<!-- END AUTOMATED GLOBAL NAV -->`)
		content = re.ReplaceAllString(content, "")
	}
	bodyRe := regexp.MustCompile(`(?i)<body[^>]*>`)
	if loc := bodyRe.FindStringIndex(content); loc != nil {
		content = injectBlockAfter(content, loc[1], globalNav)
	}

	return content
}
