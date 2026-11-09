package bigbuff

import (
	"strings"
	"testing"
)

// TestSiteWiringIsIdempotent checks that a second wiring pass changes nothing,
// and that a report carrying extra whitespace before an injected block converges
// to the clean bytes. Without both properties every site build rewrites the
// reports and the landing page again.
func TestSiteWiringIsIdempotent(t *testing.T) {
	reports := []siteReportMeta{
		{Filename: "results-01-03-report.html", Title: "bigbuff measurement run 01", Type: "measurement", Run: "01"},
		{Filename: "report-01-gemini-flash-3.8-high.html", Title: "bigbuff architectural analysis", Type: "architectural", Run: "01"},
	}
	measurement := "<!doctype html>\n<html>\n<head>\n<title>run 01</title>\n" +
		"<style>@media (max-width:1080px){#shell{grid-template-columns:1fr}#nav{display:none}}</style>\n" +
		"</head>\n<body>\n<div class=\"brand\">\n  <div class=\"name\">bigbuff</div>\n  </div>\n" +
		"<main class=\"container\">\n<p>measurement</p>\n</main>\n</body>\n</html>\n"
	architectural := "<!doctype html>\n<html>\n<head>\n<title>analysis</title>\n</head>\n" +
		"<body>\n<p>analysis</p>\n</body>\n</html>\n"
	cases := []struct {
		name   string
		file   string
		in     string
		marker string
		wire   func(string, string, []siteReportMeta) string
	}{
		{"measurement", "results-01-03-report.html", measurement, "<!-- START AUTOMATED MOBILE TOPBAR", wireMeasurementReportContent},
		{"architectural", "report-01-gemini-flash-3.8-high.html", architectural, "<!-- START AUTOMATED GLOBAL NAV", wireArchitecturalReportContent},
	}
	for _, tc := range cases {
		once := tc.wire(tc.in, tc.file, reports)
		if !strings.Contains(once, tc.marker) {
			t.Fatalf("%s: wiring injected no %q block", tc.name, tc.marker)
		}
		if twice := tc.wire(once, tc.file, reports); twice != once {
			t.Fatalf("%s: a second wiring pass changed the report (%d bytes became %d)", tc.name, len(once), len(twice))
		}
		accumulated := strings.Replace(once, tc.marker, "\n"+tc.marker, 1)
		if got := tc.wire(accumulated, tc.file, reports); got != once {
			t.Fatalf("%s: extra whitespace before the injected block did not converge to the clean report", tc.name)
		}
	}
}
