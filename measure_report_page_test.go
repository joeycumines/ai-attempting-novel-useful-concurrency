package bigbuff

// HTML assembly: embeds the dataset and registry into the report template.

import (
	"encoding/json"
	"strings"
)

// --- HTML report ---

func jsonForHTML(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	// Prevent "</script>" breakouts and HTML comment tricks.
	s := strings.ReplaceAll(string(b), "</", "<\\/")
	s = strings.ReplaceAll(s, "<!--", "<\\!--")
	return s, nil
}

func renderReportPage(data *reportData, registry map[string]any, derived derivedData) (string, error) {
	dataJSON, err := jsonForHTML(map[string]any{
		"meta":    data.meta,
		"metrics": data.metrics,
		"traces":  data.traces,
		"skipped": data.skipped,
		"summary": data.summary,
		"derived": derived,
	})
	if err != nil {
		return "", err
	}
	registryJSON, err := jsonForHTML(registry)
	if err != nil {
		return "", err
	}
	page := reportHTMLCSS + reportHTMLBody + "<script>\n" + reportJS1 + reportJS2 + reportJS3 + reportJS4 + "\n</script>\n</body>\n</html>\n"
	page = strings.Replace(page, "__DATA_JSON__", dataJSON, 1)
	page = strings.Replace(page, "__REGISTRY_JSON__", registryJSON, 1)
	return page, nil
}
