# Usage
# ---
# Landing page and documentation/report wire-up module for bigbuff, wired into GNU Make.
# Generates and synchronizes index.html, bidirectional report backlinks, and cross-linking
# between GitHub README and GitHub Pages entirely in Go (no Python).
#
# Module targets use the `site.` prefix per AGENTS.md and compose into standard root targets.

# WARNING: Must be before any includes (same contract as the root Makefile).
SITE_MK := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_ROOT ?= $(patsubst %/,%,$(dir $(SITE_MK)))

SITE_HARNESS ?= go test -run=NONE -count=1 -timeout=$(GO_TEST_TIMEOUT) -args bigbuff-measure

.PHONY: site.build
site.build: ## Generate/update index.html and wire backlinks across all reports.
	cd $(PROJECT_ROOT) && $(SITE_HARNESS) site

.PHONY: site.wire
site.wire: site.build ## Alias for site.build.

.PHONY: site.check
site.check: site.build ## Verify that all reports and index.html exist and links are intact.
	@test -f $(PROJECT_ROOT)/index.html || { echo "Error: index.html missing"; exit 1; }
	@test -f $(PROJECT_ROOT)/results-01-03-report.html || { echo "Error: results-01-03-report.html missing"; exit 1; }
	@test -f $(PROJECT_ROOT)/results-02-03-report.html || { echo "Error: results-02-03-report.html missing"; exit 1; }
	@test -f $(PROJECT_ROOT)/report-01-gemini-flash-3.8-high.html || { echo "Error: report-01-gemini-flash-3.8-high.html missing"; exit 1; }
	@echo "site.check: all landing page and report links verified successfully."

.PHONY: site.clean
site.clean: ## Site artifacts (index.html) are committed at repository root; non-destructive clean.
	@echo "Site artifacts are committed at repository root."
