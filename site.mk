# Usage
# ---
# Landing page and documentation/report wire-up module for bigbuff, wired into GNU Make.
# Generates and synchronizes index.html, bidirectional report backlinks, and cross-linking
# between GitHub README and GitHub Pages entirely in Go (no Python).
#
# Module targets use the `site.` prefix per AGENTS.md and compose into standard root targets.
#
# `site` does three things in a fixed order. It wires the reports (they gain
# navigation and backlinks, so they are rewritten in place); it re-indexes the
# run manifests, which record the SHA-256 of those reports and verify each run
# against its own registry dump; and it writes index.html from the manifests,
# the newest registry dump, the newest scores CSV, the tracker and the
# report-NN-* files. Every step is idempotent: `make site` on an up-to-date
# tree changes nothing. It exits non-zero if a run fails verification.

# WARNING: Must be before any includes (same contract as the root Makefile).
SITE_MK := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_ROOT ?= $(patsubst %/,%,$(dir $(SITE_MK)))

SITE_HARNESS ?= go test -run=NONE -count=1 -timeout=$(GO_TEST_TIMEOUT) -args bigbuff-measure

.PHONY: site.build
site.build: ## Wire backlinks across all reports, re-index the run manifests, and generate index.html from them.
	cd $(PROJECT_ROOT) && $(SITE_HARNESS) site

.PHONY: site.wire
site.wire: site.build ## Alias for site.build.

.PHONY: site.check
site.check: site.build ## Rebuild, then check the page covers every run and report and the tracker resolves.
	@test -f $(PROJECT_ROOT)/index.html || { echo "Error: index.html missing"; exit 1; }
	@cd $(PROJECT_ROOT) && for jsonl in results-[0-9][0-9]-01-results.jsonl; do \
	  report=$${jsonl%-01-results.jsonl}-03-report.html; \
	  test -f $$report || { echo "Error: $$report missing (run 'make measure-report MEASURE_IN=$$jsonl')"; exit 1; }; \
	done
	cd $(PROJECT_ROOT) && go test -count=1 -run 'TestLandingPageCoversEveryRunAndReport|TestTrackerFilesResolve|TestTrackerCoversEveryResponse|TestCommittedRunsAreIndexed' .
	@echo "site.check: index.html exists, every run has its report, the page covers every run and report file, and every tracker file resolves."

.PHONY: site.clean
site.clean: ## Site artifacts (index.html) are committed at repository root; non-destructive clean.
	@echo "Site artifacts are committed at repository root."
