# Usage
# ---
# Measurement batteries for every implementation in this repository, wired into
# GNU Make. The batteries run through the Go test binary as a subcommand binary
# (`go test -run=NONE -args bigbuff-measure …`), so plain `go test` keeps
# working untouched (see MEASUREMENT.md for the full contract).
#
# Module targets here use the `measure.` prefix. The root Makefile composes
# them into ergonomic entry points (`make measure`, `make measure-quick`,
# `make measure-report`, …).
#
# Configuration & Overrides
# ---
#   MEASURE_RESULTS_DIR   output directory for JSONL/registry/report/CSVs (default: results)
#   MEASURE_PROFILE       which profile `make measure` runs (default: standard)
#   MEASURE_SEED          optional seed override forwarded to the harness
#   MEASURE_TIMEOUT       go test -timeout for measurement runs (default: $(GO_TEST_TIMEOUT))
#   MEASURE_EXTRA_FLAGS   additional flags appended to the harness invocation
#
# Everything is overridable on the command line, e.g.
#   make measure MEASURE_PROFILE=full MEASURE_TIMEOUT=180m

# WARNING: Must be before any includes (same contract as the root Makefile).
MEASURE_MK := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_ROOT ?= $(patsubst %/,%,$(dir $(MEASURE_MK)))

MEASURE_RESULTS_DIR ?= $(PROJECT_ROOT)/results
MEASURE_PROFILE ?= standard
MEASURE_SEED ?=
MEASURE_TIMEOUT ?= $(GO_TEST_TIMEOUT)
MEASURE_EXTRA_FLAGS ?=
MEASURE_OUT ?= $(MEASURE_RESULTS_DIR)/measurements.jsonl
MEASURE_REPORT_OUT ?= $(MEASURE_RESULTS_DIR)/report.html
MEASURE_CSV_DIR ?= $(MEASURE_RESULTS_DIR)/csv

MEASURE_HARNESS := go test -run=NONE -count=1 -timeout=$(MEASURE_TIMEOUT) -args bigbuff-measure
MEASURE_SEED_ARG := $(if $(MEASURE_SEED),--seed $(MEASURE_SEED),)

.PHONY: measure.list
measure.list: ## List all measured implementation batteries, profiles and registry families.
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) list

.PHONY: measure.registry
measure.registry: ## Dump the scenario/case/attachment registry to JSON ($(MEASURE_RESULTS_DIR)/registry.json).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) dump-registry --out $(MEASURE_RESULTS_DIR)/registry.json

.PHONY: measure.run
measure.run: ## Run the $(MEASURE_PROFILE) measurement profile -> $(MEASURE_OUT).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) run --profile $(MEASURE_PROFILE) --out $(MEASURE_OUT) $(MEASURE_SEED_ARG) $(MEASURE_EXTRA_FLAGS)

.PHONY: measure.run-quick
measure.run-quick: ## Run the quick profile (CI-grade, ~2-4 minutes).
	$(MAKE) measure.run MEASURE_PROFILE=quick MEASURE_OUT=$(MEASURE_RESULTS_DIR)/quick.jsonl

.PHONY: measure.run-standard
measure.run-standard: ## Run the standard profile (decision-grade, ~4-8 minutes).
	$(MAKE) measure.run MEASURE_PROFILE=standard MEASURE_OUT=$(MEASURE_RESULTS_DIR)/measurements.jsonl

.PHONY: measure.run-full
measure.run-full: ## Run the full profile (publication-grade, tens of minutes).
	$(MAKE) measure.run MEASURE_PROFILE=full MEASURE_OUT=$(MEASURE_RESULTS_DIR)/full.jsonl

.PHONY: measure.verify
measure.verify: ## Verify $(MEASURE_OUT) against the registry (coverage, schema, meta).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) verify --in $(MEASURE_OUT)

.PHONY: measure.report
measure.report: ## Generate the interactive HTML report (+ CSV exports) from $(MEASURE_OUT).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) report --in $(MEASURE_OUT) --out $(MEASURE_REPORT_OUT) --csv $(MEASURE_CSV_DIR)

.PHONY: measure.pipeline
measure.pipeline: measure.run measure.verify measure.report ## Run -> verify -> report in one go (uses MEASURE_PROFILE).

.PHONY: measure.ci
measure.ci: ## CI gate: quick profile, verified. Fails the build on any failure or missing coverage.
	$(MAKE) measure.run-quick
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) verify --in $(MEASURE_RESULTS_DIR)/quick.jsonl

.PHONY: measure.index
measure.index: ## Write results/manifest.json (SHA-256 index of every artifact; verifies JSONL files).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) index --dir $(MEASURE_RESULTS_DIR) --out $(MEASURE_RESULTS_DIR)/manifest.json

.PHONY: measure.clean
measure.clean: ## Measurement artifacts are committed under results/; this target is intentionally non-destructive.
	@echo "Measurement artifacts are committed under results/ (see results/README.md)."
	@echo "Regenerate with 'make measure', re-index with 'make measure-index', then commit results/ explicitly."
