# Usage
# ---
# Measurement batteries for every implementation in this repository, wired into
# GNU Make. The batteries run through the Go test binary as a subcommand binary
# (`go test -run=NONE -args bigbuff-measure …`), so plain `go test` keeps
# working untouched (see measurement.md for the full contract).
#
# The repository is FLAT (AGENTS.md): results artifacts live at the repository
# root as `results-<rr>-<ss>-<slug>.<ext>` with zero-padded numeric segments:
#
#   rr  run sequence (00 = documentation group, 01.. = runs)
#   ss  slot within the run (00 = manifest, 01 = JSONL results, 02 = registry
#       dump, 03 = HTML report, 04 = metrics CSV, 05 = scores CSV, 06 = gates
#       record, 07 = generated evaluation)
#
# Each run is self-describing: results-NN-00-manifest.json carries the full
# environment fingerprint (machine, OS, Go toolchain, git commit, profile,
# seed) plus per-file SHA-256, record histograms and verification verdicts.
# Enumerating the flat tree: iterate `results-*-*-*` in sorted order; the
# manifest is always the first file of a run group.
#
# Module targets here use the `measure.` prefix. The root Makefile composes
# them into ergonomic entry points (`make measure`, `make measure-quick`,
# `make measure-report`, …).
#
# Configuration & Overrides
# ---
#   MEASURE_RUN          run sequence for the next invocation (default: auto,
#                        i.e. highest existing results-NN-* + 1, else 1)
#   MEASURE_PROFILE      which profile `make measure` runs (default: standard)
#   MEASURE_SEED         optional seed override forwarded to the harness
#   MEASURE_TIMEOUT      go test -timeout for measurement runs (default: $(GO_TEST_TIMEOUT))
#   MEASURE_EXTRA_FLAGS  additional flags appended to the harness invocation
#   MEASURE_GATES_LOG    directory `make measure-gates` has `make verify` log to
#                        (default: $${TMPDIR:-/tmp}/bigbuff-verify-log; it must be
#                        outside the repository, or the tree counts as dirty)
#
# Everything is overridable on the command line, e.g.
#   make measure MEASURE_PROFILE=full MEASURE_TIMEOUT=180m MEASURE_RUN=3

# WARNING: Must be before any includes (same contract as the root Makefile).
MEASURE_MK := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_ROOT ?= $(patsubst %/,%,$(dir $(MEASURE_MK)))

MEASURE_PROFILE ?= standard
MEASURE_SEED ?=
MEASURE_TIMEOUT ?= $(GO_TEST_TIMEOUT)
MEASURE_EXTRA_FLAGS ?=
MEASURE_SEED_ARG := $(if $(MEASURE_SEED),--seed $(MEASURE_SEED),)
MEASURE_GATES_LOG ?= $(or $(TMPDIR),/tmp)/bigbuff-verify-log

# Next run sequence: max existing rr prefix + 1 (or 1 when none exist).
MEASURE_RUN ?= auto
ifeq ($(MEASURE_RUN),auto)
MEASURE_RUN_LAST := $(shell ls $(PROJECT_ROOT)/results-*-*.* 2>/dev/null | sed -E 's/^.*results-([0-9]+)-.*/\1/' | sort -n | uniq | tail -1)
MEASURE_RUN := $(if $(MEASURE_RUN_LAST),$(shell expr $(MEASURE_RUN_LAST) + 1),1)
endif
MEASURE_NN := $(shell printf '%02d' $(MEASURE_RUN))
MEASURE_PREFIX := results-$(MEASURE_NN)
MEASURE_JSONL := $(MEASURE_PREFIX)-01-results.jsonl
MEASURE_REGISTRY := $(MEASURE_PREFIX)-02-registry.json

MEASURE_HARNESS := go test -run=NONE -count=1 -timeout=$(MEASURE_TIMEOUT) -args bigbuff-measure
# Newest committed/produced results JSONL (verify/report default to this when
# MEASURE_IN is unset).
MEASURE_LATEST_JSONL := $(shell ls $(PROJECT_ROOT)/results-*-01-results.jsonl 2>/dev/null | sort | tail -1)
REPORT_IN := $(if $(MEASURE_IN),$(MEASURE_IN),$(MEASURE_LATEST_JSONL))
REPORT_NN := $(shell echo '$(notdir $(REPORT_IN))' | sed -n -E 's/^results-([0-9]{2})-01-results\.jsonl$$/\1/p')
REPORT_STEM := $(if $(REPORT_NN),results-$(REPORT_NN),$(basename $(REPORT_IN)))
MEASURE_METRICS_CSV := $(REPORT_STEM)-04-metrics_wide.csv
MEASURE_SCORES_CSV := $(REPORT_STEM)-05-scores.csv

.PHONY: measure.list
measure.list: ## List all measured implementation batteries, profiles and registry families.
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) list

.PHONY: measure.registry
measure.registry: ## Dump the scenario/case/attachment registry to JSON (slot 02 of run $(MEASURE_NN)).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) dump-registry --out $(MEASURE_REGISTRY)

.PHONY: measure.run
measure.run: ## Run the $(MEASURE_PROFILE) measurement profile as run $(MEASURE_NN) ($(MEASURE_JSONL)).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) run --profile $(MEASURE_PROFILE) --out $(MEASURE_JSONL) $(MEASURE_SEED_ARG) $(MEASURE_EXTRA_FLAGS)

.PHONY: measure.run-quick
measure.run-quick: ## Run the quick profile (CI-grade, ~3 minutes).
	$(MAKE) measure.run MEASURE_PROFILE=quick

.PHONY: measure.run-standard
measure.run-standard: ## Run the standard profile (decision-grade, ~30-60 minutes on 2 vCPUs).
	$(MAKE) measure.run MEASURE_PROFILE=standard

.PHONY: measure.run-full
measure.run-full: ## Run the full profile (publication-grade, hours on 2 vCPUs).
	$(MAKE) measure.run MEASURE_PROFILE=full

.PHONY: measure.verify
measure.verify: ## Verify the newest results-*-01-results.jsonl against the registry (or MEASURE_IN=<file>).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) verify $(if $(MEASURE_IN),--in $(MEASURE_IN),)

.PHONY: measure.report
measure.report: ## Build/refresh the report (+ CSVs) for the newest results JSONL (or MEASURE_IN=<file>).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) report $(if $(MEASURE_IN),--in $(MEASURE_IN),) --csv-metrics $(MEASURE_METRICS_CSV) --csv-scores $(MEASURE_SCORES_CSV)
	$(MAKE) site.build

.PHONY: measure.index
measure.index: ## Rebuild every results-NN-00-manifest.json; verifies each run against its own registry dump (non-zero if one is incomplete).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) index --dir .

.PHONY: measure.pipeline
measure.pipeline: ## New run in group $(MEASURE_NN): measure -> registry -> report -> site -> index (uses MEASURE_PROFILE).
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) run --profile $(MEASURE_PROFILE) --out $(MEASURE_JSONL) $(MEASURE_SEED_ARG) $(MEASURE_EXTRA_FLAGS)
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) dump-registry --out $(MEASURE_REGISTRY)
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) report --in $(MEASURE_JSONL) --csv-metrics $(MEASURE_PREFIX)-04-metrics_wide.csv --csv-scores $(MEASURE_PREFIX)-05-scores.csv
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) evaluate --in $(MEASURE_JSONL)
	$(MAKE) site.build

.PHONY: measure.evaluate
measure.evaluate: ## Regenerate the evaluation (slot 07) of the newest run (or MEASURE_IN=<file>) from its own files.
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) evaluate $(if $(MEASURE_IN),--in $(MEASURE_IN),)
	$(MAKE) site.build

# The gates are recorded whether or not they pass: a failing leg is written
# into the record, and this target then fails.
.PHONY: measure.gates
measure.gates: SHELL := bash
measure.gates: ## Run `make verify`, record what it reported as slot 06 of the newest run, and regenerate that run's evaluation.
	rm -rf $(MEASURE_GATES_LOG) && mkdir -p $(MEASURE_GATES_LOG)
	cd $(PROJECT_ROOT) && { $(MAKE) --keep-going verify.all VERIFY_LOG=$(MEASURE_GATES_LOG) || true; } && \
	  $(MEASURE_HARNESS) gates --log $(MEASURE_GATES_LOG) --out $(REPORT_STEM)-06-gates.json; rc=$$?; \
	  $(MEASURE_HARNESS) evaluate --in $(REPORT_IN) && $(MAKE) site.build && exit $$rc

.PHONY: measure.ci
measure.ci: ## CI gate: quick profile, verified. Fails the build on any failure or missing coverage.
	cd $(PROJECT_ROOT) && $(MEASURE_HARNESS) run --profile quick --out results-ci-gate-01-results.jsonl $(MEASURE_SEED_ARG) && $(MEASURE_HARNESS) verify --in results-ci-gate-01-results.jsonl; rc=$$?; rm -f results-ci-gate-01-results.jsonl; exit $$rc

.PHONY: measure.clean
measure.clean: ## Measurement artifacts are committed at the repository root; this target is intentionally non-destructive.
	@echo "Measurement artifacts are committed at the repository root (see results-00-00-readme.md)."
	@echo "Regenerate with 'make measure', re-index with 'make measure-index', then commit 'results-*-*.*' explicitly."
