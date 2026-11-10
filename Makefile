# Usage
# ---
# This Makefile is a local development task runner, wrapping Go tooling and
# modular makefiles into composable, standard targets.
#
# Structure / Code Organization
# ---
# Makefiles consist of rules matching targets associated with recipes (shell scripts).
# GNU Make is used primarily as a task/command runner.
#
# In order to keep Makefiles modular, manageable, and composable, tool-specific
# rules are isolated into module Makefiles (such as `go.mk`). Because this
# repository must maintain a flat directory structure to sync with a GitHub
# gist, all modular Makefiles reside directly in the repository root.
#
# Module targets use a prefix (e.g. `go.`) to avoid target collisions, and are
# then composed into standard GNU targets in this root Makefile.
#
# Dependencies
# ---
# - Modern GNU Make (>= 4.0). macOS default Make (3.81) is incompatible;
#   use `gmake` (via `brew install make`).
# - Go toolchain (1.24+).
#
# Configuration & Overrides
# ---
# Behavior is configurable via environment variables, command-line arguments,
# and user-specific overrides in an uncommitted `config.mk` file. Defaults are
# established with `?=` and compose cleanly (e.g. `EXTRA_GO_TEST_FLAGS` appends
# to `GO_TEST_FLAGS` without clobbering `GO_TEST_TIMEOUT`).
#
# Documentation / Command Help
# ---
# Available targets and descriptions can be inspected via `make help` or `make h`.
#
# Resources on Makefiles
# ---
# 1. GNU Make documentation: https://www.gnu.org/software/make/manual/make.html
# 2. GNU Standard Targets: https://www.gnu.org/software/make/manual/html_node/Standard-Targets.html

ifneq ($(filter 3.%,$(MAKE_VERSION)),)
$(error GNU Make version 4.x or higher is required. You are using version $(MAKE_VERSION). Please upgrade your Make installation.)
endif

# WARNING: Must be before any includes.
ROOT_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_ROOT := $(patsubst %/,%,$(dir $(ROOT_MAKEFILE)))

.DEFAULT_GOAL := all

# -- START OF INTENDED CONFIGURATION --

# Configure the Go test timeout.
GO_TEST_TIMEOUT ?= 120m
# Additional flags to pass to go test (e.g. -v, -race, -count=1, -run).
EXTRA_GO_TEST_FLAGS ?=
# Or override the entire set of flags.
GO_TEST_FLAGS ?= -timeout=$(GO_TEST_TIMEOUT) $(EXTRA_GO_TEST_FLAGS)

# Additional flags to pass to the go toolchain across all commands.
EXTRA_GO_FLAGS ?=
GO_FLAGS ?= $(EXTRA_GO_FLAGS)

# Default Go package pattern.
GO_PACKAGES ?= ./...

# Don't depend on staticcheck mainly just to avoid any external dependencies for this project.
GO_MODULE_SLUGS_NO_STATICCHECK ?= $(GO_MODULE_SLUGS)
GO_MODULE_SLUGS_NO_BETTERALIGN ?= $(GO_MODULE_SLUGS)

# -- END OF INTENDED CONFIGURATION --

# For go.mk prefixes all targets except help with `go.`.
GO_TARGET_PREFIX := go.

# User-specific overrides and extensions.
-include $(PROJECT_ROOT)/config.mk

# Go module makefile (lives at flat root per gist constraints).
include $(PROJECT_ROOT)/go.mk

# Measurement batteries makefile (lives at flat root per gist constraints).
include $(PROJECT_ROOT)/measure.mk

# Landing page & report wire-up makefile (lives at flat root per gist constraints).
include $(PROJECT_ROOT)/site.mk

# Race / multi-GOMAXPROCS / fuzz / allocation / benchmark gates (lives at flat root per gist constraints).
include $(PROJECT_ROOT)/verify.mk

##@ Standard Targets
# https://www.gnu.org/software/make/manual/html_node/Standard-Targets.html

.PHONY: all
all: go.all site.build ## Builds all packages, runs linters, executes the test suite, and builds/wires the site.

.PHONY: check
check: all site.check ## Runs all checks (alias for all).

.PHONY: clean
clean: _clean go.clean ## Clean up all generated files and coverage reports.
.PHONY: _clean
_clean: measure.clean site.clean

##@ Other Targets

.PHONY: site
site: site.build ## Build/update landing page and wire report links and backlinks.

.PHONY: wire-reports
wire-reports: site.wire ## Wire up report links and backlinks across all reports.

.PHONY: build
build: go.build ## Compile all packages.

.PHONY: test
test: go.test ## Run unit and integration tests.

.PHONY: lint
lint: go.lint ## Run linters and vet checks.

.PHONY: vet
vet: go.vet ## Run go vet.

.PHONY: fmt
fmt: go.fmt ## Format all Go code via go fmt.

.PHONY: tidy
tidy: go.tidy ## Tidy go.mod dependencies.

.PHONY: update
update: go.update ## Update dependencies and tidy go.mod.

.PHONY: cover
cover: go.cover ## Run tests with coverage and generate coverage report.

.PHONY: doc
doc: go.doc ## Run go doc.

##@ Measurement Targets
# Batteries, verification and the interactive report. See measurement.md.

.PHONY: measure
measure: measure.pipeline ## Run the standard measurement profile, verify, and build the HTML report.

.PHONY: measure-quick
measure-quick: measure.run-quick ## Run the quick (CI-grade) measurement profile.

.PHONY: measure-standard
measure-standard: measure.run-standard ## Run the standard (decision-grade) measurement profile.

.PHONY: measure-full
measure-full: measure.run-full ## Run the full (publication-grade) measurement profile.

.PHONY: measure-list
measure-list: measure.list ## List measurement batteries, profiles and families.

.PHONY: measure-verify
measure-verify: measure.verify ## Verify a measurement results file against the registry.

.PHONY: measure-report
measure-report: measure.report ## Rebuild the interactive HTML report + CSVs from results.

.PHONY: measure-registry
measure-registry: measure.registry ## Dump the scenario/attachment registry to JSON.

.PHONY: measure-index
measure-index: measure.index ## Rebuild every results-NN-00-manifest.json (SHA-256 index of committed run groups).

.PHONY: measure-ci
measure-ci: measure.ci ## CI gate: quick profile + verification.

.PHONY: verify
verify: verify.all ## Correctness gates beyond `make all`: vet (race + cross-arch), tests at several GOMAXPROCS, race detector, allocation assertions, fuzzing.

.PHONY: verify-bench
verify-bench: verify.bench ## Run the Go benchmarks (VERIFY_BENCH=<regexp>, VERIFY_BENCHTIME=<n>x).
