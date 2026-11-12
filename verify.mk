# Usage
# ---
# Correctness and evaluation gates that `make all` does not run: build-tag and
# cross-architecture vet, multi-GOMAXPROCS tests, the race detector,
# benchmarks, fuzzing and allocation assertions.
#
# Module targets use the `verify.` prefix; the root Makefile exposes
# `make verify` (the gate) and `make verify-bench` (numbers, not a gate).
#
# Why each leg exists:
#   verify.vet    Plain `go vet` only type-checks the default build. Several
#                 implementations ship `_race.go` / `_fast.go` and per-GOARCH
#                 file pairs; a pair can rot unnoticed unless every variant is
#                 vetted. (This happened once: a race-only file stopped
#                 compiling and only `go vet -race` saw it.)
#   verify.test   Tests at GOMAXPROCS 1, 2 and 4. A single P turns "some other
#                 goroutine will run soon" assumptions into livelocks, which a
#                 2-core default run cannot show. (A reader that spun on a
#                 cell its writer needed was found exactly this way.)
#   verify.race   The whole suite under the race detector.
#   verify.alloc  The allocation assertions, verbosely, in the default build
#                 (the race build allocates by design in several places).
#   verify.fuzz   Every fuzz target for VERIFY_FUZZTIME each. Go runs one fuzz
#                 target per invocation, hence the loop.
#   verify.bench  Every benchmark matching VERIFY_BENCH. Produces numbers; it
#                 only fails if a benchmark fails or does not terminate.
#   verify.arm64  Executes the arm64 assembly: runs the tests as an arm64
#                 binary, every top-level test and, of the registry-driven
#                 subtests, those of the implementations that ship arm64
#                 assembly (VERIFY_ARM64_RUN). How the binary is run depends
#                 on the machine and is platform.mk's business: natively on an
#                 arm64 host of any operating system, otherwise as a
#                 linux/arm64 binary under a user-mode emulator or in a
#                 container (`make platform.info PLATFORM_GOARCH=arm64` says
#                 which). It FAILS when the machine has no way to do it,
#                 because the whole point is that the assembly executes. Not
#                 part of `make verify` for that reason; `make measure-gates`
#                 runs it when verify.arm64.probe succeeds and records why it
#                 did not otherwise. The log says whether the instructions ran
#                 on an arm64 CPU or were emulated, and the gates record
#                 repeats it.
#
# Configuration & Overrides
# ---
#   VERIFY_CPUS       GOMAXPROCS list for verify.test          (default: 1,2,4)
#   VERIFY_COUNT      -count for verify.test and verify.race   (default: 1)
#   VERIFY_TIMEOUT    go test -timeout for every leg           (default: 30m)
#   VERIFY_FUZZTIME   -fuzztime per fuzz target                (default: 10s)
#   VERIFY_FUZZ       regexp selecting fuzz targets            (default: ^Fuzz)
#   VERIFY_BENCH      regexp selecting benchmarks              (default: .)
#   VERIFY_BENCHTIME  -benchtime                               (default: 200x)
#   VERIFY_ARCHES     extra GOARCH values to vet               (default: arm64 386)
#   VERIFY_LOG        if set, a directory; each leg also writes <leg>.log there,
#                     and verify.all writes stamp.txt (commit, dirty-file
#                     count, start time) first. Keep it outside the repository.
#   VERIFY_ARM64_RUN  -run regexp for verify.arm64             (default: every top-level test, because the last alternative has an empty first element; every subtest of the tests the earlier alternatives name; of all other subtests, the 64-GLM and 72-Gemini ones)
#   VERIFY_ARM64_GOOS operating system of the arm64 binary     (default: the host's on an arm64 host, else linux)
#   VERIFY_ARM64_EXEC a user-mode arm64 emulator; shorthand for PLATFORM_RUNNER=qemu PLATFORM_QEMU=<emulator>
#   PLATFORM_*        everything else about how the binary is run (platform.mk): the runner, the
#                     emulator's environment, the container image
#
# Cross-architecture vet type-checks and assembles the per-architecture files;
# it does not execute them. verify.arm64 does, and `make platform.test
# PLATFORM_GOARCH=<arch>` does for any other architecture this machine can run.

# WARNING: Must be before any includes (same contract as the root Makefile).
VERIFY_MK := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_ROOT ?= $(patsubst %/,%,$(dir $(VERIFY_MK)))

VERIFY_CPUS ?= 1,2,4
VERIFY_COUNT ?= 1
VERIFY_TIMEOUT ?= 30m
VERIFY_FUZZTIME ?= 10s
VERIFY_FUZZ ?= ^Fuzz
VERIFY_BENCH ?= .
VERIFY_BENCHTIME ?= 200x
VERIFY_ARCHES ?= arm64 386
VERIFY_LOG ?=
VERIFY_ARM64_EXEC ?=
VERIFY_ARM64_GOOS ?= $(if $(filter arm64,$(PLATFORM_HOST_GOARCH)),$(PLATFORM_HOST_GOOS),linux)
VERIFY_ARM64_RUN ?= Beacon|Watcher|Bitmap72|PhaseCaster72|SymExchanger|TestScenarioBarrier|TestScenarioExchange|/.*(64-GLM|72-Gemini)
# What verify.arm64 asks of platform.mk.
VERIFY_ARM64_PLATFORM = PLATFORM_GOOS=$(VERIFY_ARM64_GOOS) PLATFORM_GOARCH=arm64 PLATFORM_CPUS='$(VERIFY_CPUS)' PLATFORM_TIMEOUT='$(VERIFY_TIMEOUT)' PLATFORM_RUN='$(VERIFY_ARM64_RUN)' $(if $(VERIFY_ARM64_EXEC),PLATFORM_RUNNER=qemu PLATFORM_QEMU='$(VERIFY_ARM64_EXEC)',)

# $(call verify_tee,<leg>) expands to a pipeline suffix that copies the leg's
# output into $(VERIFY_LOG)/<leg>.log when VERIFY_LOG is set. Recipes that use
# it run under `set -o pipefail` so the Go exit status still fails the leg.
verify_tee = $(if $(VERIFY_LOG),2>&1 | tee $(VERIFY_LOG)/$1.log,)
VERIFY_SH := set -e; set -o pipefail; $(if $(VERIFY_LOG),mkdir -p $(VERIFY_LOG);,) cd $(PROJECT_ROOT);

verify.%: SHELL := bash

.PHONY: verify.vet
verify.vet: ## go vet for the default build, the race build, and each of VERIFY_ARCHES.
	@$(VERIFY_SH) { \
	  echo '== go vet ./...'; go vet ./...; \
	  echo '== go vet -race ./...'; go vet -race ./...; \
	  for arch in $(VERIFY_ARCHES); do \
	    echo "== GOARCH=$$arch go vet ./..."; GOARCH=$$arch go vet ./...; \
	  done; \
	  echo 'verify.vet: ok'; \
	} $(call verify_tee,vet)

.PHONY: verify.test
verify.test: ## Full test suite at each GOMAXPROCS in VERIFY_CPUS.
	@$(VERIFY_SH) { \
	  echo '== go test -count=$(VERIFY_COUNT) -cpu $(VERIFY_CPUS) ./...'; \
	  go test -count=$(VERIFY_COUNT) -cpu $(VERIFY_CPUS) -timeout $(VERIFY_TIMEOUT) ./...; \
	} $(call verify_tee,test)

.PHONY: verify.race
verify.race: ## Full test suite under the race detector.
	@$(VERIFY_SH) { \
	  echo '== go test -race -count=$(VERIFY_COUNT) ./...'; \
	  go test -race -count=$(VERIFY_COUNT) -timeout $(VERIFY_TIMEOUT) ./...; \
	} $(call verify_tee,race)

.PHONY: verify.alloc
verify.alloc: ## Allocation assertions (default build), verbose.
	@$(VERIFY_SH) { \
	  echo "== go test -count=1 -run Alloc -v ./..."; \
	  go test -count=1 -run 'Alloc' -v -timeout $(VERIFY_TIMEOUT) ./...; \
	} $(call verify_tee,alloc)

.PHONY: verify.fuzz
verify.fuzz: ## Fuzz every target matching VERIFY_FUZZ for VERIFY_FUZZTIME each.
	@$(VERIFY_SH) { \
	  targets=$$(go test -list '$(VERIFY_FUZZ)' . | grep '^Fuzz'); \
	  test -n "$$targets" || { echo 'verify.fuzz: no fuzz targets match $(VERIFY_FUZZ)'; exit 1; }; \
	  echo "== go test -run=NONE -fuzz='^TARGET\$$' -fuzztime=$(VERIFY_FUZZTIME) -timeout $(VERIFY_TIMEOUT) . (once for each target)"; \
	  for t in $$targets; do \
	    echo "== $$t ($(VERIFY_FUZZTIME))"; \
	    go test -run=NONE -fuzz="^$$t\$$" -fuzztime=$(VERIFY_FUZZTIME) -timeout $(VERIFY_TIMEOUT) .; \
	  done; \
	  echo "verify.fuzz: ok ($$(echo $$targets | wc -w) targets)"; \
	} $(call verify_tee,fuzz)

.PHONY: verify.bench
verify.bench: ## Run benchmarks matching VERIFY_BENCH (numbers, not a gate).
	@$(VERIFY_SH) go test -run=NONE -bench '$(VERIFY_BENCH)' -benchtime=$(VERIFY_BENCHTIME) -benchmem -count=1 -timeout $(VERIFY_TIMEOUT) . $(call verify_tee,bench)

.PHONY: verify.arm64
verify.arm64: ## Execute the arm64 assembly: the tests of the implementations that ship it, as an arm64 binary, by whichever means this machine has (platform.mk).
	@$(VERIFY_SH) { \
	  if $(MAKE) --no-print-directory platform.test $(VERIFY_ARM64_PLATFORM); then \
	    echo 'verify.arm64: ok'; \
	  else \
	    echo 'verify.arm64: FAILED (see above; if no runner was found, the arm64 assembly was not executed)'; \
	    exit 1; \
	  fi; \
	} $(call verify_tee,arm64)

.PHONY: verify.arm64.probe
verify.arm64.probe: ## Succeeds when this machine can execute an arm64 test binary (see platform.probe).
	@$(MAKE) --no-print-directory platform.probe $(VERIFY_ARM64_PLATFORM)

# The stamp records which tree the gates ran on, at the moment they start, so
# that a record made from the logs later (bigbuff-measure gates) cannot
# attribute them to a different commit.
.PHONY: verify.stamp
verify.stamp:
	@$(VERIFY_SH) $(if $(VERIFY_LOG),{ \
	  echo "commit=$$(git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)"; \
	  echo "dirty=$$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')"; \
	  echo "started=$$(date -u +%Y-%m-%dT%H:%M:%SZ)"; \
	  echo "fuzztime=$(VERIFY_FUZZTIME)"; \
	  echo "cpus=$(VERIFY_CPUS)"; \
	} > $(VERIFY_LOG)/stamp.txt,true)

.PHONY: verify.all
verify.all: verify.stamp verify.vet verify.test verify.race verify.alloc verify.fuzz ## Every correctness gate (not benchmarks).
