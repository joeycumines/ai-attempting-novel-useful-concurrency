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
#   verify.arm64  Runs the tests of the implementations that ship arm64
#                 assembly (VERIFY_ARM64_RUN) as an arm64 binary. On an arm64
#                 host that is a plain `go test`; elsewhere it needs a
#                 user-mode emulator (VERIFY_ARM64_EXEC, default qemu-aarch64
#                 from PATH) and FAILS when there is none, because the whole
#                 point is that the assembly executes. Not part of
#                 `make verify` for that reason; `make measure-gates` runs it
#                 when the host is arm64 or an emulator is configured, and
#                 records it as not run otherwise.
#                 Under an emulator two things are set for the test binary.
#                 BIGBUFF_MEASURE_EXEC names the emulator, so that the
#                 measurement harness starts its measuring processes through
#                 it (the host cannot start an arm64 binary by itself).
#                 GODEBUG gets VERIFY_ARM64_GODEBUG, by default madvdontneed=0:
#                 qemu-user before 7.1 accepts madvise(MADV_DONTNEED) and does
#                 nothing, and the Go runtime relies on that call returning
#                 zeroed pages, so without the setting freshly allocated
#                 objects hold old contents and the binary fails in the
#                 runtime at random (seen with qemu 5.2.0 and go1.27.3). The
#                 setting makes the runtime release memory with MADV_FREE,
#                 after which it zeroes what it reuses. It does not touch the
#                 code under test.
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
#   VERIFY_ARM64_EXEC emulator used to run arm64 test binaries (default: qemu-aarch64 if on PATH)
#   VERIFY_ARM64_RUN  -run regexp for verify.arm64             (default: every top-level test; of the registry's subtests, the 64-GLM and 72-Gemini ones)
#   VERIFY_ARM64_GODEBUG  GODEBUG for the emulated arm64 binary (default: madvdontneed=0; see verify.arm64 above)
#
# Cross-architecture vet type-checks and assembles the per-architecture files;
# it does not execute them. verify.arm64 does.

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
VERIFY_ARM64_EXEC ?= $(shell command -v qemu-aarch64 2>/dev/null || command -v qemu-aarch64-static 2>/dev/null)
VERIFY_ARM64_GODEBUG ?= madvdontneed=0
VERIFY_ARM64_RUN ?= Beacon|Watcher|Bitmap72|PhaseCaster72|SymExchanger|TestScenarioBarrier|TestScenarioExchange|/.*(64-GLM|72-Gemini)

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
verify.arm64: ## Execute the tests of the arm64-assembly implementations as an arm64 binary (native, or under VERIFY_ARM64_EXEC).
	@$(VERIFY_SH) { \
	  if [ "$$(go env GOHOSTARCH)" = arm64 ]; then \
	    echo '== native arm64'; \
	    go test -count=1 -cpu $(VERIFY_CPUS) -run '$(VERIFY_ARM64_RUN)' -timeout $(VERIFY_TIMEOUT) .; \
	  elif [ -n '$(VERIFY_ARM64_EXEC)' ]; then \
	    echo '== GOARCH=arm64 under $(VERIFY_ARM64_EXEC)'; \
	    $(VERIFY_ARM64_EXEC) --version 2>/dev/null | head -n 1 | sed 's/^/== emulator: /'; \
	    echo '== GODEBUG=$(VERIFY_ARM64_GODEBUG) BIGBUFF_MEASURE_EXEC=$(VERIFY_ARM64_EXEC)'; \
	    GODEBUG='$(VERIFY_ARM64_GODEBUG)' BIGBUFF_MEASURE_EXEC='$(VERIFY_ARM64_EXEC)' GOARCH=arm64 go test -exec '$(VERIFY_ARM64_EXEC)' -count=1 -cpu $(VERIFY_CPUS) -run '$(VERIFY_ARM64_RUN)' -timeout $(VERIFY_TIMEOUT) .; \
	  else \
	    echo 'verify.arm64: this host is not arm64 and no emulator was found.'; \
	    echo 'verify.arm64: install qemu-user (qemu-aarch64) or set VERIFY_ARM64_EXEC=<emulator>.'; \
	    echo 'verify.arm64: FAILED (the arm64 assembly was not executed)'; \
	    exit 1; \
	  fi; \
	  echo 'verify.arm64: ok'; \
	} $(call verify_tee,arm64)

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
