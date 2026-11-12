# Usage
# ---
# Runs the test binary built for one platform (GOOS/GOARCH) on this machine,
# by whichever means this machine has. The goal is the platform; the means is
# a "runner", and the runner is chosen here so that the same command works on
# a Linux PC, an Apple Silicon Mac and an arm64 server:
#
#   make platform.test PLATFORM_GOARCH=arm64
#
# Module targets use the `platform.` prefix. Goal-level targets elsewhere
# (`make verify-arm64`, the arm64 leg of `make measure-gates`) are thin
# wrappers that set PLATFORM_* and call these.
#
# How it works: the test binary is cross-compiled on the host
# (`go test -c`, CGO off, so it is static), and then started through the
# runner with the repository as its working directory. Nothing is built
# inside a container or an emulator, so a runner needs no Go toolchain.
#
# Runners
# ---
#   native     The host can start the binary itself: same GOOS and GOARCH, or
#              a linux/386 binary on a linux/amd64 host.
#   qemu       A user-mode emulator (qemu-<arch> or qemu-<arch>-static) on a
#              Linux host, for a linux binary. The binary is started as
#              `<emulator> <binary>`, with PLATFORM_QEMU_ENV set.
#   container  Apple's `container` (macOS): a Linux VM. On Apple Silicon a
#              linux/arm64 binary then runs on the arm64 CPU itself.
#   docker, podman, nerdctl
#              A Linux container of the target architecture. Whether that is
#              the CPU or an emulator (binfmt_misc/qemu, Rosetta) depends on
#              the host; see "Execution" below.
#   custom     PLATFORM_EXEC is the whole command prefix: it is called as
#              `$(PLATFORM_EXEC) <binary> <test flags>` from the repository
#              root. For anything else: a remote arm64 machine over ssh, a
#              system emulator, another container tool.
#
# PLATFORM_RUNNER=auto (the default) takes the first runner of
# PLATFORM_RUNNERS that is available. `make platform.info` prints what was
# found, what was chosen and the exact command; `make platform.probe` starts
# the binary once without running a test and fails if the runner cannot
# execute it (for example a container engine with no emulation registered).
#
# Adding a runner: define `platform_available_<name>` (non-empty when it can
# be used) and `platform_prefix_<name>` (the command prefix), and add <name>
# to PLATFORM_RUNNERS. config.mk is the place for a machine-specific one.
#
# Execution: hardware or emulated
# ---
# What a run shows depends on whether the target's instructions ran on a CPU
# of that architecture. PLATFORM_EXECUTION says which, and every run prints
# it, because a gate record must not present an emulation as hardware:
#
#   hardware   the host CPU has the target architecture (a VM or a container
#              on such a CPU counts: the instructions and the memory ordering
#              are the CPU's)
#   emulated   it does not, so something translates the instructions; a
#              translator on a strongly ordered host cannot show a missing
#              memory fence
#   unknown    the custom runner, unless PLATFORM_EXECUTION is given
#
# The host CPU is read with `uname -m` (and `sysctl hw.optional.arm64` on
# macOS, where a translated shell reports x86_64). Set PLATFORM_HOST_CPU if
# that is wrong for your machine.
#
# What has been exercised
# ---
# On a linux/amd64 host: native; qemu (qemu-user 5.2.0, the whole arm64 leg
# of `make verify-arm64`, passing); custom; and the failure when no runner is
# available. Not on any other host: nothing here has run on macOS, on
# Windows, or on an arm64 CPU.
#
# The docker runner and Apple's `container` runner were exercised only
# against stand-in scripts that check the arguments and start the binary,
# because the machine this was written on has no container engine; the
# macOS host was simulated by setting PLATFORM_HOST_GOOS and
# PLATFORM_HOST_CPU. podman and nerdctl share docker's code path and were
# not run at all. Treat the engines' flag sets (PLATFORM_ENGINE_RUN_FLAGS,
# PLATFORM_CONTAINER_RUN_FLAGS) as a starting point, run
# `make platform.probe` first, and override them if your engine differs.
#
# Requirements: GNU Make 4, bash with the usual POSIX tools, and Go. On
# Windows use WSL (which is Linux) or MSYS2.
#
# Configuration & Overrides
# ---
#   PLATFORM_GOOS, PLATFORM_GOARCH  target platform            (default: the host's)
#   PLATFORM_GOENV       extra environment for the build, e.g. GOARM64=v8.1
#   PLATFORM_RUNNER      auto, or one runner name              (default: auto)
#   PLATFORM_RUNNERS     the order auto tries                  (default: native qemu container docker podman nerdctl)
#   PLATFORM_RUN         -test.run regexp                      (default: .)
#   PLATFORM_CPUS        -test.cpu list                        (default: 1,2,4)
#   PLATFORM_COUNT       -test.count                           (default: 1)
#   PLATFORM_TIMEOUT     -test.timeout                         (default: 30m)
#   PLATFORM_TEST_FLAGS  further flags for the test binary     (e.g. -test.v)
#   PLATFORM_ENV         KEY=VALUE words set for the test binary in every runner
#   PLATFORM_BUILD_DIR   where the binary is built             (default: $TMPDIR/bigbuff-platform; outside the repository)
#   PLATFORM_BUILD_FLAGS further flags for `go test -c`
#   PLATFORM_QEMU        the user-mode emulator                (default: qemu-<arch> or qemu-<arch>-static from PATH)
#   PLATFORM_QEMU_ENV    environment for the emulated binary   (default: GODEBUG=madvdontneed=0 BIGBUFF_MEASURE_EXEC=<emulator>)
#                        qemu-user before 7.1 accepts madvise(MADV_DONTNEED)
#                        and does nothing, and the Go runtime relies on that
#                        call returning zeroed pages, so without the GODEBUG
#                        setting the binary fails in the runtime at random
#                        (seen with qemu 5.2.0 and go1.27.3). The setting
#                        makes the runtime release memory with MADV_FREE and
#                        zero what it reuses; it does not touch the code under
#                        test. BIGBUFF_MEASURE_EXEC makes the measurement
#                        harness start its child processes through the
#                        emulator, which the host cannot do by itself.
#   PLATFORM_IMAGE       container image                       (default: golang:<host Go version>)
#                        It needs `env`, and `git` for the tests that read the
#                        repository's state; the binary itself is static.
#   PLATFORM_ENGINE_RUN_FLAGS     flags of `docker|podman|nerdctl run`
#   PLATFORM_CONTAINER_RUN_FLAGS  flags of Apple's `container run`
#   PLATFORM_EXEC        command prefix of the custom runner
#   PLATFORM_EXECUTION   hardware | emulated | unknown         (default: derived, see above)
#   PLATFORM_HOST_CPU    the host CPU's architecture, as a GOARCH value

# WARNING: Must be before any includes (same contract as the root Makefile).
PLATFORM_MK := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_ROOT ?= $(patsubst %/,%,$(dir $(PLATFORM_MK)))

# The host is looked at the first time one of these is used and never again,
# instead of on every make invocation whether or not a platform target was
# asked for: each is a recursive variable that replaces itself with a simple
# one on first expansion.
ifeq ($(origin PLATFORM_HOST_GOOS),undefined)
PLATFORM_HOST_GOOS = $(eval PLATFORM_HOST_GOOS := $$(shell go env GOHOSTOS))$(PLATFORM_HOST_GOOS)
endif
ifeq ($(origin PLATFORM_HOST_GOARCH),undefined)
PLATFORM_HOST_GOARCH = $(eval PLATFORM_HOST_GOARCH := $$(shell go env GOHOSTARCH))$(PLATFORM_HOST_GOARCH)
endif
# The CPU, as a GOARCH value. GOHOSTARCH is the architecture of the Go
# toolchain, which is not the CPU's when the toolchain itself is translated.
define platform_host_cpu_script
m=$$(uname -m 2>/dev/null); \
if [ "$$(uname -s 2>/dev/null)" = Darwin ] && [ "$$(sysctl -n hw.optional.arm64 2>/dev/null)" = 1 ]; then m=arm64; fi; \
case "$$m" in \
  (x86_64|amd64) echo amd64;; \
  (aarch64|arm64) echo arm64;; \
  (i?86) echo 386;; \
  (armv*) echo arm;; \
  (loongarch64) echo loong64;; \
  ("") go env GOHOSTARCH;; \
  (*) echo "$$m";; \
esac
endef
ifeq ($(origin PLATFORM_HOST_CPU),undefined)
PLATFORM_HOST_CPU = $(eval PLATFORM_HOST_CPU := $$(shell $$(platform_host_cpu_script)))$(PLATFORM_HOST_CPU)
endif

PLATFORM_GOOS ?= $(PLATFORM_HOST_GOOS)
PLATFORM_GOARCH ?= $(PLATFORM_HOST_GOARCH)
PLATFORM_GOENV ?=
PLATFORM_RUNNER ?= auto
PLATFORM_RUNNERS ?= native qemu container docker podman nerdctl
PLATFORM_RUN ?= .
PLATFORM_CPUS ?= 1,2,4
PLATFORM_COUNT ?= 1
PLATFORM_TIMEOUT ?= 30m
PLATFORM_TEST_FLAGS ?=
PLATFORM_ENV ?=
PLATFORM_BUILD_DIR ?= $(or $(TMPDIR),/tmp)/bigbuff-platform
PLATFORM_BUILD_FLAGS ?=
PLATFORM_EXEC ?=

PLATFORM_BIN = $(patsubst %/,%,$(PLATFORM_BUILD_DIR))/bigbuff-$(PLATFORM_GOOS)-$(PLATFORM_GOARCH).test$(if $(filter windows,$(PLATFORM_GOOS)),.exe,)

# -- the host CPU and the target ------------------------------------------

# Non-empty when the host CPU executes the target's instructions itself.
platform_same_cpu = $(or $(filter $(PLATFORM_GOARCH),$(PLATFORM_HOST_CPU)),$(and $(filter amd64,$(PLATFORM_HOST_CPU)),$(filter 386,$(PLATFORM_GOARCH))))

# -- runner: native ---------------------------------------------------------

platform_available_native = $(and $(filter $(PLATFORM_GOOS),$(PLATFORM_HOST_GOOS)),$(or $(filter $(PLATFORM_GOARCH),$(PLATFORM_HOST_GOARCH)),$(and $(filter linux,$(PLATFORM_HOST_GOOS)),$(filter amd64,$(PLATFORM_HOST_GOARCH)),$(filter 386,$(PLATFORM_GOARCH)))))
platform_prefix_native = $(if $(strip $(PLATFORM_ENV)),env $(PLATFORM_ENV),)
platform_describe_native = the host starts the binary

# -- runner: qemu (user mode) ----------------------------------------------

# qemu names architectures its own way.
platform_qemu_arch_386 := i386
platform_qemu_arch_amd64 := x86_64
platform_qemu_arch_arm := arm
platform_qemu_arch_arm64 := aarch64
platform_qemu_arch_loong64 := loongarch64
platform_qemu_arch_mips := mips
platform_qemu_arch_mipsle := mipsel
platform_qemu_arch_mips64 := mips64
platform_qemu_arch_mips64le := mips64el
platform_qemu_arch_ppc64 := ppc64
platform_qemu_arch_ppc64le := ppc64le
platform_qemu_arch_riscv64 := riscv64
platform_qemu_arch_s390x := s390x
platform_qemu_name = qemu-$(or $(platform_qemu_arch_$(PLATFORM_GOARCH)),$(PLATFORM_GOARCH))

ifeq ($(origin PLATFORM_QEMU),undefined)
PLATFORM_QEMU = $(shell command -v $(platform_qemu_name) 2>/dev/null || command -v $(platform_qemu_name)-static 2>/dev/null)
endif
PLATFORM_QEMU_ENV ?= GODEBUG=madvdontneed=0 BIGBUFF_MEASURE_EXEC=$(PLATFORM_QEMU)

platform_available_qemu = $(and $(filter linux,$(PLATFORM_HOST_GOOS)),$(filter linux,$(PLATFORM_GOOS)),$(PLATFORM_QEMU))
platform_prefix_qemu = env $(PLATFORM_ENV) $(PLATFORM_QEMU_ENV) $(PLATFORM_QEMU)
platform_describe_qemu = $(PLATFORM_QEMU) ($(shell $(PLATFORM_QEMU) --version 2>/dev/null | head -n 1))

# -- runners: docker, podman, nerdctl ---------------------------------------

ifeq ($(origin PLATFORM_IMAGE),undefined)
PLATFORM_IMAGE = golang:$(patsubst go%,%,$(shell go env GOVERSION))
endif

# The repository is mounted at its own path, so that every path the harness
# prints means the same inside and outside, and the binary's directory beside
# it, read-only.
PLATFORM_ENGINE_RUN_FLAGS ?= --rm --platform $(PLATFORM_GOOS)/$(PLATFORM_GOARCH) -v $(PROJECT_ROOT):$(PROJECT_ROOT) -v $(patsubst %/,%,$(PLATFORM_BUILD_DIR)):$(patsubst %/,%,$(PLATFORM_BUILD_DIR)):ro -w $(PROJECT_ROOT) $(foreach e,$(PLATFORM_ENV),-e $(e))

define platform_engine
platform_available_$1 = $$(and $$(filter linux,$$(PLATFORM_GOOS)),$$(shell command -v $1 2>/dev/null))
platform_prefix_$1 = $1 run $$(PLATFORM_ENGINE_RUN_FLAGS) $$(PLATFORM_IMAGE)
platform_describe_$1 = $$(shell $1 --version 2>/dev/null | head -n 1), image $$(PLATFORM_IMAGE)
endef
$(foreach engine,docker podman nerdctl,$(eval $(call platform_engine,$(engine))))

# -- runner: container (Apple) ----------------------------------------------

PLATFORM_CONTAINER_RUN_FLAGS ?= --rm --arch $(PLATFORM_GOARCH) --volume $(PROJECT_ROOT):$(PROJECT_ROOT) --volume $(patsubst %/,%,$(PLATFORM_BUILD_DIR)):$(patsubst %/,%,$(PLATFORM_BUILD_DIR)) --workdir $(PROJECT_ROOT) $(foreach e,$(PLATFORM_ENV),--env $(e))

platform_available_container = $(and $(filter darwin,$(PLATFORM_HOST_GOOS)),$(filter linux,$(PLATFORM_GOOS)),$(shell command -v container 2>/dev/null))
platform_prefix_container = container run $(PLATFORM_CONTAINER_RUN_FLAGS) $(PLATFORM_IMAGE)
platform_describe_container = $(shell container --version 2>/dev/null | head -n 1), image $(PLATFORM_IMAGE)

# -- runner: custom ---------------------------------------------------------

platform_available_custom = $(strip $(PLATFORM_EXEC))
platform_prefix_custom = $(PLATFORM_EXEC)
platform_describe_custom = $(PLATFORM_EXEC)

# -- choosing ---------------------------------------------------------------

platform_available = $(if $(strip $(platform_available_$1)),yes,)
PLATFORM_RUNNER_CHOSEN = $(if $(filter auto,$(PLATFORM_RUNNER)),$(firstword $(foreach r,$(PLATFORM_RUNNERS),$(if $(call platform_available,$(r)),$(r)))),$(PLATFORM_RUNNER))
PLATFORM_PREFIX = $(strip $(platform_prefix_$(PLATFORM_RUNNER_CHOSEN)))
PLATFORM_EXECUTION ?= $(if $(filter custom,$(PLATFORM_RUNNER_CHOSEN)),unknown,$(if $(platform_same_cpu),hardware,emulated))
PLATFORM_COMMAND = $(strip $(PLATFORM_PREFIX) $(PLATFORM_BIN) -test.count=$(PLATFORM_COUNT) -test.cpu $(PLATFORM_CPUS) -test.timeout $(PLATFORM_TIMEOUT) $(PLATFORM_TEST_FLAGS) -test.run '$(PLATFORM_RUN)')

# Fails with what to do when no runner can be used.
define PLATFORM_REQUIRE_RUNNER
if [ -z '$(PLATFORM_RUNNER_CHOSEN)' ]; then \
  echo 'platform: no runner on this machine can execute a $(PLATFORM_GOOS)/$(PLATFORM_GOARCH) binary (host $(PLATFORM_HOST_GOOS)/$(PLATFORM_HOST_GOARCH), tried: $(PLATFORM_RUNNERS)).'; \
  echo 'platform: install a user-mode qemu ($(platform_qemu_name)) on Linux, or a container engine (docker, podman, nerdctl, Apple container), or set PLATFORM_RUNNER=custom PLATFORM_EXEC=<command>. See platform.mk.'; \
  exit 1; \
fi; \
if [ -z '$(call platform_available,$(PLATFORM_RUNNER_CHOSEN))' ]; then \
  echo 'platform: runner "$(PLATFORM_RUNNER_CHOSEN)" is not available on this machine for $(PLATFORM_GOOS)/$(PLATFORM_GOARCH). `make platform.info` lists what is.'; \
  exit 1; \
fi
endef

platform.%: SHELL := bash

.PHONY: platform.info
platform.info: ## Print the target platform, the runners found on this machine, the one chosen and the command it gives.
	@echo 'target:    $(PLATFORM_GOOS)/$(PLATFORM_GOARCH)'
	@echo 'host:      $(PLATFORM_HOST_GOOS)/$(PLATFORM_HOST_GOARCH), CPU $(PLATFORM_HOST_CPU)'
	@$(foreach r,$(sort $(PLATFORM_RUNNERS) custom),echo 'runner:    $(r): $(if $(call platform_available,$(r)),available ($(platform_describe_$(r))),not available)';)
	@echo 'chosen:    $(or $(PLATFORM_RUNNER_CHOSEN),none) (PLATFORM_RUNNER=$(PLATFORM_RUNNER))'
	@echo 'execution: $(if $(PLATFORM_RUNNER_CHOSEN),$(PLATFORM_EXECUTION),none)'
	@echo 'command:   $(if $(PLATFORM_RUNNER_CHOSEN),$(subst ','\'',$(PLATFORM_COMMAND)),none)'

.PHONY: platform.runner
platform.runner: ## Print the name of the runner that would be used; nothing, and a non-zero exit, when there is none.
	@test -n '$(PLATFORM_RUNNER_CHOSEN)' && test -n '$(call platform_available,$(PLATFORM_RUNNER_CHOSEN))' && echo '$(PLATFORM_RUNNER_CHOSEN)'

.PHONY: platform.build
platform.build: ## Cross-compile the test binary for PLATFORM_GOOS/PLATFORM_GOARCH into PLATFORM_BUILD_DIR.
	@set -e; mkdir -p '$(PLATFORM_BUILD_DIR)'; cd '$(PROJECT_ROOT)'; \
	echo '== CGO_ENABLED=0 GOOS=$(PLATFORM_GOOS) GOARCH=$(PLATFORM_GOARCH) $(PLATFORM_GOENV) go test -c $(PLATFORM_BUILD_FLAGS)'; \
	env CGO_ENABLED=0 GOOS=$(PLATFORM_GOOS) GOARCH=$(PLATFORM_GOARCH) $(PLATFORM_GOENV) go test -c -o '$(PLATFORM_BIN)' $(PLATFORM_BUILD_FLAGS) .

.PHONY: platform.probe
platform.probe: platform.build ## Start the binary once through the runner without running a test; fails when the runner cannot execute it.
	@set -e; $(PLATFORM_REQUIRE_RUNNER); cd '$(PROJECT_ROOT)'; \
	if $(PLATFORM_PREFIX) $(PLATFORM_BIN) -test.run '^$$' >/dev/null 2>&1; then \
	  echo 'platform.probe: ok ($(PLATFORM_GOOS)/$(PLATFORM_GOARCH), runner $(PLATFORM_RUNNER_CHOSEN), $(PLATFORM_EXECUTION))'; \
	else \
	  echo 'platform.probe: runner "$(PLATFORM_RUNNER_CHOSEN)" could not execute a $(PLATFORM_GOOS)/$(PLATFORM_GOARCH) binary on this machine.'; \
	  echo 'platform.probe: tried: $(subst ','\'',$(PLATFORM_PREFIX) $(PLATFORM_BIN)) -test.run "^$$"'; \
	  exit 1; \
	fi

.PHONY: platform.test
platform.test: platform.build ## Run the tests matching PLATFORM_RUN as a PLATFORM_GOOS/PLATFORM_GOARCH binary, through the chosen runner.
	@set -e; $(PLATFORM_REQUIRE_RUNNER); cd '$(PROJECT_ROOT)'; \
	echo '== platform: $(PLATFORM_GOOS)/$(PLATFORM_GOARCH) on host $(PLATFORM_HOST_GOOS)/$(PLATFORM_HOST_GOARCH), CPU $(PLATFORM_HOST_CPU)'; \
	echo '== runner: $(PLATFORM_RUNNER_CHOSEN): $(subst ','\'',$(platform_describe_$(PLATFORM_RUNNER_CHOSEN)))'; \
	echo '== execution: $(PLATFORM_EXECUTION)'; \
	echo '== $(subst ','\'',$(PLATFORM_COMMAND))'; \
	started=$$(date +%s); \
	$(PLATFORM_COMMAND); \
	echo "platform.test: ok ($(PLATFORM_GOOS)/$(PLATFORM_GOARCH), runner $(PLATFORM_RUNNER_CHOSEN), $(PLATFORM_EXECUTION), $$(( $$(date +%s) - started )) s)"

.PHONY: platform.clean
platform.clean: ## Remove the binaries platform.build made.
	rm -f '$(patsubst %/,%,$(PLATFORM_BUILD_DIR))'/bigbuff-*.test '$(patsubst %/,%,$(PLATFORM_BUILD_DIR))'/bigbuff-*.test.exe
