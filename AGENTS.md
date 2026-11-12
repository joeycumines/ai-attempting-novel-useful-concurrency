# Guidance for agents working in this repository

This file records only judgement, policy and rationale that cannot be
discovered by reading the code. It deliberately contains no catalogue of the
code under test, no API documentation and nothing that duplicates another
source of truth; if a fact can be found in the code, keep it there, not here.

## Layout constraint

The project is flat and must stay flat: every file lives at the repository
root. This is intentional — the repository is kept in sync with a gist that
was published from this layout. Do not introduce packages, subdirectories or
a module restructure, however tidier they might seem.

## Make and build

Build and automation tooling must use modular, composable GNU Make (4.0+) following best practices: all module makefiles (such as `go.mk`) must reside directly at the repository root due to the flat-gist constraint, use target prefixes (e.g., `go.`) to avoid collisions, and compose into root standard GNU targets (`all`, `check`, `clean`).
The root Makefile must establish `PROJECT_ROOT` before any includes to anchor path resolution, tool execution, and recursive makes across modules per the monorepo example.
User-specific and project configuration must remain cleanly overrideable via `config.mk`, supporting configurable defaults such as test timeouts while preserving easy composition.

`make all` builds, vets and tests the default configuration only. Before a change to any implementation, adapter or shared test is considered done, also run `make verify` (`verify.mk`): vet for the race, arm64 and 386 builds; the suite at GOMAXPROCS 1, 2 and 4; the suite under the race detector; the allocation assertions; every fuzz target. Each leg exists because it caught a defect the default run could not (the reasons are at the top of `verify.mk`). `make verify-bench` runs the Go benchmarks and is not a gate.
Python scripts and interpreters are disallowed across the repository; all tooling, generators, and automation must be implemented strictly in Go or GNU Make.

## Terminology: scenarios, cases, attachments, baselines

Coverage is organised around four concepts; use these words precisely and do
not overload them (in particular, never use "scenario" for a single test).

- A **scenario** is a broad use-case, framed as "… for use case X with Y
  characteristics". Scenarios are distinguished only where the distinction
  changes trade-offs, relative effectiveness or considerations for use of at
  least one implementation. A scenario is a perspective for framing what
  matters, not a load event: "a specific saturation incident" is not a
  scenario, whereas the class of workload it came from may be.
- A **case** is one concrete, runnable facet of a scenario (a specific test,
  benchmark or fuzz entry point). Cases exist as a separate declarative
  dimension, with a registry generated at init, primarily so results can
  later be reported, sliced and drilled into. Cases are the management unit
  that makes exhaustiveness checkable; when you add a facet, register it.
- An **attachment** binds an implementation to a scenario. Every attachment
  carries free-text remarks explaining *why* it is attached, and a boolean
  recording whether it is *expected* to be a good fit. Attaching an
  implementation to a scenario it is expected to serve badly is valuable and
  encouraged — it is how a mismatch is documented rather than asserted in
  prose — but only with the fit flag set accordingly.
- A **baseline** answers, for one scenario: "in a vacuum, how would a staff
  engineer expert in Go, with none of these implementations on hand, solve
  this in standard practice?" It must be the simplest sound textbook design,
  deliberately neither novel nor optimised, and must be attached to (and
  evaluated alongside) the scenario it baselines. If you find yourself making
  a baseline clever, stop: its entire value is that it is ordinary.

Registries built at init must fail loudly (panic) on duplicate identities,
dangling references or incomplete metadata. Exhaustiveness is a structural
property enforced by those checks and by registry-completeness tests, not a
matter of reviewer vigilance.

## Test quality: no flakiness

Flakiness under load is unacceptable. Synchronise with channels, WaitGroups
and context cancellation; counts of completed work must be exact and bounded.
A timeout is legitimate only as a deadlock guard that *fails* the test (a
verifiable bound), never as a retry loop, a sleep-until-probably-ready, or a
generous pause. If a facet cannot be made deterministic, make it bounded
non-determinism with a verifiable invariant (e.g. exact total delivery count,
strict monotonicity, final-value observation) and say why that invariant is
the right one. Do not bump timeouts or add sleeps to make a failure go away:
that is hiding a bug, not fixing a test.

Benchmarks must measure only the operation they claim to measure, with
subscribers drained or otherwise accounted for deterministically, and must
never produce favourable-looking numbers for behaviourally-wrong behaviour
(see the exposure-limitation policy below: a deficient variant is excluded
rather than benchmarked into a misleading win).

## Deficient implementations

The implementations here originated as one-shot outputs from models that
could not run Go; some arrived with defects ranging from "did not compile"
to subtle races found only under stress. The litmus test for any fix is:
*could the original output still fairly be described as the basis of the
result?*

1. **Fix in spirit (the default).** Every defect that can be repaired while
   remaining faithful to the original design gets repaired — including
   defects found by testing that the original iteration never identified.
   Make the judgement call yourself; a fix that preserves the design's
   identity, mechanism and trade-offs is in spirit even if the diff is not
   tiny.
2. **Limit exposure explicitly.** If a deficiency is structural — it cannot
   be fixed without replacing the design — do not fix it by stealth and do
   not let it contaminate results. Artificially limit the implementation's
   exposure to tests, benchmarks, fuzzing and reporting so the deficiency is
   never exercised, using the attachment metadata designed for exactly this:
   an explicit exposure-limitation flag plus remarks stating what is avoided
   and why. Typical triggers: a correctness failure, a panic or deadlock
   path, a data race in a mode no facet may touch concurrently, a lifecycle
   operation that does not exist, or a blocked operation with no
   cancellation escape. Benchmarks in particular must never report numbers
   obtained while a known deficiency silently voided the measured behaviour.
3. **Remove (break-glass only).** If an implementation can neither be fixed
   in spirit nor meaningfully exposure-limited, remove it entirely and
   record why alongside its entry in the response index (`01-readme.md`).
   This is the last resort; prefer options 1 and 2.

Record the *reasoning* for an exposure limitation at the attachment, where a
reader deciding whether to use the implementation will see it — not buried
in a test that merely avoids the path.

## Measurement

Measurement methodology, profiles and the committed run log (`results-*-*-*`) are documented in `measurement.md`; consult it rather than duplicating its contents here.

Policy that the documentation of the mechanics does not carry:

- **Reported facts are generated, not written.** The run manifests, the
  evaluation of a run (`results-NN-07-evaluation.md`), the gates record
  (`results-NN-06-gates.json`) and the landing page (`index.html`) are produced
  by the harness from the committed files and are never edited by hand. A count
  typed into prose goes stale the next time anything changes, and did. If a
  fact about a run is worth stating, make the generator state it. Written
  analysis is a separate kind of file (`report-NN-*.md`), names the run it
  reads, and is expected to age.
- **A run is judged against the registry it was made from**, never against
  today's. Adding an implementation must not turn an old run into a failure,
  and must not let an old run pass for covering it.
- **Do not rank a family that is too small to rank.** Scores here are
  percentiles within a family. With a handful of members a percentile is an
  artifact of the head count (two members score 0 and 100 on every dimension
  whatever the numbers are), so small families are measured, checked for
  integrity and reported unranked. Do not lower the threshold to make a table
  look fuller.
- **A dimension nobody was measured on is not a perfect score.** It is left out
  of the composite and shown as having no data. The opposite convention once
  gave every implementation full marks for stall resilience.
- **Nothing measured may share a process with something measured before it.**
  A stalled-consumer probe ends with a publisher nothing can stop, and one that
  waits by spinning uses a CPU until its process exits. Six runs were taken in
  one process each before anyone looked at what the process was doing between
  batteries: ten implementations had the machine and the rest did not. The run
  is one process per battery per implementation. Do not add an in-process
  shortcut to a committed run, and when a result looks too good (a publish cost
  that does not change between 1 and 256 subscribers), measure it alone before
  believing it.
- **Time what the battery says it times.** A timed loop must start after the
  system is in the state the battery describes (receivers registered,
  subscribers running), and its length must not rest on an estimate taken
  before that. Two batteries here measured something else for five runs: one
  timed Sends that delivered nothing, one sized repetitions from publishes that
  reached no running subscriber.
- **A cost is a cost of something. Record the something.** A publish that
  returns in 30 ns may have delivered its value, queued it, or thrown it away.
  The broadcast throughput battery recorded the 30 ns and not which, and for
  seven runs the top of the family was the implementations that throw most
  away. A battery that times an operation must also count what the operation
  achieved (values delivered, items dispatched, pairs exchanged), and a
  ranking must only set against each other subjects that achieved the same
  thing. When one table holds subjects with different contracts, split it.
- **Do not rank what the measurement cannot separate.** A percentile rank
  turns any set of numbers into an order, including thirteen numbers whose
  confidence intervals all overlap. Before publishing an order, check that a
  second run gives the same one, and withhold it when the intervals say it
  will not.
- **Say what was executed and where.** A cross-compiled or cross-vetted file
  has not run. Emulating a routine is not running the program, and neither says
  anything about memory ordering on real hardware. `make verify-arm64` exists
  so that the arm64 assembly can be executed on a machine that can execute it,
  and fails where it cannot rather than passing vacuously. It passes under
  `qemu-aarch64` (`VERIFY_ARM64_EXEC`); it has not run on arm64 hardware, and an
  emulator on an x86-64 host cannot show a wrong memory fence.
- **Record the gates with the run.** A statement that the suite passed is worth
  what its record is worth: which commit, whether the tree was clean, which
  legs ran, what each reported. `make measure-gates` writes that record.
