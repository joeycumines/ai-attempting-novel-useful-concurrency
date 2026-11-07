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
