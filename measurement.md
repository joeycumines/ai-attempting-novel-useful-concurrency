# measurement.md — the bigbuff measurement harness

This repository carries two contracts at once:

1. **`go test` must keep working exactly as before.** All measurement code lives in
   `measure_*_test.go` and is only reachable through a subcommand binary
   (`go test -run=NONE -args bigbuff-measure …`). Plain `go test ./...` compiles the
   same files but executes nothing of the harness; the measurement batteries never
   run unless explicitly requested.
2. **Measurement runs must be passable.** A measurement run exits non-zero iff it
   fails verification (coverage, schema, meta) or a battery aborts. Batteries assert
   only what their design can guarantee; everything else is recorded, never hidden.

Everything below is enforced by `go test -run=NONE -args bigbuff-measure verify`.

---

## Quick start

```sh
make measure            # new run: measure → registry → report → evaluation → site (default profile: standard)
make measure-quick      # CI-grade quick profile (~2–4 min on 2 vCPUs)
make measure-standard   # decision-grade standard profile (~30–60 min on 2 vCPUs)
make measure-full       # publication-grade full profile (hours on 2 vCPUs; use more cores)
make measure-report     # rebuild the report (+ CSVs) for the newest committed run
make measure-list       # families, batteries, profiles
make measure-verify     # verify the newest results-*-01-results.jsonl
make measure-ci         # quick + verify, intended for CI
make measure-index      # rewrite every results-NN-00-manifest.json
make measure-evaluate   # regenerate the newest run's evaluation (slot 07) from its own files
make measure-gates      # run `make verify`, record what it reported (slot 06), regenerate the evaluation
```

Direct invocation (what Make does), using the flat layout (see next section):

```sh
NN=$(ls results-*-01-results.jsonl 2>/dev/null | sed -E 's/^results-([0-9]+)-.*/\1/' | sort -n | tail -1); NN=${NN:-00}; NN=$(printf '%02d' $((10#$NN + 1)))
go test -run=NONE -count=1 -timeout=120m -args bigbuff-measure run --profile standard --out results-$NN-01-results.jsonl
go test -run=NONE -count=1 -args bigbuff-measure verify --in results-$NN-01-results.jsonl
go test -run=NONE -count=1 -args bigbuff-measure report --in results-$NN-01-results.jsonl --csv-metrics results-$NN-04-metrics_wide.csv --csv-scores results-$NN-05-scores.csv
go test -run=NONE -count=1 -args bigbuff-measure evaluate --in results-$NN-01-results.jsonl
go test -run=NONE -count=1 -args bigbuff-measure site        # wires the reports, re-indexes the manifests, writes index.html
```

`--seed`, `--impl`, `--battery`, `--no-verify` and `list --family <name>` narrow the
run. Partial JSONL is streamed line-by-line; a crashed run leaves a prefix that
`verify` will reject (missing coverage), so a green verify means a *complete* run.

---

## Committed artifacts & the run log (flat layout)

This repository is **flat** (see AGENTS.md): every file lives at the root. The
measurement artifacts therefore live at the root too — there is **no `results/`
directory**. Artifacts are grouped into zero-padded, numerically ordered *run
groups* so the tree reads as a log:

```
results-<rr>-<ss>-<slug>.<ext>
```

- `rr` — run sequence, two digits: `00` is the documentation group, `01..` are
  measurement runs in commit order. `results-00-00-readme.md` lists them.
- `ss` — slot within the run, two digits:
  - `00` — `results-NN-00-manifest.json` (the run's manifest; **first file of the group**)
  - `01` — `results-NN-01-results.jsonl` (the JSONL results)
  - `02` — `results-NN-02-registry.json` (scenario/case/attachment registry dump)
  - `03` — `results-NN-03-report.html` (self-contained interactive report)
  - `04` — `results-NN-04-metrics_wide.csv` (flattened metric records)
  - `05` — `results-NN-05-scores.csv` (adoptability scores and verdicts)
  - `06` — `results-NN-06-gates.json` (what `make verify` reported, and on which commit; written by `make measure-gates`)
  - `07` — `results-NN-07-evaluation.md` (the evaluation, generated from slots 01, 02 and 06)
- `slug` — free-form lower-snake descriptor (`results`, `manifest`, `report`, `readme`, …).

**Enumeration contract.** Iterate the listing `results-*-*-*` in sorted
(lexicographic = numeric, thanks to zero-padding) order. Each run group consists
exactly of the files sharing its `rr`; the first file of a group is always the
manifest, which carries every detail needed to interpret the rest (mirroring a
`go test -bench` output header): the run sequence, the full meta fingerprint —
machine (`cpu`, `num_cpu`, `gomaxprocs`), OS/arch, Go toolchain, git commit,
profile, seed, hostname captured at index time — plus duration, record histogram,
per-file SHA-256 and byte counts, and the verification verdict. Reading the log
of runs is: glob, sort, then walk groups; manifest first, artifacts after.

**The documentation group** (`rr = 00`) holds exactly one file,
`results-00-00-readme.md` — the human index of record: what each run contains,
provenance, and how to refresh.

**Manifests are regenerated, never hand-edited.** `make measure-index` (powered
by `bigbuff-measure index --dir .`) rewrites every `results-NN-00-manifest.json`
from the bytes on disk: hashes, sizes, record histograms, and a fresh
verification verdict per JSONL (it exits non-zero if any run fails
verification). The manifest does not hash itself. `make site` runs the same
index after it has wired the reports, because wiring rewrites them; `make all`
runs `make site`. A test (`TestCommittedRunsAreIndexed`) fails when a committed
manifest does not match the committed files.

**A run is verified against the registry it was made from**, which is the dump
in its own slot 02: the dump records, under `measured`, the implementations a
run made from it must have records for. The manifest names what the run was
verified against in `verified_against`. A run group with no dump (run 02) is
verified against the nearest earlier dump and its manifest says so. Adding an
implementation or a family later therefore never fails an old run. To check a
results file against a particular dump by hand:
`bigbuff-measure verify --in <jsonl> --registry <dump>`; without `--registry`
the live registry is used, which is what a new run is checked against.

**The evaluation is generated, never written by hand.** `bigbuff-measure
evaluate` writes `results-NN-07-evaluation.md` from the run's own files. It
reports the run, the recorded gates, coverage, integrity, the ranked families,
the measurements of the unranked ones, the stalled-consumer outcomes, the skips
and the within-run variation. It makes no recommendation. Written analyses are
separate files, `report-NN-*.md`.

**The landing page is generated.** `index.html` is written by `make site` from
the manifests, the newest registry dump, the newest scores CSV, the tracker
(`01-readme.md`) and the `report-NN-*` files. `TestCommittedLandingPageIsCurrent`
fails when the committed page is not what the committed files render to.

**Refreshing a run.** Runs are produced with `make measure` (or the per-profile
targets), which allocates the next free `rr` automatically (override with
`MEASURE_RUN=N`). To refresh the committed log: run the pipeline, then
`git add results-*-*.*` and commit. `make clean` intentionally does not touch
these committed artifacts.

## Data contracts

### Results: JSONL schema v1 (`results-NN-01-results.jsonl`)

One JSON object per line. The committed files are `results-01-01-results.jsonl`
(standard profile) and `results-02-01-results.jsonl` (quick profile). `type` is one of:

| type      | meaning                                                            |
|-----------|--------------------------------------------------------------------|
| `meta`    | environment header (first line; required by `verify`)              |
| `metric`  | one measured statistic with distributional summaries                |
| `trace`   | per-lane event timeline (publisher + subscribers)                   |
| `skipped` | an implementation deliberately excluded from a battery, with reason |
| `summary` | run footer: records, failures, skipped, duration                   |

`metric` record glossary:

| field | meaning |
|---|---|
| `impl`, `family`, `metric` | identity; `family` ∈ broadcast, caster, scatter, gather, payload |
| `dims` | sweep dimension — `subs` (fan-out), `receivers` (caster/scatter), `senders` (gather) |
| `ops`, `n`, `reps` | operations per repetition, samples, repetitions |
| `mean`, `stddev`, `cv` | distributional summaries across repetitions |
| `min`, `p50`, `p90`, `p99`, `max` | per-operation latency reservoir (ADR-004 style) |
| `ci95_low`, `ci95_high`, `ci95_halfwidth` | 95% CI on the per-op mean (t-distribution, df = n−1) |
| `lower_is_better` | sort direction for scoreboards |
| `unit` | `ns`, `bytes`, `fraction`, `count` |
| `scenarios`, `expected_good_fit`, `exposure_limited` | attachment-registry metadata (authored, not measured) |
| `ops_completed`, `ops_planned`, `outcome` | pathology probes (see below); `outcome` ∈ `completed`, `publisher_blocked`, `publisher_panicked`, `sender_blocked`, `sender_panicked` |
| `approx_retained_bytes` | post-GC heap attributed to one permanently stalled subscriber |
| `exactly_once_violations` | scatter: items neither delivered exactly once nor counted as evicted, i.e. `|sent − (received + evicted)|` (all receivers, warmup included). Any non-zero value fails the implementation's integrity gate |
| `evicted`, `delivered_fraction` | scatter: items a bounded queue evicted by contract (its own eviction counter), and the fraction of sent items that were delivered. `dispatch_ns_op` is wall time per **delivered** item, so an evicting queue is not credited with the items it dropped. Always `0` and `1` for implementations without an eviction counter |
| `torn_reads`, `checked` | payload: multi-word ([4]uint64) read integrity vs loads checked |
| `withdrawal_compensation_success` | caster: fraction of rounds where `Add(-1)` unstuck a parked `Send` |
| `median_round_ns` | withdrawal round latency |

### Registry: `results-NN-02-registry.json` (`dump-registry`)

Committed at slot `02` of the run that ships the matching report
(`results-01-02-registry.json`). Dump fresh with `make measure-registry`.

`{type, schema, scenarios, cases, attachments, measured, runnables}`. Attachments
bind implementations to scenarios with `expected_good_fit` (bool),
`exposure_limited` (bool), `remarks` (free text) and `exposure` (free text).
Registries panic loudly on duplicates and dangling references; the report's
coverage matrix consumes the same file.

`measured` is the coverage contract: per family, the implementations a run made
from this registry must have records (or reasoned skips) for.

`runnables` lists every top-level `Test`, `Benchmark` and `Fuzz` function with its
class and the scenarios it bears on:

| class | what it is | scenarios |
|---|---|---|
| `case` | registered in `Cases`; runs every implementation attached to its scenario, so its results compare them | the scenarios of its cases |
| `unit` | lives in the test file of one implementation source file and tests that implementation's own contract | every scenario that implementation is attached to |
| `harness` | adapter conformance across all scenarios, or the tooling testing itself, with the reason recorded | none |

A unit runnable shows that one implementation holds where it is attached. It does
not compare implementations and is not meant to. `TestScenarioCaseRunnablesExhaustive`
fails when a runnable fits no class or a unit runnable resolves to no scenario.

### Report exports

`make measure-report` (or `bigbuff-measure report`) additionally writes the
run's `-04-metrics_wide.csv` (one row per metric record) and `-05-scores.csv`
(per-family adoptability scores + verdicts); output paths default into the same
run group (`--csv-metrics` / `--csv-scores` override). CSV output is
deterministic: `metrics_wide.csv` follows the JSONL record order, `scores.csv`
is sorted by family asc, score desc, then impl asc — regenerating from the same
results file and registry reproduces both byte-for-byte (so does the HTML
report). The HTML report embeds the full JSONL + registry and can download all
three, plus a JSONL re-export, client-side.

---

## Profiles

| profile | reps | sweep | latency samples | churn ops | stalled deadline | use |
|---|---|---|---|---|---|---|
| quick | 3 | subs 1, 8, 64 / casters 1, 8, 32 | 160 | 80 | 150 ms | CI gate |
| standard | 5 | subs 1…256 / casters 1…32 | 400 | 200 | 250 ms | default decisions |
| full | 7 | subs 1…512 / casters 1…64 | 1000 | 320 | 600 ms | publication-grade |

All profiles: warmup runs before measurement; alloc batteries pin GOGC=off and measure
with an explicit GC; latency batteries use a bounded reservoir; stalled-consumer probes
run a shared wall-clock deadline after which a still-parked publisher is *recorded*,
not killed.

## Batteries

29 batteries across 8 families (`make measure-list`). Highlights: throughput sweeps,
p99-focused latency, allocations (and zero-subscriber cost), retained bytes per idle
subscriber, subscribe/unsubscribe churn, stalled-consumer pathology (completed ops +
retained bytes + fast-lane minimum), withdrawal compensation for rendezvous casters,
exactly-once scatter accounting, torn-read payload integrity, wait-time barriers for
gathers, merge cost and batch-drain batteries for fan-ins, and full swimlane traces.

The exchange and barrier families have two batteries each:

| battery | metric | what is measured |
|---|---|---|
| `exchange.pair` | `exchange_ns_op` (dims `senders` = 2 and 8) | time per completed swap with that many goroutines exchanging; every swap is checked for pairing (A got B's value exactly when B got A's) |
| `exchange.cancel_race` | `cancel_race_ns_op`, fields `swapped` and `withdrawn` | one party's context is cancelled while its partner arrives; counts which way each race went and checks no value was lost or duplicated |
| `barrier.send` | `send_ns_op` (dims `receivers`) | time per send acknowledged by every member |
| `barrier.deadline` | `deadline_overshoot_ns`, `deadline_recovery_ns` (4 members, one that never acknowledges) | how far past its deadline a send returns, and the cost of the next send after the stalled member has left |

Every record of these four carries `exactly_once_violations`, and `verify` counts them
toward coverage. Both families have fewer than five implementations, so they are
measured and not scored (see Adoptability score).

## Measurement discipline (the rules that make this comparable)

- **One harness, one timer.** Every battery constructs the implementation from the
  registry factory, warms it, then measures under identical conditions. No
  implementation may special-case its own path.
- **No sleeps, no retry loops.** Contention and churn use channels/WaitGroups for
  happens-before; timeouts exist solely as deadlock guards; all counts are exact.
- **Distributional, never bare means.** Reports carry n, cv, p50/p90/p99, 95% CIs;
  cross-implementation comparison uses Welch-style thinking, not point estimates.
- **Honest pathology.** A publisher parked *by construction* (stalled-consumer probes)
  is an expected outcome and is recorded as `publisher_blocked`; panics are converted
  by `recover` to `publisher_panicked` / `sender_panicked`. The harness never crashes
  on a deliberately adversarial design.
- **Parked publishers are never cleaned up synchronously.** After any non-`completed`
  probe outcome, `Unsubscribe`/`Close` are skipped (or watchdog-bounded in a helper
  goroutine). `recover` cannot unpark a goroutine blocked inside a send; a blocked
  cleanup in the main path trips the Go runtime's global deadlock detector
  (`fatal error: all goroutines are asleep`). This is the single hardest-won rule in
  this harness and it is enforced everywhere.
- **Exposure limiting, never mis-measuring.** An implementation that cannot survive a
  battery by construction is skipped with a recorded reason (see attachment registry,
  e.g. concurrent withdrawal during `Send`). It is excluded from that battery's
  statistics — it is never fed a rigged benchmark.
- **Integrity gates are hard.** Any torn multi-word payload read, or any exactly-once
  scatter violation, is recorded explicitly; the report surfaces them, and the
  adoptability score caps at 50 on integrity failure.

## Adoptability score

Percentile rank within family, weighted: **throughput .28 · stall resilience .20 ·
scaling .16 · latency .14 · allocations .14 · churn .08**. Stall resilience uses
log₁₀(completed ops) under a shared stalled-consumer deadline (higher = better);
scaling is the 64/1 relative publish-cost ratio (flatter = better). Payload integrity
failure caps the score at 50. A 100 means *best measured on these axes within this
family*, not “use everywhere” — read the radar chart, the pathology panel, and the
registry remarks.

Three rules that hold from generator 1.1.0 (run 05) on, and why:

- **The composite is the weighted mean over the dimensions that have data.** A
  dimension an implementation has no data for (its family has no such battery, or
  the sweep did not reach 64 subscribers) is listed in `dims_missing`, shown as
  "no data", empty in the scores CSV, and left out. Before, such a dimension
  scored 100: every caster, which has no allocation or stall battery, started
  with 34 of 100 points for free, and the verdict thresholds are absolute.
- **A family with fewer than 5 measured implementations has no score.** A
  percentile rank over n subjects takes n values; between two, the faster is 100
  and the other 0 whatever separates them. Scatter, gather, fan-in, exchange and
  barrier are reported with their measurements and confidence intervals, with
  verdict `Not ranked (family of N)`. An integrity failure is still reported.
- **Stall resilience now has data.** In runs 01 to 04 (generator 1.0.0) the
  stalled-consumer records did not carry the `ops_completed` value the metric is
  named for, so that dimension, a fifth of the weight, was a constant 100 for
  every implementation. `verify` now rejects a metric record without a value.

Scores are therefore not comparable between runs 01 to 04 and later runs, and a
score is never comparable between families or between profiles.

## Verification semantics

`verify` fails (non-zero exit) unless: the `meta` record is present and well-formed;
every expected implementation of every family (see `make measure-list` for the
current sets, or the `measured` key of a registry dump for a past run's) appears in
≥1 metric, or is recorded in a `skipped` record with a reason; no record is
malformed; and every metric record carries the value it is named for (a `mean`,
an `ops_completed`, or `violations` with `checked`). `skipped` records are
first-class citizens of the report (see “Skipped implementations” in Data & Exports).

The last rule exists because it was once broken without anyone noticing: the
stalled-consumer records of runs 01 to 04 (generator 1.0.0) carry no
`ops_completed`. For those files `verify` prints a note and does not fail, so the
committed runs keep verifying as what they are; for any later generator it is a
failure.

## Correctness gates and arm64

`make verify` runs vet (default, race, arm64, 386), the tests at GOMAXPROCS 1, 2
and 4, the race detector, the allocation assertions and every fuzz target.
`make measure-gates` runs it and records the outcome with the newest run.

`make verify-arm64` executes the tests of the implementations that ship arm64
assembly (64 and 72) as an arm64 binary: natively on an arm64 host, otherwise under
`qemu-aarch64`. It fails when neither is available, and it is not part of
`make verify` for that reason. It has not been run for real: the machine this
repository was developed on is amd64 with no emulator. What has been done instead
is recorded in `extraction_audit.md` ("arm64 assembly: executed under emulation").

## Known limitations

- Shared hardware (this run: 2×2.60 GHz, ~183% single-core utilization — the batteries
  are CPU-bound, so more cores shave the p99-heavy cells, not the scaling). Compare
  *ratios*, not absolutes; use `--profile full` on quiet machines for publication numbers.
- The measure process deliberately accumulates parked goroutines (blocked publishers
  and their pumps) across a full run; this is by construction and bounded by the
  batteries' watchdogs. If a battery ever appears wedged, the global deadlock detector
  (`fatal error: all goroutines are asleep`) is the tripwire, and any parked cleanup
  will be reported as a harness bug — please file it.
- Stalled-consumer retention is post-GC *attribution*, approximate by nature;
  a `0` means “nothing attributable”, e.g. pre-sized or ring-based designs
  (GenCaster), or coalesce-and-overwrite semantics.
- Trace event budgets truncate the timeline (defaults: 40/80/120 ops, 512 events,
  `truncated: true` is recorded); counters remain exact.
- Torn-read coverage depends on actual deliveries under the probe; an implementation
  with zero deliveries reports `insufficient-data`, never a fabricated pass.
- The attachment registry is authored judgment about *fit*; it contextualizes, it does
  not measure.
- Every run so far was taken on 2 logical CPUs at GOMAXPROCS 2. Nothing has been
  measured with more parallelism.
- A score is a rank among the implementations of one family in one run. It is not
  comparable between families, profiles or record contracts.

## Adding a new implementation

1. Register it in the appropriate factory (`AllBroadcasters`, `casterLikes`, …).
2. Add an attachment (good-fit/bad-fit + remarks + exposure-limit if applicable).
3. Re-run `make measure-quick && go test ./...` — `verify` will demand a measured or
   explicitly skipped result for it; `go test` must stay green.
