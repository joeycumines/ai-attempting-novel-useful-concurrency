# MEASUREMENT.md — the bigbuff measurement harness

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
make measure            # standard profile: run → verify → interactive HTML report
make measure-quick      # CI-grade quick profile (~2–4 min on 2 vCPUs)
make measure-standard   # decision-grade standard profile (~30–60 min on 2 vCPUs)
make measure-full       # publication-grade full profile (hours on 2 vCPUs; use more cores)
make measure-report     # rebuild results/report.html + CSVs from results
make measure-list       # families, batteries, profiles
make measure-verify     # verify results/measurements.jsonl
make measure-ci         # quick + verify, intended for CI
```

Direct invocation (what Make does):

```sh
go test -run=NONE -count=1 -timeout=120m -args bigbuff-measure run --profile standard --out results/measurements.jsonl
go test -run=NONE -count=1 -args bigbuff-measure verify --in results/measurements.jsonl
go test -run=NONE -count=1 -args bigbuff-measure report --in results/measurements.jsonl --out results/report.html --csv results/csv
```

`--seed`, `--impl`, `--battery`, `--no-verify` and `list --family <name>` narrow the
run. Partial JSONL is streamed line-by-line; a crashed run leaves a prefix that
`verify` will reject (missing coverage), so a green verify means a *complete* run.

---

## Committed artifacts & index

The canonical measurement outputs are **committed to this repository** under
`results/` (standard + quick profiles, the interactive report, the registry
dump, CSV exports and the manifest). `results/README.md` is the index of record:
it names every artifact, how it was generated, and how to refresh it.
`results/manifest.json` is the machine-readable companion — per file: path,
SHA-256, byte count, and for the JSONL files the record histogram, meta
fingerprint, summary footer and verification verdict. Rebuild it with
`make measure-index` (powered by `bigbuff-measure index`); it exits non-zero if
any JSONL file fails verification. `make clean` intentionally does not touch
`results/`.

## Data contracts

### Results: JSONL schema v1 (`results/*.jsonl`)

One JSON object per line. The committed files are `results/measurements.jsonl`
(standard profile) and `results/quick.jsonl` (quick profile). `type` is one of:

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
| `exactly_once_violations` | scatter: messages received ≠ 1 times (all receivers, warmup included) |
| `torn_reads`, `checked` | payload: multi-word ([4]uint64) read integrity vs loads checked |
| `withdrawal_compensation_success` | caster: fraction of rounds where `Add(-1)` unstuck a parked `Send` |
| `median_round_ns` | withdrawal round latency |

### Registry: `results/registry.json` (`dump-registry`)

`{type, schema, scenarios, cases, attachments}`. Attachments bind implementations to
scenarios with `expected_good_fit` (bool), `exposure_limited` (bool), `remarks`
(free text) and `exposure` (free text). Registries panic loudly on duplicates and
dangling references; the report's coverage matrix consumes the same file.

### Report exports

`make measure-report` additionally writes `results/csv/metrics_wide.csv` (one row per
metric record) and `scores.csv` (per-family adoptability scores + verdicts). The HTML
report embeds the full JSONL + registry and can download all three, plus a JSONL
re-export, client-side.

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

23 batteries across 5 families (`make measure-list`). Highlights: throughput sweeps,
p99-focused latency, allocations (and zero-subscriber cost), retained bytes per idle
subscriber, subscribe/unsubscribe churn, stalled-consumer pathology (completed ops +
retained bytes + fast-lane minimum), withdrawal compensation for rendezvous casters,
exactly-once scatter accounting, torn-read payload integrity, wait-time barriers for
gathers, and full swimlane traces.

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

## Verification semantics

`verify` fails (non-zero exit) unless: the `meta` record is present and well-formed;
every expected implementation (77 broadcast / 14 caster / 4 scatter / 2 gather /
19 payload, deduped to measured leaves) appears in ≥1 metric, or is recorded in a
`skipped` record with a reason; and no record is malformed. `skipped` records are
first-class citizens of the report (see “Skipped implementations” in Data & Exports).

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
- Two bounded-scatter implementations (StableRing32Nano, BaselineBoundedScatterQueue)
  do not yet have unified adapters and are therefore not in the scatter battery; they
  are tracked here rather than silently dropped.

## Adding a new implementation

1. Register it in the appropriate factory (`AllBroadcasters`, `casterLikes`, …).
2. Add an attachment (good-fit/bad-fit + remarks + exposure-limit if applicable).
3. Re-run `make measure-quick && go test ./...` — `verify` will demand a measured or
   explicitly skipped result for it; `go test` must stay green.
