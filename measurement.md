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

`run` measures nothing itself: it starts one process per battery per
implementation (see "Process isolation"). `--in-process` measures everything in
the one process instead. That is for debugging a battery under a debugger or a
profiler; such a run says `"isolation": "none"` in its meta record and `verify`
rejects it.

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

One JSON object per line. Every committed run has one (`results-NN-01-results.jsonl`;
`results-00-00-readme.md` lists the runs and the record contract of each). `type` is one of:

| type      | meaning                                                            |
|-----------|--------------------------------------------------------------------|
| `meta`    | environment header (first line; required by `verify`); `generator_version` is the record contract, `isolation` says how the run was isolated |
| `metric`  | one measured statistic with distributional summaries                |
| `trace`   | per-lane event timeline (publisher + subscribers)                   |
| `skipped` | an implementation deliberately excluded from a battery, with reason |
| `residue` | what one battery of one implementation left running in its process: `goroutines` above the count before it, and `cpu_cores`, the CPUs the process went on using afterwards. Written only when something was left |
| `summary` | run footer: records, failures (with `failure_msg`), skipped, duration |

`metric` record glossary:

| field | meaning |
|---|---|
| `impl`, `family`, `metric` | identity; `family` ∈ broadcast, caster, scatter, gather, payload, fanin, exchange, barrier |
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
| `publishes`, `receives`, `delivered_fraction` | broadcast and payload `publish_ns_op` (contract 1.4.0 on): the publishes made over all repetitions of the cell, warm-up included; the values the subscribers received; and `receives / (publishes × subscribers)`. `publish_ns_op` is the cost of the call whatever became of the value. The count is read once the subscribers have gone 50 ms without receiving, so a backlog that is still draining is waited for; a cell whose subscribers are still receiving after 20 s fails the run. `verify` rejects a 1.4.0 run whose throughput records lack `publishes` or `receives` |
| `torn_reads`, `checked` | payload: multi-word ([4]uint64) read integrity vs loads checked |
| `deliveries`, `deliveries_expected`, `deliveries_missing` | caster throughput and latency: values the receivers took, values the timed Sends should have delivered, and the difference. A Send is timed only once every receiver is registered for it. Any missing delivery fails the implementation's integrity gate |
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

A throughput repetition of the broadcast and payload families is sized by the
clock, not by an estimate: the publisher warms up for an eighth of the target
time, then publishes in batches until the target has passed (`opBudget`), and
the subscribers' goroutines are running before either starts. `ops` is what the
last repetition ran. Until contract 1.3.0 the count was fixed from 16 publishes
made straight after subscribing, before any subscriber had run. For an
implementation whose publish is cheap only while its subscribers sleep that
estimate was the cheap cost: the count went to its maximum, and a repetition
with a 60 ms target ran for half a minute.

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

## Process isolation

Every battery of every implementation is measured in a process of its own. The
`run` command is a parent that measures nothing: it lists the units of the run
(`measurePlan`: families in run order, implementations in registration order or
by name, batteries in registry order), starts a child process for each, and
copies the child's records into the results file byte for byte. The meta record
of such a run says `"isolation": "process-per-battery"`, and from record
contract 1.3.0 `verify` fails a run that does not.

The reason is what a battery can leave behind. A stalled-consumer probe ends
with a publisher that is blocked in a `Send` that will never return, and nothing
can stop it. For most implementations that goroutine is parked. For an
implementation whose `Send` waits by spinning it is not: it uses a whole CPU
until the process ends. Runs 01 to 06 were one process each. The eleventh
broadcast implementation in run order, `EpochCaster (13-OpenRouter)`, is such
an implementation, and its two stall probes left two spinning publishers on a
machine with two CPUs. The ten implementations before it were measured on an
idle machine. Everything after it, 83 broadcast implementations and all seven
other families, was measured while those publishers held the CPUs, which kept
the measured implementation's own subscribers from running while its publisher
was timed. `Latest3260Terra`, measured alone, publishes in 173 to 450 ns at 4
to 256 subscribers; in run 06 it reads a flat 10 ns at every count. No family
of those runs is ranked (see Adoptability score).

What isolation guarantees:

- Every unit starts from the same state: a new process with no goroutines, no
  heap and no garbage from anything measured before it. The order of the run
  cannot affect a measurement.
- A unit that kills its process (a runtime fatal error, a deadlock the runtime
  detects) or hangs it (killed after 10 minutes) is a recorded failure of that
  unit. The run goes on, exits non-zero, and its summary names the unit.
- What a unit leaves behind is recorded. After the unit the child counts its
  goroutines; if there are more than before, it watches its own CPU use for
  100 ms and writes a `residue` record. A parked publisher shows as goroutines
  with no CPU. A spinning one shows as about one CPU, and the evaluation lists
  it under "One subscriber that never reads".

The child is the same binary, started as
`<test binary> -test.run=NONE bigbuff-measure run --unit-family F --unit-impl NAME --unit-battery ID`.
A process starts in a few milliseconds, so the 1,041 units of a standard run cost
seconds in total.

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
  (`fatal error: all goroutines are asleep`).
- **A blocked publisher ends with its process.** Skipping the cleanup leaves the
  goroutine where it is, and one that spins goes on using a CPU. Nothing else is
  ever measured in that process (see Process isolation).
- **Exposure limiting, never mis-measuring.** An implementation that cannot survive a
  battery by construction is skipped with a recorded reason (see attachment registry,
  e.g. concurrent withdrawal during `Send`). It is excluded from that battery's
  statistics — it is never fed a rigged benchmark.
- **Integrity gates are hard.** Any torn multi-word payload read, or any exactly-once
  scatter violation, is recorded explicitly; the report surfaces them, and the
  adoptability score caps at 50 on integrity failure.

## Adoptability score

Percentile rank within a scoring group, weighted: **throughput .28 · stall
resilience .20 · scaling .16 · latency .14 · allocations .14 · churn .08**. Stall
resilience uses log₁₀(completed ops) under a shared stalled-consumer deadline
(higher = better); scaling is the 64/1 relative publish-cost ratio (flatter =
better). Payload integrity failure caps the score at 50. A 100 means *best measured
on these axes within this group*, not “use everywhere” — read the radar chart, the
pathology panel, and the registry remarks.

A scoring group is a family, or, for broadcast and payload, one **delivery class**
of the family:

- `delivers-all` ("delivers every value"): in every cell and every repetition of
  the throughput battery, the subscribers received every value published to them.
- `drops` ("drops or coalesces"): in at least one, they did not.

The class is measured, not declared. The throughput battery publishes as fast as
it can to subscribers that do nothing but receive, and counts what arrives
(`publishes`, `receives`). It is a statement about that battery on this machine:
an implementation that drops delivers everything to subscribers that keep up. An
implementation with no delivery figure (every throughput cell skipped) has no
class and is not ranked. A class with fewer than 5 members is not ranked, with
verdict `Not ranked (class of N)`; the other class of the family still is.

Why the classes exist. A publish is cheaper when the value need not arrive. Up
to contract 1.3.0 the throughput record gave the cost of the call and nothing
else, and the family was one table. In run 07 the eight broadcast "Strong
candidates" delivered between 0.1 and 79 percent of what was published at 8
subscribers, and every implementation that delivers everything ranked below
them. That table answered "which publish returns soonest", which is not a
question anybody choosing between a lossless and a lossy design is asking.

Rules about which runs and families are scored, and why:

- **Broadcast and payload of a run before record contract 1.4.0 are not scored**,
  for the reason above. That is run 07; runs 01 to 06 are unscored anyway.

- **No family of a run before record contract 1.3.0 is scored.** Runs 01 to 06
  were one process each, and from the eleventh broadcast implementation on they
  were measured on CPUs that two leftover spinning publishers were using (see
  Process isolation). Their reports show every measurement and the verdict
  `Not ranked (measurement not valid)`.
- **The caster send figures of contracts before 1.2.0 do not measure delivery.**
  Those batteries looped on `Send` without waiting for the receivers to register
  again, so most Sends found nobody and returned at once. From 1.2.0 a Send is
  timed only when every receiver is registered, and the deliveries are counted.

The decision is made in `familyRanking` and, for the classes, in `deriveReport`
(`measure_report_test.go`), which the report, the score CSV, the evaluation and
the landing page all read.

Three rules that hold from contract 1.4.0 (run 08) on. They are rules of the
report, not of the records, so they apply to every run when it is re-rendered:

- **A tie counts as half a place.** The percentile rank of a value is the share
  of the other members of the group that are worse, plus half the share that are
  equal. Before, a tie counted as nothing: half the broadcast family completes
  the stalled-consumer probe, so the best any of them could score on that
  dimension was 51, and nothing in the family could reach 100 overall.
- **A group the measurement does not separate is not ranked.** For each scoring
  group the report takes the 95 percent interval of every member's headline
  cost (publish or send at 8 subscribers) and counts the pairs whose intervals
  do not overlap. With fewer than half the pairs apart, the group gets the
  verdict `Not ranked (not distinguishable)` and its measurements are printed
  instead (`indistinguishable` in `measure_report_test.go`). The caster family
  is the case: 13 implementations within 800 to 1,130 ns, 16 of 78 pairs
  apart, and an order that changed between runs 07 and 08 with unchanged
  batteries. The rule looks at the headline cost only. It does not test the
  other dimensions, and it does not make the ranking of a group that passes
  reproducible; it withholds the ones that plainly are not.
- **A dimension on which the whole group has one value is left out.** It is
  listed in `dims_uniform`, shown as "tied", empty in the scores CSV, and not
  part of the composite. Every member of the dropping broadcast class completes
  the stalled-consumer probe; leaving the dimension in would cap the class at
  90 and say nothing.

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

A score is never comparable between families, between the delivery classes of a
family, between profiles or between record contracts.

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
`make measure-gates` runs it and records the outcome with the newest run
(`bigbuff-measure gates --log <dir> --run results-NN-01-results.jsonl`). The
record names the commit the gates ran on. When that is not the commit the run
was measured at, `--run` makes it list the files that differ between the two,
so that a reader can see whether the gates and the measurement are about the
same code. The fuzz leg records the command line it ran.

`make verify-arm64` executes the tests of the implementations that ship arm64
assembly (64 and 72) as an arm64 binary: natively on an arm64 host, otherwise under
`qemu-aarch64`. It fails when neither is available, and it is not part of
`make verify` for that reason. It has not been run for real: the machine this
repository was developed on is amd64 with no emulator. What has been done instead
is recorded in `extraction_audit.md` ("arm64 assembly: executed under emulation").

## Known limitations

- Shared hardware: every committed run was taken on a shared 2-CPU virtual machine.
  Compare *ratios*, not absolutes; use `--profile full` on quiet machines for
  publication numbers.
- The delivery class is what one battery saw: a publisher that never pauses,
  subscribers that only receive, on 2 CPUs. A different load would put some
  implementations in the other class, and the delivered share of an
  implementation that drops varies from run to run with the scheduler. The
  class membership itself has one confirmation: a second pass of the
  throughput batteries under the same settling rule put the 108
  implementations it covered in the classes run 08 gives them. Nothing here measures how an implementation that drops behaves when
  its subscribers keep up, which is the case it is built for.
- Within the dropping class, a publish that delivers 0.1 percent of its values
  and one that delivers 70 percent are still scored on the cost of the call.
  The evaluation prints the delivered share beside each score so that the two
  can be told apart; the score does not weigh it.
- A ranked group is one whose headline cost separates at least half of its
  pairs. That is a floor, not a guarantee: adjacent places in a ranked class
  can still be within each other's intervals, and only the broadcast and
  payload classes of run 08 have been through it with a run that is not
  otherwise withdrawn.
- The broadcast and payload throughput and latency batteries size a repetition
  by the clock while it runs (`opBudget`). The churn and stalled-consumer
  probes, and the caster, scatter, exchange and barrier batteries, still size
  it once from a short estimate (`calibrateOps`). That is sound where an
  operation completes before the next begins, which is what those batteries
  measure, and wrong for an operation that is cheap until something else
  wakes; the per-unit timeout is what stands behind it.
- Isolation covers what one battery leaves for the next. It does not cover what
  else the machine is doing: another tenant of the host still shows up as noise,
  and the within-run variation in the evaluation is the only measure of it.
- With more goroutines than CPUs, a cell whose subscribers poll or spin measures
  the scheduler as much as the implementation. The 128- and 256-subscriber cells
  on 2 CPUs have coefficients of variation above 100% for some implementations;
  the report shows the interval, and such a cell supports no fine comparison.
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
