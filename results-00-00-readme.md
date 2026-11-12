# results-00-00-readme.md — the bigbuff measurement run log

This repository is **flat** (see `AGENTS.md`): every file lives at the repository
root, including the measurement artifacts. They are organised as a zero-padded,
numerically ordered **run log** — there is intentionally **no `results/`
directory**.

## File grammar

```
results-<rr>-<ss>-<slug>.<ext>
```

| segment | meaning |
|---|---|
| `rr` | run sequence, two digits. `00` = this documentation group; `01..` = measurement runs, in commit order. |
| `ss` | slot within the run, two digits. See the slot table below. |
| `slug` | free-form lower-snake descriptor. |
| `ext` | usual file extension (`.jsonl`, `.json`, `.html`, `.csv`, `.md`). |

Slots within a run group:

| ss | file | what it is |
|---|---|---|
| `00` | `results-NN-00-manifest.json` | **the run's manifest — always the first file of the group**: environment fingerprint (machine, OS/arch, Go toolchain, git commit, profile, seed, hostname), duration, record histogram, per-file SHA-256 + byte counts, verification verdicts. |
| `01` | `results-NN-01-results.jsonl` | JSONL results (schema v1) — one line per meta/metric/trace/skipped/residue/summary record. |
| `02` | `results-NN-02-registry.json` | scenario/case/attachment registry dump at the revision that produced the run. |
| `03` | `results-NN-03-report.html` | self-contained interactive HTML report (charts, Pathology Lab, coverage matrix, downloads) built from `-01-results.jsonl`. |
| `04` | `results-NN-04-metrics_wide.csv` | flattened metric records (one row each, all statistical columns). |
| `05` | `results-NN-05-scores.csv` | per-family adoptability scores, dimension breakdowns and verdicts. |
| `06` | `results-NN-06-gates.json` | what `make verify` reported for the tree the run belongs to: commit, whether the tree was clean, each leg's commands and outcome, each fuzz target's budget and executions. Written by `make measure-gates`. |
| `07` | `results-NN-07-evaluation.md` | the evaluation of the run, generated from slots 01, 02 and 06. It states facts and recommends nothing. |

## Enumeration contract

The tree is designed so that **iterating `results-*-*-*` in sorted order yields
every run, manifest first**. Lexicographic order on zero-padded names is numeric
order, so:

1. List `results-*-*-*` sorted by name.
2. Each run group consists of exactly the files sharing an `rr` segment.
3. The `00` slot of each run group is its manifest; read it for the machine,
   toolchain, commit, profile and the SHA-256 of every other file in the group,
   then the artifacts follow in slot order.
4. Group `00` is documentation (this file); groups `01+` are runs.

No directory walking, no side indexes required — `bigbuff-measure index`
implements exactly this walk to (re)write the manifests.

## Committed runs

The table of runs is on the landing page (`index.html`), which is generated
from the manifests, and in each manifest. It is deliberately not repeated here:
this file used to carry one by hand and it went stale. What a reader needs to
know that the manifests do not say:

**Each run is verified against the registry it was made from.** That is the
dump in its own slot 02, which records the implementations a run made from it
must cover. The manifest names it in `verified_against`. Run 02 has no dump of
its own and is verified against run 01's, the nearest earlier one; its manifest
says so. A run therefore keeps its verdict when implementations or families are
added later. Whether the newest run still covers the live registry is a
separate question, which `bigbuff-measure index` answers with a note and the
landing page shows.

**Run 12 is the current run, and run 11 is the same code measured before
it.** Runs 09 and 10 are the pair before them, under the previous contract;
they were the first runs that could be compared, and
`report-04-run-10-analysis.md` is the written account of those two. Run 08 is
the first run whose rankings mean what they say. **No family of runs 01 to 06 is
ranked, and the broadcast and payload families of run 07 are not either.**
Six defects of the measurement were found, each by looking at what a
suspicious number was made of, and each has its own record contract
(`generator_version` in the meta record and the manifest):

| Contract | Runs | What it fixed |
|---|---|---|
| 1.0.0 | 01 to 04 | Nothing; the starting point. Its stalled-consumer records carried no `ops_completed`, and a dimension with no data scored 100, so every implementation had full marks for stall resilience. |
| 1.1.0 | 05 | `ops_completed` is recorded, a dimension without data is left out of the composite, `verify` rejects a metric record without its value, and families of fewer than five are not ranked. Exchange and barrier batteries. |
| 1.2.0 | 06 | The caster batteries time a `Send` only once every receiver is registered, and count the deliveries. Before, they timed Sends that found no receiver and delivered nothing (2 to 8 ns each). |
| 1.3.0 | 07 | Every battery of every implementation is measured in a process of its own. Before, the run was one process, and the publishers left spinning by the stall probes of the eleventh broadcast implementation held both CPUs for the rest of it. Throughput repetitions are sized by the clock. |
| 1.4.0 | 08 | The broadcast and payload throughput records say how much of what was published the subscribers received, and the two families are scored in two delivery classes: implementations that delivered everything, and implementations that dropped or coalesced. Before, the record gave the cost of calling Publish only, and in run 07 the eight top broadcast places went to implementations that delivered 0.1 to 79 percent of their values. A tie counts as half a place in a percentile rank, and a dimension on which a whole class has one value is left out. |
| 1.5.0 | 09 on | Three things. Every timed repetition of every battery is sized by the clock; before, the churn, allocation, caster, scatter, gather and barrier batteries fixed their count from a short estimate. The stalled-consumer probes record the CPU a blocked publisher uses (`blocked_cpu_cores`), and one that uses half a CPU or more ranks last on stall resilience; before, `EpochCaster (13-OpenRouter)`, which waits by spinning, was 2nd of 50. The allocation dimension ranks to a tenth of an allocation per publish; before, the 53 or so implementations that allocate nothing were ordered by stray allocations of the runtime, differently in each run. The third was found by comparing runs 09 and 10, so their reports, score files and evaluations were regenerated after they were first committed. |
| 1.6.0 | 11 on | A latency battery times 31 fresh instances (episodes) of 400 publishes each instead of one, and the record carries each episode's median, 90th and 99th percentile and mean; its interval is taken across episodes. The latency dimension ranks the mean over episodes of the 90th percentile; before, the mean of the single episode. Before, two runs of the same code ordered the implementations that drop differently on latency (rank correlation 0.77 in broadcast, 0.42 in payload, runs 09 and 10). |

One more rule came out of comparing runs 07 and 08: a group whose members the
measurement cannot tell apart is not ranked. The caster family's 13
implementations lie within 800 to 1,130 ns with overlapping intervals, and the
two runs ordered them differently. It is reported with its measurements and
the verdict `Not ranked (not distinguishable)` in every run since, and runs 09
and 10 again order it differently (the fastest of run 09 is 10th of 13 in run
10). In run 08 the same rule withholds the ranking of the payload
implementations that drop; in runs 09 and 10 that class is ranked, because the
repair of `EpochTape` removed the one very wide interval that overlapped most
of it.

The fourth is why the broadcast and payload tables of run 07 are withdrawn. Its
measurements are sound (they were taken in isolation); what was wrong was ranking a publish that delivers against one that
does not on the cost of the call. Run 07's evaluation and report now say `Not
ranked (measurement not valid)` for those two families, with the reason.

The third is why nothing before run 07 is ranked: ten implementations were
measured on an idle machine and everything else was not, and the effect was
not small (`Latest3260Terra`: 10 ns per publish at every subscriber count in
run 06, 173 to 450 ns at 4 to 256 subscribers measured alone). The reports,
score files and evaluations of runs 01 to 06 were regenerated; they show every
measurement those runs took, with the verdict `Not ranked (measurement not
valid)` and the reason. The JSONL files are unchanged. `measurement.md`
("Process isolation", "Adoptability score") has the detail.

Runs 01 to 04 also predate the repairs listed in `extraction_audit.md`
("Second pass", "Third pass"). Their numbers for the repaired implementations
describe code that no longer exists.

**A run numbered 08 was taken once before and discarded.** Its settling rule
read a subscriber's count after 2 ms of quiet, which called one implementation
that delivers everything (`GenCaster (06-Meta)`) lossy. The rule was replaced
(50 ms of quiet, and a run that is still receiving after 20 s fails), and the
run was taken again from a clean tree. The committed run 08 is the second one.

**Three implementations changed after run 08, and runs 09 and 10 measure the
repaired code** (`extraction_audit.md`, "Fifth pass" and "Sixth pass").
`Horizon70Meta (70-Meta)` had a lost wakeup: run 08 has it at 132 ns per
publish to 8 subscribers and 8th of 44, runs 09 and 10 at 317 and 286 ns and
17th and 15th. `EpochTape (10-Gemini)` had a reader that starved the writer:
184 µs in run 08, 271 and 294 ns after. `Beacon64GLM (64-GLM)` could deliver a
value under the wrong sequence number: 422 ns in run 08, 544 and 628 ns after
the repair. Run 08's rows for these three describe code that no longer
exists. Its scores are not recomputed under the rules of contract 1.5.0.

**Runs 11 and 12 measure the same code under contract 1.6.0**, so run 12's
evaluation compares the two. Scores agree at rank correlation 0.93 (broadcast,
delivers), 0.98 (broadcast, drops) and 0.96 (payload, delivers); the latency
dimension alone at 0.92, 0.86 and 0.89, where runs 09 and 10 had 0.94, 0.77
and 0.96. The payload implementations that drop are not ranked in either run:
their publish costs separate 30 and 32 of 66 pairs, under the half that a
ranking needs. A first attempt at run 12 was discarded and is not in the
repository, because the machine was used while it measured (`measurement.md`,
"Known limitations").

**Gates.** Runs 05, 08, 10 and 12 have a gates record (slot 06). Runs 06, 07,
09 and 11 do not: nothing recorded what `make verify` reported for their
trees. Run 10's record is the first whose arm64 leg ran (under `qemu-aarch64`,
not on hardware), and run 12's ran the same way. Both were taken some files
after the measured commit; each record and evaluation names the files.

**Families with fewer than five measured implementations are not ranked.** They
are measured, checked for integrity and reported with their raw numbers. See
`measurement.md`, "Adoptability score".

**Quick-profile runs are for checking that the pipeline works**, not for
choosing between implementations: three repetitions per cell. Each
evaluation file reports the run's own within-run variation.

**Machine (all runs):** 2 logical CPUs (Intel Xeon @ 2.60 GHz, shared),
linux/amd64, go1.27.3, GOMAXPROCS=2. Each manifest's fingerprint block has the
exact values and the commit. Nothing has been measured with more parallelism.

**Provenance note (honest fingerprint).** Run 01 predates the commit that
landed the harness: when it executed, the harness existed only in the working
tree, so its meta records the then-current HEAD (`07066ea`, i.e. *before* the
harness landed). The harness sources it ran are exactly those committed as
`887a4d8` (the harness's first commit, 25 minutes after the run). Run 02 (quick
refresh, later the same day) ran after `887a4d8` and records it faithfully.

## Reading the bytes

- JSONL schema, batteries, profiles and verification semantics: `measurement.md`
  at the repository root.
- Any file in a run group can be checked against its manifest:
  `sha256sum <file>` vs `manifest.files[].sha256`.

## Refreshing or adding a run

```sh
make measure            # next rr: run → verify → registry → report + CSVs → evaluation → site
make measure-quick      # the same with the quick profile
make measure-standard
make measure-gates      # run `make verify`, record it as slot 06 of the newest run, regenerate slot 07
make measure-report     # rebuild the report (+ CSVs) of the newest run
make measure-evaluate   # rebuild the evaluation of the newest run
make site               # wire the reports, re-index every manifest, write index.html
git add results-*-*.* index.html
```

- Run sequences are allocated by `make` (highest existing `rr` + 1); override
  with `MEASURE_RUN=N`.
- A run is one process per battery per implementation, and `verify` rejects a
  run that says otherwise. `run --in-process` exists for debugging a battery
  and does not produce a run.
- Measure from a clean, committed tree, so the commit in the run's meta record
  is the code that ran. Commit the run before `make measure-gates`, so the
  gates record also names a clean tree.
- Manifests are regenerated by `make site` (or `make measure-index`), never
  hand-edited. The command verifies every run and exits non-zero if one fails.
  `TestCommittedRunsAreIndexed` and `TestCommittedLandingPageIsCurrent` fail
  when the committed manifests or landing page do not match the committed
  files, so a stale index does not pass `make all`.
- `make clean` deliberately does not touch these committed artifacts.
- The `full` profile is regenerable on demand (`make measure-full`) and is not
  committed.
