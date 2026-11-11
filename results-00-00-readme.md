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

**Run 08 is the first run whose rankings mean what they say. No family of runs
01 to 06 is ranked, and the broadcast and payload families of run 07 are not
either.** Four defects of the measurement were found, each by looking at what a
suspicious number was made of, and each has its own record contract
(`generator_version` in the meta record and the manifest):

| Contract | Runs | What it fixed |
|---|---|---|
| 1.0.0 | 01 to 04 | Nothing; the starting point. Its stalled-consumer records carried no `ops_completed`, and a dimension with no data scored 100, so every implementation had full marks for stall resilience. |
| 1.1.0 | 05 | `ops_completed` is recorded, a dimension without data is left out of the composite, `verify` rejects a metric record without its value, and families of fewer than five are not ranked. Exchange and barrier batteries. |
| 1.2.0 | 06 | The caster batteries time a `Send` only once every receiver is registered, and count the deliveries. Before, they timed Sends that found no receiver and delivered nothing (2 to 8 ns each). |
| 1.3.0 | 07 | Every battery of every implementation is measured in a process of its own. Before, the run was one process, and the publishers left spinning by the stall probes of the eleventh broadcast implementation held both CPUs for the rest of it. Throughput repetitions are sized by the clock. |
| 1.4.0 | 08 on | The broadcast and payload throughput records say how much of what was published the subscribers received, and the two families are scored in two delivery classes: implementations that delivered everything, and implementations that dropped or coalesced. Before, the record gave the cost of calling Publish only, and in run 07 the eight top broadcast places went to implementations that delivered 0.1 to 79 percent of their values. A tie counts as half a place in a percentile rank, and a dimension on which a whole class has one value is left out. |

One more rule came out of comparing runs 07 and 08: a group whose members the
measurement cannot tell apart is not ranked. The caster family's 13
implementations lie within 800 to 1,130 ns with overlapping intervals, and the
two runs ordered them differently. It is reported with its measurements and
the verdict `Not ranked (not distinguishable)` in both runs. In run 08 the
same rule withholds the ranking of the payload implementations that drop.

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
