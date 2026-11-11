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
| `01` | `results-NN-01-results.jsonl` | JSONL results (schema v1) — one line per meta/metric/trace/skipped/summary record. |
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

**Runs 01 to 04 and later runs are not comparable by score.** Runs 01 to 04
were written by generator 1.0.0, which had two defects:

- its stalled-consumer records carried no `ops_completed`, so the
  stall-resilience dimension (a fifth of the composite) had no data in any of
  them; and
- a dimension with no data scored 100.

Together these gave every implementation full marks for stall resilience. From
generator 1.1.0 the value is recorded, a dimension without data is left out of
the composite and shown as such, and `verify` rejects a metric record that
lacks the value it is named for. The reports and score files of runs 01 to 04
were regenerated with the corrected scoring, so what they show today is the
composite over the dimensions those runs do have data for. Their measurements
are unchanged.

Runs 01 to 04 also predate the repairs listed in `extraction_audit.md`
("Second pass") and the exchange and barrier batteries. Their numbers for the
repaired implementations describe code that no longer exists.

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
