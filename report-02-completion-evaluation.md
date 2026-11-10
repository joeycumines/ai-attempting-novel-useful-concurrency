# report-02 — evaluation of the completion work (responses 62, 71–74)

**Verdict.** Every response from 03 to 74 that delivered an implementation is now integrated, tested and listed in the tracker. All correctness gates pass on this branch. The measurements support three narrow claims and no rankings; the limits are in [What the numbers do not show](#what-the-numbers-do-not-show) and the work that was planned and not done is in [Not done](#not-done).

Everything below was produced on one machine: 2 vCPU Intel Xeon @ 2.60 GHz, about 4 GB RAM, linux/amd64, Go 1.27.3, GOMAXPROCS=2 unless stated. The Go toolchain came from the `go-bin==1.27.3` wheel on PyPI, not from go.dev, because the sandbox could not reach go.dev. The repository has no CI, so "passes" means "passed when run locally at the stated commit".

## Correctness gates

`make all` and `make verify` at commit `f5ba759` (the integration commit). The one later code commit, `2723ed4`, changes two measurement-tooling test files; the test and race legs were re-run there, the fuzz leg was not.

| Gate | Command | Result |
|---|---|---|
| Build, vet, tests, site | `make all` | pass |
| Vet: default, race, arm64, 386 | `make verify.vet` | pass |
| Tests at GOMAXPROCS 1, 2, 4 | `make verify.test` | pass, 139 s |
| Race detector | `make verify.race` | pass, 67 s |
| Allocation assertions | `make verify.alloc` | pass, 21 passing results |
| Fuzzing, 15 targets × 10 s | `make verify.fuzz` | pass, no new failing input |
| Measurement verification | `bigbuff-measure verify` on run 04 | pass |

Fuzz executions in the 10 s windows ranged from about 58 000 to 148 000 for twelve targets. Three were far lower: `FuzzBitmap72Gemini` 8 812, `FuzzUnifiedBroadcaster` 2 616 and `FuzzScenarioBroadcast` **6**. One execution of `FuzzScenarioBroadcast` drives every broadcast attachment, so six executions is a smoke test, not a fuzz campaign.

## What the work found

**Two defects were already on `main`.** Both were found by running the existing tests harder, not by the new code.

- `Broadcaster58Spark.Next` livelocked whenever GOMAXPROCS was 1: `TestStandardHarness_ConcurrentChurn` hung every time at `-cpu 1`. The default 2-CPU run never showed it.
- `TelemetryRing63Qwen` lost wakeups: 3 guard timeouts in 15 000 runs of its scenario test on the untouched tree, none in 60 000 runs after the repair.

**Two of the four new responses did not build as delivered.**

| Response | As delivered | Repairs made |
|---|---|---|
| 71 | builds, vets, own tests pass | none |
| 72 | does not compile; arm64 assembly does not assemble; with the compile error fixed, one of its tests deadlocks and one panics by construction | 6 to the broadcaster, 3 to the exchanger, 6 to the bitmap kernels |
| 73 | builds; its test file does not compile | 2 |
| 74 | builds, vets, own tests pass | none |

Each repair, and each behaviour that was left as designed and written down instead, is in `extraction_audit.md`.

**One claim in a response is false and is pinned by a test:** response 74 says abandoning `Next` releases the broadcaster. It does not (`TestFanout74Deepseek_AbandonedNextDoesNotReleaseSend`).

## Measurements

### Deadline-bounded barrier broadcast (Go benchmark)

One operation is one send that every member receives and acknowledges. Median of 5 runs of 20 000 operations, with the range.

| Implementation | 1 member, ns | 8 members, ns | 64 members, ns | allocs/op at 64 |
|---|---|---|---|---|
| `PhaseCaster72Gemini` (acknowledged) | 1 169 (1 146–1 280) | 4 864 (4 662–4 954) | 46 922 (45 462–48 365) | 2 |
| `Cohort73Meta` | 1 226 (1 165–1 271) | 5 159 (5 005–5 451) | 52 132 (49 072–58 222) | 1 |
| `Fanout74Deepseek` | 771 (678–887) | 5 984 (5 420–6 256) | 54 276 (52 778–57 248) | 0 |
| `BaselineDeadlineBarrier` (baseline) | 1 639 (1 398–2 237) | 11 851 (10 823–12 613) | 111 061 (104 312–131 843) | 67 |

- All three candidates take about half the baseline's time at 8 and 64 members, and the baseline is the only one whose allocations grow with membership.
- The three candidates cannot be ranked against each other. In this set of five runs response 72's range at 64 members sits just below the other two (10–14 % by median); in an earlier set of five on the same code all three ranges overlapped.
- Cost is close to linear in members for all four (about 0.8 µs per member for the candidates), which is the cost of waking a goroutine and handing back. On two CPUs none of the designs has room to show a parallel advantage.

### Symmetric pairwise exchange (Go benchmark)

One operation is one caller's completed exchange. Median of 5 runs of 20 000 operations, with the range.

| Implementation | 2 parties, ns | 8 parties, ns | 64 parties, ns | allocs/op |
|---|---|---|---|---|
| `SymExchanger72Gemini`, default arena | 1 023 (894–1 173) | 560 (454–606) | 683 (606–722) | 0 |
| `SymExchanger72Gemini`, one-slot arena | 1 335 (1 063–2 659) | 1 504 (1 445–1 917) | 7 095 (5 898–7 348) | 0 |
| `BaselineExchanger` (baseline) | 319 (309–436) | 540 (447–566) | 506 (462–610) | 1 |

- **The textbook baseline — one mutex and one waiting offer — is about three times faster than response 72 with two parties (2.5 times in an earlier set of five), and no slower with 8 or 64.** The response's advantage on this machine is one avoided 72-byte allocation per exchange.
- The arena matters to the response's own design: with one slot it is ten times slower at 64 parties.
- An arena exists to spread contention across cores. Two CPUs cannot produce that contention, so this result says the response is not worth its complexity *here*; it does not say what happens at 16 or 64 cores.

### Broadcast family (measurement run 04, quick profile)

Run 04 (`results-04-*`, 1 755 records, verified, produced from `2723ed4`) covers 94 broadcast-family implementations. The composite score rewards throughput and latency, so lossy rings lead and synchronous acknowledged designs trail by construction.

| Implementation | Score | Rank of 94 | Range over three quick runs |
|---|---|---|---|
| `Ring62Haiku` | 83 | 9 | 83–84 |
| `TelemetryRing63Qwen` | 83 | 10 | 83–84 |
| `ChanRing71Fable` | 80 | 16 | 80–85 |
| `Cohort73Meta` | 45 | 66 | 44–45 |
| `PhaseCaster72Gemini` (acknowledged) | 45 | 68 | 44–45 |
| `PhaseCaster72Gemini` (one-phase) | 44 | 74 | 42–44 |
| `Fanout74Deepseek` | 39 | 81 | 38–43 |
| `BaselineBarrierPubSub` (closest baseline to 72–74) | 33 | 88 | 31–33 |

- Responses 62, 63 and 71 land in the same band as the other lossy rings.
- Responses 72, 73 and 74 score above the baseline with the same delivery guarantee and well below the lossy rings. The first comparison is the one that means something for them; their rank among 94 mostly reflects what the score rewards.

The "three quick runs" are run 04 and two earlier quick runs of the same implementations that were not committed (the first was lost with its commit when the sandbox was reset; the second was superseded by the tooling fix in `2723ed4`).

## What the numbers do not show

- **Quick-profile scores move by more than the gaps between neighbours.** Across the three quick runs, `Latest3260Terra` scored 92, 81 and 90 (rank 1, 10, 1) and `ChanRing71Fable` 83, 85 and 80 (rank 9, 5, 16). No ordering inside a ten-point band should be read from one quick run. A standard-profile run takes 30–60 minutes on this machine and was not done.
- **The fan-in family's ranking is noise.** It has two members and the score is relative, so the slower one gets 0 for throughput whatever the margin. Run 03 put the baseline ahead (135 vs 183 ns per merged value); run 04 puts the candidate ahead on score while its median is *behind* (146 vs 115 ns), each figure being three samples with a coefficient of variation of 0.4–0.8.
- **Two scatter-family verdicts in the report are an artifact, and predate this work.** `StableRing32Nano` and `BaselineBoundedScatterQueue` are shown as "Do not adopt (integrity failure)". Both evict the oldest item when full, by design and with a counter; the scatter battery counts every item not delivered exactly once as a violation. The verdict reports the battery's assumption, not a defect. It was the same in run 03 and was not fixed.
- **Exchange and barrier results are Go benchmarks, not measurement batteries.** They have no latency distribution, no confidence intervals and no place in the interactive report.
- **Nothing here was run on more than two CPUs or on arm64.** The arm64 assembly for responses 64 and 72 is assembled and vetted, never executed.
- **Fuzzing was short** (10 s per target), and for the broadcast scenario target effectively absent.

## Not done

Planned at the start and not completed:

1. **A measurement battery for the exchange and barrier scenarios.** They are tested, benchmarked and fuzzed, and now appear in the registry dump, but produce no JSONL records.
2. **Registering every runnable as a scenario case.** Of 659 top-level runnables, 121 are registered cases; 513 are unit tests in an implementation's own test file and 25 are listed harness functions. A test now enforces that every runnable is one of the three; it does not make the 513 comparable across implementations.
3. **Dedicated unit tests for responses 28, 29, 30, 31 and 33**, and the further lifecycle review of 52. Their tracker rows now say exactly what does cover them.
4. **Wiring the response-72 bitmap kernels into its broadcaster.** The response's sketch for that does not compile; the kernels are tested stand-alone and excluded from measurements.
5. **A generated evaluation report.** This file was written by hand from the logs of the commands above; `verify.mk` produces the logs (`VERIFY_LOG=<dir>`) but nothing assembles them.
6. **A standard-profile measurement run**, and the scatter-battery fix described above.
7. **A fix for `make measure` stopping at the index step** when older runs fail re-verification (described in `results-00-00-readme.md`); only the workaround is documented.

## Reproducing

```
make all                      # build, vet, test, site
make verify                   # vet variants, GOMAXPROCS 1/2/4, race, alloc, fuzz (about 8 minutes here)
make verify-bench VERIFY_BENCH='BenchmarkScenario(Exchange|Barrier)' VERIFY_BENCHTIME=20000x
make measure-verify           # verifies results-04-01-results.jsonl
```
