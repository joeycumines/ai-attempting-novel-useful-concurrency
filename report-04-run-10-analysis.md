# Analysis of measurement runs 09 and 10

Runs 09 and 10 measured the same code twice, ten minutes apart, and are the first pair of runs here that can be checked against each other. The rankings of all four ranked groups came out nearly the same both times (rank correlation 0.91 to 0.98), and no implementation changed delivery class. This report says what the two runs show, what was repaired since run 08 and what that did to the numbers, and what is still not done. It was written by hand from `results-10-01-results.jsonl`, `results-09-01-results.jsonl` and the generated evaluations `results-10-07-evaluation.md` and `results-09-07-evaluation.md`, and it is not regenerated.

## What to take from it

- **The rankings reproduce.** Between the two runs the broadcast classes have rank correlations of 0.97 and 0.98, and the payload classes 0.96 and 0.91. 103 of the 113 ranked implementations keep their verdict. The five highest places of each broadcast class are the same five implementations in both runs.
- **Not every part of a score reproduces equally.** Throughput, allocations, churn, scaling and stall resilience reproduce at 0.84 or better in every group but one. Latency does not in the classes that drop: 0.77 in broadcast and 0.42 in payload. It carries 14 percent of the weight there, and that share is mostly noise.
- **Two implementations were repaired and one row of run 08 was wrong because of it.** `EpochTape (10-Gemini)` published in 184 µs at 8 subscribers in run 08; that was a defect of the port, and it now publishes in 271 and 294 ns. `Beacon64GLM (64-GLM)` could hand a reader a value under the wrong sequence number; the defect is in the original response, and after the repair a publish costs 544 and 628 ns against 422 ns in run 08.
- **`EpochCaster (13-OpenRouter)` is no longer near the top.** It waits for a stalled subscriber by spinning, which run 08 could describe and could not score. The stalled-consumer probe now measures the CPU a blocked publisher uses (1.04 to 1.05 CPUs for this one, under 0.01 for every other), and a publisher that spins ranks last on stall resilience. It went from 2nd of 50 to 9th.
- **The caster family is still not ranked, and the two runs show why.** The implementation that was fastest in run 09 was 10th of 13 in run 10.
- **The arm64 code has now run as an arm64 program, under an emulator.** That shows the instructions execute correctly inside the real test suite. It still says nothing about memory ordering on an arm64 processor.
- **A score is a position among peers on one 2-CPU machine.** Two runs agreeing is evidence about that machine on that day.

## The runs

| | Run 09 | Run 10 |
|---|---|---|
| Profile, seed | standard, 1 | standard, 1 |
| Commit measured | `6631abcd06ed` | `633423b277d3` |
| Started (UTC) | 2026-11-12 00:23 | 2026-11-12 00:33 |
| Duration | 582 s | 581 s |
| Record contract | 1.5.0 | 1.5.0 |
| Records | 2,183 metric, 201 trace, 25 skipped | 2,183 metric, 201 trace, 25 skipped |
| Harness failures | 0 | 0 |
| Payload reads checked for tearing | 1,070,624, 0 torn | 1,069,659, 0 torn |
| Exactly-once records | 135, 0 violations | 135, 0 violations |

Both ran on the same machine as run 08: 2 logical CPUs, Intel Xeon 2.60 GHz, a shared virtual machine, linux/amd64, go1.27.3, GOMAXPROCS 2, one process per battery per implementation (1,041 processes). The two commits differ only in the files of run 09 itself, which were committed between the runs; no Go source file differs. Each run covers every implementation in its registry dump. The 25 skip records (9 distinct) are the same as in run 08.

Run 10 is the one the tables below quote, with run 09's place beside it.

## Do the rankings reproduce?

This is the question run 08 could not answer, and the evaluation of run 10 now answers it in a section of its own ("Agreement with the previous run"). Rank correlation is Spearman's: 1 is the same order, 0 is no relation.

| Group | Ranked in both | Rank correlation of scores | Same verdict | Median places moved | Most places moved | Top five in common |
|---|---|---|---|---|---|---|
| broadcast, delivers every value | 50 | 0.97 | 46 of 50 | 2 | 8 (`EpochBroadcast54Terra`, 18th to 26th) | 5 |
| broadcast, drops or coalesces | 44 | 0.98 | 40 of 44 | 1 | 10 (`SeqCaster23Meta`, 39th to 29th) | 5 |
| payload, delivers every value | 7 | 0.96 | 6 of 7 | 0 | 1 | 4 |
| payload, drops or coalesces | 12 | 0.91 | 11 of 12 | 1 | 4 (`ChanBeacon_c64`, 4th to 8th) | 4 |

The top of each table is stable and the middle is not exact: an implementation in the middle of a broadcast class can move eight or ten places between two runs of the same code. Read a place in the middle of a class as "about there".

The same correlation for each dimension of the score:

| Group | Throughput | Latency | Allocations | Churn | Scaling | Stall resilience |
|---|---|---|---|---|---|---|
| broadcast, delivers every value | 0.97 | 0.94 | 1.00 | 0.97 | 0.94 | 1.00 |
| broadcast, drops or coalesces | 0.99 | 0.77 | 1.00 | 0.96 | 0.95 | not scored (all tied) |
| payload, delivers every value | 0.96 | 0.96 | no battery | 0.89 | 0.89 | 1.00 |
| payload, drops or coalesces | 0.75 | 0.42 | no battery | 0.84 | 0.95 | not scored (all tied) |

Latency is the weak dimension, as the run 08 analysis suspected without being able to show it. `publish_latency_ns` has a median coefficient of variation of 202 percent across its samples in run 10. In the classes that drop, its ranks in one run say little about its ranks in the next. It has not been changed or removed; that is a decision about what the score should be, and it is listed under "What has not been done".

The measurements under the ranks agree as follows. The headline cost is one publish or send at 8 subscribers.

| Family | Implementations | 95% intervals of the two runs overlap | Median ratio | 90th percentile ratio | Largest ratio |
|---|---|---|---|---|---|
| broadcast | 94 | 81 (86%) | 1.08× | 1.23× | 1.61× (`SelectBroadcaster49Qwen`, 608 and 982 ns) |
| caster | 13 | 11 (85%) | 1.17× | 1.30× | 1.40× (`BaselineRendezvousCaster`, 979 and 1,370 ns) |
| payload | 19 | 18 (95%) | 1.10× | 1.23× | 3.40× (`EpochCasterGemini_c64`, 645 and 190 ns) |

A typical figure moves 8 to 17 percent between runs. A difference between two implementations that is smaller than that is not established by either run. One payload figure moved by a factor of 3.4, and I have not looked for the cause.

The allocation dimension reproduces at 1.00 because of a correction made after these two runs showed it did not. See "Allocations were ranked on noise" below.

## Broadcast, implementations that deliver every value (50)

Verdicts, the same in both runs: 6 Strong candidate, 6 Situational, 12 Niche use only, 26 Avoid for most uses.

| Rank | Implementation | Score | Publish cost, 8 subscribers | Run 09: rank (score) |
|---|---|---|---|---|
| 1 | RingPubSub (18-ChatGPT) | 89 | 356 ns | 1 (89) |
| 2 | BaselineQueuedFanout (baseline) | 86 | 978 ns | 3 (85) |
| 3 | GenCaster (06-Meta) | 85 | 235 ns | 3 (85) |
| 4 | Carillon (21-ClaudeHaiku) | 84 | 403 ns | 2 (86) |
| 5 | ChanRing47Gemini (47-Gemini) | 80 | 611 ns | 5 (80) |
| 6 | ChanHub (15-Nemotron) | 77 | 1.54 µs | 6 (77) |
| 7 | BaselineBoundedReplay (baseline) | 73 | 2.57 µs | 8 (72) |
| 8 | QueuedFanout24Meta (24-MetaAI) | 72 | 2.10 µs | 7 (74) |
| 9 | EpochCaster (13-OpenRouter) | 69 | 289 ns | 9 (70) |
| 10 | Fanout45ChatGPT/buffered (45-ChatGPT) | 68 | 1.03 µs | 11 (67) |

The other baselines of this class: ChanPubSub 13th (58, 3.77 µs), BaselineBufferedBroadcast 20th (52, 4.76 µs), BaselineBarrierPubSub 46th (31, 8.37 µs).

Three things to read with this table.

- **The first four are within five points of each other and changed order between the runs.** `RingPubSub` is first in both. Places 2 to 4 are `BaselineQueuedFanout`, `GenCaster` and `Carillon` in one order or another. The table supports "these four lead" and not an order among the last three.
- **`GenCaster`, `BaselineQueuedFanout` and `QueuedFanout24Meta` never block the publisher because they buffer without bound.** They score 98 on stall resilience for completing every publish behind a subscriber that never reads. The memory that costs is reported by the stalled-consumer battery and is not part of the score.
- **`EpochCaster` has the second lowest publish cost of the class (289 ns) and is 9th.** Its stall-resilience score is 0. That is the new rule at work; "One subscriber that never reads" below has the detail.

A baseline is second in run 10 and third in run 09. `BaselineQueuedFanout` was 6th (74) in run 08. Run 08 is under the earlier contract and was not re-scored, so its places are not directly comparable; two of the differences are that the allocation dimension no longer orders implementations by stray allocations and that `EpochCaster` no longer scores on stall resilience.

## Broadcast, implementations that drop or coalesce (44)

Verdicts in run 10: 7 Strong candidate, 7 Situational, 10 Niche use only, 20 Avoid for most uses (run 09: 8, 8, 7, 21). Every member completes the stalled-consumer probe, so that dimension distinguishes none of them and is left out of the score.

| Rank | Implementation | Score | Publish cost, 8 subscribers | Delivered, 8 subscribers | Delivered, least over all cells | Run 09: rank (score) |
|---|---|---|---|---|---|---|
| 1 | RingPubSub TrySend (18-ChatGPT) | 89 | 39 ns | 0.93% | 0.44% | 1 (89) |
| 2 | ChanRing71Fable (71-Fable) | 87 | 65 ns | 6.5% | 0.78% | 3 (87) |
| 2 | HorizonRing59Gemini (59-Gemini) | 87 | 137 ns | 63% | 2.2% | 2 (88) |
| 4 | Pulse61Deepseek (61-DeepSeek) | 81 | 109 ns | 0.52% | 0.04% | 5 (81) |
| 5 | Horizon66Gemini (66-Gemini) | 79 | 208 ns | 40% | 1.1% | 4 (86) |
| 6 | StripedCombiner59Gemini (59-Gemini) | 78 | 206 ns | 82% | 3.6% | 6 (77) |
| 7 | RingFanOut14Xing (14-Xing) | 77 | 146 ns | 2.0% | 1.0% | 8 (75) |
| 8 | Arcadia (30-dots3) | 74 | 87 ns | 3.4% | 0.20% | 9 (71) |
| 8 | Ring62Haiku (62-Haiku) | 74 | 194 ns | 27% | 1.5% | 10 (69) |
| 10 | BaselineAsyncFanout (baseline) | 73 | 98 ns | 0.73% | 0.23% | 6 (77) |

The other baselines of this class: BaselineFanout 33rd (35, 660 ns, 21% delivered), BaselineLatestBroadcast 37th (25, 1.06 µs, 14% delivered).

What the run 08 analysis said about this class still holds, and the second run does not weaken it. The battery publishes as fast as it can, with no pause, to subscribers that do nothing but receive. Most of these implementations then deliver a few percent of what is published, and the score ranks the cost of the call without weighing the share delivered. `Pulse61Deepseek` at 0.52 percent and `StripedCombiner59Gemini` at 82 percent are two places apart. Read the delivered columns before the score. The delivered share itself moves between runs with the scheduler: `HorizonRing59Gemini` delivered 44 percent at 8 subscribers in run 09 and 63 percent in run 10.

No battery measures these implementations with subscribers that keep up, which is the case they are built for.

## Payload (19)

The payload family carries a four-word value and checks every read for tearing. It has no allocation battery.

Implementations that deliver every value (7): GenCaster 95 (250 ns), RingPubSub_c64 76 (404 ns), EpochCaster13Crouton_r64 57 (322 ns), ChanFeed_block_c64 51 (1.03 µs), ChanHub_c16 35 (1.77 µs), ChanRelay_w16 26 (2.05 µs), ChanRelay_sync 10 (4.95 µs). Run 09 has the same order except that the 5th and 6th change places. With seven members a percentile rank moves in steps of 17 points, so the order is supported and the distances are not.

Implementations that drop (12) are ranked in these runs, where run 08 did not rank them. In run 08 the measurement told apart 29 of the 66 pairs, under the half that a ranking needs. In runs 09 and 10 it tells apart 38 and 49. The difference is `EpochTape_c64`: its publish cost in run 08 was 14.7 µs with a very wide interval, which overlapped most of the class, and after the repair it is 306 and 277 ns. The first three places are the same in both runs: RingFanOut14Xing_c64 91 (106 ns, 1.3% delivered), BaselineAsyncFanout_c64 83 (111 ns, 1.5%), EpochTape_c64 64 (277 ns, 38%). Below them this is the least reproducible of the four groups (0.91, with throughput at 0.75 and latency at 0.42), and `ChanBeacon_c64` was 4th in one run and 8th in the other. Twelve members and that much movement support the top three and the bottom (`BaselineFanout_c64`, 12th and 10th) and little between.

## Each scenario against its baseline

The evaluation compares the implementations attached to each scenario as a good fit with that scenario's baseline, on publish cost at 8 subscribers, and only within the baseline's delivery class. "Faster" means the 95 percent intervals do not overlap. The three counts are faster / not distinguishable / slower.

| Scenario | Baseline (cost in run 10) | Compared | Run 10 | Run 09 |
|---|---|---|---|---|
| lockstep-barrier-lossless | BaselineBarrierPubSub (8.37 µs) | 20 | 11 / 9 / 0 | 11 / 1 / 8 |
| synchronous-handoff-no-barrier | BaselineBarrierPubSub (8.37 µs) | 14 | 13 / 0 / 1 | 13 / 0 / 1 |
| buffered-lossless-backpressured | BaselineBufferedBroadcast (4.76 µs) | 10 | 8 / 1 / 1 | 7 / 2 / 1 |
| unbounded-catchup-replay | BaselineBufferedBroadcast (4.76 µs) | 1 | 1 / 0 / 0 | 1 / 0 / 0 |
| nonblocking-lossless-overflow | BaselineQueuedFanout (978 ns) | 1 | 0 / 0 / 1 | 0 / 0 / 1 |
| bounded-catchup-replay | BaselineBoundedReplay (2.57 µs) | 1 | 0 / 0 / 1 | 0 / 0 / 1 |
| bounded-async-fanout-dispatch | BaselineAsyncFanout (98 ns) | 1 | 0 / 0 / 1 | 0 / 0 / 1 |
| bounded-ring-lossy-detectable | BaselineFanout (660 ns) | 25 | 19 / 5 / 1 | 22 / 2 / 1 |
| large-fanout-scalability | BaselineFanout (660 ns) | 18 | 14 / 4 / 0 | 15 / 3 / 0 |
| cancellation-and-close-lifecycle | BaselineFanout (660 ns) | 22 | 17 / 5 / 0 | 19 / 3 / 0 |
| ephemeral-churn-membership | BaselineFanout (660 ns) | 32 | 20 / 8 / 4 | 23 / 5 / 4 |
| zero-subscriber-and-edge-lifecycle | BaselineFanout (660 ns) | 33 | 21 / 8 / 4 | 24 / 5 / 4 |
| latest-value-conflating-watch | BaselineLatestBroadcast (1.06 µs) | 14 | 7 / 5 / 2 | 8 / 3 / 3 |
| wide-payload-integrity (payload) | BaselineFanout_c64 (973 ns) | 11 | 9 / 2 / 0 | 8 / 2 / 1 |

The counts of "faster" hold between the runs to within three. The counts of "slower" do not always: in `lockstep-barrier-lossless`, eight implementations were slower than the baseline in run 09 and none was in run 10, where the baseline itself measured 8.37 µs instead of 7.57 µs and those eight fell inside its interval. A "slower than the baseline" from one run is weak evidence.

Most model-written implementations beat the baseline of the scenario they were attached to, in both runs, as in run 08. The same two cautions apply. Each baseline is the plainest implementation of its contract, so beating one is a low bar. And the number is the steady-state publish cost only: it does not time what defines most scenarios (churn, close, replay, a deadline).

Three baselines are still not beaten by anything attached to them: BaselineAsyncFanout, BaselineBoundedReplay and BaselineQueuedFanout. Each has one comparable attachment, and in both runs it is slower than its baseline.

## The small families and the caster family

None of these is ranked. Figures are means; "distinguishable" means the 95 percent intervals do not overlap. Where the two runs disagree, the text says so.

- **Caster (13), not ranked because the measurement does not separate them.** A send to 8 receivers costs 728 to 1,366 ns in run 09 and 736 to 1,370 ns in run 10, and the intervals tell apart 8 and 10 of the 78 pairs. `ChanCaster14Xing` was the fastest in run 09 (728 ns) and 10th in run 10 (892 ns). `BaselineRendezvousCaster` was 9th and then last. Ranking them would have reported a different winner each time, which is what the rule is for.
- **Barrier (4).** The three model-written barriers send faster than the baseline to 8 receivers in both runs, with intervals that do not overlap it: 5.3 to 8.1 µs against 10.1 µs in run 09, and 4.9 to 5.3 µs against 9.3 µs in run 10. At 32 receivers they take 22 to 25 µs against the baseline's 51 µs in run 10; in run 09 they take 27 to 38 µs against 52 µs, and one interval (`PhaseCaster72Gemini_phased`) reaches the baseline's. Deadline overshoot is 280 to 462 µs, and no difference among the four holds in both runs (`Cohort73Meta` is below the baseline in run 10 and above it, inside the intervals, in run 09).
- **Exchange (3).** With 2 goroutines the baseline exchanger is about twice as fast as both configurations of `SymExchanger72Gemini` in both runs (848 and 887 ns against 1.5 to 1.8 µs). With 8 the baseline and the default configuration are close: 1,323 and 1,213 ns in run 09, where the intervals put the default slightly ahead, and 1,005 and 1,009 ns in run 10, where they do not differ. The `_a1` configuration takes 3.5 to 3.7 µs. Run 08, under the earlier contract, had the baseline ahead at 8 (1.88 against 2.27 µs); these runs do not show that.
- **Fan-in (2).** `EdgeFanIn68Chatgpt` drains a batch of 32 in 139 and 160 ns against the baseline's 877 and 857 ns, distinguishable in both runs. Its merge cost is lower too (132 and 123 ns against 225 and 260 ns), distinguishable in run 10 and not in run 09.
- **Scatter (4).** `ChanScatter` and `BaselineScatterQueue` are within 15 percent of each other at one receiver (322 to 367 ns); run 09 tells them apart and run 10 does not. The bounded baseline and `StableRing32Nano` are slower at one receiver in both runs (10.5 µs, and 2.9 to 4.3 µs).
- **Gather (2).** The intervals are half the mean to more than the whole of it (±53 to ±113 percent) and overlap in both runs. Nothing can be said.

## One subscriber that never reads

47 of 94 broadcast implementations complete all 1,000 publishes behind a subscriber that never reads, and 47 block (0 to 81 publishes completed). In payload, 13 complete and 6 block. The counts are the same in runs 08, 09 and 10. Blocking is the specified behaviour of a lossless lock-step design and a defect only in one that claims not to block.

New in these runs: the probe reads the process's CPU time for 100 ms while the publisher is blocked. Of the 53 blocked publishers, 51 used between 0.00 and 0.01 CPUs: they are parked. Two used 1.04 to 1.05: `EpochCaster (13-OpenRouter)` in the broadcast family and `EpochCaster13Crouton_r64`, the same implementation, in payload. Its `Send` waits for a full ring in a loop around `runtime.Gosched`. The source was left as the response delivered it. That is deliberate: the behaviour is the response's design, not an error of the port, and the measurement is what should say so. It now does, in the record (`blocked_cpu_cores`), in the evaluation, in the report's score table ("· spins"), and in the score: a blocked publisher that uses half a CPU or more ranks below every publisher of its group that parked or completed.

The effect on its places: broadcast, 2nd of 50 (92) in run 08 and 9th (70, then 69) in runs 09 and 10; payload, 2nd of 7 (76) and then 3rd (59, then 57).

## What changed since run 08, and what it did

### Source repairs

- **`EpochTape (10-Gemini)`: a reader spun against a writer it was starving.** A subscriber that found a slot being written retried at once, without yielding. With more subscribers than CPUs the retrying readers held the CPUs the writer needed to finish. Run 08 measured the result and I recorded it as unexplained: 184 µs a publish at 8 subscribers and 597 µs at 256, with a coefficient of variation up to 2.2. The reader now yields when it finds a write in progress. Runs 09 and 10: 271 and 294 ns at 8 subscribers, 347 and 340 ns at 256, coefficient of variation at most 0.18. It went from 38th of 44 (29, Avoid) to 12th and 13th (63 and 62, Situational). The defect was in the port's reading loop; a regression test fails within its time limit at GOMAXPROCS 1, 2 and 4 when the yield is removed.
- **`Beacon64GLM (64-GLM)`: a value could be delivered under the wrong sequence number.** Each slot counted its writes, and a reader turned the count back into a sequence number on the assumption that writers reach a slot in the order they claimed sequences. A producer descheduled between claiming and writing could be overtaken by a whole ring of 256; the newer value was then labelled with the older sequence, and the late write replaced it under the newer one. Two overlapping writes could also let a torn value through. The response's own `TestMultiProducerStress` failed in 2 of 90 rounds on amd64. The defect is in the response as delivered, not in the port: its file describes the slot counter as a count of writes (line 1096) and the reader's conversion of that count into a sequence (from line 1590). The slot's counter now names the sequence itself, a writer takes the slot before writing, and a writer that finds a newer value there leaves it. 450 rounds then passed with no failure, and a new test puts a producer in that position deterministically and fails on the old code. The cost: a producer can now wait for another producer in the same slot, and a publish at 8 subscribers went from 422 ns in run 08 to 544 and 628 ns. It was 24th of 44 (42) in run 08 and is 29th (38, then 37). `main` before this change still has the defect, so its race and test gates can fail intermittently.
- **`Horizon70Meta (70-Meta)` now has a row that describes the code.** It was repaired after run 08 (a lost wakeup), and run 08's row was too favourable: 132 ns, 8th of 44, score 73. Measured after the repair: 317 and 286 ns, 17th and 15th, score 59 in both runs (Niche use only).

No other implementation's source changed.

### Measurement changes (record contract 1.5.0)

- **Every timed repetition is sized by the clock.** In run 08 only the broadcast and payload throughput and latency batteries were. The allocation, churn, zero-subscriber, caster, scatter, gather and barrier batteries still fixed their operation count from a short estimate taken before the timed loop, which is wrong for any operation whose cost changes once something else wakes up. The estimating function is deleted; every battery now runs batches until its target time has passed.
- **The stalled-consumer probes measure the CPU of a blocked publisher**, as described above.
- **Allocations were ranked on noise, and are now ranked to a tenth of an allocation per publish.** This was found by comparing runs 09 and 10 and fixed afterwards. The battery counts the process's allocations over the timed publishes, which includes the runtime's own. 53 and 54 of the 94 broadcast implementations measured under 0.01 allocations per publish in the two runs, and for those the stray count was the whole figure: `RingPubSub` measured 0.00006 and 0.00002 and `EpochCaster` 0.00004 and 0, and their allocation scores moved from 76 to 92 and from 80 to 98 on that alone. The largest such figure is 0.016 (`ChanPubSub17Gemini` in run 09, under 0.01 in run 10). Figures are now rounded to 0.1 before ranking, and implementations with the same rounded figure tie. This changes the score and not the measurement: the reports, score files and evaluations of runs 09 and 10 were regenerated from their unchanged results files, and the reports show the figure as measured.
  - What it changed: the rank correlation of the allocation dimension between the runs went from 0.94 and 0.97 to 1.00 in the two broadcast classes. The correlation of the whole score was 0.97 in both before, and is 0.97 and 0.98. Before the rule an implementation's allocation score differed between the two runs by up to 29 points (`ChanPubSub47Gemini`); now by up to 12 (`Vecast`), and by 3 or less for every other. Re-deriving moved the total score of an implementation in run 10 by at most 5 points (`EpochCaster` 74 to 69, `BaselineQueuedFanout` 81 to 86, `RingPubSub` 93 to 89). So the rule steadied one dimension and changed the overall agreement very little.
  - What it costs: three implementations measured 0.01 to 0.03 in both runs (`BaselineQueuedFanout`, `Pulse61Deepseek`, `Ring62Haiku`), which is probably a real allocation once in 35 to 100 publishes. They now tie with the implementations that never allocate.
  - The scores of runs 09 and 10 in the repository are therefore not the ones first committed with those runs. The first versions are in the history at commits `633423b` and `a85bad4`.

Runs under earlier contracts keep the scores their own reports give them; neither new scoring rule is applied to them.

### Tooling

- **The evaluation compares a run with the one before it** when both are under the same contract and profile, and says so when they are not. Run 09's evaluation says it was not compared with run 08 (contracts 1.5.0 and 1.4.0). Run 10's has the tables quoted above.
- **`make verify-arm64` runs under an emulator**, and `make measure-gates` runs it when one is named. See the next section.
- **Tests for two earlier repairs.** The repair of the response 57 adapter (a deadlock between a blocked `Subscribe` and an unsubscribe) had no test that failed without it; it has one now, and it fails when the old lock is put back. The response 40 repairs were checked the same way: with the wait for in-flight deliveries disabled, its existing tests report a data race. That needed no new test.

## Correctness gates

Run 10 has a gates record, `results-10-06-gates.json`: `make verify` on commit `d5368ab51dd6`, clean tree, recorded 2026-11-12 00:57 UTC. Every leg passed.

| Leg | Result | Detail |
|---|---|---|
| vet | pass | default, race, arm64 and 386 builds |
| test | pass | the suite at GOMAXPROCS 1, 2 and 4; 193.5 s |
| race | pass | the suite under the race detector; 134.2 s |
| alloc | pass | 32 allocation assertions |
| fuzz | pass | 15 of 15 targets, 10 s each |
| arm64 | pass | the suite as an arm64 binary under qemu-aarch64 5.2.0; 216.8 s |

Three things limit what that table says.

- **The gates ran on a later commit than the one measured.** Run 10 was measured at `633423b277d3`. Five files other than results differ between the two commits, and the record names them: `measure_evaluate_repro_test.go`, `measure_isolate_run_test.go`, `measure_report_markup_test.go`, `measure_report_test.go` and `scenario_registry_test.go`. All five are measurement tooling and its tests (the allocation rule and the comparison between runs). No implementation and no adapter differs.
- **Run 09 has no gates record.** It was measured at `6631abcd06ed`. On that commit I ran the suite, the suite under the race detector and the arm64 leg under the emulator by hand, and they passed, but nothing recorded it in the form the evaluation reads, so run 09's evaluation says no record exists. Runs 06 and 07 have none either.
- **The arm64 leg is an emulation.** Until this change `make verify-arm64` had never run to completion; the gates of run 08 record it as not run. It now runs the whole suite as an arm64 binary under `qemu-aarch64` (the PyPI package `shellphish-qemu`, on the amd64 host), including the tests of the two implementations with arm64 assembly (responses 64 and 72), and passes. That shows the assembly is encoded correctly and behaves inside a running Go program. The emulator executes on an x86-64 processor, which orders memory more strongly than arm64 does, so a fence that is missing or wrong on arm64 cannot fail there. Nothing in this repository has run on arm64 hardware.

One gate run is one sample. The Beacon defect above failed about one stress round in 45; a suite that passes once does not show the absence of a defect of that kind.

## A line in the logs that is not a failure

Every run of the test suite prints `bigbuff-measure: FAIL: Phantom: a Send reported a delivery that no receiver took`. It is the expected output of `TestCasterBatteriesFailOnAPhantomDelivery`, which hands the caster batteries a deliberately wrong implementation named `Phantom` and checks that they reject it. It is a self-test of the measurement, and the test passes when the line appears.

## What has not been done

- **Nothing was measured on more than 2 CPUs.** Every claim about scaling here is about subscriber count on 2 CPUs. Contention behaviour with real parallelism is unknown.
- **The arm64 assembly has not run on arm64 hardware.** It has run inside the whole test suite under qemu, on an x86-64 host. That cannot show a missing or wrong memory fence.
- **The two runs were taken ten minutes apart on one machine.** They show that the measurement repeats under the same conditions. They do not show what another machine, another day or another Go version would give.
- **The latency dimension is still in the score.** It is now shown to reproduce poorly in the two classes that drop (0.77 and 0.42). I have not removed or reweighted it.
- **`EpochCaster`'s spinning `Send` is measured and scored, not changed.** Anyone adopting it gets a publisher that holds a CPU while a subscriber is stalled.
- **One payload figure moved by 3.4 times between the runs** (`EpochCasterGemini_c64`, 645 and 190 ns) and I have not investigated it.
- **No battery measures a dropping implementation with subscribers that keep up.**
- **Fuzzing is 10 seconds per target.** That is a smoke test.
- **Run 09 has no gates record, and run 10's gates ran five tooling files later than its measurement.** See "Correctness gates".
- **The scores of runs before 09 were not recomputed under the two new scoring rules.** `EpochCaster` is still 2nd of 50 in run 08's report.
- **Three implementations that probably allocate once in 35 to 100 publishes now tie with those that never do** on the allocation dimension.
- **`report-01-gemini-flash-3.8-high.html` predates all of this.** It covers 18 responses and cites no run. The landing page says so on its card.
