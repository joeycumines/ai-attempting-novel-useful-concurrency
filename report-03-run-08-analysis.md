# Analysis of measurement run 08

Run 08 is the first run whose rankings can be read as rankings. It measured 141 implementations in 8 families, each battery in a process of its own, and found no integrity failure. This report says what the run shows, which of the earlier results were withdrawn and why, and what has not been done. It was written by hand from `results-08-01-results.jsonl` and the generated `results-08-07-evaluation.md`, and it is not regenerated.

## What to take from it

- **Two families are ranked, each in two classes.** Broadcast (94 implementations) and payload (19) are split into the implementations whose subscribers received every published value in the throughput battery, and those that dropped or coalesced some. Three of the four classes are ranked. The fourth, payload implementations that drop, is not: the measurement tells apart fewer than half of its pairs.
- **The caster family is not ranked.** Its 13 implementations send to 8 receivers in 800 to 1,130 ns, with 95 percent half-widths of 11 to 240 ns. Only 16 of the 78 pairs can be told apart. Runs 07 and 08 used the same caster batteries. Ranked, run 07 put `ChanCaster2` first and `ChanCaster` 3rd; run 08 would have put `ChanCaster17Pumped` first (8th in run 07) and `ChanCaster` last.
- **Five families are too small to rank** (scatter 4, gather 2, fan-in 2, exchange 3, barrier 4). Their measurements are printed with intervals.
- **Nothing in runs 01 to 06 is ranked, and the broadcast and payload tables of run 07 are withdrawn.** Four measurement defects were found, one after another. Each is described below.
- **A score is a position among peers on one 2-CPU machine.** It does not say an implementation is correct, and it says nothing about more than 2 CPUs.

## The run

| | |
|---|---|
| Profile, seed | standard, 1 |
| Commit measured | `df776172e692` |
| Machine | 2 logical CPUs, Intel Xeon 2.60 GHz, shared virtual machine, linux/amd64, go1.27.3, GOMAXPROCS 2 |
| Record contract | 1.4.0, one process per battery per implementation (1,041 processes) |
| Duration | 789 s |
| Records | 2,183 metric, 201 trace, 25 skipped, 0 harness failures |
| Verification | every implementation in the registry dump of the run has records |
| Integrity | 1,069,316 multi-word payload reads checked, 0 torn; 135 records with an exactly-once count, 0 violations |

The 25 skip records are 9 distinct cases. Two bounded implementations (`ChanRing26Kimi`, `RingFanOut14Xing`) cannot hold 128 or 256 subscribers. `ChanPubSub35ChatJimmy` is skipped by all five caster batteries because its design cannot acknowledge a withdrawal during a Send; the registry records that as an exposure-limited attachment.

## Broadcast, implementations that deliver every value (50)

These are the lock-step, backpressured and queueing designs. Verdicts: 5 Strong candidate, 8 Situational, 13 Niche use only, 24 Avoid for most uses.

| Rank | Implementation | Score | Publish cost, 8 subscribers |
|---|---|---|---|
| 1 | RingPubSub (18-ChatGPT) | 93 | 355 ns |
| 2 | EpochCaster (13-OpenRouter) | 92 | 268 ns |
| 3 | Carillon (21-ClaudeHaiku) | 89 | 408 ns |
| 4 | GenCaster (06-Meta) | 86 | 282 ns |
| 5 | ChanRing47Gemini (47-Gemini) | 78 | 517 ns |
| 6 | BaselineQueuedFanout (baseline) | 74 | 1.62 µs |
| 7 | QueuedFanout24Meta (24-MetaAI) | 73 | 1.87 µs |
| 8 | BaselineBoundedReplay (baseline) | 72 | 2.14 µs |
| 9 | ChanHub (15-Nemotron) | 69 | 1.95 µs |
| 10 | ChanCascade52Opus (52-ClaudeOpus) | 68 | 3.32 µs |

The other baselines of this class: BaselineBufferedBroadcast 15th (58), ChanPubSub 19th (54), BaselineBarrierPubSub 39th (33).

Three things qualify the top of this table.

- **`EpochCaster (13-OpenRouter)` waits by spinning.** When a subscriber never reads, its publisher loops on `runtime.Gosched` and uses a whole CPU until the process ends (1.08 CPUs measured after its stall probes). It is the only implementation of the 141 that leaves a process busy. Its score does not reflect this, because the score has no dimension for it. This behaviour is also what invalidated runs 01 to 06.
- **`GenCaster (06-Meta)` and the two queued fan-outs never block the publisher because they buffer without bound.** They score 98 on stall resilience for completing every publish behind a subscriber that never reads. The memory that costs is reported by the stalled-consumer battery and is not part of the score.
- **The four leaders publish in 268 to 408 ns; the lock-step baseline `ChanPubSub` takes 3.54 µs.** That is a real difference of roughly ten times at 8 subscribers on this machine. The leaders are not doing the same work: they hand off through a buffer or ring and block only when it is full, where a lock-step design waits for every subscriber on every publish.

## Broadcast, implementations that drop or coalesce (44)

These are rings that overwrite, latest-value cells and non-blocking fan-outs. Verdicts: 7 Strong candidate, 8 Situational, 7 Niche use only, 22 Avoid for most uses. Every member completes the stalled-consumer probe, so that dimension distinguishes none of them and is left out of the score.

| Rank | Implementation | Score | Publish cost, 8 subscribers | Delivered, 8 subscribers | Delivered, least over all cells |
|---|---|---|---|---|---|
| 1 | RingPubSub TrySend (18-ChatGPT) | 90 | 30 ns | 0.96% | 0.61% |
| 2 | Horizon66Gemini (66-Gemini) | 87 | 110 ns | 40% | 1.2% |
| 3 | ChanRing71Fable (71-Fable) | 86 | 55 ns | 2.5% | 0.95% |
| 4 | Pulse61Deepseek (61-DeepSeek) | 84 | 65 ns | 0.07% | 0.02% |
| 5 | HorizonRing59Gemini (59-Gemini) | 82 | 99 ns | 35% | 1.3% |
| 6 | StripedCombiner59Gemini (59-Gemini) | 81 | 156 ns | 68% | 3.2% |
| 7 | RingFanOut14Xing (14-Xing) | 76 | 135 ns | 2.1% | 0.84% |
| 8 | BaselineAsyncFanout (baseline) | 73 | 95 ns | 0.93% | 0.19% |
| 8 | Horizon70Meta (70-Meta) | 73 | 132 ns | 11% | 0.35% |
| 8 | Latest3260Terra (60-Terra) | 73 | 131 ns | 2.5% | 0.12% |

`Horizon70Meta`'s row describes code that was repaired after the run, and is too favourable; see "Source defects found while fixing the measurement".

The other baselines of this class: BaselineFanout 29th (34, 993 ns, 23% delivered), BaselineLatestBroadcast 35th (30, 910 ns, 15% delivered).

Read the delivered columns before the score. The battery publishes as fast as it can, with no pause, to subscribers that do nothing but receive. Under that load most of these implementations deliver a few percent of what is published, and the cheapest publish is the one that delivers least. The score ranks the cost of the call and does not weigh the share delivered, so `Pulse61Deepseek` at 0.07 percent and `StripedCombiner59Gemini` at 68 percent sit three places apart. For a latest-value design, delivering 0.07 percent of a flood is the design working. For a ring meant to carry a stream, it is not. The table cannot tell you which you need.

The delivered share is a property of this battery, not a constant of the implementation. An implementation that drops delivers everything to subscribers that keep up. That case, which is the one these designs are built for, is not measured by any battery here.

## Payload (19)

The payload family carries a four-word value and checks every read for tearing.

Implementations that deliver every value (7), ranked: GenCaster 97 (183 ns), EpochCaster13Crouton_r64 76 (355 ns), RingPubSub_c64 71 (375 ns), ChanFeed_block_c64 52, ChanHub_c16 31, ChanRelay_w16 19, ChanRelay_sync 3 (3.97 µs). With seven members a percentile rank moves in steps of 17 points; the order is supported, the distances are not.

Implementations that drop (12), not ranked: only 29 of 66 pairs can be told apart. Five of the twelve publish in 384 to 426 ns. The two fastest, BaselineAsyncFanout_c64 (118 ns) and RingFanOut14Xing_c64 (122 ns), each deliver under 2 percent at 8 subscribers.

## Each scenario against its baseline

The evaluation compares the implementations attached to each scenario as a good fit with that scenario's baseline, on publish cost at 8 subscribers, and only within the baseline's delivery class. "Faster" means the 95 percent intervals do not overlap.

| Scenario | Baseline (cost) | Compared | Faster | Not distinguishable | Slower |
|---|---|---|---|---|---|
| lockstep-barrier-lossless | BaselineBarrierPubSub (7.44 µs) | 20 | 10 | 10 | 0 |
| synchronous-handoff-no-barrier | BaselineBarrierPubSub (7.44 µs) | 14 | 13 | 0 | 1 |
| buffered-lossless-backpressured | BaselineBufferedBroadcast (4.28 µs) | 10 | 7 | 2 | 1 |
| bounded-ring-lossy-detectable | BaselineFanout (993 ns) | 25 | 22 | 2 | 1 |
| latest-value-conflating-watch | BaselineLatestBroadcast (910 ns) | 14 | 7 | 4 | 3 |
| large-fanout-scalability | BaselineFanout (993 ns) | 18 | 15 | 3 | 0 |
| nonblocking-lossless-overflow | BaselineQueuedFanout (1.62 µs) | 1 | 0 | 1 | 0 |
| bounded-async-fanout-dispatch | BaselineAsyncFanout (95 ns) | 1 | 0 | 0 | 1 |
| bounded-catchup-replay | BaselineBoundedReplay (2.14 µs) | 1 | 0 | 0 | 1 |
| unbounded-catchup-replay | BaselineBufferedBroadcast (4.28 µs) | 1 | 1 | 0 | 0 |

Most model-written implementations beat the baseline of the scenario they were attached to. The table lists the scenarios that define a delivery contract; three lifecycle scenarios that share `BaselineFanout` are in the evaluation. Two cautions. Each baseline is the plainest implementation of its contract, so beating one is a low bar. And this number is the steady-state publish cost only: it does not time what defines most scenarios (churn, close, replay, a deadline). The tests exercise those; no battery times them per scenario.

Three baselines are not beaten by anything attached to them: BaselineAsyncFanout, BaselineBoundedReplay and BaselineQueuedFanout. Each has one comparable attachment.

## The small families

Unranked, measurements only. Mean and 95 percent half-width.

- **Barrier (4).** The three model-written barriers send faster than the baseline at every receiver count from 4 up, with intervals that do not overlap it: 2.4 to 3.0 µs against 5.6 µs at 4 receivers, and 19.5 to 25.9 µs against 57.7 µs at 32. Run 07 shows the same. This is the clearest result among the small families. Deadline overshoot is the same for all four (325 to 343 µs).
- **Exchange (3).** The baseline exchanger is faster than both configurations of `SymExchanger72Gemini`: 796 ns against 1.96 and 2.30 µs at 2 senders, and 1.88 µs against 2.27 and 6.10 µs at 8.
- **Scatter (4), gather (2), fan-in (2).** The intervals on the headline metrics run from 40 to over 100 percent. The gather pair shows what that means: run 07 measured the baseline at 2.9 µs and `ChanGatherer` at 17.9 µs, and run 08 measured them at 10.8 µs and 4.0 µs. These measurements establish orders of magnitude and nothing finer.

## One subscriber that never reads

47 of 94 broadcast implementations complete all 1,000 publishes behind a subscriber that never reads; 47 block (0 to 81 publishes completed). In payload, 13 complete and 6 block. Blocking is the specified behaviour of a lossless lock-step design and a defect only in one that claims not to block. The class split and this split nearly coincide but are not the same: three implementations deliver everything and still never block, because they queue without bound.

## What was wrong with the earlier results

Four defects of the measurement, in the order found. Each was found by asking what a suspicious number was made of. Each has its own record contract, and `results-00-00-readme.md` lists which run is under which.

1. **Stall resilience had no data (runs 01 to 04).** The stalled-consumer records did not carry the value the metric is named for, and a dimension without data scored 100. Every implementation had full marks on a fifth of the weight.
2. **The caster batteries timed Sends that delivered nothing (runs 01 to 05).** They looped on `Send` without waiting for receivers to register again. Between 0 and 31 of 20,000 values reached a receiver, and a Send cost 2 to 8 ns.
3. **Runs were one process, and one implementation left it spinning (runs 01 to 06).** The stall probes of the eleventh broadcast implementation, `EpochCaster (13-OpenRouter)`, left two publishers spinning on a 2-CPU machine. The ten implementations before it were measured on an idle machine. The other 83, and seven whole families, were not. The effect ran both ways. Comparing run 06 with run 07 at 8 subscribers, 31 of those 83 read at least 1.5 times slower than they are (up to 4 times; these are the lock-step designs), and 40 read at least 1.5 times faster (up to 22 times; these are free-running publishers whose subscribers were kept off the CPUs). Run 06 put `Latest3260Terra` first in broadcast at a flat 10 ns per publish at every subscriber count; alone, it takes roughly 130 to 450 ns from 4 subscribers up.
4. **The throughput battery did not record what was delivered (runs 01 to 07).** It timed the call and did not count receives. In run 07 the eight broadcast "Strong candidates" delivered between 0.1 and 79 percent of what was published, and every implementation that delivers everything ranked below them.

Two further corrections came from the same reading and change scores, not measurements. A tie in a percentile rank counted as nothing, so the 47 implementations that complete the stall probe each scored 51 on it and no broadcast implementation could approach 100. And a ranking was produced for any family of five or more, however close its members; the caster ranking was noise.

I got the fourth fix wrong once. The first rule for counting receives stopped waiting after 200 ms, and recorded `GenCaster`, which delivers everything, as delivering 96 percent while its backlog was still draining. That take was discarded and is not a committed run. The count is now read after 50 ms without a receive, and a cell still receiving after 20 s fails the run.

The earlier runs keep their files. Their reports and evaluations were regenerated and show every measurement with the verdict "Not ranked (measurement not valid)" and the reason.

## Source defects found while fixing the measurement

Found by tests written for this work, and repaired in the ports with the repair recorded in each file's header and in `extraction_audit.md`:

- `Broadcast28Flint`: two concurrent `Stop` calls raced on the stop field.
- `ChanMultiCast34Lightning`: a subscriber leaving, or `Close`, during a blocked `Send` deadlocked; `Unsubscribe` removed the newest subscriber, not the caller's.
- `SlabCaster40Gemini`: five defects under concurrent churn, including a send on a closed channel.
- `SlotPubSub43Grok`: `Send` held its mutex across unbuffered sends, and shared one acknowledgement count between concurrent Sends.
- `ChanPubSub57DeepSeek`: the primitive is sound; the harness adapter deadlocked.
- `Horizon70Meta`: a lost wakeup. A publish that arrived while a consumer was between its last check and parking skipped the wakeup, and the consumer slept until the next publish. Found after run 08, by one failure of its own test under the race detector, and repaired: the publisher now takes the mutex instead of trying it. **Run 08 measured the code before this repair.** Its row for `Horizon70Meta` (8th of the dropping class, 132 ns at 8 subscribers) describes a publish that could skip a wakeup. Two trial passes of the repaired code gave 236 and 516 ns. Treat that row as too favourable; no other implementation's source changed after the run.

No adapter is exempt from the concurrent churn tests any more.

## Correctness gates

`results-08-06-gates.json` records what `make verify` reported and on which commit: vet for four build configurations, the full suite at GOMAXPROCS 1, 2 and 4, the full suite under the race detector, the allocation assertions, and every fuzz target for 10 seconds each. Every leg passed. The evaluation of run 08 prints each one, with its commands. The arm64 leg is recorded as not run.

The gates ran after the measurements, on a later commit; the evaluation names both commits and lists every file that differs. Those files are documents, tests, the report, evaluation and landing-page code, and two harness changes that touch no battery (the `--impl` filter, and a check that a measuring process measured something). No implementation and no adapter changed.

Runs 06 and 07 have no gates record. Run 05 has one.

## What has not been done

- **Nothing was measured on more than 2 CPUs.** Every claim about scaling here is about subscriber count on 2 CPUs. Contention behaviour with real parallelism is unknown.
- **The arm64 assembly has not run on arm64 hardware.** The assembly of responses 64 and 72 was executed one routine at a time under an emulator (744 cases pass). That checks the instructions. It says nothing about memory ordering on a real machine.
- **One row of run 08 is out of date.** `Horizon70Meta` was repaired after the run (see "Source defects"), and the run was not taken again.
- **Run 08 is one run.** The rankings have not been reproduced by a second run under the same contract. Class membership is the one thing with a second take: a separate pass of the throughput batteries under the same settling rule put all 108 implementations it covered (of 113) in the same class as run 08. That pass is not committed. It missed five because `--impl '*'` did not match a name with a `/` in it; that is fixed and tested. The caster comparison between runs 07 and 08 is the only between-run evidence, and it showed a ranking that did not reproduce, which is why that family is now unranked.
- **The latency dimension is weak.** `publish_latency_ns` has a median coefficient of variation of 183 percent across its samples. It carries 14 percent of the weight and I have not established that its ranks are stable.
- **`EpochTape (10-Gemini)` is unexplained.** Its publish cost at 8 subscribers is 184 µs with a coefficient of variation of 1.68. Its subscribers poll while its publisher takes a write lock; I have not determined whether that is a property of the design or a defect of the port.
- **No battery measures a dropping implementation with subscribers that keep up.**
- **Fuzzing is 10 seconds per target.** That is a smoke test.
- **`report-01-gemini-flash-3.8-high.html` predates all of this.** It covers 18 responses and cites no run. The landing page says so on its card.
