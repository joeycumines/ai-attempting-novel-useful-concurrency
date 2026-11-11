# Extraction audit — responses 59–74

> Responses 59–70 (prompt-bait) are recorded first, as originally written.
> Responses 71–74, the late adapter for 62, and the repairs made to earlier
> ports during the completion phase are recorded in
> [Completion phase](#completion-phase-7174-62-and-repairs-to-earlier-ports) at the end.

Status of the port of responses 59–70 (`59-gemini-argo-high-prompt-bait.md` …
`70-meta-ai-prompt-bait.md`) into package `bigbuff`. This file was referenced
by earlier source comments but did not exist; it is created here and covers the
whole extraction. Responses 01–58 were integrated earlier in this repository
without a dedicated audit; the same standard is applied below to 59–70.

## Method

For each response: extract the embedded source (txtar/`code` blocks) with the
scratch extractor, copy into the repo root as `<name>_<num>_<model>.go` (plus
`_fast`/`_race`/`_slot_*`/`_fence_*` variants where the candidate split its
build), mechanically rename every identifier with the `<Num><Model>` suffix so
nothing collides package-wide, then iterate: build → vet → full test runtime →
`-race` runtime → benchmark inventory. Implementation bugs found by that loop
were fixed **in the spirit of the candidate** (see “Fixes in spirit”);
candidate tests were repaired only where the test contradicted the
implementation’s documented contract or was itself racy/deadlocking; the
implementation was never distorted to satisfy a broken expectation.

Every response 59–70 is accounted for below. “Verified” means: `go vet .`
clean, `go test -run '<candidate tests>' -count=N` green (N as noted), module
`go test -count=1 -timeout 1800s ./...` and `go test -race -count=1 -timeout 1800s ./...`
green on the working branch (2026-11-10, Intel Xeon @ 2.60GHz, 2 CPUs).
Benchmark figures below are 100 iterations on that machine; they are
instrumented, not tuned.

## Per-response record

### 59 — HorizonRing59Gemini + StripedCombiner59Gemini (59-Gemini)
Source: `59-gemini-argo-high-prompt-bait.md`. Files: `horizonring_59_gemini.go`,
`horizonring_59_gemini_fast.go`, `horizonring_59_gemini_race.go`,
`horizonring_59_gemini_test.go`, `combiner_59_gemini.go` (the response’s
flat-combining publish front-end over the same ring; no tests shipped with it,
and it is exercised through the ring by `TestStripedCombiner_ExactSequenceAssignment`).
Verification: full suite green; fuzz-adjacent adversarial tests
(`AdversarialMPMCNoTornReads`) pass `-count=5`. The delivered
sequence-conservation test had two defects, both repaired (they leaked the
dropped/skip accounting on non-delivery paths and its post-produce drain
“probe” consumed a frame without verifying or accounting for it).
Benchmarks: `BenchmarkPublish_SingleProducer` 31.8 ns/op (36.1 Mmsgs/s);
`BenchmarkPublishBatch_64` 1784 ns/op; `Benchmark1P1C_HorizonRing59Gemini_ReadBatch`
1487 ns/op; `BenchmarkFanout_1P_64C_HorizonRing59Gemini` 262 ns/op (254 Mdeliveries/s);
`BenchmarkHighContentionMPMC_StripedCombiner` 1042 ns/op (0.97 Mmsgs/s).
Attachment: `HorizonRing59Gemini (59-Gemini)` + `StripedCombiner59Gemini (59-Gemini)`
in `bounded-ring-lossy-detectable`, `ephemeral-churn-membership`,
`zero-subscriber-and-edge-lifecycle`, `cancellation-and-close-lifecycle`,
`large-fanout-scalability` (lossy-overwrite semantics; conflating flag set).

### 60 — Latest3260Terra (60-Terra)
Source: `60-arena-gpt-6.1-terra-low-prompt-bait.md`. Files: `latest32_60_terra.go`,
`latest32_60_terra_test.go`. Single packed atomic word (value+revision+closed);
no constructor (zero value ready). Verification: full suite green; `-race` green.
Benchmark: `BenchmarkLatest32/atomic/writeEvery=1` 64.8 ns/op.
Attachment: `latest-value-conflating-watch` (adapter parks on a bounded
exponential backoff between word reads — the candidate is a non-parking
polling register by design, so there is no wake primitive to block on; the
backoff is capped and the goroutine sleeps, never spins),
`ephemeral-churn-membership`, `zero-subscriber-and-edge-lifecycle`,
`cancellation-and-close-lifecycle`, `large-fanout-scalability`.

### 61 — Pulse61Deepseek (61-DeepSeek)
Source: `61-arena-deepseek-4.1-flash-high-prompt-bait.md`. Files:
`pulse_61_deepseek.go`, `pulse_61_deepseek_fast.go`, `pulse_61_deepseek_race.go`,
`pulse_61_deepseek_bench_test.go`, `pulse_61_deepseek_test.go`,
`pulse_61_deepseek_norace_test.go`, `pulse_61_deepseek_race_test.go`.
`TestPulse_Linearizable` was effectively unbounded under `-race` (the detector’s
RWMutex appeasement serialises every Set/Load while 32 spinner goroutines serve
the machine): the test is now iteration-bounded in both build modes — 5000
publishers / 20000 observations (non-race), 200/200 (`-race`) — which preserves
the assertion (versions observed by each spinner are monotonic; no torn
value+version pairs) while terminating deterministically. No implementation
change. Verification: green `-race` and non-race.
Benchmarks: `BenchmarkPulse_Set` 34.0 ns/op; `BenchmarkPulse_Load` 22.6 ns/op;
`BenchmarkPulse_SetParallel` 104.3 ns/op; `BenchmarkBaseline_RWMutex_Load` 148.3 ns/op
(reference). Attachment: `latest-value-conflating-watch`, churn, lifecycle,
zero-subscriber, fanout.

### 62 — Ring62Haiku (62-Haiku)
Source: `62-arena-claude-haiku-5.5-xhigh-prompt-bait.md`. Files: `ring_62_haiku.go`,
`ring_62_haiku_test.go`, `ring_62_haiku_bench_test.go`. The markdown's Go
composite literals with double braces were remediated upstream on `main`
(explicit typed literals, commit eb750d4); this branch no longer touches the
file. No Go source semantics changed.
Verification: green incl. `-race` (module suite). Fan-out benchmarks
(`BenchmarkRingFanout`, `BenchmarkRingFanoutDrain`, `BenchmarkChanFanoutBaseline`)
exist but were not re-measured in this pass.

### 63 — TelemetryRing63Qwen (63-Qwen)
Source: `63-qwen-chat-qwen-3.8-max-prompt-bait.md`. Files: `telemetryring_63_qwen.go`,
`telemetryring_63_qwen_fast.go`, `telemetryring_63_qwen_race.go`, `telemetryring_63_qwen_test.go`.
Note: single-producer by contract (`Publish` panics on concurrent use) and the
ring exposes no published-count accessor, so the attachment replays from the
retained window start and is **not** attached to the zero-subscriber scenario
(its construction semantics would fail the “late subscriber observes the 999th
publication first” facet); it is attached everywhere else a replay cursor is
expressible. Verification: green incl. `-race` (module suite).
Benchmarks: `BenchmarkRing_Publish` 40.2 ns/op; `BenchmarkRing_Latest` 7.4 ns/op.
Attachment: `bounded-ring-lossy-detectable`, `ephemeral-churn-membership`,
`cancellation-and-close-lifecycle`.

### 64 — Beacon64GLM (64-GLM)
Source: `64-z.ai-chat-glm-5.3-max-prompt-bait.md`. Files: `beacon_64_glm.go`,
`beacon_64_glm_slot_fast.go`, `beacon_64_glm_slot_race.go`, `beacon_64_glm_slot_plain.go`,
`beacon_64_glm_fence_amd64.s`, `beacon_64_glm_fence_arm64.s`,
`beacon_64_glm_fence_fallback.go`, `beacon_64_glm_fence_generic.go`,
`beacon_64_glm_test.go`. This response was deferred by earlier triage; it is
now ported and verified. Four implementation defects were found and fixed
(“Fixes in spirit” below); the delivered test suite additionally contained a
latent cross-goroutine deadlock (ModeBlock readers park on the gate while the
test’s `stop` channel is never selected on) — the test now closes the Beacon
before joining readers.
Verification: green across `-count=10`, `-race`, close-burst `-count=20`;
cross-compiled for `linux/arm64` and `linux/386` (the arm64 fence was
compile-verified at this point; it has since been executed under an ARMv8
emulator, never on ARM hardware: see "arm64 assembly: executed under
emulation"). **No benchmarks were delivered with the
candidate** (its README claims a matrix that does not exist in the extract);
measurement harness coverage comes from the scenario facets instead.
Attachment: `bounded-ring-lossy-detectable`, `ephemeral-churn-membership`,
`zero-subscriber-and-edge-lifecycle`, `cancellation-and-close-lifecycle`,
`large-fanout-scalability` (lossy with Skip accounting).

### 65 — Broadcaster65Deepseek (65-DeepSeek)
Source: `65-deepseek-flash-chat-prompt-bait.md`. Files: `broadcast_65_deepseek.go`,
`broadcast_65_deepseek_fast.go`, `broadcast_65_deepseek_race.go`,
`broadcast_65_deepseek_test.go`, `broadcast_65_deepseek_bench_test.go`.
Verification: green incl. `-race` (module suite) and `-count=50` concurrent lag tests.
Benchmarks: `BenchmarkBroadcaster_Publish_NoConsumers` 35.2 ns/op;
`BenchmarkBroadcaster_Publish_WithConsumers/16_consumers` 75.5 ns/op;
`BenchmarkBroadcaster_Read` 15.5 ns/op.
Attachment: `bounded-ring-lossy-detectable`, `ephemeral-churn-membership`,
`zero-subscriber-and-edge-lifecycle`, `cancellation-and-close-lifecycle`.
(Receive polls `TryNext` with a 100 µs back-off because the subscription’s
blocking `Next` only wakes on its own context.)

### 66 — Horizon66Gemini (66-Gemini)
Source: `66-gemini-argo-high-prompt-bait.md`. Files: `horizon_66_gemini.go`,
`horizon_66_gemini_fast.go`, `horizon_66_gemini_race.go`, `horizon_66_gemini_test.go`.
Verification: green incl. `-race` (module suite).
Benchmarks: `BenchmarkPublishAndLatestContention/Horizon_32B_Payload` 83.3 ns/op;
`BenchmarkFanoutBroadcast1ToN/Horizon_Consumers_32` 69.3 ns/op (462 Mdeliveries/s);
`BenchmarkHorizonBatchThroughput` 1286 ns/op.
Attachment: `bounded-ring-lossy-detectable`, `ephemeral-churn-membership`,
`zero-subscriber-and-edge-lifecycle`, `cancellation-and-close-lifecycle`,
`large-fanout-scalability`.

### 67 — Ring67Haiku (67-Haiku)
Source: `67-claude-haiku-extra-prompt-bait.md`. Files: `ring_67_haiku.go`,
`ring_67_haiku_fast.go`, `ring_67_haiku_race.go`, `ring_67_haiku_test.go`,
`ring_67_haiku_bench_test.go`.
Verification: green incl. `-race`; `BenchmarkFanout67Haiku` runs under benchmark
constraints (0 loss across subs=1/8/64, 2-CPU).
Benchmarks: `BenchmarkPublishSerial/ring-uint64-striped` 28.4 ns/op;
`BenchmarkPublishParallel/ring-uint64-striped` 89.6 ns/op (11.2 Mmsgs/s);
`BenchmarkFanout67Haiku/ring/subs=64` 1809 ns/op (35.4 Mdeliveries/s) vs
`chan-drop/subs=64` 5637 ns/op (11.4 Mdeliveries/s).
Attachment: `bounded-ring-lossy-detectable`, `ephemeral-churn-membership`,
`zero-subscriber-and-edge-lifecycle`, `cancellation-and-close-lifecycle`,
`large-fanout-scalability`.

### 68 — EdgeFanIn68Chatgpt (68-ChatGPT)
Source: `68-chatgpt-prompt-bait.md`. Files: `edgefanin_68_chatgpt.go`,
`edgefanin_68_chatgpt_test.go`. M:1 fan-in (SPSC producer lanes → one
batch-oriented consumer) — it is **not** a `Broadcaster` (one shared publish
path, N subscribers each observing every value), so it is not attached to the
broadcast scenario matrix; per the maintainer’s direction it is attached via the
**implemented fan-in scenario** `bounded-fanin-merge` (case runnables
`TestScenarioFanin`, `BenchmarkScenarioFaninMerge`, `FuzzScenarioFaninModel`;
batteries `fanin.merge` + `fanin.batch_drain`), next to its vacuum baseline
`BaselineFanIn`. Measured in run 03 (quick profile @ `9581dec`, verify PASS):
gate score **72 — Situational** (integrity verified, but the throughput
dimension scores 0 against the baseline: merged cost 165.2 ns/op vs 129.3
ns/op). Verification: full suite green incl. `-race` and `GOARCH=386`; the
extracted module’s own `go vet` was clean before rename (the stray fenced usage
block `block_1123.go` in the extract is documentation, not source, and was not ported).
Benchmark: `BenchmarkEdgeFanIn/uint64/edge/producers=1` 82.6 ns/op;
`producers=4` 77.4 ns/op (12.9 Mmsgs/s); `128B/producers=16` 191.6 ns/op.

### 69 — WatchCell69Deepseek (69-DeepSeek)
Source: `69-deepseek-flash-chat-prompt-bait.md`. Files: `watchcell_69_deepseek.go`,
`watchcell_69_deepseek_fast.go`, `watchcell_69_deepseek_race.go`,
`watchcell_69_deepseek_test.go`, `watchcell_69_deepseek_bench_test.go`.
Latest-value broadcast with coalesced generation-gate wakeups and a registry of
nothing. Verification: green incl. `-race` and `-count=100` Watchers churn tests.
Benchmarks: `BenchmarkWatchCell_Store_0Watchers` 38.3 ns/op; `…_Store_10Watchers`
295.8 ns/op; `BenchmarkWatchCell_Load` 9.4 ns/op; `BenchmarkBaselinePtr_Store`
35.7 ns/op (reference).
Attachment: `latest-value-conflating-watch`, `ephemeral-churn-membership`,
`zero-subscriber-and-edge-lifecycle`, `cancellation-and-close-lifecycle`,
`large-fanout-scalability`. Note: the candidate's `Watch` iterator always
replays the current value as its first yield, which is incompatible with the
zero-subscriber facet's tail-attach contract; the adapter therefore drops the
catch-up yield and tail-attaches by a subscribe-time epoch anchor, and blocks
on the cell's condvar-gated `Watch` path (re-anchored at subscribe) instead of
polling.

### 70 — Horizon70Meta (70-Meta)
Source: `70-meta-ai-prompt-bait.md`. Files: `horizon_70_meta.go`,
`horizon_70_meta_slot_fast.go`, `horizon_70_meta_slot_race.go`,
`horizon_70_meta_test.go` (`doc.go` and `block_*.go` from the extract were not
ported — pure documentation). The candidate’s publish path contained a genuine
duplicate-delivery bug (a stalled writer’s blind `seq.Store(seq+1)` could
regress the slot’s seqlock tag after a newer lap of the same slot had been
committed; the reader-side double-check then accepted a mixed
(stale payload, transitional tag) pair as a valid older sequence and re-delivered
the value). Fixed in spirit by a mandatory CAS claim before the payload write
with an odd/even lap tag encoding (`0` = lap −1 committed; odd = lap writing;
even = lap committed). Full mechanism and blast radius are documented in
`horizon_70_meta.go`. Publish is therefore no longer wait-free under contention
(bounded spin/slide per lap, elision of stale writes) — this is documented in
the type doc. The candidate’s `TestHorizon_Concurrent` additionally assumed
claim-order monotonicity that the algorithm does not provide; the test was
repaired to detect duplicates with a seen-message bitmap instead of asserting
an impossible order. Verification: green non-race (`-count=50` concurrent + lag
tests), green `-race` (module suite), `TestHorizon_LagSkips` pre-drain
lag/missed accounting asserted before draining.
Benchmarks: `BenchmarkHorizon_Publish_NoConsumer` 30.3 ns/op;
`BenchmarkHorizon_Fanout_128` 570.6 ns/op; `BenchmarkHorizon_PublishConsume_1`
33.9 ns/op; `BenchmarkChan_Fanout_128` 6551 ns/op (reference);
`BenchmarkMutex_Watch_Publish` 15.4 ns/op (reference).
Attachment: `bounded-ring-lossy-detectable`, `ephemeral-churn-membership`,
`zero-subscriber-and-edge-lifecycle`, `cancellation-and-close-lifecycle`,
`large-fanout-scalability`.

## Fixes in spirit (implementation bugs, with mechanism)

1. **70/Horizon70Meta — duplicate delivery.** Mechanism: stalled writer for lap
   *k* committed its `seq.Store` after lap *k+1* had already committed; the tag
   regressed; readers validating `seq == 2*(k+1)` could match the stale write.
   Fix: mandatory CAS claim (writing marker) before touching the payload;
   reader validates first and second loads against the same expected committed
   tag. Stale/slow writers whose lap is superseded elide their write.
   Blast radius: publish progress is no longer wait-free (documented); values
   from a superseded lap are silently dropped, which the lossy ring contract
   already permits; no torn values (seqlock discipline kept, monitored by the
   canary tests).
2. **64/Beacon64GLM — ModeBlock lap event silently dropped.** The documented
   contract says a lapped watcher receives `(zero, Skip, true)` exactly once per
   lap in ModeBlock; the code coalesced the lap into the value for Latest/NonBlock
   and returned `readSlotAt`’s always-zero Skip for Block. Fix: dedicated
   `(zero, Skip(lap), true)` return for ModeBlock before the value read.
3. **64/Beacon64GLM — lost wakeup in `waitGate`.** A publisher landing between
   the watcher’s caught-up check and its parked registration closed no gate
   (parked counter read 0) and the watcher slept on a gate nobody would close.
   Fix: re-load the claim word *after* incrementing `parked` (the publisher’s
   armGate reads `parked` after the claim CAS, so either the publish sees the
   registration and closes the gate, or the waiter’s re-check sees the advanced
   claim).
4. **64/Beacon64GLM — gate recycling data race (TSan).** A waiter released by
   `close(ch)` may run its release bookkeeping only after the publisher decides
   it is idle; the recycled gate was then re-armed and raced with the waiter.
   Fix: gate pool removed entirely; closed gates are garbage-collected; gates
   are allocated only while at least one watcher is parked, so the no-watcher
   publish path still allocates nothing. The pre-existing
   `TestBeaconNoTornValuesUnderLapping` failure this produced
   (`gatePool.get` vs `waitGate` release) is thereby resolved.
5. **64/Beacon64GLM — SIGSEGV when a publisher races `Close`.** `Publish`
   passed the pre-publish `closed` check; `Close` stored nil into `b.wake`;
   the in-flight publisher then dereferenced nil in `armGate`. Fix: nil-gate
   bail-out in `armGate` (Close’s close of the live gate already releases all
   parked watchers).
6. **59/HorizonRing59Gemini — `TestHorizonRing_AdversarialMPMCNoTornReads`
   accounting leak (test-side).** Non-delivery reads’ skip counts are now
   accumulated on every call (delivered test ignored them on the no-value path),
   and the post-production drain no longer consumes frames with an unverified,
   unaccounted probe; cursors exit only when idle *and* skip accounting is
   exhausted *and* producers are done.

## Test-side repairs (test wrong, implementation right)

- 70/`TestHorizon_LagSkipsButNeverTornOrDuplicate`: post-drain `Lag`/`Missed`
  assertions were unsatisfiable (drain fast-forwards by definition); asserts now
  happen pre-drain.
- 70/`TestHorizon_Concurrent`: delivered test assumed claim-order monotonicity
  the algorithm does not guarantee; repaired with a seen-message bitmap for
  duplicate detection (retained after the implementation fix — it is a stronger
  detector).
- 64/`TestOrderedDeliveryNoGapsUnderKeepup`: ring sized 1024 < 20000 with no
  scheduling guarantee — a lagging watcher *must* lap; now 32768.
- 64/`TestSkipAccountingExact`: the lap-event watcher must be created before
  any publication (position 1); now created alongside the first watcher.
- 64/`TestThousandWatchersReceiveAll`: `WatchLatest` attachers racing `Close`
  are *specified* to see nothing; the test’s “all watchers receive all values”
  expectation is only expressible with an oldest-retained replay cursor; now
  uses `Watch()`.
- 64/`TestBeaconNoTornValuesUnderLapping`: dedicated ModeBlock lap events are
  not values and are excluded from the payload invariant check; the test now
  closes the Beacon before joining readers (previously a caught-up ModeBlock
  reader could park on a gate nobody would ever close — a latent test deadlock).
- 61/`TestPulse_Linearizable`: bounded per above (no implementation change).
- 64/`TestMultiProducerStress` (test-side): the stream encoding `p*1e9 + i`
  overflows a 32-bit `int` on GOARCH=386 (values wrapped, `v/1e9` indexed the
  per-lane tracking array out of range and panicked). The multiplier is now
  `1e6`, which fits every 32-bit int; the cross-producer assertions are
  unchanged. Verified on `GOARCH=386`.
- `TestChanCaster58Helpers` (58 batch, tree-wide portability): the 128-byte
  `ChanCaster58Spark` size assertion is now scoped to 64-bit targets, matching
  the candidate's own documented claim ("no runtime cache-line-size claim is
  made for other targets").

## Post-port 32-bit portability batch (GOARCH=386)

A `GOARCH=386` sweep after the port found and fixed four defects — three in
ported code, one in a test — none of them visible on amd64/arm64:

1. **Beacon64GLM (64-GLM, this batch) — payload copies overran the
   destination on 32-bit.** The slot load/store helpers derived the word
   count from `unsafe.Sizeof(uintptr(0))` — 4 on 386 — but copied with 8-byte
   `uint64` granularity per word, so on 32-bit targets a 4-byte `int` payload
   was copied as 8 bytes: a deterministic 4-byte stack overwrite of adjacent
   caller locals (observed indices 3→5→0 and a deterministic wrong `held`).
   The linux/386 suite hung in `TestBeaconBasics` and corrupted memory whenever
   it ran on. Fixed by exact byte-count copies: `unsafe.Slice`+`copy` in the
   plain build, an explicit `n/8` atomic word loop with a byte-assembled
   source in the `-race` build (an `atomic.LoadUint64(&v)` there was also a
   latent unaligned-atomic panic on 32-bit), and `n/8` in the amd64/arm64 fast
   build. `wordSize64GLM` and the dead helper were deleted from the module.
   amd64/arm64 were never affected (`uintptr` is 8 there) — which is why the
   race detector never saw it.
2. **Package-level 64-bit atomics on 4-aligned storage (386/ARM).** Plain
   `uint64` fields/globals used with the `atomic.LoadUint64`-family panic on
   strict-alignment targets. Switched to typed `atomic.Uint64` (aligned on
   every target): `seqcaster_23_meta.go` (`head`, `lost` — the 23 batch, same
   defect class, surfaced by the same sweep), `beacon_64_glm_fence_generic.go`
   and `beacon_64_glm_slot_race.go` (`fenceWord64GLM`), and
   `broadcast_65_deepseek_bench_test.go` (benchmark counter).
   `ring_62_haiku.go` / `ring_67_haiku_race.go`'s atomic words were audited and
   are already 8/64-aligned by construction; unchanged.
3. **Carillon (21 batch, tree-wide portability) — `carillonSlot` was 64
   bytes only on 64-bit targets** (56 on 32-bit: the `bell` channel field is
   pointer-sized). The pad is now width-independent
   (`38 - unsafe.Sizeof(uintptr(0))`); the slot is exactly one cache line on
   both targets, and `TestCarillonSlotIsOneCacheLine` passes on GOARCH=386.

AMD64 and arm64 builds are unchanged in behaviour; the `-race` suites are
green. `GOARCH=386` full suite: green (≈29s).

## Scenario attachments

`port_attachments_test.go` adds adapters for all port candidates **except**
`EdgeFanIn68Chatgpt`, wired through `AllBroadcasters()` and
`portBroadcastAttachments()` appended in `init()` (registry validation green:
no duplicate `(scenario, impl)` pairs, all remarks present, all flags
well-formed). Honest-fit rules: every port candidate is lossy by design, so
`IsCoalescing()==true` everywhere, and attachments carry the scenario that
matches the loss semantics (`bounded-ring-lossy-detectable` for rings,
`latest-value-conflating-watch` for cells, plus churn/lifecycle/zero/fanout
where semantics allow). `TelemetryRing63Qwen` is deliberately **not** attached
to `zero-subscriber-and-edge-lifecycle` (no published-count accessor exists to
tail-attach); `StripedCombiner59Gemini` is attached with the ring’s semantics
(its publish path is the ring). No attachment weakens an implementation;
facets that do not fit a primitive are simply not created.

`EdgeFanIn68Chatgpt` is a fan-in primitive, not a broadcaster, so it is
attached via the **implemented fan-in scenario** `bounded-fanin-merge`
(`scenario_fanin_test.go`: `faninLikes` factories, one shared contract test
(`TestScenarioFanin`) for the port and its `BaselineFanIn` vacuum baseline,
a fuzz model (`FuzzScenarioFaninModel`) and `BenchmarkScenarioFaninMerge`). It
is registered in the fanin family next to the baseline; the registry validates
it like every other family — the committed registry dump lists 19 scenarios,
135 cases and 345 attachments (see run 03’s `results-03-02-registry.json`).

No adapter busy-polls. `Latest3260Terra` (no wake primitive by design) parks on
a bounded exponential backoff that sleeps — never spins — between word reads.
`WatchCell69Deepseek`'s adapter blocks on the cell’s condvar-gated `Watch` path
(the candidate's iterator always replays the current value as a first yield,
which the zero-subscriber facet forbids for a late attacher; the adapter drops
that catch-up yield and tail-attaches by a subscribe-time epoch anchor, then
blocks). Both are documented in their attachment remarks.

## Not done / open items

- #64 shipped no benchmarks; four were added in the completion phase
  (`BenchmarkBeacon64GLM_*` in `beacon_64_glm_test.go`: publish with no
  watcher, publish then read, snapshot, publish with eight blocked watchers).
- #64’s ARM64 fence (`beacon_64_glm_fence_arm64.s`, `DMB ISHLD/ISH`) has
  been executed under an ARMv8 emulator, one function at a time, and never on
  ARM hardware; see "arm64 assembly: executed under emulation" below. AMD64
  fences are covered by the full amd64 suite; GOARCH=386 is green as of the
  portability batch above.
- `Build tags`: fast slot paths are `!race`; `-race` swaps in the atomic slot
  protocol (`beacon_64_glm_slot_race.go`) or per-slot mutex
  (`horizon_70_meta_slot_race.go`) so the race detector verifies the same
  algorithm the fast build runs.
- Measurement: the runs taken while this audit was written (01 to 06) are not
  ranked. See "Third pass" below for the two measurement defects that void
  them, and `results-00-00-readme.md` for the run log. The fan-in figures this
  bullet once quoted from run 03 came from such a run and are withdrawn.

## Completion phase: 71–74, 62, and repairs to earlier ports

Method as above, with three additions that each caught something the earlier
method did not:

- every stress run is repeated at `-cpu 1,2,4` (GOMAXPROCS=1 exposed the #58
  livelock below, which a 2-CPU default run never shows);
- `go vet -race` is run after touching any `_race.go`/`_fast.go` pair (a
  race-only file of #63 was left uncompilable by the first version of the
  repair below, and plain `go vet` did not see it);
- new tests order events through blocking operations (`apex_helpers_test.go`)
  and never sleep or poll internal state to detect progress.

`make verify` (`verify.mk`) packages those legs.

### As delivered

| Response | Builds | Vet | Own tests |
|---|---|---|---|
| 71 | yes | clean | pass |
| 72 | **no** — `phasecaster.go` passes `*atomic.Uint64` where `*uint64` is required; `bitmap_arm64.s` uses `UADDLV`, which the Go assembler does not accept | n/a | with the compile error fixed: `TestSeqAndSeqPhasedEarlyBreakCompensation` deadlocks; `TestSymExchangerSelectTicketComposability` panics by construction |
| 73 | yes | clean | **test file does not compile** (`cohort_test.go:634:2: declared and not used: v`) |
| 74 | yes | clean | pass |

### 71 — ChanRing71Fable (no repairs)

Lossy multi-sender ring; cursors are plain values; readers park and are woken
by a cascading token. Ported with renames only. Recorded behaviour, not
defects: starting a round is one blocking send; `Close` waits for the whole
round in progress; `Send` on a closed ring panics; a sender spins while a
lapped reader is mid-copy.

### 72 — PhaseCaster72Gemini (six repairs)

1. `Ack` racing the broadcaster's transition from sending to awaiting-ack
   poisoned the caster. `Ack` now yields and retries.
2. The "late acknowledgement owed" marker was a status value, erased when the
   next broadcast claimed the slot; the late `Ack` then found the slot in a
   sending state and poisoned the caster. It is now a flag bit that survives
   claims.
3. `Close` during the acknowledgement wait returned `(n, nil)`. It now returns
   `ErrClosed72Gemini`: the barrier was not reached.
4. A subscriber iterating with `Seq()` (which never acknowledges) hung every
   `BroadcastPhased`. Plain subscribers are now delivered to in one phase.
5. Each broadcast allocated its bank mask above four banks; replaced by a
   per-caster scratch mask.
6. Every broadcast heap-allocated the phase gate. It is now allocated only on
   the acknowledged path, so one-phase `Broadcast` is allocation-free in the
   default build (asserted). `BroadcastPhased` still allocates the gate; the
   race build allocates one snapshot cell per broadcast by design.

Left as designed and recorded: fixed capacity (`ErrCapacity72Gemini`); an
`Ack` that is not owed poisons the subscription; one late acknowledgement per
slot is absorbed; `Close` waits for a blocked send rather than interrupting
it; delivery is sequential, so one slow subscriber delays the rest; a
compensated delivery may be counted as zero; break-compensation is slow at
GOMAXPROCS=1 (about 20 ms per iteration here).

### 72 — bitmap scan kernels (six repairs; wired in the second pass)

The response describes hoisting the scan into assembly but its wiring sketch
does not compile. Repairs to the kernels themselves: the compile error; a
POPCNT feature guard; a destination bounds check; normalisation of the start
index; a dead `CMOVQGE`; `VUADDLV` on arm64. They are parity-tested against a
reference implementation and fuzzed.

In the first completion report they stood alone. They are now wired in:
`PhaseCaster72Gemini` keeps its per-bank masks in two contiguous arrays
(`activeMasks`, `allocMasks`) instead of inside each padded bank, every
broadcast calls `scanActiveBanks72Gemini`, and every subscribe calls
`findNonFullBank72Gemini`. Measurement runs up to 04 predate this.

What the wiring costs and buys, linux/amd64, 2 vCPU, GOMAXPROCS 2, median of
6 runs of 150 ms, ns per operation (`BenchmarkPhaseCaster72Gemini_*`):

| Benchmark | banks | masks inside banks (before) | contiguous, Go loop | contiguous, assembly (now) |
|---|---|---|---|---|
| Broadcast, no subscribers | 4 (default) | 123 | 123 | 131 |
| | 64 | 205 | 183 | 161 |
| | 1024 | 1518 | 1146 | 778 |
| Subscribe + Unsubscribe, only the last bank free | 4 (default) | 68 | 69 | 74 |
| | 64 | 129 | 106 | 107 |
| | 1024 | 1320 | 808 | 717 |

So at the default size (256 slots) the wired version is 6 to 9 percent
slower on these two paths: the assembly call costs more than it saves over
four words. At 64 banks and above it is faster, and most of that gain comes
from the contiguous layout, not from the assembly. The masks of neighbouring
banks now share cache lines, which the original layout avoided; that cost
would show under concurrent subscribe and unsubscribe on different banks and
was not measured (2 CPUs). The scenario benchmarks, which are dominated by
channel handoffs, did not move outside run-to-run variation.

### 72 — SymExchanger72Gemini (three repairs)

1. Lost pairing: two parties could each publish an offer after the other's
   scan and both park. Each party now rescans once after publishing.
2. Stale wake token: the token was sent after the fulfilment was published
   and could land in a slot that had already been recycled.
3. A ticket matched inside `BeginExchange` busy-looped for a second free slot;
   such a ticket now carries its partner's value itself.

`Done()` was added so ticket holders can select on closure (closing does not
fire `Ready`). Recorded: a ticket is a heap allocation (`Exchange` itself is
not — asserted); progress of the rescan under contention is probabilistic; a
full arena spins with `Gosched`. The response's ticket-composability test
cannot pass as written and was replaced (`symexchanger_72_gemini_test.go`).

### 73 — Cohort73Meta (two repairs)

1. The epoch was bumped after the value lock was released, so `Snapshot`
   could pair a new value with the old epoch.
2. `Close` during a barrier returned `(n, nil)`; it now returns
   `(0, ErrClosed73Meta)`.

Recorded: a context cancellation racing completion may report the context
error for a broadcast that completed; a stalled member conflates (it sees
only the latest value); a stale `Ack` returns false; `Wait` returns the
latest *unacknowledged* epoch again immediately. The response's tests were
sleep-based and one did not compile; they were rewritten.

### 74 — Fanout74Deepseek (no repairs)

Synchronous hand-off through per-subscriber mailboxes; reading is the
acknowledgement. Recorded: when the sender's context is cancelled the value
stays readable in the mailbox until overwritten; the response's claim that
abandoning `Next` releases the broadcaster is false (pinned by
`TestFanout74Deepseek_AbandonedNextDoesNotReleaseSend` in
`fanout_74_deepseek_test.go`). The response's tests were sleep-based and were
replaced.

### 62 — Ring62Haiku (adapter only)

The source was extracted earlier but had no adapter, so it ran in nothing but
its own tests. It is now attached to the lossy-ring, churn, lifecycle and
fan-out scenarios. The candidate panics on overlapping `Publish` calls by
contract, and several shared tests publish from more than one goroutine; the
adapter declares `SinglePublisher` and those tests serialise publishes for it
(`harnessPublish` in `harness_test.go`). The lock is in the test driver, not
in the adapter, so it is not part of any measured publish cost. #63 uses the
same mechanism.

### Repairs to earlier ports

- **#58 `Broadcaster58Spark.Next` — livelock, present on `main` before this
  work.** A cursor that had been lapped re-aimed at `head - size`, which is the
  cell an in-flight `Send` is overwriting; the reader then spun on that cell's
  sequence lock. With one P the sender never ran again:
  `TestStandardHarness_ConcurrentChurn/Broadcaster58Spark` hung
  deterministically at `-cpu 1`. The lap target is now derived from the
  published count (`next`), and a single-cell ring with a send in flight
  reports "nothing yet" instead of waiting on the cell. `Next` never returns an
  unpublished sequence. Regression tests: `broadcaster_58_spark_test.go`.
- **#63 `TelemetryRing63Qwen` — lost wakeups, present on `main` before this
  work.** One capacity-1 token for the whole ring woke one parked reader; the
  others slept until the next publish, or forever if there was none: 3 guard
  timeouts in 15 000 runs of its scenario test on the untouched tree. Replaced
  by a count of armed waiters plus a token each woken reader relays
  (`NextWait`); 60 000 runs afterwards without a failure. Rejected
  alternatives: re-posting the token unconditionally (two readers ping-pong and
  spin), closing and replacing a channel per publish (an allocation per wake),
  a waiter list walked by the publisher (publish cost linear in readers).
- **#65** gained `NextContext`; **#60** and **#67** adapters lost their
  timing-based waits (see the tracker rows in `01-readme.md`).

### Second pass: defects found after the first completion report

The first completion report listed #28–#31 and #33 as "no dedicated unit
tests, no defect known". That was a statement about coverage, not about the
code. Two things changed it.

**The broadcast op-stream fuzz target was rewritten.** The old
`FuzzScenarioBroadcast` ran every implementation on every input with a 200 ms
context and blocking receives: 6 executions in 10 s, so it explored nothing.
The new one drives one implementation per input (chosen by the input) through
at most 24 operations with at most 4 consumers, and uses no clock except the
deadlock guard: about 7,800 executions per second. Operations: open a
consumer, publish on a goroutine, wait for all publishes, probe, retire the
newest consumer. `TestScenarioBroadcastOpStreams` replays the streams that
found a defect and 300 fixed-seed streams per implementation on every
`go test`.

**Each of #28–#31 and #33 was read in full and given a unit test file.**

Defects, each with a regression test that fails on the previous code unless
stated:

- **#28 `Broadcast28Flint.Send`** (op streams). On meeting a retired
  subscriber that was not the list head it restarted the walk and never
  unlinked the node: subscribers ahead of it received the value again and
  `Send` never returned. It now steps past and unlinks by CAS.
- **#47 `ChanPubSub47Gemini`** (op streams; the defect was on `main`). The
  last subscriber to receive released the pong barrier before the sender had
  cleared ping's sending state, so an `Unsubscribe` straight after `Wait` was
  treated as a mid-flight withdrawal and parked forever. The sender is now a
  participant in the barrier (`pongRem = subscribers + 1`), and `Add(-n)`
  decides whether it is mid-flight under `subMu`. An acknowledged iterator now
  leaves through `Unsubscribe`.
- **`BaselineBarrierPubSub`** (op streams). A subscriber that joined
  mid-round and then left was counted against that round:
  `sync: negative WaitGroup counter`.
- **#11 `EpochPubSub`** (op streams). `Subscribe` followed by `Current` was
  two steps, so a round armed in between counted a subscriber that never
  acknowledged; `Unsubscribe` compensated the barrier even after an
  acknowledgement. `SubscribeEpoch` and `UnsubscribeAcked` make each atomic.
  The dedicated tests pin the new contract; the failure itself is shown by
  `TestScenarioBroadcastOpStreams`, which deadlocks in 3 of 10 runs
  (GOMAXPROCS 2 and 4) with the old code.
- **#29 `Relay29Nemotron`** (review). `WaitNext` waited on the shared
  one-token doorbell, so one `Publish` woke one of N parked links (1 of 3 in
  the test). Links now park on a channel that `Publish` closes. `store` also
  refuses to put an older sequence over a newer one in a slot; that part is
  shown by a white-box test, not by a reproduced race. The tests also record
  that `Publish` allocates once per call (the boxed value, since the first
  integration; the response claims none); the repair adds a channel
  allocation only to a `WaitNext` that parks.
- **#30 `Arcadia`** (review). Slots had no position tag. An overrun
  subscriber re-read and reordered (4-slot ring, 0..9 sent: `8 9 6 7 8 9 6 7
  8 9`), a reader could take the previous lap's value after a wrap, a slot
  could regress with concurrent senders, the waiter's generation snapshot came
  after the emptiness check (a subscriber could park with a value available),
  `Len` exceeded the capacity, and the iterator could return with its
  subscription still registered. Behaviour change: an overrun subscriber now
  skips to the oldest retained value. The tests also record that `Send`
  allocates once per call, as it did before the repair; the response claims
  none.
- **#31 `Vecast`** (review, then measured). Two concurrent `Subscribe` calls
  could take the same slot (a subscriber lost in 15 of 200 runs with 8
  subscribers). `Unsubscribe` drained the channel, so it swallowed a value a
  blocked `Send` was offering and the `Send` counted it (returned 2 with one
  receiver); if the drain came first the `Send` stayed blocked until `Close`.
  Channels were recycled through a pool while a sender could still hold them.
  `Close` closed channels a concurrent `Send` had loaded; this one was read,
  not reproduced (20,000 attempts). Shard affinity used the low bits of a
  stack address, which are the same in every goroutine: 64 goroutines used 1
  shard, now about 40. Repair: per-shard mutex for membership, a `gone`
  channel per subscription, no pool, a read/write lock between `Send` and
  `Close`. The adapter no longer skips the churn facets and the attachment is
  no longer exposure-limited.
- **#33 `ChanMulticast33North`** (review). The iterator returned at once for
  an already cancelled context while the removal was still queued on the
  context's goroutine. It was also attached to the lossy-ring scenario with a
  remark calling its channels unbuffered; it is a lossless relay with one
  value of slack per subscriber, and is now attached to
  `buffered-lossless-backpressured`.
- **#52 `ChanCascade52Opus`** (lifecycle review). No defect found. Four
  lifecycle tests added, including the exact-count claim under churn.

Also in the second pass:

- **`BaselineDeadlineBarrier.Send`** miscounted a member that acknowledged
  and then left: both its acknowledgement and its departure were ready and
  `select` chose between them, so `Send` sometimes reported 0 for 1. The
  departure branch now checks for the acknowledgement. Found while writing
  the baseline's unit tests (`baseline_deadline_barrier_test.go`, 8 tests;
  `baseline_exchange_test.go`, 5 tests); the regression test fails without
  the fix at GOMAXPROCS 2 and 4.
- **#63 `TelemetryRing63Qwen`** gained `Head()`, so a reader can start after
  everything already published; the adapter now tail-attaches like the other
  ring adapters and #63 is attached to `zero-subscriber-and-edge-lifecycle`.

### arm64 assembly: executed under emulation

No arm64 hardware and no `qemu-aarch64` were available (the sandbox reaches
only GitHub, PyPI and npm, and has no root). The assembly was therefore
executed one routine at a time: the test binary was cross-compiled
(`GOARCH=arm64 go test -c`), each routine's machine code was read out of it
with `go tool objdump`, and it was run on Unicorn 2.1.4's ARMv8 CPU model
(PyPI package `unicorn`) with arguments laid out on the stack as Go's ABI0
does, comparing results with the Go reference loops. The driver is a scratch
Python script and is not committed (AGENTS.md allows no Python in the
repository).

| Routine | Cases | Result |
|---|---|---|
| `asmScanAndPopcnt72Gemini` | 200: lengths 0 to 19, 63, 64, 65, 255, 1024; random, all-zero, all-one and single-bit words; negative length | count, copied words and no write past the destination all match |
| `asmFindNonFullBank72Gemini` | 544: lengths 0 to 11, 64, 65, 300; all full, all free, up to two free banks; several start indexes | index matches, including wrap-around and -1 |
| `asmHasPopcnt72Gemini` | 1 | returns true |
| `asmCPUPause72Gemini`, `fenceAcquire64GLM`, `fenceRelease64GLM`, `spinHint64GLM` | each on two CPU models | the expected instruction word executes and the routine returns to its caller with the link register unchanged |

**This found a defect.** `spinHint64GLM` in `beacon_64_glm_fence_arm64.s` was
written as `WORD $0xd503233f // YIELD`. That word is `HINT #25`, which is
`PACIASP` (sign the link register with the stack pointer); `YIELD` is
`HINT #1`, `0xd503203f`. Go's own disassembler prints the shipped word as
`HINT $25`. The function is declared and never called, so the wrong
instruction never ran. It is now written with the assembler's `YIELD`
mnemonic. What the wrong word would do on hardware with pointer
authentication enabled was not established here.

Limits of this check. It shows the instructions compute the right results.
It does not exercise the routines inside a running Go program on arm64, and
an emulator says nothing about the memory-ordering effect of the `DMB`
fences or the acquire loads, which only real multi-core arm64 hardware can
show. `make verify-arm64` runs the tests of the implementations concerned as
an arm64 binary, natively or under `qemu-aarch64`; it fails when neither is
available, and it has not been run to completion anywhere: only its build
step and its failure path were exercised here.

Measurement run 04 measured the defective versions of #28, #29, #30, #31,
#47, `BaselineBarrierPubSub` and the `EpochPubSub` adapter. No run before
run 07 is ranked in any case (see "Third pass").

### Third pass: the churn exemption removed, and what the measurement was measuring

Four adapters were exempt from the standard concurrent-churn test
(`SkipStandardConcurrentChurn`), each with a recorded reason. The exemption is
gone: the hook no longer exists and every adapter runs the test. Turning it off
passed for one of the four and exposed defects in the other three.

- **#06 `ChanPubSubV2_06meta`.** The exemption was unnecessary. A subscriber
  that withdraws takes its own share of the ping with `Add(-1)`. Passed as it
  was (churn ×2000, race ×300).
- **#57 `ChanPubSub57DeepSeek`.** The primitive is sound. The adapter held an
  exclusive `subsMu` around a blocking `Subscribe`, which deadlocked against a
  publisher waiting for that subscriber. The adapter now takes the lock shared
  on the subscribe and unsubscribe paths and uses `TryLock` in `TryReceive`.
  Its fuzz exemption went with the churn one.
- **#43 `SlotPubSub43Grok`** (two source defects). `Send` held `mu` across its
  unbuffered sends while `unsubscribe` needed `mu`, so a subscriber leaving
  during a `Send` deadlocked both; the departing subscriber now takes and
  acknowledges the value in flight, and `Send` still counts it. Concurrent
  `Send`s shared one acknowledgement count, which stranded a publisher; a
  `sendMu` now serialises them. Three regression tests; the one for the second
  defect fails 5 of 5 without `sendMu`.
- **#40 `SlabCaster40Gemini`** (five source defects). A send on a closed
  channel and a data race on the slot's channel (now a per-slot in-flight lease
  and a closed flag, checked in that order); a closer stuck behind a
  Block-policy `Send` (it drains while it waits, and that `Send` reports the
  value as not delivered); a slot returned to the free list before its channel
  was replaced; `Subscribe` racing the caster's `Close` (now panics with
  `ErrSlabCaster40Closed`); `SubscribeContext` closing twice. Four regression
  tests. What remains is a property of the design: a handle is valid only
  until it is closed.

Found in the same pass, in implementations that were not exempt:

- **#28 `Broadcast28Flint`**: `Stop` tested the subscription's stop field and
  then set it to nil, both unsynchronised, so two goroutines stopping the same
  subscription raced (reported by the race detector). The field is now written
  once, before the subscription is visible. Regression test
  `TestBroadcast28Flint_ConcurrentStop`.
- **#34 `ChanMultiCast34Lightning`** (repairs 7 and 8 in its header): a
  subscriber that stopped receiving and then left during a `Send` waited for
  that `Send`, which was waiting for it, and `Close` hung the same way; and
  `Unsubscribe` could only remove the newest subscriber, so with A then B
  subscribed, A leaving retired B. `UnsubscribeChan` removes the owner of a
  channel, and the adapter uses it.

Two defects were in the measurement, not in an implementation. Both made
implementations look better than they are, and both went unnoticed for five
runs because the numbers were plausible.

- **The caster batteries timed Sends that delivered nothing.** They looped on
  `Send` without waiting for the receivers to register again. Most Sends found
  no receiver and returned in 2 to 8 ns; of 20,000 values most casters
  delivered between 0 and 31. From record contract 1.2.0 a Send is timed only
  once every receiver is registered, the deliveries are counted, and a missing
  one fails the integrity gate. `BenchmarkScenarioCaster` had the same flaw and
  the same fix. A delivered send to 8 receivers costs 700 to 1,100 ns.
- **The run was one process, and a spinning publisher poisoned it.** The
  stalled-consumer probes of `EpochCaster (13-OpenRouter)`, eleventh in run
  order, each end with its publisher spinning in `Send`. On the 2-CPU machine
  every run was taken on, those two goroutines held both CPUs for the rest of
  the run. The ten implementations before them were measured on an idle
  machine. For everything after them, the measured implementation's own
  subscribers were kept off the CPUs while its publisher was timed.
  `Latest3260Terra` reads 10 ns per publish at every subscriber count in run
  06 and 173 to 450 ns at 4 to 256 subscribers measured alone. From record
  contract 1.3.0 every battery of every implementation runs in a process of
  its own (`measure_isolate_test.go`), the run records what each one left
  behind, and no family of an earlier run is ranked.

  Looking for this also showed that the throughput batteries sized a
  repetition from 16 publishes made before any subscriber had run. For
  `EpochTape (10-Gemini)`, whose publish is cheap only while its subscribers
  sleep, that put 400,000 publishes into a repetition with a 60 ms target, at
  up to 75 µs each. Repetitions are now sized by the clock after a timed
  warm-up.

### Fourth pass: what the broadcast ranking was ranking

Run 07 was the first run taken in isolation, and its broadcast table was read
before being reported. Every row at the top had the same stall-resilience
figure, 51, and the fastest of them published in 27 ns with 8 subscribers,
which is less than the cost of one channel operation per subscriber.

- **Defect 17: the throughput battery did not record what was delivered.** It
  timed `Publish` against draining subscribers and wrote the cost of the call.
  It did not count receives. Counted, the eight "Strong candidates" of run 07
  delivered between 0.1 and 79 percent of what was published at 8 subscribers,
  and each of the implementations that delivered everything ranked below all
  eight. This is the broadcast form of the caster defect of the third pass
  (Sends that found no receiver), with one difference: there the batteries
  were wrong about the implementations, and here the implementations do what
  they say (a ring overwrites, a latest-value cell coalesces) and the table
  was wrong to rank them with implementations that do not. Record contract
  1.4.0: `publishes`, `receives` and `delivered_fraction` on every broadcast
  and payload throughput record; the two families scored in two delivery
  classes; the per-scenario comparison confined to the baseline's class;
  broadcast and payload of run 07 withdrawn from ranking.
- **The stall-resilience figure of 51** was the percentile rank counting a tie
  as nothing: 47 of 94 implementations complete the stalled-consumer probe, so
  each beat 47 of the other 93. No broadcast implementation could score above
  about 90. A tie now counts as half a place, and a dimension on which a whole
  scoring group is tied is left out of its composite.
- **The caster ranking did not reproduce.** Runs 07 and 08 have the same
  caster batteries. Run 07 put `ChanCaster2` first and `ChanCaster09Gemini`
  second; run 08 put them 4th and 7th. The 13 implementations send to 8
  receivers within 800 to 1,130 ns and the intervals of 62 of the 78 pairs
  overlap. A group is now ranked only when its headline cost tells at least
  half of its pairs apart; the caster family is unranked in both runs.
- **The first settling rule misclassified one implementation.** To count
  receives after the timed loop, the battery first waited until the count had
  not moved for 2 ms, for at most 200 ms. `GenCaster (06-Meta)` buffers
  without bound and delivers everything; with 256 subscribers its backlog was
  still draining at 200 ms, and one take recorded it as delivering 96 percent,
  which put it in the dropping class. That take was discarded and is not a
  committed run. The count is now read after 50 ms without a receive, and a
  cell still receiving after 20 s fails the run. Class membership was then
  identical in three takes.

### Not done

- arm64 assembly (#64 fence, #72 bitmap) has run under an emulator, one
  function at a time, and never on arm64 hardware (previous section).
- Nothing has been measured on more than 2 CPUs.
