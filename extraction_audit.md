# Extraction audit — rawLLM prompt-bait responses 59–70

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
Attachment: `latest-value-conflating-watch` (adapter polls the word; no wake
primitive by design), `ephemeral-churn-membership`, `zero-subscriber-and-edge-lifecycle`,
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
cross-compiled for `linux/arm64` and `linux/386` (asm fences compile-verified
only — not run on ARM hardware). **No benchmarks were delivered with the
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
path, N subscribers each observing every value), so it is **not attached** to
the broadcast scenario matrix. Verification: green incl. `-race`; the extracted
module’s own `go vet` was clean before rename (the stray fenced usage block
`block_1123.go` in the extract is documentation, not source, and was not ported).
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
zero-subscriber facet's tail-attach contract; the adapter therefore
re-implements tail-attach by epoch (synchronous epoch capture at subscribe,
poll `Load` until the epoch advances) and busy-polls with `Gosched`.

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

## Scenario attachments

`port_attachments_test.go` adds adapters for all port candidates **except**
`EdgeFanIn68Chatgpt` (fan-in is not a broadcast primitive) wired through
`AllBroadcasters()` (491 standard-harness subtests green) and
`portBroadcastAttachments()` appended in `init()` (registry validation green:
no duplicate `(scenario, impl)` pairs, all remarks present, all flags
well-formed). Honest-fit rules: every port candidate is lossy by design, so
`IsCoalescing()==true` everywhere, and attachments carry the scenario that
matches the loss semantics (`bounded-ring-lossy-detectable` for rings,
`latest-value-conflating-watch` for cells, plus churn/lifecycle/zero/fanout
where semantics allow). `TelemetryRing63Qwen` is deliberately **not** attached
to `zero-subscriber-and-edge-lifecycle` (no published-count accessor exists to
tail-attach); `StripedCombiner59Gemini` is attached with the ring’s semantics
(its publish path is the ring). Two adapters busy-poll their cells with
`Gosched` because the primitive exposes no wake primitive wired to a
subscription — `Latest3260Terra` (none by design) and `WatchCell69Deepseek`
(its `Watch` iterator always replays the current value as a first yield, which
the zero-subscriber facet forbids for a late attacher; the adapter drops that
catch-up yield and tail-attaches by epoch instead) — both documented in their
attachment remarks. No attachment weakens an implementation; facets that do not
fit a primitive are simply not created.

## Not done / open items

- #64 ships no candidate benchmarks (see above); #62’s fan-out benchmark not
  re-measured in this pass; both are one-liners if wanted later.
- #64’s ARM64 fence (`beacon_64_glm_fence_arm64.s`, `DMB ISHLD/ISH`) is
  compile-verified only; untested on ARM hardware.
- `Build tags`: fast slot paths are `!race`; `-race` swaps in the atomic slot
  protocol (`beacon_64_glm_slot_race.go`) or per-slot mutex
  (`horizon_70_meta_slot_race.go`) so the race detector verifies the same
  algorithm the fast build runs.
- `measurement.md` / result site were not regenerated for this batch; the
  benchmark inventory above is the source of truth until then.
