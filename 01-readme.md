# Model benchmark: Novel concurrency implementations

[Project Landing Page (GitHub Pages)](https://joeycumines.github.io/ai-attempting-novel-useful-concurrency/) · [Local Landing Page](index.html) · [README (GitHub Repository)](README.md) · [Original Gist](https://gist.github.com/joeycumines/a20369832bf361063748d1bd18924d9d)

N.B. If you're reading this as a gist it is also an actual [repository](https://github.com/joeycumines/ai-attempting-novel-useful-concurrency).

## Introduction

This gist contains a single ad-hoc benchmark and results for a prompt I found particularly interesting.
In brief, I concluded providing AI with diverse inputs and challenging highly-technical problems is a good way to evaluate its capabilities.
This is particularly true in limited (cheap or free) in-browser chat or basic cloud agent contexts, where input contexts are strictly limited.
This gist documents the prompt, the model's responses, and my own evaluation of the results produced.

## Background

I've produced (manually, for the record) a distinctive implementation technique for synchronous broadcast in Go:

1. [ChanCaster](https://github.com/joeycumines/go-bigbuff/blob/04f9b9f58d17938686a4d5014f8ed5531c97ac34/chancaster.go)
2. [ChanPubSub](https://github.com/joeycumines/go-bigbuff/blob/04f9b9f58d17938686a4d5014f8ed5531c97ac34/chanpubsub.go)

While the core patterns - synchronous channel rendezvous, fan-out, registration, and wait-for-all synchronization - are longstanding concurrency patterns,
the implementation/design is (as far as I am aware) entirely novel within the Go ecosystem.

The interesting things these bring to the table are:

1. A protocol construction
2. Semantics where correctness depends on an explicit registration/receive contract
3. A mechanism for safe mid-flight receiver withdrawal
4. O (1) channel/control-plane state
5. Compatibility with Go's ordinary channel and `select` semantics

I also performed empirical evaluation showing the memory benefit and the throughput trade-off.

## Responses

I've tried to encode the more important details like provider, model, effort, and prompt, in the filename, where possible.
Lacking in scientific rigor perhaps, but it's worth noting that models are often opaque when accessed through AI chat interfaces.
The tools available, especially the presence of a sandbox, search engine, and (in this case) pre-installed or installable Go toolchain, are also often opaque, and can have a significant impact on the results produced.

In cases where there were individual files attached, they are encoded as a txtar down the bottom of the markdown, in a code block or similar.

**Integration progress (2026-11-10):** every response 03–74 that delivered an implementation is extracted, integrated and listed in the [tracker](#extraction-status-integration-tracker) below. Responses 03–20 are cross-checked against their transcripts in each source file's `Provenance:` header (not in `extraction_audit.md`, which covers 59–74 and the completion-phase repairs). Verification is `make all` plus `make verify` (see `verify.mk`): vet for the default, race, arm64 and 386 builds; the full suite at GOMAXPROCS 1, 2 and 4; the full suite under the race detector; allocation assertions; every fuzz target. The evaluation of the result, including what was *not* done, is [report-02-completion-evaluation.md](report-02-completion-evaluation.md).

- [03-chatgpt-1.md](03-chatgpt-1.md)
- [04-deepseek-v4.1-flash-chat.md](04-deepseek-v4.1-flash-chat.md)
- [05-qwen-3.8-max-chat.md](05-qwen-3.8-max-chat.md)
- [06-meta-chat-probably-muse-spark-1.3.md](06-meta-chat-probably-muse-spark-1.3.md)
- [07-qwen-3.8-max-arena.md](07-qwen-3.8-max-arena.md)
- [08-muse-spark-open-arena.md](08-muse-spark-open-arena.md)
- [09-gemini-argon-medium.md](09-gemini-argon-medium.md)
- [10-gemini-argon-medium.md](10-gemini-argon-medium.md)
- [11-meta-chat-probably-muse-spark-1.3.md](11-meta-chat-probably-muse-spark-1.3.md)
- [12-openrouter-crouton-alpha-chat.md](12-openrouter-crouton-alpha-chat.md)
- [13-openrouter-crouton-alpha-chat.md](13-openrouter-crouton-alpha-chat.md)
- [14-xing4.0-29b-a4b.md](14-xing4.0-29b-a4b.md)
- [15-nemotron-super-vl.md](15-nemotron-super-vl.md)
- [16-deepseek-v4.1-flash-chat.md](16-deepseek-v4.1-flash-chat.md)
- [17-gemini-argon-medium.md](17-gemini-argon-medium.md)
- [18-chatgpt-2.md](18-chatgpt-2.md)
- [19-chatgpt-3.md](19-chatgpt-3.md)
- [20-arena-agent.md](20-arena-agent.md)
- [21-claude-haiku-extra.md](21-claude-haiku-extra.md) ([link](https://claude.ai/share/bddce53b-2891-4698-95c7-b3af42f46dfd))
- [22-chatgpt.md](22-chatgpt.md) ([link](https://chatgpt.com/share/6aee742e-fdc0-83ec-bb9c-7227206d2528))
- [23-meta-ai.md](23-meta-ai.md)
- [24-meta-ai-prompt-mini.md](24-meta-ai-prompt-mini.md) ([link](https://meta.ai/share/TiLaDoJVDD))
- [25-claude-haiku-extra-prompt-mini.md](25-claude-haiku-extra-prompt-mini.md) ([link](https://claude.ai/share/b7f1326f-456f-4807-8d24-f77aed549dac))
- [26-kimi-ai-instant-prompt-mini.md](26-kimi-ai-instant-prompt-mini.md)
- [27-mistral-ai-chat-prompt-mini.md](27-mistral-ai-chat-prompt-mini.md)
- [28-openrouter-chat-flint-free-prompt-mini.md](28-openrouter-chat-flint-free-prompt-mini.md)
- [29-openrouter-chat-nemotron-3.5-super-vl-free-prompt-mini.md](29-openrouter-chat-nemotron-3.5-super-vl-free-prompt-mini.md)
- [30-openrouter-chat-dots3-note-preview-free-prompt-mini.md](30-openrouter-chat-dots3-note-preview-free-prompt-mini.md)
- [31-openrouter-chat-nemotron-3-ultra-free-prompt-mini.md](31-openrouter-chat-nemotron-3-ultra-free-prompt-mini.md)
- [32-openrouter-chat-nemotron-3-nano-omni-free-prompt-mini.md](32-openrouter-chat-nemotron-3-nano-omni-free-prompt-mini.md)
- [33-openrouter-chat-north-mini-code-free-prompt-mini.md](33-openrouter-chat-north-mini-code-free-prompt-mini.md)
- [34-openrouter-chat-nemotron-3.5-lightning-free-prompt-mini.md](34-openrouter-chat-nemotron-3.5-lightning-free-prompt-mini.md)
- [35-chatjimmy-prompt-mini.md](35-chatjimmy-prompt-mini.md) (Generated in 0.149s)
- [36-meta-ai-prompt-mini.md](36-meta-ai-prompt-mini.md)
- [37-gemini-3.8-flash-high-prompt-mini.md](37-gemini-3.8-flash-high-prompt-mini.md)
- [38-chatgpt-prompt-mini.md](38-chatgpt-prompt-mini.md) ([link](https://chatgpt.com/share/6aee8fc4-aad0-83ec-9cdc-d07e45cb9fab))
- [39-arena-mistral-large-4-high-prompt-mini.md](39-arena-mistral-large-4-high-prompt-mini.md)
- [40-arena-gemini-3.8-flash-high-prompt-mini.md](40-arena-gemini-3.8-flash-high-prompt-mini.md)
- [41-arena-claude-sonnet-5.5-high-prompt-mini.md](41-arena-claude-sonnet-5.5-high-prompt-mini.md)
- [42-arena-muse-spark-1.3-xhigh-prompt-mini.md](42-arena-muse-spark-1.3-xhigh-prompt-mini.md)
- [43-grok-chat-prompt-mini.md](43-grok-chat-prompt-mini.md)
- [44-qwen-chat-qwen-3.7-max-prompt-mini.md](44-qwen-chat-qwen-3.7-max-prompt-mini.md)
- [45-chatgpt-prompt-mini.md](45-chatgpt-prompt-mini.md)
- [46-arena-grok-4.6-medium-prompt-mini.md](46-arena-grok-4.6-medium-prompt-mini.md)
- [47-arena-gemini-3.8-flash.md](47-arena-gemini-3.8-flash.md)
- [48-grok-chat.md](48-grok-chat.md)
- [49-qwen-chat-qwen-3.7-max.md](49-qwen-chat-qwen-3.7-max.md)
- [50-meta-ai.md](50-meta-ai.md)
- [51-deepseek-flash-chat.md](51-deepseek-flash-chat.md)
- [52-arena-claude-opus-5-medium.md](52-arena-claude-opus-5-medium.md)
- [53-arena-gpt-6-sol.md](53-arena-gpt-6-sol.md)
- [54-arena-gpt-6.1-terra-medium-prompt-safe.md](54-arena-gpt-6.1-terra-medium-prompt-safe.md)
- [55-arena-claude-haiku-5.5-high-prompt-safe.md](55-arena-claude-haiku-5.5-high-prompt-safe.md)
- [56-github-cloud-mai-code-1.1-flash-high.md](56-github-cloud-mai-code-1.1-flash-high.md)
- [57-arena-deepseek-v4.1-flash-high-prompt-safe.md](57-arena-deepseek-v4.1-flash-high-prompt-safe.md)
- [58-arena-muse-spark-1.3-xhigh-prompt-safe.md](58-arena-muse-spark-1.3-xhigh-prompt-safe.md)
- [59-gemini-argo-high-prompt-bait.md](59-gemini-argo-high-prompt-bait.md)
- [60-arena-gpt-6.1-terra-low-prompt-bait.md](60-arena-gpt-6.1-terra-low-prompt-bait.md)
- [61-arena-deepseek-4.1-flash-high-prompt-bait.md](61-arena-deepseek-4.1-flash-high-prompt-bait.md)
- [62-arena-claude-haiku-5.5-xhigh-prompt-bait.md](62-arena-claude-haiku-5.5-xhigh-prompt-bait.md)
- [63-qwen-chat-qwen-3.8-max-prompt-bait.md](63-qwen-chat-qwen-3.8-max-prompt-bait.md)
- [64-z.ai-chat-glm-5.3-max-prompt-bait.md](64-z.ai-chat-glm-5.3-max-prompt-bait.md)
- [65-deepseek-flash-chat-prompt-bait.md](65-deepseek-flash-chat-prompt-bait.md)
- [66-gemini-argo-high-prompt-bait.md](66-gemini-argo-high-prompt-bait.md)
- [67-claude-haiku-extra-prompt-bait.md](67-claude-haiku-extra-prompt-bait.md) ([link](https://claude.ai/artifact/4H5av1QqiCLCNDF5ckg5ts))
- [68-chatgpt-prompt-bait.md](68-chatgpt-prompt-bait.md) ([link](https://chatgpt.com/share/6af2529b-8210-83ec-ad47-033d3be5cc6e))
- [69-deepseek-flash-chat-prompt-bait.md](69-deepseek-flash-chat-prompt-bait.md)
- [70-meta-ai-prompt-bait.md](70-meta-ai-prompt-bait.md)
- [71-claude-fable-5.2-medium.md](71-claude-fable-5.2-medium.md) ([link](https://claude.ai/share/1befca6e-fcdc-464a-8816-164ed8081d85))
- [72-gemini-argon-high-prompt-apex.md](72-gemini-argon-high-prompt-apex.md)
- [73-meta-ai-prompt-apex.md](73-meta-ai-prompt-apex.md)
- [74-deepseek-flash-chat-prompt-apex.md](74-deepseek-flash-chat-prompt-apex.md)

## Extraction status (integration tracker)

This tracker distinguishes source extraction from project integration: implementations are registered in the relevant harnesses, represented by scenarios/cases/attachments, and covered by meaningful tests/fuzz/benchmarks where applicable.
Documented exposure limits remain where the source semantics cannot satisfy a facet.
See also `AGENTS.md` for the repair policy.

Rows 03–20 are generated from the `Provenance:` header of each source file and from the attachment registry (`bigbuff-measure dump-registry`); an attachment count includes the per-family labels of the same implementation (for example `ChanFeed (04-DeepSeek)` in the broadcast family and `ChanFeed_drop_c64` in the payload family).
"No dedicated test file" is a statement about unit tests only: every attached implementation runs under the conformance tests of its family harness, the facets of each scenario it is attached to, the scenario fuzz targets and the scenario benchmarks.

| Response | Implementation | Source file(s) | Current state |
|---|---|---|---|
| 03 | `ChanLatest` | `chanlatest.go` | integrated; 7 scenario attachment(s); own test file(s): `chanlatest_test.go` |
| 04 | `ChanFeed` | `chanfeed.go` | integrated; 8 scenario attachment(s); no dedicated test file: covered by the family harness, its scenario facets, scenario fuzz and benchmarks |
| 05 | `ChanGatherer`, `ChanGathererPubSub` | `changatherer.go` | integrated; 4 scenario attachment(s); no dedicated test file: covered by the family harness, its scenario facets, scenario fuzz and benchmarks |
| 06 | `ChanCasterV2`, `ChanPubSubV2`, `GenCaster` | `chancaster_06_meta.go`, `chanpubsub_06_meta.go`, `gencaster.go` | integrated; 11 scenario attachment(s); no dedicated test file: covered by the family harness, its scenario facets, scenario fuzz and benchmarks |
| 07 | `ChanBeacon` | `chanbeacon.go` | integrated; 4 scenario attachment(s); no dedicated test file: covered by the family harness, its scenario facets, scenario fuzz and benchmarks |
| 08 | `Beacon`, `ChanCaster2`, `ReliableBeacon`, `RingBeacon` | `beacon.go`, `chancaster2.go`, `reliable_beacon.go`, `ring_beacon.go` | integrated; 16 scenario attachment(s); no dedicated test file: covered by the family harness, its scenario facets, scenario fuzz and benchmarks |
| 09 | `ChanCaster09Gemini`, `ChanPubSub09Gemini`, `EpochCasterGemini` | `chancaster_09_gemini.go`, `chanpubsub_09_gemini.go`, `epochcaster_09_gemini.go` | integrated; 8 scenario attachment(s); no dedicated test file: covered by the family harness, its scenario facets, scenario fuzz and benchmarks |
| 10 | `ChanPubSub10Gemini`, `EpochTape` | `chanpubsub_10_gemini.go`, `epochtape.go` | integrated; 7 scenario attachment(s); own test file(s): `epochtape_test.go` |
| 11 | `ChanCaster11Meta`, `EpochPubSub` | `chancaster_11_meta.go`, `epochpubsub_11_meta.go` | integrated; 6 scenario attachment(s); own test file(s): `chancaster_11_meta_test.go` |
| 12 | `ChanFanout` | `chanfanout.go` | integrated; 6 scenario attachment(s); own test file(s): `chanfanout_test.go` |
| 13 | `EpochCaster13Crouton`, `EpochCasterWithRingSize13Crouton` | `epochcaster_13_crouton.go` | integrated; 1 scenario attachment(s); own test file(s): `epochcaster_13_crouton_test.go` |
| 14 | `ChanCaster14Xing`, `ChanPubSub14Xing`, `RingFanOut14Xing` | `chancaster_14_xing.go`, `chanpubsub_14_xing.go`, `ringfanout_14_xing.go` | integrated; 9 scenario attachment(s); own test file(s): `ringfanout_14_xing_test.go` |
| 15 | `ChanCaster15Nemotron`, `ChanHub` | `chancaster_15_nemotron.go`, `chanhub.go` | integrated; 10 scenario attachment(s); no dedicated test file: covered by the family harness, its scenario facets, scenario fuzz and benchmarks |
| 16 | `ChanPubSub16DeepSeek`, `ChanScatter` | `chanpubsub_16_deepseek.go`, `chanscatter.go` | integrated; 3 scenario attachment(s); no dedicated test file: covered by the family harness, its scenario facets, scenario fuzz and benchmarks |
| 17 | `ChanCaster17FastPath`, `ChanCaster17Pumped`, `ChanPubSub17Gemini`, `ChanRotor` | `chancaster_17_gemini.go`, `chanpubsub_17_gemini.go`, `chanrotor.go` | integrated; 9 scenario attachment(s); no dedicated test file: covered by the family harness, its scenario facets, scenario fuzz and benchmarks |
| 18 | `RingPubSub` | `ringpubsub.go` | integrated; 8 scenario attachment(s); own test file(s): `ringpubsub_test.go` |
| 19 | `PhasePubSub`, `RelayPubSub` | `phasepubsub.go`, `relaypubsub.go` | integrated; 6 scenario attachment(s); own test file(s): `phasepubsub_test.go`, `relaypubsub_test.go` |
| 20 | `ChanRelay` | `chanrelay.go` | integrated; 11 scenario attachment(s); own test file(s): `chanrelay_test.go` |
| 21 | `Carillon` | `carillon.go` | adapter + broadcast scenario attachments; focused tests/benchmarks |
| 22 | `ChanPubSubPacked` | `chanpubsub_packed.go` | adapter + lockstep attachment; focused tests |
| 23 | `SeqCaster23Meta`, `AdaptiveCaster23Meta` | `seqcaster_23_meta.go`, `chancaster_23_meta.go` | `SeqCaster` adapter + loss scenario, loss/close/redirect tests and send benchmark; adaptive focused test |
| 24 | `QueuedFanout24Meta` | `queuedfanout_24_meta.go` | Extracted and wired to nonblocking unbounded overflow with `BaselineQueuedFanout`; repaired concurrent per-producer FIFO admission and withdrawal backlog drain; focused tests, FIFO fuzz, mailbox/overflow benchmarks. |
| 25 | `SenseCaster` | `sensecaster.go` | adapter + lockstep attachment; focused tests/benchmarks |
| 26 | `ChanRing26Kimi` | `chanring_26_kimi.go` | adapter + lossy-ring attachment; focused tests |
| 27 | `Multicast27Mistral` | `multicast_27_mistral.go` | eager-subscription adapter + synchronous-handoff attachment; focused tests |
| 28 | `Broadcast28Flint` | `broadcast_28_flint.go` | adapter + synchronous-handoff attachment; no dedicated unit test file; exercised by the six standard-harness conformance tests, its scenario facets, `FuzzScenarioBroadcast` and `BenchmarkScenarioBroadcast`, all green at GOMAXPROCS 1/2/4 and under the race detector. No defect is known; no adversarial review beyond that coverage was done |
| 29 | `Relay29Nemotron` | `relay_29_nemotron.go` | adapter + lossy-ring attachment; no dedicated unit test file; exercised by the six standard-harness conformance tests, its scenario facets, `FuzzScenarioBroadcast` and `BenchmarkScenarioBroadcast`, all green at GOMAXPROCS 1/2/4 and under the race detector. No defect is known; no adversarial review beyond that coverage was done |
| 30 | `Arcadia` | `arcadia_30_dots3.go` | adapter + lossy-ring attachment; no dedicated unit test file; exercised by the six standard-harness conformance tests, its scenario facets, `FuzzScenarioBroadcast` and `BenchmarkScenarioBroadcast`, all green at GOMAXPROCS 1/2/4 and under the race detector. No defect is known; no adversarial review beyond that coverage was done |
| 31 | `Vecast` | `vecast_31_nemotron.go` | adapter + synchronous-handoff attachment; no dedicated unit test file; exercised by the six standard-harness conformance tests, its scenario facets, `FuzzScenarioBroadcast` and `BenchmarkScenarioBroadcast`, all green at GOMAXPROCS 1/2/4 and under the race detector. No defect is known; no adversarial review beyond that coverage was done |
| 32 | `StableRing32Nano` | `stablering_32_nano.go` | Extracted and wired to bounded competing-consumer overflow with `BaselineBoundedScatterQueue`; exact eviction tests/fuzz/admission benchmark. Explicit poor-fit attachment for work-distribution scatter. |
| 33 | `ChanMulticast33North` | `chanmulticast_33_north.go` | adapter + handoff attachment; no dedicated unit test file; exercised by the six standard-harness conformance tests, its scenario facets, `FuzzScenarioBroadcast` and `BenchmarkScenarioBroadcast`, all green at GOMAXPROCS 1/2/4 and under the race detector. No defect is known; no adversarial review beyond that coverage was done |
| 34 | `ChanMultiCast34Lightning` | `chanmulticast_34_lightning.go` | exposure-limited adapter + focused test; churn excluded because removal can deadlock behind Send |
| 35 | `ChanPubSub35ChatJimmy` | `chanpubsub_35_chatjimmy.go` | caster-family registration + single-receiver acknowledged facet; explicit no-churn/identity exposure limit |
| 36 | `ShardCaster36Meta` | `shardcaster_36_meta.go` | adapter + synchronous-handoff attachment; direct unsubscribe/send tests; node pooling removed to prevent stale snapshot reuse |
| 37 | `SelectCaster37Gemini` | `selectcaster_37_gemini.go` | adapter + latest attachment; policy and concurrent Subscribe/Close tests; membership mutex added while Send remains lock-free |
| 38 | `SelectFanout` | `selectfanout.go` | Unified adapter + synchronous-handoff/churn/lifecycle/edge attachments; cancellation/close and churn benchmark coverage. |
| 39 | `ShardCast39Mistral` | `shardcast_39_mistral.go` | latest-policy adapter + latest attachment; drop/latest/close tests |
| 40 | `SlabCaster40Gemini` | `slabcaster_40_gemini.go` | DropOldest-only exposure-limited adapter + latest attachment; policy tests; concurrent slot lifecycle excluded |
| 41 | `EpochCast41Sonnet` | `epochcast_41_sonnet.go` | lossy adapter + bounded-ring attachment; lossless/lossy/cancel/close tests |
| 42 | `Broadcast42Spark` | `broadcast_42_spark.go` | adapter + bounded-ring attachment; batch/replay/overrun/close tests |
| 43 | `SlotPubSub43Grok` | `slotpubsub_43_grok.go` | exposure-limited lockstep adapter + acknowledgement attachment; deterministic fan-out test |
| 44 | `PhantomCast44Qwen` | `phantomcast_44_qwen.go` | repaired atomic-version ring + adapter + bounded-ring attachment; cursor/loss/wide-payload/close tests |
| 45 | `Fanout`, `RingFanout` | `fanout.go`, `ringfanout.go` | Unified sync/buffered Fanout and RingFanout adapters; handoff/backpressure/bounded-ring/replay/churn/lifecycle/edge attachments; replay repair and shared coverage. |
| 46 | `ChanHub46Grok` | `chanhub_46_grok.go` | cursor-ring adapter + buffered/churn/lifecycle/zero-subscriber attachments; atomic payload/closure/cursor repair; focused backpressure/close tests; shared broadcast fuzz + benchmark |
| 47 | `ChanCaster47Gemini`, `ChanPubSub47Gemini`, `ChanRing47Gemini` | `chancaster_47_gemini.go`, `chanpubsub_47_gemini.go`, `chanring_47_gemini.go` | caster + broadcaster adapters; lockstep, buffered, churn, cancellation, and edge attachments; direct tests/benchmarks + shared fuzz; repaired pong/cancellation accounting, serialized iterator cleanup, and removed racy slot clearing (payload retention bounded by capacity). Local probes observed nonzero allocations for both pub/sub and ring sends, so the response's zero-allocation claims are not treated as verified. |
| 48 | `WatchCaster48Grok` | `watchcaster_48_grok.go` | latest-watch adapter + latest/churn/edge attachments; coherent-snapshot, nil-payload, multi-observer wake tests; benchmark + shared fuzz. Replaced the non-broadcast cap-1 signal and split value/generation atomics with a rotating close-notify channel and locked snapshot; measured 1 alloc/send, so zero-allocation claim is not verified. |
| 49 | `SelectBroadcaster49Qwen` | `selectbroadcaster_49_qwen.go` | latest/churn/edge attachments plus explicit bad-fit attachment to detectable-loss scenario (no loss counter); immutable epochs, sequence-stamped slots, eager subscription point; overrun/multi-observer/state tests + terminal-checked benchmark + shared fuzz. One-shot benchmark observed 3 allocs/send; no zero-allocation claim accepted. |
| 50 | `GenPubSub50Meta` | `genpubsub_50_meta.go` | Completed the disconnected generation-wait design into a runnable namespaced implementation; repaired pre-ping round arming versus parity epoch identity and added a same-round Done gate. Wired to lockstep/churn/lifecycle/edge with send benchmark; measured 1 alloc/Send. |
| 51 | `ChanReplay51DeepSeek` | `chanreplay_51_deepseek.go` | lockstep/live adapter + new bounded-catch-up scenario with `BaselineBoundedReplay`; synchronized replay/live-boundary test, churn/edge/lifecycle coverage, snapshot + live-send benchmarks, shared fuzz. Bounded retention is evaluated separately from the unbounded-history scenario; one-shot probes observed nonzero allocations, so the source’s zero-allocation claim is not verified. |
| 52 | `ChanCascade52Opus` | `chancascade_52_opus.go` | Namespaced cooperative two-phase relay; fixed disabled-instance `Subscribe` contract gap; wired to synchronous handoff with exact fan-out, active-round join/withdraw tests, fuzz/harness/churn, and warmed direct send benchmark (observed 0 allocs/op for the measured Send loop). No further lifecycle review was done in the completion phase; no defect is known. |
| 53 | `ReplayCast53Gpt6Sol` | `replaycast_53_gpt6sol.go` | Bounded lossy ring + generation signal wired to bounded-ring and large-fanout scenarios; exact-missed-count model/fuzz, zero-subscriber/close/concurrent-publisher tests, adapter lifecycle/churn, direct publish benchmark. Fixed a zero-value guard race found by `-race`; active Publish measured 1 alloc/op (112 B/op), consistent with the one-notification-channel design. |
| 54 | `EpochBroadcast54Terra` | `epochbroadcast_54_terra.go` | Selectable epoch notification + receipt barrier integrated into lockstep, churn, zero-subscriber, lifecycle (per-subscription only; no hub Close), and large-fanout (explicit poor-fit measurement). Added membership-boundary/mid-send withdrawal tests and direct benchmark; focused race passed, warmed 8-subscriber probe observed 2 allocs/op. |
| 55 | `RingCaster55Haiku` | `ringcaster_55_haiku.go` | Immutable slab cells + caller-owned cursors wired to bounded-ring, large-fanout, churn, zero-subscriber, lifecycle; direct no-sleep Arm/recheck/Close test, concurrent publication, exact resume/lap accounting, allocation assertions, and ring-model fuzz/bench. Repaired resume-gap accounting, lapping that skipped still-retained cells, zero-value errors, late Arm-after-Close, and uint64 sequence wrap. Full tests/vet, focused race and fuzz passed; direct probes: slab allocation 1/256 values, TryRecv 0 allocs. |
| 56 | `Fanout56MaiCode` | `fanout_56_maicode.go` | Namespaced COW subscriber-slice fan-out attached to buffered backpressure, churn, zero-subscriber, lifecycle (sub-close only), and explicitly poor-fit lockstep/large-fanout reports. Added per-sub send/close exclusion and serialized Send order to honor no-post-Close delivery and consistent subscriber ordering; full tests/vet and focused race passed. Warmed 8-subscriber send measured 0 allocs/op. |
| 57 | `ChanPubSub57DeepSeek` | `chanpubsub_57_deepseek.go` | Namespaced phase-gated shared-channel pub/sub with an atomic acknowledgement plane and one-slot final-ack notification; retains ChanCaster withdrawal protocol. Adapter/harness/scenario attachments, direct tests, model-free shared fuzz, and eight-subscriber benchmark added. `go vet ./...`, `go test ./...`, shared FuzzScenarioBroadcast, focused race tests (direct x10 and churn/TryReceive x3) pass. Direct Send benchmark: 0 allocs/op. No hub-wide Close exists; lifecycle is explicitly cancellation/unsubscribe only. |
| 58 | `ChanCaster58Spark`, `ChanPubSub58Spark`, `Broadcaster58Spark` | `chancaster_58_spark.go`, `chanpubsub_58_spark.go`, `broadcaster_58_spark.go` | All three designs extracted and wired. PubSub uses a same-round Done gate (1 alloc/Send measured); caster TryRecv steal-block documented; broadcaster ring model fuzz/wide-payload/send benchmark. **Completion-phase repair:** `Broadcaster58Spark.Next` livelocked at GOMAXPROCS=1 (a lapped cursor re-aimed at the cell an in-flight `Send` was overwriting and spun on it while the sender could not run); the lap target is now derived from the published count, and a single-cell ring with a send in flight reports "nothing yet". Regression tests in `broadcaster_58_spark_test.go`; details in `extraction_audit.md`. |
| 59 | `HorizonRing59Gemini`, `StripedCombiner59Gemini` | `horizonring_59_gemini.go`, `combiner_59_gemini.go` | integrated (see `extraction_audit.md`); the combiner has no test file of its own and is tested from `horizonring_59_gemini_test.go` |
| 60 | `Latest3260Terra` | `latest32_60_terra.go` | integrated. The candidate is a non-parking polling register by design; the adapter adds a test-side wake gate (one non-blocking channel send per publish, charged to the adapter and stated in its attachment remarks) instead of the earlier timed backoff |
| 61 | `Pulse61Deepseek` | `pulse_61_deepseek.go` | integrated (see `extraction_audit.md`) |
| 62 | `Ring62Haiku` | `ring_62_haiku.go` | integrated in the completion phase: the source was already in the tree but had no adapter. Now attached to the lossy-ring, churn, lifecycle and fan-out scenarios. Single publisher by contract (overlapping `Publish` panics); the concurrent test drivers serialise publishes for it (`harnessPublish`), measurements publish from one goroutine |
| 63 | `TelemetryRing63Qwen` | `telemetryring_63_qwen.go`, `_fast.go`, `_race.go` | integrated. **Completion-phase repair:** the ring-wide capacity-1 wake token woke one parked reader of many (3 guard timeouts in 15 000 runs of its scenario on the untouched tree); replaced by an armed-waiter count and a relayed token (`NextWait`), 60 000 runs clean afterwards. Not attached to the zero-subscriber scenario (no published-count accessor). Single publisher, serialised as for 62 |
| 64 | `Beacon64GLM` | `beacon_64_glm*.go`, `beacon_64_glm_fence_{amd64,arm64}.s` | integrated; ships no benchmarks of its own (scenario benchmarks only); arm64 fence assembled, never executed |
| 65 | `Broadcaster65Deepseek` | `broadcast_65_deepseek.go` | integrated. Completion phase added `NextContext` (the candidate's wait plus a context) so the adapter no longer polls on a timed backoff |
| 66 | `Horizon66Gemini` | `horizon_66_gemini.go` | integrated (see `extraction_audit.md`) |
| 67 | `Ring67Haiku` | `ring_67_haiku.go` | integrated. The adapter's non-blocking read used a 1 ns timeout; it now uses an already-cancelled context |
| 68 | `EdgeFanIn68Chatgpt` | `edgefanin_68_chatgpt.go` | integrated; the only candidate in the fan-in family |
| 69 | `WatchCell69Deepseek` | `watchcell_69_deepseek.go` | integrated (see `extraction_audit.md`) |
| 70 | `Horizon70Meta` | `horizon_70_meta*.go` | integrated (see `extraction_audit.md`) |
| 71 | `ChanRing71Fable` | `chanring_71_fable.go` | integrated in the completion phase, **no repairs**: compiled, vetted and passed its own tests as delivered. Lossy multi-sender ring with value cursors. Attached to five broadcast scenarios; candidate tests ported |
| 72 | `PhaseCaster72Gemini`, `SymExchanger72Gemini`, bitmap scan kernels | `phasecaster_72_gemini{,_fast,_race}.go`, `symexchanger_72_gemini.go`, `bitmap_72_gemini{,_asm,_generic}.go`, `bitmap_72_gemini_{amd64,arm64}.s` | integrated in the completion phase. **Did not compile as delivered** (and the arm64 assembly did not assemble). Six repairs to the broadcaster, three to the exchanger, six to the bitmap kernels; all listed in `extraction_audit.md`. The broadcaster is attached twice (one-phase and acknowledged) to the broadcast family and to the new barrier scenario; the exchanger to the new exchange scenario. The bitmap kernels are not wired into the broadcaster (the response's wiring sketch does not compile); they are parity-tested stand-alone and excluded from measurements. The response's zero-allocation claim holds for one-phase `Broadcast` and for `Exchange` in the default build (tested); `BroadcastPhased` allocates its gate |
| 73 | `Cohort73Meta` | `cohort_73_meta.go` | integrated in the completion phase. The response's test file did not compile. Two repairs (snapshot/epoch mispairing; `Close` during a barrier reported success); listed in `extraction_audit.md`. Attached to the broadcast family and the new barrier scenario |
| 74 | `Fanout74Deepseek` | `fanout_74_deepseek{,_fast,_race}.go` | integrated in the completion phase, **no repairs**. Synchronous hand-off where reading is the acknowledgement. The response's statement that abandoning `Next` releases the broadcaster is false and is pinned by a test. Zero allocations per `Send` in the default build (tested). Attached to the broadcast family and the new barrier scenario |

## Scenarios added in the completion phase

Responses 72–74 answer a different prompt from the earlier ones (acknowledged delivery under a deadline; pairwise exchange). Two scenario kinds were added so they are compared with each other and with a textbook implementation, rather than only being adapted into the broadcast family:

| Scenario | Kind | Baseline | Attached |
|---|---|---|---|
| `symmetric-pairwise-exchange` | exchange | `BaselineExchanger` (`baseline_exchange.go`: one mutex, one waiting offer) | `SymExchanger72Gemini` with its default arena; the same with a one-slot arena (attached as *not* an expected good fit, to show what the arena buys) |
| `deadline-bounded-barrier-broadcast` | barrier | `BaselineDeadlineBarrier` (`baseline_deadline_barrier.go`: mutex, per-send pending set, done channel) | `PhaseCaster72Gemini` (acknowledged), `Cohort73Meta`, `Fanout74Deepseek` |

Each has a test with named facets, a benchmark and a fuzz target (`scenario_exchange_test.go`, `scenario_barrier_test.go`), registered as cases.
**Neither has a measurement battery**: they do not appear in the `results-NN-*` JSONL or the interactive report. Their numbers come from `go test -bench 'BenchmarkScenario(Exchange|Barrier)'` and are reported in `report-02-completion-evaluation.md`.

## Registry guarantees

The registry now refuses to load (package `init` panics) if a scenario's declared baseline is not attached to it, or if an attachment in a key-indexed family names an implementation with no factory. Broadcast-family attachments are resolved to factories by `TestScenarioRegistryComplete`, because their factories are indexed by a name only a constructed instance reports.
`TestScenarioCaseRunnablesExhaustive` accounts for every top-level `Test`/`Benchmark`/`Fuzz` function: each is a registered case, or lives in an implementation's own test file, or is listed with a reason in `harnessRunnables`. It does **not** turn the ~500 implementation unit tests into registered cases.
