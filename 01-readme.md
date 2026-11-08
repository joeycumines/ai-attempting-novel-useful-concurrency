# Model benchmark: Novel concurrency implementations

N.B. If you're reading this as a gist it is also an actual [repository](https://github.com/joeycumines/ai-attempting-novel-useful-concurrency).

## Extraction status (integration tracker)

Responses 21-58 are **not fully integrated yet**. This tracker distinguishes
source extraction from the required project integration: implementations must
also be registered in the relevant harnesses, represented by scenarios/cases/
attachments, and covered by meaningful tests and benchmarks where applicable.
`source present` is not a claim of complete integration. See `AGENTS.md` and
provenance comments in extracted source files for the repair policy.

| Response | Implementation | Source file(s) | Current state |
|---|---|---|---|
| 21 | `Carillon` | `carillon.go` | adapter + broadcast scenario attachments; focused tests/benchmarks |
| 22 | `ChanPubSubPacked` | `chanpubsub_packed.go` | adapter + lockstep attachment; focused tests |
| 23 | `SeqCaster23Meta`, `AdaptiveCaster23Meta` | `seqcaster_23_meta.go`, `chancaster_23_meta.go` | `SeqCaster` adapter + loss scenario, loss/close/redirect tests and send benchmark; adaptive focused test |
| 24 | `CoalescingBroadcaster24Meta` | — | not integrated |
| 25 | `SenseCaster` | `sensecaster.go` | adapter + lockstep attachment; focused tests/benchmarks |
| 26 | `ChanRing26Kimi` | `chanring_26_kimi.go` | adapter + lossy-ring attachment; focused tests |
| 27 | `Multicast27Mistral` | `multicast_27_mistral.go` | eager-subscription adapter + synchronous-handoff attachment; focused tests |
| 28 | `Broadcast28Flint` | `broadcast_28_flint.go` | adapter + synchronous-handoff attachment; source-level validation only |
| 29 | `Relay29Nemotron` | `relay_29_nemotron.go` | adapter + lossy-ring attachment; source-level validation only |
| 30 | `Arcadia` | `arcadia_30_dots3.go` | adapter + lossy-ring attachment; probe test passed, repository-specific test still outstanding |
| 31 | `Vecast` | `vecast_31_nemotron.go` | adapter + synchronous-handoff attachment; probe test passed, repository-specific test still outstanding |
| 32 | `StableRing` | `stablering_32_nano.go` | source only; tests and wiring outstanding |
| 33 | `ChanMulticast33North` | `chanmulticast_33_north.go` | adapter + handoff attachment; probe test passed, repository-specific test still outstanding |
| 34 | `ChanMultiCast34Lightning` | `chanmulticast_34_lightning.go` | exposure-limited adapter + focused test; churn excluded because removal can deadlock behind Send |
| 35 | `ChanPubSub35ChatJimmy` | `chanpubsub_35_chatjimmy.go` | caster-family registration + single-receiver acknowledged facet; explicit no-churn/identity exposure limit |
| 36 | `ShardCaster36Meta` | `shardcaster_36_meta.go` | adapter + synchronous-handoff attachment; direct unsubscribe/send tests; node pooling removed to prevent stale snapshot reuse |
| 37 | `SelectCaster37Gemini` | `selectcaster_37_gemini.go` | adapter + latest attachment; policy and concurrent Subscribe/Close tests; membership mutex added while Send remains lock-free |
| 38 | `SelectFanout` | `selectfanout.go` | existing source + focused tests; unified adapter/attachment still outstanding |
| 39 | `ShardCast39Mistral` | `shardcast_39_mistral.go` | latest-policy adapter + latest attachment; drop/latest/close tests |
| 40 | `SlabCaster40Gemini` | `slabcaster_40_gemini.go` | DropOldest-only exposure-limited adapter + latest attachment; policy tests; concurrent slot lifecycle excluded |
| 41 | `EpochCast41Sonnet` | `epochcast_41_sonnet.go` | lossy adapter + bounded-ring attachment; lossless/lossy/cancel/close tests |
| 42 | `Broadcast42Spark` | `broadcast_42_spark.go` | adapter + bounded-ring attachment; batch/replay/overrun/close tests |
| 43 | `SlotPubSub43Grok` | `slotpubsub_43_grok.go` | exposure-limited lockstep adapter + acknowledgement attachment; deterministic fan-out test |
| 44 | `PhantomCast44Qwen` | `phantomcast_44_qwen.go` | repaired atomic-version ring + adapter + bounded-ring attachment; cursor/loss/wide-payload/close tests |
| 45 | `Fanout`, `RingFanout` | `fanout.go`, `ringfanout.go` | existing source + focused tests; unified adapter/attachment still outstanding |
| 46 | `ChanHub46Grok` | `chanhub_46_grok.go` | cursor-ring adapter + buffered/churn/lifecycle/zero-subscriber attachments; atomic payload/closure/cursor repair; focused backpressure/close tests; shared broadcast fuzz + benchmark |
| 47 | `ChanCaster47Gemini`, `ChanPubSub47Gemini`, `ChanRing47Gemini` | `chancaster_47_gemini.go`, `chanpubsub_47_gemini.go`, `chanring_47_gemini.go` | caster + broadcaster adapters; lockstep, buffered, churn, cancellation, and edge attachments; direct tests/benchmarks + shared fuzz; repaired pong/cancellation accounting, serialized iterator cleanup, and removed racy slot clearing (payload retention bounded by capacity). Local probes observed nonzero allocations for both pub/sub and ring sends, so the response's zero-allocation claims are not treated as verified. |
| 48 | `WatchCaster48Grok` | `watchcaster_48_grok.go` | latest-watch adapter + latest/churn/edge attachments; coherent-snapshot, nil-payload, multi-observer wake tests; benchmark + shared fuzz. Replaced the non-broadcast cap-1 signal and split value/generation atomics with a rotating close-notify channel and locked snapshot; measured 1 alloc/send, so zero-allocation claim is not verified. |
| 49 | `SelectBroadcaster49Qwen` | `selectbroadcaster_49_qwen.go` | latest/churn/edge attachments plus explicit bad-fit attachment to detectable-loss scenario (no loss counter); immutable epochs, sequence-stamped slots, eager subscription point; overrun/multi-observer/state tests + terminal-checked benchmark + shared fuzz. One-shot benchmark observed 3 allocs/send; no zero-allocation claim accepted. |
| 50 | `GenPubSub` | — | assessed: response is design prose plus disconnected snippets, not a complete implementation; its claimed no-`Done` unsubscribe accounting and two-generation WaitGroup reuse do not establish correct participant pairing/reuse. Revisit against response 57; no runnable source claimed. |
| 51 | `ChanReplay51DeepSeek` | `chanreplay_51_deepseek.go` | lockstep/live adapter + new bounded-catch-up scenario with `BaselineBoundedReplay`; synchronized replay/live-boundary test, churn/edge/lifecycle coverage, snapshot + live-send benchmarks, shared fuzz. Bounded retention is evaluated separately from the unbounded-history scenario; one-shot probes observed nonzero allocations, so the source’s zero-allocation claim is not verified. |
| 52 | `ChanCascade52Opus` | `chancascade_52_opus.go` | Namespaced cooperative two-phase relay; fixed disabled-instance `Subscribe` contract gap; wired to synchronous handoff with exact fan-out, active-round join/withdraw tests, fuzz/harness/churn, and warmed direct send benchmark (observed 0 allocs/op for the measured Send loop). Continue adversarial lifecycle review with later batches. |
| 53 | `ReplayCast` | — | not integrated |
| 54 | `EpochBroadcast` | — | not integrated |
| 55 | `RingCaster` | — | not integrated |
| 56 | `Fanout56MaiCode` | — | not integrated |
| 57 | `ChanPubSub57DeepSeek` | — | not integrated |
| 58 | `Broadcaster58Spark` | — | not integrated |

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

**Integration progress:** responses 46–49 and 51–52 are wired and tested; response 50 is assessed as incomplete pseudocode; continuing with responses 53–58 and the remaining tracker follow-ups.

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
