# AI: Attempting novel useful concurrency as a one-shot prompt

[Original gist](https://gist.github.com/joeycumines/a20369832bf361063748d1bd18924d9d)

## Premise

Free chat and generous quotas (for chat-sized contexts) are widely available - if you don't mind what happens to your data, anyway.

Armed with these various chatbots, I decided to test how well AI would respond to a highly technical programming challenge - something strictly in their wheelhouse - where *novelty* was one of the factors (part of the ask).

## Input

The [prompt](02-prompt.md) (built with [osm prompt-flow](https://github.com/joeycumines/one-shot-man)) lead with:

> Advance the implementation to its ultimate form by evolving one or both implementations to achieve a much greater height of sophistication AND cleverness, AND/OR implement one additional, complementary implementation, of greater sophistication and extremely strong broad applicability, with similarly nuanced and interesting characteristics as this implementation's extremely stable and predictable (very low) allocation coupled with performance comparable (if lesser) to a linear "callback channel" oriented design + its obvious extremely valuable support for use within a select (which the callback-oriented design achieves, though in a manner that doesnt scale with churn of consumers or large numbers of consumers), as an expert in low-level Go runtime optimisations. N.B. Novelty of design coupled with exceptionally strong characteristics is the kind of solution I require.

Followed by a txtar containing two hand-written, distinctive implementations, which are unlikely to be represented in any training corpus in any material capacity:

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

The synchronous rendezvous has an obvious drawback - it doesn't scale well with number of subscribers.
Essentially all outputs identified this characteristic.

## Output

Still in the original gist flat project layout, so start at [01-readme.md](./01-readme.md). That file contains an index linking to each model response, in some cases annotated with a link to the actual chat session.

## Baselines

For apples-to-apples comparison, each major behavioral variant has a
standard-practice baseline (simplest sound design, deliberately not novel -
see [baseline.go](./baseline.go)):

| Variant                                  | Baseline                       |
|------------------------------------------|--------------------------------|
| Lock-step pub-sub (lossless broadcast)   | `BaselineBarrierPubSub` (baseline.go) |
| Rendezvous caster (N receivers per Send) | `BaselineRendezvousCaster` (baseline.go) |
| Latest-value coalescing broadcast        | `BaselineLatestBroadcast` (baseline.go) |
| Buffered lossless broadcast (backpressure) | `BaselineBufferedBroadcast` (baseline.go) |
| Work distribution (exactly one receiver) | `BaselineScatterQueue` (baseline.go) |
| Signal gathering (N senders, 1 waiter)   | `BaselineGatherBarrier` (baseline.go) |
| Async fan-out (never blocks the sender)  | `BaselineFanout` (baseline.go) |

Coverage is organised by scenario / case / attachment (see `scenario_test.go`):
each scenario in `Scenarios` has a staff-engineer-in-a-vacuum baseline above
attached to it, every ported implementation is attached to at least one
scenario with remarks and an expected-good-fit flag, and known structural
deficiencies are flagged as exposure-limited attachments whose facets avoid
exercising them. `TestScenarioRegistryComplete` enforces that exhaustiveness.

All baselines are wired into the unified harnesses (`TestStandardHarness_*`, `TestAsyncFamily`, `TestPubSubFamily`,
`TestCasterFamily`, `BenchmarkUnified_*`, `BenchmarkAsync`).

## License

Copyright 2026 Joseph Cumines
