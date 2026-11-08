package bigbuff

// Scenario / Case / Attachment architecture (see AGENTS.md for rationale).
//
//   - A Scenario is a broad use-case: "... for use case X with Y
//     characteristics". It is defined by its trade-offs, loss policy and
//     invariants — NOT by a single load event.
//   - A Case is one concrete, runnable facet of a scenario (a specific test,
//     benchmark or fuzz entry point). Cases live in the declarative Cases
//     slice; CaseRegistry (built in init) is the reporting dimension.
//   - An Attachment binds one implementation (or operational variant of one)
//     to a scenario, with remarks, an expected-good-fit flag, and an explicit
//     exposure-limitation flag + remarks for known deficiencies that facets
//     must avoid exercising.
//
// Registries are validated in init(): any duplicate ID, unknown scenario,
// missing remarks/fit justification, or mismatched exposure metadata panics
// at test-binary startup, so an incompletely-registered addition can never
// silently pass.

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

type ScenarioID string
type CaseID string

type LossPolicy int

const (
	Lossless          LossPolicy = iota // every value, in order, to every subscriber
	CoalescingLatest                    // newest value always observed; intermediates may merge
	LossyDetectable                     // values may be skipped, but the skip is reported/counted
	LossySnapToLatest                   // overrun silently snaps the cursor to the latest value
	ExactlyOne                          // each value delivered to exactly one receiver (scatter)
	GatherAll                           // signals from all senders collected (gather)
)

type ScenarioKind int

const (
	KindBroadcast ScenarioKind = iota
	KindCaster
	KindScatter
	KindGather
	KindPayload // multi-word payload integrity, driven via asyncAPI
)

type Scenario struct {
	ID         ScenarioID
	Title      string
	UseCase    string // "... for use case X with Y characteristics"
	Tradeoffs  string
	Kind       ScenarioKind
	Loss       LossPolicy
	Baseline   string // Impl key of the staff-engineer-in-a-vacuum baseline
	Invariants []string
}

var Scenarios = []Scenario{
	{ID: "lockstep-barrier-lossless", Title: "Lock-step lossless broadcast with acknowledgement barrier",
		UseCase:   "Ordered command/config distribution for consumers that must all receive and acknowledge each value before the publisher may proceed",
		Tradeoffs: "Publisher latency is bounded by the slowest subscriber; throughput does not scale with fan-out, in exchange for a hard delivery+ack guarantee and in-order delivery.",
		Kind:      KindBroadcast, Loss: Lossless, Baseline: "BaselineBarrierPubSub (baseline)",
		Invariants: []string{"every subscriber receives every value exactly once, in publish order", "Send does not return before every subscriber registered at send time has received (and, where the API separates it, acknowledged)", "no delivery to subscribers registered after the send"}},
	{ID: "rendezvous-cast-no-ack", Title: "Rendezvous cast to N receivers without post-receive barrier",
		UseCase:   "Synchronous hand-off of each value to a known set of receivers for lock-step simulation ticks and staged pipelines where receipt itself is the synchronisation",
		Tradeoffs: "Send performs one rendezvous per receiver and returns at receipt, not after processing; cheapest possible synchronous fan-out, but receivers must re-register per round and a missing receiver stalls the sender.",
		Kind:      KindCaster, Loss: Lossless, Baseline: "ChanCaster",
		Invariants: []string{"each Send is received by exactly the receivers registered for that round", "Send's return equals the number of actual receives", "a withdrawing receiver can never hang a Send"}},
	{ID: "synchronous-handoff-no-barrier", Title: "Synchronous handoff broadcast without acknowledgement barrier",
		UseCase:   "Tick/phase distribution for workers that must all be handed the current value before it is superseded, where processing acknowledgement is unnecessary",
		Tradeoffs: "Send waits for handoff (receipt/readiness) but not processing; faster than a full barrier and still lossless, but a subscriber that never becomes ready stalls the publisher.",
		Kind:      KindBroadcast, Loss: Lossless, Baseline: "BaselineBarrierPubSub (baseline)",
		Invariants: []string{"every subscriber receives every value in order", "Send returns after handoff to all subscribers registered at send time"}},
	{ID: "buffered-lossless-backpressured", Title: "Buffered lossless broadcast with backpressure",
		UseCase:   "Event streaming for consumers with independent pacing and bounded lag tolerance, where no event may be lost and a lagging consumer should slow the publisher rather than drop data",
		Tradeoffs: "Memory is bounded by per-subscriber capacity; publisher can be stalled by the slowest consumer (or must use a context/try variant); delivery stays lossless and ordered while lag < capacity.",
		Kind:      KindBroadcast, Loss: Lossless, Baseline: "BaselineBufferedBroadcast (baseline)",
		Invariants: []string{"while subscribers keep lag below capacity, every value is delivered in order", "Send applies backpressure instead of dropping", "memory stays bounded regardless of publish volume"}},
	{ID: "bounded-ring-lossy-detectable", Title: "Bounded ring broadcast, lossy under overrun with detectable skips",
		UseCase:   "High-rate telemetry/market-data style feeds for many consumers where keeping up matters more than completeness and a consumer must be able to tell (and count) what it missed",
		Tradeoffs: "Send is O(1)-ish and never waits for consumers; consumers that fall more than capacity behind lose values, but the loss is reported (skipped/missed) so they can resync.",
		Kind:      KindBroadcast, Loss: LossyDetectable, Baseline: "BaselineFanout (standard)",
		Invariants: []string{"received values are strictly increasing (no duplicates, no reordering, no torn values)", "the final value is always eventually observable", "Send never blocks on a slow consumer"}},
	{ID: "latest-value-conflating-watch", Title: "Latest-value conflating broadcast / watch",
		UseCase:   "State observation — configuration snapshots, health/progress, cache invalidation — for watchers that only ever care about the newest state and integrate with select",
		Tradeoffs: "Intermediate values may be conflated away entirely; in exchange Send is non-blocking, cheap at any fan-out, and watchers can never be stalled by (or stall) the publisher.",
		Kind:      KindBroadcast, Loss: CoalescingLatest, Baseline: "BaselineLatestBroadcast (baseline)",
		Invariants: []string{"observed values are strictly increasing and the latest published value is always eventually observed", "Send never blocks on any subscriber", "no stale value is observed after a newer one"}},
	{ID: "unbounded-catchup-replay", Title: "Unbounded catch-up broadcast (full history replay)",
		UseCase:   "Audit/event-sourcing style distribution for late or slow consumers that must eventually see every value in order, where retention cost is accepted",
		Tradeoffs: "Delivery is lossless for arbitrarily lagging consumers; in exchange memory retention is pinned by the slowest live consumer and grows without bound if one never reads.",
		Kind:      KindBroadcast, Loss: Lossless, Baseline: "BaselineBufferedBroadcast (baseline)",
		Invariants: []string{"every subscriber can eventually read every value published after it subscribed, in order, however far behind it falls"}},
	{ID: "bounded-catchup-replay", Title: "Bounded recent-history catch-up with lossless live delivery",
		UseCase:   "Late observers need a recent context window before joining a live, ordered stream, and deliberately accept that history older than a fixed retention bound is unavailable",
		Tradeoffs: "Memory remains bounded and a new observer can replay the recent window atomically with live registration; a larger window costs more snapshot/copy work and cannot satisfy full audit-history requirements.",
		Kind:      KindBroadcast, Loss: Lossless, Baseline: "BaselineBoundedReplay (baseline)",
		Invariants: []string{"a joining observer sees the retained suffix in order with no gap or duplicate at the live boundary", "all values published after registration are delivered once in order", "retention is bounded by configured capacity and older history is intentionally absent"}},

	{ID: "work-distribution-scatter", Title: "Work distribution to exactly one receiver (scatter / competing consumers)",
		UseCase:   "Task dispatch for a dynamic worker pool where each item must be processed by exactly one worker and workers join/leave freely",
		Tradeoffs: "No broadcast cost and natural load balancing (whichever worker is ready wins); in exchange delivery is exactly-one, so this is useless for notification, and a withdrawing worker must never orphan an in-flight dispatch.",
		Kind:      KindScatter, Loss: ExactlyOne, Baseline: "BaselineScatterQueue",
		Invariants: []string{"every dispatched value is received by exactly one receiver", "Send returns 0 iff no receiver was registered, 1 iff exactly one received (including via steal compensation)", "withdrawal of the last receiver never hangs a Send"}},
	{ID: "signal-gather-barrier", Title: "Signal gathering from N senders (N-to-1 barrier)",
		UseCase:   "Phase/completion barriers for a dynamic set of producers whose signals must all be collected before a coordinator proceeds (the dual of casting)",
		Tradeoffs: "Wait performs one rendezvous per registered sender; a sender that withdraws without signalling must be compensated or Wait hangs, which is the central correctness hazard of the family.",
		Kind:      KindGather, Loss: GatherAll, Baseline: "BaselineGatherBarrier",
		Invariants: []string{"Wait returns after exactly one signal per registered sender", "a withdrawing sender is compensated so Wait cannot hang", "Wait's return equals the number of real (non-compensated) signals"}},
	{ID: "ephemeral-churn-membership", Title: "Broadcast under ephemeral membership (high subscribe/unsubscribe churn)",
		UseCase:   "Short-lived consumer workloads — per-request subscriptions, websocket sessions, plugin hooks — where subscriptions are created and destroyed continuously while publishing proceeds",
		Tradeoffs: "Subscribe/unsubscribe cost and its contention with Send dominate; implementations that rebuild or lock the whole registry per membership change degrade, while per-value guarantees may legitimately weaken to 'no corruption, no stall, no duplicate/reordered delivery within a session'.",
		Kind:      KindBroadcast, Loss: LossyDetectable, Baseline: "BaselineFanout (standard)",
		Invariants: []string{"within one subscription session, values are never duplicated or reordered", "churn never deadlocks, panics or corrupts a publisher", "every long-lived session eventually observes the final value (lossless variants) or the latest value (conflating variants)"}},
	{ID: "large-fanout-scalability", Title: "Broadcast at large fan-out (hundreds of subscribers)",
		UseCase:   "Fan-out to hundreds of simultaneous consumers for dashboards, game-state replication and notification storms, where per-Send cost scaling with N is the deciding factor",
		Tradeoffs: "O(N)-rendezvous designs pay linearly per Send; O(1)-wake designs pay in buffering/conflation. The scenario measures that scaling honestly rather than declaring a universal winner.",
		Kind:      KindBroadcast, Loss: LossyDetectable, Baseline: "BaselineFanout (standard)",
		Invariants: []string{"correctness invariants of the implementation's own loss policy hold at N=32 in tests (N up to 512 in benchmarks)", "Send completes without deadlock at full fan-out"}},
	{ID: "wide-payload-integrity", Title: "Wide/multi-word payload broadcast integrity",
		UseCase:   "Dissemination of multi-word values (structs, snapshots, frames) for consumers that must never observe a torn, half-published value",
		Tradeoffs: "Pointer/slot publication costs an indirection but guarantees atomic observation; direct struct writes into shared slots are faster but can tear or trip the race detector under overrun.",
		Kind:      KindPayload, Loss: LossyDetectable, Baseline: "BaselineFanout_c64",
		Invariants: []string{"no consumer ever observes a torn value (all words equal)", "values are never duplicated or reordered within a session"}},
	{ID: "cancellation-and-close-lifecycle", Title: "Cancellation and close lifecycle",
		UseCase:   "Long-running services where contexts are cancelled and broadcasters are closed mid-stream, and shutdown must be prompt, idempotent and leak-free",
		Tradeoffs: "Implementations without a Close/context story are structurally limited here regardless of hot-path merit; facets must only exercise the lifecycle an implementation actually documents.",
		Kind:      KindBroadcast, Loss: LossyDetectable, Baseline: "BaselineFanout (standard)",
		Invariants: []string{"a cancelled subscriber stops promptly and never blocks a publisher afterwards", "Close (where it exists) is idempotent", "no facet exercises a lifecycle operation the implementation does not document"}},
	{ID: "zero-subscriber-and-edge-lifecycle", Title: "Zero-subscriber and edge lifecycle",
		UseCase:   "Idle-period publishing and start-up/shut-down edges for any broadcaster: publishing with nobody listening, subscribing after idle periods, double unsubscribe",
		Tradeoffs: "Fast paths for the empty case matter for idle-heavy workloads; several synchronous designs legitimately treat 'no subscribers' specially, which must be verified rather than assumed.",
		Kind:      KindBroadcast, Loss: Lossless, Baseline: "BaselineFanout (standard)",
		Invariants: []string{"Publish with zero subscribers never blocks and reports 0 (or the documented equivalent)", "a subscriber registered after idle publishes receives subsequent values per the scenario's loss policy"}},
}

var scenarioByID map[ScenarioID]Scenario

// --- Cases: the concrete runnable facet identity dimension (for reporting) ---

type CaseKind string

const (
	CaseTest      CaseKind = "test"
	CaseBenchmark CaseKind = "bench"
	CaseFuzz      CaseKind = "fuzz"
)

type Case struct {
	ID       CaseID
	Scenario ScenarioID
	Kind     CaseKind
	Title    string
	Runnable string // Go test / benchmark / fuzz function that executes this case
}

var Cases = []Case{
	{"lockstep/test/ordered-ack-fanout", "lockstep-barrier-lossless", CaseTest, "Ordered fan-out with ack barrier, 4 subscribers x 20 values", "TestScenarioBroadcast"},
	{"lockstep/bench/fanout", "lockstep-barrier-lossless", CaseBenchmark, "Publish throughput at 8 subscribers", "BenchmarkScenarioBroadcast"},
	{"lockstep/fuzz/op-stream", "lockstep-barrier-lossless", CaseFuzz, "Randomised subscribe/publish/receive/unsubscribe stream", "FuzzScenarioBroadcast"},
	{"epochbroadcast/test/fanout-count", "lockstep-barrier-lossless", CaseTest, "Eight cursors receive each value before Send returns", "TestEpochBroadcast54Fanout"},
	{"epochbroadcast/test/join-withdraw", "lockstep-barrier-lossless", CaseTest, "Join at epoch boundary and release an outstanding obligation by unsubscribe", "TestEpochBroadcast54MembershipAndUnsubscribe"},
	{"epochbroadcast/bench/send", "lockstep-barrier-lossless", CaseBenchmark, "Eight-subscriber epoch publication and receive acknowledgement", "BenchmarkEpochBroadcast54Send"},
	{"cast/test/steady-and-churn", "rendezvous-cast-no-ack", CaseTest, "Caster steady + churn stress (return == actual receives)", "TestScenarioCaster"},
	{"cast/bench/rendezvous", "rendezvous-cast-no-ack", CaseBenchmark, "Rendezvous cost at 8 receivers", "BenchmarkScenarioCaster"},
	{"cast/fuzz/op-stream", "rendezvous-cast-no-ack", CaseFuzz, "Randomised add/send/withdraw stream", "FuzzScenarioCasterScatterGather"},
	{"handoff/test/ordered-fanout", "synchronous-handoff-no-barrier", CaseTest, "Ordered handoff fan-out, 4 subscribers x 20 values", "TestScenarioBroadcast"},
	{"handoff/bench/fanout", "synchronous-handoff-no-barrier", CaseBenchmark, "Handoff publish throughput at 8 subscribers", "BenchmarkScenarioBroadcast"},
	{"cascade/test/fanout-phases", "synchronous-handoff-no-barrier", CaseTest, "Exact delivery over repeated alternating-phase rounds at fan-outs 1–32", "TestChanCascade52FanoutAndHandoff"},
	{"cascade/test/join-unsubscribe", "synchronous-handoff-no-barrier", CaseTest, "Join at the active-round boundary and absorb a mid-send unsubscribe", "TestChanCascade52JoinAndUnsubscribeDuringSend"},
	{"cascade/test/disabled-instance", "synchronous-handoff-no-barrier", CaseTest, "Disabled nil-transport instances reject subscriber registration", "TestChanCascade52DisabledInstancePanicsOnSubscribe"},
	{"cascade/bench/live-send", "synchronous-handoff-no-barrier", CaseBenchmark, "Eight-receiver live-send throughput and allocations", "BenchmarkScenarioBroadcast"},
	{"cascade/bench/source-send", "synchronous-handoff-no-barrier", CaseBenchmark, "Response-52 direct send measurement with eight receivers", "BenchmarkChanCascade52Send"},
	{"buffered/test/backpressured-fanout", "buffered-lossless-backpressured", CaseTest, "Lossless ordered fan-out with draining subscribers", "TestScenarioBroadcast"},
	{"buffered/bench/fanout", "buffered-lossless-backpressured", CaseBenchmark, "Buffered publish throughput at 8 subscribers", "BenchmarkScenarioBroadcast"},
	{"buffered/fuzz/op-stream", "buffered-lossless-backpressured", CaseFuzz, "Randomised op stream over buffered attachments", "FuzzScenarioBroadcast"},
	{"ring-lossy/test/monotonic-final", "bounded-ring-lossy-detectable", CaseTest, "Monotonic delivery, final value always observed, sends bounded to capacity where exposure-limited", "TestScenarioBroadcast"},
	{"ring-lossy/bench/fanout", "bounded-ring-lossy-detectable", CaseBenchmark, "Ring publish throughput at 8 subscribers", "BenchmarkScenarioBroadcast"},
	{"ring-lossy/fuzz/op-stream", "bounded-ring-lossy-detectable", CaseFuzz, "Randomised op stream over ring attachments", "FuzzScenarioBroadcast"},
	{"latest/test/conflated-final", "latest-value-conflating-watch", CaseTest, "Strictly increasing observations, latest always observed, Send non-blocking", "TestScenarioBroadcast"},
	{"latest/bench/fanout", "latest-value-conflating-watch", CaseBenchmark, "Conflating publish throughput at 8 subscribers", "BenchmarkScenarioBroadcast"},
	{"latest/fuzz/op-stream", "latest-value-conflating-watch", CaseFuzz, "Randomised op stream over conflating attachments", "FuzzScenarioBroadcast"},
	{"replay/test/full-catchup", "unbounded-catchup-replay", CaseTest, "Lagging subscriber reads full ordered history", "TestScenarioBroadcast"},
	{"replay/bench/fanout", "unbounded-catchup-replay", CaseBenchmark, "Replay publish throughput at 8 subscribers", "BenchmarkScenarioBroadcast"},
	{"bounded-replay/test/snapshot-live-boundary", "bounded-catchup-replay", CaseTest, "Retained suffix plus concurrent live value has no gap or duplicate", "TestScenarioBoundedReplayCatchup"},
	{"bounded-replay/test/live-fanout", "bounded-catchup-replay", CaseTest, "Lossless ordered live delivery after subscriptions attach", "TestScenarioBroadcast"},
	{"bounded-replay/bench/live-fanout", "bounded-catchup-replay", CaseBenchmark, "Live publish throughput at 8 draining subscribers", "BenchmarkScenarioBroadcast"},
	{"bounded-replay/bench/snapshot", "bounded-catchup-replay", CaseBenchmark, "Recent-history snapshot and iterator catch-up cost", "BenchmarkScenarioBoundedReplaySnapshot"},
	{"scatter/test/exactly-one", "work-distribution-scatter", CaseTest, "Every value received exactly once across the pool; last-withdrawal cannot hang Send", "TestScenarioScatter"},
	{"scatter/bench/dispatch", "work-distribution-scatter", CaseBenchmark, "Dispatch cost with 8 competing receivers", "BenchmarkScenarioScatterGather"},
	{"scatter/fuzz/op-stream", "work-distribution-scatter", CaseFuzz, "Randomised register/dispatch/withdraw stream", "FuzzScenarioCasterScatterGather"},
	{"gather/test/all-signals", "signal-gather-barrier", CaseTest, "Wait collects one signal per sender; withdrawal compensated", "TestScenarioGather"},
	{"gather/bench/wait", "signal-gather-barrier", CaseBenchmark, "Gather cost with 8 senders", "BenchmarkScenarioScatterGather"},
	{"gather/fuzz/op-stream", "signal-gather-barrier", CaseFuzz, "Randomised register/signal/withdraw stream", "FuzzScenarioCasterScatterGather"},
	{"churn/test/membership-churn", "ephemeral-churn-membership", CaseTest, "Continuous subscribe/unsubscribe while publishing; no corruption/stall", "TestScenarioChurn"},
	{"churn/bench/subscribe-unsubscribe", "ephemeral-churn-membership", CaseBenchmark, "Subscribe/unsubscribe cost (BenchmarkUnified_Churn + scenario bench)", "BenchmarkScenarioBroadcast"},
	{"fanout/test/32-subscribers", "large-fanout-scalability", CaseTest, "Correct fan-out at 32 subscribers", "TestScenarioLargeFanout"},
	{"fanout/bench/up-to-512", "large-fanout-scalability", CaseBenchmark, "Publish scaling at 1/8/64/512 subscribers", "BenchmarkUnified_Broadcast"},
	{"payload/test/torn-read", "wide-payload-integrity", CaseTest, "Multi-word payload never torn under steady + churn load", "TestScenarioPayload"},
	{"payload/bench/broadcast", "wide-payload-integrity", CaseBenchmark, "Wide-payload publish throughput", "BenchmarkAsync"},
	{"payload/fuzz/model", "wide-payload-integrity", CaseFuzz, "Ring model fuzz (existing) + scenario op streams", "FuzzRingPubSubModel"},
	{"lifecycle/test/cancel-close", "cancellation-and-close-lifecycle", CaseTest, "Context cancellation and idempotent Close where documented", "TestScenarioLifecycle"},
	{"lifecycle/fuzz/op-stream", "cancellation-and-close-lifecycle", CaseFuzz, "Randomised op stream including unsubscribe/close edges", "FuzzScenarioBroadcast"},
	{"edge/test/zero-subscriber", "zero-subscriber-and-edge-lifecycle", CaseTest, "Publish with zero subscribers never blocks; late subscriber gets later values", "TestScenarioZeroSubscriber"},
	{"edge/bench/no-subscribers", "zero-subscriber-and-edge-lifecycle", CaseBenchmark, "Publish cost with zero subscribers", "BenchmarkScenarioBroadcast"},
	{"ring-lossy/test/seqcaster-loss-accounting", "bounded-ring-lossy-detectable", CaseTest, "SeqCaster23Meta reports exact overwrite loss and shuts down its workers", "TestSeqCaster23MetaLossAndClose"},
	{"ring-lossy/bench/seqcaster-empty-send", "bounded-ring-lossy-detectable", CaseBenchmark, "SeqCaster23Meta publication cost with no subscriptions", "BenchmarkSeqCaster23MetaSendNoSubscribers"},
	{"replaycast/test/exact-overrun", "bounded-ring-lossy-detectable", CaseTest, "Exact missed count and retained suffix after overwrite", "TestReplayCast53LossAccounting"},
	{"replaycast/test/zero-close-concurrency", "bounded-ring-lossy-detectable", CaseTest, "No stale values after zero-subscriber publications; idempotent close/drain and concurrent sequence publication", "TestReplayCast53EdgesAndConcurrency"},
	{"replaycast/bench/publish", "bounded-ring-lossy-detectable", CaseBenchmark, "Bounded-ring publication with an active cursor", "BenchmarkReplayCast53Publish"},
	{"replaycast/fuzz/ring-model", "bounded-ring-lossy-detectable", CaseFuzz, "Operation streams compared with an exact bounded-ring/missed-count model", "FuzzReplayCast53RingModel"},
	{"cast/test/adaptive-redirect", "rendezvous-cast-no-ack", CaseTest, "AdaptiveCaster23Meta toggles its redirect state at the configured threshold and delivers on its sequence path", "TestAdaptiveCaster23MetaRedirect"},
	{"cast/test/chatjimmy-single-ack", "rendezvous-cast-no-ack", CaseTest, "ChatJimmy's shared-channel ack barrier under a single receiver, the explicitly exposed-safe case", "TestChanPubSub35ChatJimmySingleReceiver"},
	{"hub46/test/cursor-backpressure-close", "buffered-lossless-backpressured", CaseTest, "Independent cursor ordering, capacity backpressure, and close drain", "TestChanHub46GrokBackpressureAndClose"},
	{"cast47/test/concurrent-churn", "rendezvous-cast-no-ack", CaseTest, "Atomic-state caster send and concurrent receiver withdrawal", "TestChanCaster_ConcurrentChurnAndSelect"},
	{"lockstep47/test/scale-cancel", "lockstep-barrier-lossless", CaseTest, "128-subscriber acknowledged broadcasts with context cancellation", "TestChanPubSub_ScalableBroadcastAndContextCancel"},
	{"lockstep47/bench/broadcast-128", "lockstep-barrier-lossless", CaseBenchmark, "Acknowledged broadcasts at 128 subscribers", "BenchmarkChanPubSub_Broadcast128"},
	{"ring47/test/select-backpressure", "buffered-lossless-backpressured", CaseTest, "Native-select ring delivery with lossless backpressure", "TestChanRing_PipelinedSelectAndBackpressure"},
	{"ring47/test/wide-payload", "buffered-lossless-backpressured", CaseTest, "Concurrent multiword payload integrity and unread-slot release", "TestChanRing47GeminiWidePayloadIntegrity"},
	{"ring47/test/unsubscribe-release", "buffered-lossless-backpressured", CaseTest, "Closing a slow reader releases its retained ring slots", "TestChanRing47GeminiFullSlotReleasedByUnsubscribe"},
	{"ring47/test/context-cancel", "buffered-lossless-backpressured", CaseTest, "Context-bound sequence cancellation detaches without racing its cursor", "TestChanRing47GeminiSubscribeContextCancellation"},
	{"watch48/test/latest-generation-wake", "latest-value-conflating-watch", CaseTest, "Coherent latest snapshot and broadcast notification wake all observers", "TestWatchCaster48GrokBroadcastWakeAndCoalescing"},
	{"watch48/test/nil-payload", "latest-value-conflating-watch", CaseTest, "Zero and nil payloads remain representable across generations", "TestWatchCaster48GrokNilPayloadAndGeneration"},
	{"watch48/test/coherent-snapshot", "latest-value-conflating-watch", CaseTest, "Concurrent readers observe value and generation from one publication", "TestWatchCaster48GrokCoherentSnapshot"},
	{"watch48/test/concurrent-senders", "latest-value-conflating-watch", CaseTest, "Concurrent publishers retain unique monotonic generations", "TestWatchCaster48GrokConcurrentSenders"},
	{"watch48/bench/send", "latest-value-conflating-watch", CaseBenchmark, "Latest-state publication cost with no active readers", "BenchmarkWatchCaster48GrokSend"},
	{"select49/test/broadcast-epoch", "latest-value-conflating-watch", CaseTest, "Eager subscription snapshots and close-notification wake all observers", "TestSelectBroadcaster49QwenBroadcastEpochAndEagerSubscribe"},
	{"select49/test/overrun-integrity", "latest-value-conflating-watch", CaseTest, "Bounded overrun skips stale values without torn or out-of-order payloads", "TestSelectBroadcaster49QwenOverrunKeepsSnapshotsOrdered"},
	{"select49/test/state-boundaries", "latest-value-conflating-watch", CaseTest, "Manual state load rejects stale and future sequences", "TestSelectBroadcaster49QwenStateRejectsStaleOrFuture"},
	{"select49/bench/send", "latest-value-conflating-watch", CaseBenchmark, "Ring-backed select broadcast with deterministic terminal-value observation", "BenchmarkSelectBroadcaster49QwenSend"},
	{"ring47/bench/broadcast-128", "buffered-lossless-backpressured", CaseBenchmark, "Select-native pipelined ring publish at 128 subscribers", "BenchmarkChanRing_Broadcast128"},
}

var caseByID map[CaseID]Case

// CaseRegistry returns the init-validated case registry (reporting dimension).
func CaseRegistry() map[CaseID]Case { return caseByID }

// ScenarioRegistry returns the init-validated scenario registry.
func ScenarioRegistry() map[ScenarioID]Scenario { return scenarioByID }

// --- Attachments ---

type Attachment struct {
	Scenario        ScenarioID
	Impl            string // factory key: Broadcaster Name(), casterImpls key, or family-local key
	Remarks         string // why this implementation is attached to this scenario
	ExpectedGoodFit bool   // whether a staff engineer would expect this to be a good fit
	ExposureLimited bool   // true iff facets must artificially avoid a known deficiency
	Exposure        string // what is avoided and why; required iff ExposureLimited, forbidden otherwise
}

func att(s ScenarioID, impl, remarks string, good bool) Attachment {
	return Attachment{Scenario: s, Impl: impl, Remarks: remarks, ExpectedGoodFit: good}
}
func attX(s ScenarioID, impl, remarks string, good bool, exposure string) Attachment {
	return Attachment{Scenario: s, Impl: impl, Remarks: remarks, ExpectedGoodFit: good, ExposureLimited: true, Exposure: exposure}
}

const (
	bChanPubSub  = "ChanPubSub (Baseline)"
	bRingPubSub  = "RingPubSub (18-ChatGPT)"
	bPhase       = "PhasePubSub (19-ChatGPT)"
	bRelayPS     = "RelayPubSub (19-ChatGPT)"
	bChanRelay   = "ChanRelay (20-Arena)"
	bChanLatest  = "ChanLatest (03-ChatGPT)"
	bEpochTape   = "EpochTape (10-Gemini)"
	bGenCaster   = "GenCaster (06-Meta)"
	bChanFanout  = "ChanFanout (12-OpenRouter)"
	bEpoch13     = "EpochCaster (13-OpenRouter)"
	bChanBeacon  = "ChanBeacon (07-Qwen)"
	bChanFeed    = "ChanFeed (04-DeepSeek)"
	bChanRotor   = "ChanRotor (17-Gemini)"
	bGathererPS  = "ChanGathererPubSub (05-Qwen)"
	bChanHub     = "ChanHub (15-Nemotron)"
	bBaselineFan = "BaselineFanout (standard)"
	bBeacon      = "Beacon (01-baseline)"
	bRingBeacon  = "RingBeacon (08-Qwen)"
	bEpochGemini = "EpochCasterGemini (09-Gemini)"
	bEpochPubSub = "EpochPubSub (11-meta)"
	bReliable    = "ReliableBeacon (14-Qwen)"
	bBaselineBar = "BaselineBarrierPubSub (baseline)"
	bBaselineLat = "BaselineLatestBroadcast (baseline)"
	bBaselineBuf = "BaselineBufferedBroadcast (baseline)"
	bRelayTry    = "ChanRelay TrySend (20-Arena)"
	bHubTry      = "ChanHub TrySend (15-Nemotron)"
	bRingTry     = "RingPubSub TrySend (18-ChatGPT)"
)

var broadcastAttachments = []Attachment{
	// lockstep-barrier-lossless
	att("lockstep-barrier-lossless", bBaselineBar, "Vacuum baseline: mutex + per-subscriber unbuffered channels + WaitGroup barrier.", true),
	att("lockstep-barrier-lossless", bChanPubSub, "Reference lock-step design (also the historical baseline for this family).", true),
	att("lockstep-barrier-lossless", "ChanPubSubV2_06meta", "Fused-barrier variant of the reference design.", true),
	attX("lockstep-barrier-lossless", "ChanPubSubV2_06meta", "Attached via its manual Add/C/Wait adapter because it intentionally has no SubscribeContext.", true, "Exposure limited: facets unsubscribe only between rounds via the adapter; the manual Add(-1) steal panics if exercised mid-Send before all sends complete, so no facet withdraws mid-round."),
	att("lockstep-barrier-lossless", "ChanPubSub09Gemini", "Lock-step variant with cond-based pong barrier.", true),
	att("lockstep-barrier-lossless", "ChanPubSub10Gemini", "Lock-step variant with semaphore two-phase barrier.", true),
	att("lockstep-barrier-lossless", "ChanPubSub14Xing", "Lock-step variant.", true),
	att("lockstep-barrier-lossless", "ChanPubSub16DeepSeek", "Lock-step variant with WaitGroup pong.", true),
	att("lockstep-barrier-lossless", "ChanPubSub17Gemini", "Lock-step variant.", true),
	att("lockstep-barrier-lossless", bGathererPS, "Lock-step pub/sub composed from caster+gatherer primitives.", true),
	att("lockstep-barrier-lossless", bEpochPubSub, "Epoch-published value with ack barrier; barrier semantics match, dissemination is latest-value so gaps are possible under racing sends — attached as barrier, expected fit only where conflation is acceptable.", false),
	att("lockstep-barrier-lossless", bReliable, "Latest-value dissemination with reliable ack barrier; same caveat as EpochPubSub.", false),
	att("lockstep-barrier-lossless", bChanLatest, "Conflating by design: cannot honour a per-value ack barrier. Attached to document the mismatch.", false),
	att("lockstep-barrier-lossless", bBeacon, "Single latest-value slot: structurally cannot barrier per value. Attached to document the mismatch.", false),

	// synchronous-handoff-no-barrier
	att("synchronous-handoff-no-barrier", bChanRotor, "Dual-channel rotor: Send hands off to every subscriber without a post-receive barrier.", true),
	att("synchronous-handoff-no-barrier", bPhase, "Phase-slot handoff broadcast.", true),
	att("synchronous-handoff-no-barrier", bRelayPS, "Relayed handoff with configurable credit width (w=4 in the unified factory).", true),
	att("synchronous-handoff-no-barrier", bChanRelay, "window=0 ChanRelay is a synchronous handoff.", true),
	att("synchronous-handoff-no-barrier", bBaselineBar, "Vacuum baseline also satisfies handoff (it is strictly stronger: it additionally waits for processing acks), at higher latency.", false),
	att("synchronous-handoff-no-barrier", bChanPubSub, "Reference design is stronger than handoff (adds the ack barrier); correct but more synchronisation than the scenario needs.", false),

	// buffered-lossless-backpressured
	att("buffered-lossless-backpressured", bBaselineBuf, "Vacuum baseline: per-subscriber buffered channels, blocking Send.", true),
	att("buffered-lossless-backpressured", bChanRelay, "ChanRelay with the unified (synchronous) factory is lossless; buffered windows are covered by its dedicated tests and the w16 async variant.", true),
	attX("buffered-lossless-backpressured", bEpoch13, "Bounded ring with relay blocks: lossless while subscribers drain.", true, "Exposure limited: a stalled subscriber makes Send spin (Gosched) with no context escape, so facets keep every subscriber draining and publish at most ring capacity ahead."),
	attX("buffered-lossless-backpressured", bChanHub, "ChanHub.Send is a blocking per-subscriber channel broadcast.", true, "Exposure limited: Close closes subscriber channels while Send/TrySend hold only a read lock after a closed check, and Send blocks indefinitely on a stalled subscriber — facets never Close concurrently with Send and always drain."),
	att("buffered-lossless-backpressured", bRingPubSub, "RingPubSub.Send(ctx) is buffered and lossless until capacity, then backpressures via ctx.", true),
	att("buffered-lossless-backpressured", bChanFeed, "ChanFeed in drop-oldest mode (unified factory) is NOT lossless; the blocking mode is covered as ChanFeed_block_c64 in the payload/async families. Attached here to document the mode distinction.", false),
	att("buffered-lossless-backpressured", bGenCaster, "Unbounded history is lossless without backpressure; fits delivery-wise but trades away bounded memory (see unbounded-catchup-replay).", false),

	// bounded-ring-lossy-detectable
	att("bounded-ring-lossy-detectable", bBaselineFan, "Vacuum baseline for async fan-out (drop-oldest per-subscriber buffers).", true),
	att("bounded-ring-lossy-detectable", bChanFanout, "Shared ring with per-subscriber skipped counts: the canonical detectable-loss design.", true),
	att("bounded-ring-lossy-detectable", bEpochGemini, "Seqlock ring reporting skipped counts on overrun.", true),
	att("bounded-ring-lossy-detectable", bEpochTape, "Epoch tape reporting missed counts on overrun.", true),
	attX("bounded-ring-lossy-detectable", bRingBeacon, "Bounded ring cursor that snaps to latest on overrun (loss is silent, hence LossySnapToLatest behaviour within a Detectable scenario).", false, "Exposure limited: facets publish no more than ring capacity (64) values ahead of subscribers; concurrent-overrun behaviour is exercised only by its dedicated tests, keeping scenario facets deterministic."),
	att("bounded-ring-lossy-detectable", bChanFeed, "Drop-oldest ring feed: subscribers can observe Dropped() counts.", true),

	// latest-value-conflating-watch
	att("latest-value-conflating-watch", bBaselineLat, "Vacuum baseline: capacity-1 mailboxes with overwrite.", true),
	att("latest-value-conflating-watch", bBeacon, "Atomic latest-value slot + signal channel: the minimal watch primitive.", true),
	att("latest-value-conflating-watch", bChanBeacon, "Ring-backed beacon with per-subscriber cursors; watch semantics with bounded history.", true),
	att("latest-value-conflating-watch", bChanLatest, "Slab-registry capacity-1 mailbox broadcast built for exactly this scenario.", true),
	att("latest-value-conflating-watch", bRelayTry, "ChanRelay.TrySend with window=1 conflates per subscriber and never blocks.", true),
	att("latest-value-conflating-watch", bHubTry, "ChanHub.TrySend drops the newest value when a subscriber buffer is full (keeps the oldest pending, so 'latest' lags under sustained load).", false),
	att("latest-value-conflating-watch", bChanFeed, "Drop-oldest feed always retains the newest values in its ring.", true),
	att("latest-value-conflating-watch", bEpochPubSub, "Publishes only the latest epoch value (with an ack barrier the scenario does not need).", false),
	att("latest-value-conflating-watch", bReliable, "Latest-value beacon dissemination (with an ack barrier the scenario does not need).", false),

	// unbounded-catchup-replay
	attX("unbounded-catchup-replay", bGenCaster, "Linked-epoch caster: every subscriber can replay full history since subscription.", true, "Exposure limited: GenCaster has no Close and retention is pinned by the slowest subscriber; facets never assert close behaviour and bound published volume."),
	att("unbounded-catchup-replay", bBaselineBuf, "Vacuum buffered baseline replays only up to its fixed capacity, then backpressures instead of retaining.", false),
	att("unbounded-catchup-replay", bRingPubSub, "Ring retains only capacity values; a further-behind subscriber loses history by design.", false),

	// ephemeral-churn-membership (attachments exercised by TestScenarioChurn)
	att("ephemeral-churn-membership", bBaselineFan, "Vacuum baseline under churn.", true),
	att("ephemeral-churn-membership", bChanPubSub, "Reference lock-step under churn (dedicated family stress also covers this).", true),
	att("ephemeral-churn-membership", bRingPubSub, "Ring pub/sub under churn.", true),
	att("ephemeral-churn-membership", bPhase, "Phase pub/sub under churn.", true),
	att("ephemeral-churn-membership", bRelayPS, "Relay pub/sub under churn.", true),
	att("ephemeral-churn-membership", bChanRelay, "ChanRelay under churn.", true),
	att("ephemeral-churn-membership", bChanLatest, "ChanLatest under churn.", true),
	att("ephemeral-churn-membership", bEpochTape, "EpochTape under churn.", true),
	attX("ephemeral-churn-membership", bGenCaster, "GenCaster under churn.", true, "Exposure limited: no Close; churn facets only subscribe/consume/cancel, never close."),
	att("ephemeral-churn-membership", bChanFanout, "ChanFanout under churn.", true),
	attX("ephemeral-churn-membership", bEpoch13, "EpochCaster13 under churn.", true, "Exposure limited: subscribers always drain; a stalled subscriber would spin the publisher (see buffered scenario)."),
	att("ephemeral-churn-membership", bChanBeacon, "ChanBeacon under churn.", true),
	att("ephemeral-churn-membership", bChanFeed, "ChanFeed under churn.", true),
	att("ephemeral-churn-membership", bChanRotor, "ChanRotor under churn.", true),
	att("ephemeral-churn-membership", bGathererPS, "ChanGathererPubSub under churn.", true),
	attX("ephemeral-churn-membership", bChanHub, "ChanHub under churn.", true, "Exposure limited: facets never Close the hub (it has no safe concurrent Close) and always drain subscriber channels."),
	att("ephemeral-churn-membership", bBeacon, "Beacon under churn (no membership at all: watchers are stateless).", true),
	attX("ephemeral-churn-membership", bRingBeacon, "RingBeacon under churn (stateless cursors).", true, "Exposure limited: published volume per session stays within ring capacity (see ring-lossy scenario)."),
	att("ephemeral-churn-membership", bEpochGemini, "EpochCasterGemini under churn.", true),
	att("ephemeral-churn-membership", bEpochPubSub, "EpochPubSub under churn.", true),
	att("ephemeral-churn-membership", bReliable, "ReliableBeacon under churn.", true),
	att("ephemeral-churn-membership", bBaselineBar, "Vacuum barrier baseline under churn.", true),
	att("ephemeral-churn-membership", bBaselineLat, "Vacuum latest baseline under churn.", true),
	att("ephemeral-churn-membership", bBaselineBuf, "Vacuum buffered baseline under churn.", true),
	att("ephemeral-churn-membership", bRelayTry, "ChanRelay TrySend variant under churn.", true),
	att("ephemeral-churn-membership", bHubTry, "ChanHub TrySend variant under churn.", true),
	att("ephemeral-churn-membership", bRingTry, "RingPubSub TrySend variant under churn.", true),

	// large-fanout-scalability (attachments exercised by TestScenarioLargeFanout + benchmarks)
	att("large-fanout-scalability", bBaselineFan, "Vacuum baseline at fan-out.", true),
	att("large-fanout-scalability", bChanLatest, "O(N) mailbox sweep, zero-alloc: a strong large-fan-out contender.", true),
	att("large-fanout-scalability", bChanFanout, "O(1) send-side wake with shared ring: built for large fan-out.", true),
	att("large-fanout-scalability", bBeacon, "O(1) publish regardless of watcher count.", true),
	att("large-fanout-scalability", bChanPubSub, "O(N) rendezvous + barrier: expected to scale worst; attached so the cost is measured, not asserted.", false),
	attX("large-fanout-scalability", bGenCaster, "O(1) publish (one alloc + one close) at any fan-out.", true, "Exposure limited: no Close and unbounded retention; fan-out facets bound volume and never close."),
	att("large-fanout-scalability", bEpochTape, "Shared-tape broadcast at fan-out.", true),
	att("large-fanout-scalability", bChanRelay, "Registry-sweep broadcast at fan-out.", true),
	att("large-fanout-scalability", "EpochBroadcast54Terra (54-GPT-6.1-Terra)", "Correct lossless handoff, but the per-publication barrier waits for every receiver and is deliberately measured as a poor fit for high-fan-out, nonblocking notification workloads.", false),

	// cancellation-and-close-lifecycle
	att("cancellation-and-close-lifecycle", bBaselineFan, "Vacuum baseline: Close is idempotent and closes subscriber channels.", true),
	att("cancellation-and-close-lifecycle", bChanLatest, "Close closes mailboxes; Subscribe after Close returns nil (dedicated tests assert the details).", true),
	att("cancellation-and-close-lifecycle", bChanRelay, "Close releases blocked publishers and closes subscriptions.", true),
	att("cancellation-and-close-lifecycle", bRingPubSub, "Close wakes blocked publishers and caught-up readers.", true),
	attX("cancellation-and-close-lifecycle", bGenCaster, "No Close exists; only context cancellation of consumers is documented.", false, "Exposure limited: facets exercise context cancellation only; Close is never called (the adapter's Close is a no-op)."),
	attX("cancellation-and-close-lifecycle", bChanHub, "No Close method; lifecycle is per-subscription Unsubscribe only.", false, "Exposure limited: facets exercise Unsubscribe/context only and never race a hub Close (none exists) with Send."),
	att("cancellation-and-close-lifecycle", bChanFeed, "Close terminates subscriptions; SubscribeContext honours ctx.", true),
	att("cancellation-and-close-lifecycle", bChanFanout, "Close/Done lifecycle plus ctx-aware SubscribeContext.", true),

	// zero-subscriber-and-edge-lifecycle
	att("zero-subscriber-and-edge-lifecycle", bBaselineFan, "Vacuum baseline edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bBaselineBar, "Vacuum barrier baseline edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bBaselineLat, "Vacuum latest baseline edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bBaselineBuf, "Vacuum buffered baseline edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bChanPubSub, "Reference: Send with no subscribers returns 0 immediately.", true),
	att("zero-subscriber-and-edge-lifecycle", bRingPubSub, "RingPubSub edge behaviour (dedicated tests also cover it).", true),
	att("zero-subscriber-and-edge-lifecycle", bPhase, "PhasePubSub edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bRelayPS, "RelayPubSub edge behaviour (dedicated tests also cover it).", true),
	att("zero-subscriber-and-edge-lifecycle", bChanRelay, "ChanRelay edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bChanLatest, "ChanLatest edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bEpochTape, "EpochTape edge behaviour.", true),
	attX("zero-subscriber-and-edge-lifecycle", bGenCaster, "GenCaster edge behaviour.", true, "Exposure limited: no Close assertions (none exists)."),
	att("zero-subscriber-and-edge-lifecycle", bChanFanout, "ChanFanout edge behaviour.", true),
	attX("zero-subscriber-and-edge-lifecycle", bEpoch13, "EpochCaster13 edge behaviour.", true, "Exposure limited: no stalled-subscriber publishes (publisher would spin)."),
	att("zero-subscriber-and-edge-lifecycle", bChanBeacon, "ChanBeacon edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bChanFeed, "ChanFeed edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bChanRotor, "ChanRotor edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bGathererPS, "ChanGathererPubSub edge behaviour.", true),
	attX("zero-subscriber-and-edge-lifecycle", bChanHub, "ChanHub edge behaviour.", true, "Exposure limited: no Close (none exists); Send facets always drain."),
	att("zero-subscriber-and-edge-lifecycle", bBeacon, "Beacon: publish with no watchers is a single atomic store.", true),
	attX("zero-subscriber-and-edge-lifecycle", bRingBeacon, "RingBeacon edge behaviour.", true, "Exposure limited: volume stays within ring capacity."),
	att("zero-subscriber-and-edge-lifecycle", bEpochGemini, "EpochCasterGemini edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bEpochPubSub, "EpochPubSub edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bReliable, "ReliableBeacon edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bRelayTry, "ChanRelay TrySend edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bHubTry, "ChanHub TrySend edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", bRingTry, "RingPubSub TrySend edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "ChanPubSubV2_06meta", "Manual-API lock-step edge behaviour via adapter.", true),
	att("zero-subscriber-and-edge-lifecycle", "ChanPubSub09Gemini", "Lock-step variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "ChanPubSub10Gemini", "Lock-step variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "ChanPubSub14Xing", "Lock-step variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "ChanPubSub16DeepSeek", "Lock-step variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "ChanPubSub17Gemini", "Lock-step variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "Caster:ChanCaster", "Caster-family edge behaviour via the caster Broadcaster adapter.", true),
	att("zero-subscriber-and-edge-lifecycle", "Caster:ChanCaster2", "Caster variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "Caster:ChanCasterV2_06meta", "Caster variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "Caster:ChanCaster09Gemini", "Caster variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "Caster:ChanCaster14Xing", "Caster variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "Caster:ChanCaster15Nemotron", "Caster variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "Caster:ChanCaster17FastPath", "Caster variant edge behaviour.", true),
	att("zero-subscriber-and-edge-lifecycle", "Caster:ChanCaster17Pumped", "Caster variant edge behaviour.", true),
}

var casterAttachments = []Attachment{
	att("rendezvous-cast-no-ack", "ChanCaster", "Reference rendezvous caster (historical baseline for the family).", true),
	att("rendezvous-cast-no-ack", "ChanCaster2", "Caster variant.", true),
	att("rendezvous-cast-no-ack", "ChanCasterV2_06meta", "Caster variant with wait-free positive Add fast path.", true),
	att("rendezvous-cast-no-ack", "ChanCaster09Gemini", "Caster variant.", true),
	att("rendezvous-cast-no-ack", "ChanCaster14Xing", "Caster variant.", true),
	att("rendezvous-cast-no-ack", "ChanCaster15Nemotron", "Caster variant.", true),
	att("rendezvous-cast-no-ack", "ChanCaster17FastPath", "Caster variant with fast-path send.", true),
	att("rendezvous-cast-no-ack", "ChanCaster17Pumped", "Caster variant with pumped send.", true),
	att("rendezvous-cast-no-ack", "BaselineRendezvousCaster", "Vacuum baseline: mutex-guarded count + one unbuffered channel + compensating dummy sends on withdrawal.", true),
	att("rendezvous-cast-no-ack", "AdaptiveCaster23Meta", "Adaptive hybrid retains ChanCaster's synchronous rendezvous path and provides a separate SeqCaster path for registered ring subscribers; this attachment measures its shared-channel caster mode.", true),
	attX("rendezvous-cast-no-ack", "ChanPubSub35ChatJimmy", "Shared ChanCaster channel with a condition-variable pong barrier; the registered receive count is acknowledged before Send returns.", false, "Exposure limited: no concurrent receiver withdrawal or cancellation during a Send/Wait round; the response counts acknowledgements globally and cannot reclaim a missing acknowledgement, so no churn/abandonment facet may run."),
	att("rendezvous-cast-no-ack", "ChanCaster47Gemini", "Atomic packed-state one-shot receiver accounting; Add and Send coordinate through CAS and a parked positive-add barrier.", true),
}

var scatterAttachments = []Attachment{
	att("work-distribution-scatter", "ChanScatter", "Single-channel scatter with steal compensation on last withdrawal: the novel design under test.", true),
	att("work-distribution-scatter", "BaselineScatterQueue", "Vacuum baseline: an ordinary unbuffered channel already dispatches to exactly one ready receiver.", true),
}

var gatherAttachments = []Attachment{
	att("signal-gather-barrier", "ChanGatherer", "Dual of the reference caster: Wait receives once per sender, Add(-1) compensates.", true),
	att("signal-gather-barrier", "BaselineGatherBarrier", "Vacuum baseline: mutex-guarded count + one unbuffered channel + compensating sends.", true),
}

// payloadAttachments bind asyncAPI (pv) factories to the payload scenario.
var payloadAttachments = []Attachment{
	att("wide-payload-integrity", "BaselineFanout_c64", "Vacuum async baseline (BaselineFanout) carrying multi-word payloads.", true),
	att("wide-payload-integrity", "Beacon", "Atomic slot publication: payload is observed via a single pointer/word swap.", true),
	att("wide-payload-integrity", "ChanBeacon_c64", "Ring slot publication for wide payloads.", true),
	att("wide-payload-integrity", "ChanFanout_c64", "Shared ring carrying wide payloads by value.", true),
	att("wide-payload-integrity", "ChanFeed_drop_c64", "Ring feed carrying wide payloads (drop-oldest).", true),
	att("wide-payload-integrity", "ChanFeed_block_c64", "Ring feed carrying wide payloads (blocking/lossless mode).", true),
	att("wide-payload-integrity", "ChanLatest", "Capacity-1 mailboxes carrying wide payloads by value.", true),
	att("wide-payload-integrity", "EpochCasterGemini_c64", "Seqlock ring: wide-payload torn-read risk is exactly what this scenario probes.", true),
	att("wide-payload-integrity", "EpochCaster13Crouton_r64", "Ring caster carrying wide payloads.", true),
	att("wide-payload-integrity", "EpochTape_c64", "Tape slots carrying wide payloads.", true),
	att("wide-payload-integrity", "GenCaster", "Epoch nodes carry the payload by value behind an atomic pointer: no tearing by construction.", true),
	att("wide-payload-integrity", "RingBeacon_c64", "Ring entries are published via atomic pointer store: no tearing by construction.", true),
	att("wide-payload-integrity", "RingPubSub_c64", "Ring slots carrying wide payloads.", true),
	att("wide-payload-integrity", "ChanRelay_sync", "Per-subscriber channels carry wide payloads by value.", true),
	att("wide-payload-integrity", "ChanRelay_w16", "Buffered per-subscriber channels carrying wide payloads.", true),
	att("wide-payload-integrity", "ChanHub_c16", "Caller-supplied buffered channels carrying wide payloads.", true),
}

func allAttachments() []Attachment {
	out := make([]Attachment, 0, len(broadcastAttachments)+len(casterAttachments)+len(scatterAttachments)+len(gatherAttachments))
	out = append(out, broadcastAttachments...)
	out = append(out, casterAttachments...)
	out = append(out, scatterAttachments...)
	out = append(out, gatherAttachments...)
	out = append(out, payloadAttachments...)
	return out
}

func init() {
	// Keep response-derived implementations declarative while ensuring the
	// same strict init-time reference and metadata validation applies to them.
	broadcastAttachments = append(broadcastAttachments, extractedBroadcastAttachments()...)
	scenarioByID = make(map[ScenarioID]Scenario, len(Scenarios))
	for _, s := range Scenarios {
		if s.ID == "" || s.Title == "" || s.UseCase == "" || s.Baseline == "" || len(s.Invariants) == 0 {
			panic(fmt.Sprintf("bigbuff: scenario %q is incompletely defined", s.ID))
		}
		if _, dup := scenarioByID[s.ID]; dup {
			panic(fmt.Sprintf("bigbuff: duplicate scenario %q", s.ID))
		}
		scenarioByID[s.ID] = s
	}
	caseByID = make(map[CaseID]Case, len(Cases))
	for _, c := range Cases {
		if c.ID == "" || c.Title == "" || c.Runnable == "" {
			panic(fmt.Sprintf("bigbuff: case %q is incompletely defined", c.ID))
		}
		if _, ok := scenarioByID[c.Scenario]; !ok {
			panic(fmt.Sprintf("bigbuff: case %q references unknown scenario %q", c.ID, c.Scenario))
		}
		if _, dup := caseByID[c.ID]; dup {
			panic(fmt.Sprintf("bigbuff: duplicate case %q", c.ID))
		}
		caseByID[c.ID] = c
	}
	// Every scenario must have at least one test case.
	haveTest := map[ScenarioID]bool{}
	for _, c := range Cases {
		if c.Kind == CaseTest {
			haveTest[c.Scenario] = true
		}
	}
	for _, s := range Scenarios {
		if !haveTest[s.ID] {
			panic(fmt.Sprintf("bigbuff: scenario %q has no test case", s.ID))
		}
	}
	for _, a := range allAttachments() {
		if _, ok := scenarioByID[a.Scenario]; !ok {
			panic(fmt.Sprintf("bigbuff: attachment %q references unknown scenario %q", a.Impl, a.Scenario))
		}
		if a.Remarks == "" {
			panic(fmt.Sprintf("bigbuff: attachment %q/%q has no remarks", a.Scenario, a.Impl))
		}
		if a.ExposureLimited && a.Exposure == "" {
			panic(fmt.Sprintf("bigbuff: exposure-limited attachment %q/%q has no exposure remarks", a.Scenario, a.Impl))
		}
		if !a.ExposureLimited && a.Exposure != "" {
			panic(fmt.Sprintf("bigbuff: attachment %q/%q has exposure remarks but is not flagged exposure-limited", a.Scenario, a.Impl))
		}
	}
}

// scenarioBroadcastFactories indexes every Broadcaster factory by Name().
// It is AllBroadcasters plus the caster-family adapters: casters share one
// channel across all subscribers, so they cannot honour the independent-
// subscriber assumptions of the generic harness (a fast subscriber could
// take another's copy of a round), but through this adapter they are sound
// for the single-subscriber / zero-subscriber scenario facets that use it.
func scenarioBroadcastFactories() map[string]func() Broadcaster {
	m := map[string]func() Broadcaster{}
	for _, f := range AllBroadcasters() {
		b := f()
		m[b.Name()] = f
	}
	for _, key := range []string{"ChanCaster", "ChanCaster2", "ChanCasterV2_06meta", "ChanCaster09Gemini", "ChanCaster14Xing", "ChanCaster15Nemotron", "ChanCaster17FastPath", "ChanCaster17Pumped"} {
		f := casterBroadcasterFactory(key)
		m[f().Name()] = f
	}
	return m
}

func attachmentsFor(s ScenarioID, all []Attachment) []Attachment {
	var out []Attachment
	for _, a := range all {
		if a.Scenario == s {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Impl < out[j].Impl })
	return out
}

// --- Registry completeness tests ---

func TestScenarioRegistryComplete(t *testing.T) {
	// Every Broadcaster factory must be attached to at least one scenario.
	attached := map[string]bool{}
	for _, a := range broadcastAttachments {
		attached[a.Impl] = true
	}
	for _, f := range AllBroadcasters() {
		name := f().Name()
		if !attached[name] {
			t.Errorf("broadcaster %q is not attached to any scenario", name)
		}
	}
	// Every attachment must resolve to a real factory of its family.
	factories := scenarioBroadcastFactories()
	for _, a := range broadcastAttachments {
		if _, ok := factories[a.Impl]; !ok {
			t.Errorf("broadcast attachment %q does not resolve to a factory", a.Impl)
		}
	}
	casters := casterImpls()
	casterAttached := map[string]bool{}
	// The vacuum caster baseline is driven through its own adapter (below),
	// not through casterImpls.
	casterAttached["BaselineRendezvousCaster"] = true
	for _, a := range casterAttachments {
		casterAttached[a.Impl] = true
		if a.Impl == "BaselineRendezvousCaster" {
			continue
		}
		if _, ok := casters[a.Impl]; !ok {
			t.Errorf("caster attachment %q does not resolve", a.Impl)
		}
	}
	for name := range casters {
		if !casterAttached[name] {
			t.Errorf("caster %q is not attached to the rendezvous scenario", name)
		}
	}
	// Every scenario's declared baseline must itself be attached to it.
	for _, s := range Scenarios {
		found := false
		for _, a := range allAttachments() {
			if a.Scenario == s.ID && a.Impl == s.Baseline {
				found = true
			}
		}
		if !found {
			t.Errorf("scenario %q baseline %q is not attached to it", s.ID, s.Baseline)
		}
	}
	payloadAttached := map[string]bool{}
	for _, a := range payloadAttachments {
		payloadAttached[a.Impl] = true
		if scenarioByID[a.Scenario].Kind != KindPayload {
			t.Errorf("payload attachment in non-payload scenario %q", a.Scenario)
		}
	}
	for name := range asyncImpls() {
		if !payloadAttached[name] {
			t.Errorf("async/payload impl %q is not attached to the payload scenario", name)
		}
	}
	// Non-broadcast families are covered by their dedicated runners; assert
	// their scenario kinds line up.
	for _, a := range casterAttachments {
		if scenarioByID[a.Scenario].Kind != KindCaster {
			t.Errorf("caster attachment in non-caster scenario %q", a.Scenario)
		}
	}
	for _, a := range scatterAttachments {
		if scenarioByID[a.Scenario].Kind != KindScatter {
			t.Errorf("scatter attachment in non-scatter scenario %q", a.Scenario)
		}
	}
	for _, a := range gatherAttachments {
		if scenarioByID[a.Scenario].Kind != KindGather {
			t.Errorf("gather attachment in non-gather scenario %q", a.Scenario)
		}
	}
}

// --- Deterministic scenario runners ---
//
// All synchronisation below is via channels/WaitGroups. Timeouts exist only
// as deadlock guards that fail the test (bounded, verifiable), never as a
// retry/sleep mechanism.

const scenarioGuard = 15 * time.Second

func runBroadcastFacet(t *testing.T, a Attachment, subs, msgs int) {
	t.Helper()
	sc := scenarioByID[a.Scenario]
	factory, ok := scenarioBroadcastFactories()[a.Impl]
	if !ok {
		t.Fatalf("no factory for %q", a.Impl)
	}
	b := factory()
	defer b.Close()
	subscribers := make([]BroadcastSubscriber, subs)
	for i := range subscribers {
		subscribers[i] = b.Subscribe()
		defer subscribers[i].Unsubscribe()
	}
	ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
	defer cancel()
	results := make([][]int, subs)
	var wg sync.WaitGroup
	for i, s := range subscribers {
		wg.Add(1)
		go func(i int, s BroadcastSubscriber) {
			defer wg.Done()
			for {
				v, ok := s.Receive(ctx)
				if !ok {
					return
				}
				results[i] = append(results[i], v)
				// Terminal-value termination works for every loss policy:
				// even a conflating attachment to a lossless scenario must
				// eventually surface the final value. Strict per-value
				// assertions below only apply to expected-good-fit lossless
				// attachments.
				if v == msgs {
					return
				}
			}
		}(i, s)
	}
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for v := 1; v <= msgs; v++ {
			b.Publish(v)
		}
	}()
	select {
	case <-pubDone:
	case <-time.After(scenarioGuard):
		t.Fatalf("%s/%s: publisher blocked (guard timeout)", a.Scenario, a.Impl)
	}
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(scenarioGuard):
		t.Fatalf("%s/%s: subscribers did not observe terminal value (guard timeout)", a.Scenario, a.Impl)
	}
	for i, got := range results {
		if len(got) == 0 {
			t.Fatalf("%s/%s: subscriber %d received nothing", a.Scenario, a.Impl, i)
		}
		for j := 1; j < len(got); j++ {
			if got[j] <= got[j-1] {
				t.Fatalf("%s/%s: subscriber %d duplicate/reorder: %v", a.Scenario, a.Impl, i, got)
			}
		}
		if got[len(got)-1] != msgs {
			t.Fatalf("%s/%s: subscriber %d final=%d want %d (%v)", a.Scenario, a.Impl, i, got[len(got)-1], msgs, got)
		}
		if sc.Loss == Lossless && a.ExpectedGoodFit {
			if len(got) != msgs {
				t.Fatalf("%s/%s: subscriber %d got %d values want %d (%v)", a.Scenario, a.Impl, i, len(got), msgs, got)
			}
			for v := 1; v <= msgs; v++ {
				if got[v-1] != v {
					t.Fatalf("%s/%s: subscriber %d gap at %d: %v", a.Scenario, a.Impl, i, v, got)
				}
			}
		}
	}
}

func TestScenarioBroadcast(t *testing.T) {
	for _, sc := range Scenarios {
		if sc.Kind != KindBroadcast {
			continue
		}
		// Churn / large-fanout / lifecycle / zero-subscriber scenarios have
		// dedicated facets below (their invariants differ from steady fan-out).
		switch sc.ID {
		case "ephemeral-churn-membership", "large-fanout-scalability", "cancellation-and-close-lifecycle", "zero-subscriber-and-edge-lifecycle":
			continue
		}
		for _, a := range attachmentsFor(sc.ID, broadcastAttachments) {
			t.Run(string(sc.ID)+"/"+a.Impl, func(t *testing.T) {
				msgs := 20
				// Exposure-limited ring facets: never publish past capacity.
				runBroadcastFacet(t, a, 4, msgs)
			})
		}
	}
}

func TestScenarioChurn(t *testing.T) {
	// Bounded, deterministic churn: a fixed number of subscription sessions,
	// each receiving with a context that the test itself cancels after the
	// publisher finishes. No sleeps: completion is signalled by channels.
	for _, a := range attachmentsFor("ephemeral-churn-membership", broadcastAttachments) {
		t.Run(a.Impl, func(t *testing.T) {
			factory := scenarioBroadcastFactories()[a.Impl]
			b := factory()
			defer b.Close()
			const sessions = 24
			const msgs = 60
			ctx, cancel := context.WithCancel(context.Background())
			var violations sync.Map // impl -> first error string
			var wg sync.WaitGroup
			for s := 0; s < sessions; s++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sub := b.Subscribe()
					defer sub.Unsubscribe()
					last := 0
					for {
						v, ok := sub.Receive(ctx)
						if !ok {
							return
						}
						if v <= last {
							violations.Store(a.Impl, fmt.Sprintf("duplicate/reorder %d after %d", v, last))
							return
						}
						last = v
						if v == msgs {
							return
						}
					}
				}()
			}
			pubDone := make(chan struct{})
			go func() {
				defer close(pubDone)
				for v := 1; v <= msgs; v++ {
					b.Publish(v)
				}
			}()
			select {
			case <-pubDone:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: publisher blocked during churn", a.Impl)
			}
			cancel()
			waitDone := make(chan struct{})
			go func() { wg.Wait(); close(waitDone) }()
			select {
			case <-waitDone:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: churn sessions did not terminate", a.Impl)
			}
			if v, ok := violations.Load(a.Impl); ok {
				t.Fatalf("%s: %v", a.Impl, v)
			}
		})
	}
}

func TestScenarioLargeFanout(t *testing.T) {
	for _, a := range attachmentsFor("large-fanout-scalability", broadcastAttachments) {
		t.Run(a.Impl, func(t *testing.T) { runBroadcastFacet(t, a, 32, 10) })
	}
}

func TestScenarioPayload(t *testing.T) {
	// Wide-payload integrity is driven through the existing async pv harness:
	// steady (lossless variants must show no gaps) — deterministic sizes.
	for name, mk := range asyncImpls() {
		t.Run(name, func(t *testing.T) { runAsyncStress(t, mk, 8, 800, false, false) })
	}
}

func TestScenarioLifecycle(t *testing.T) {
	for _, a := range attachmentsFor("cancellation-and-close-lifecycle", broadcastAttachments) {
		t.Run(a.Impl, func(t *testing.T) {
			factory := scenarioBroadcastFactories()[a.Impl]
			b := factory()
			sub := b.Subscribe()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					if _, ok := sub.Receive(ctx); !ok {
						return
					}
				}
			}()
			pubDone := make(chan struct{})
			go func() { defer close(pubDone); b.Publish(1) }()
			select {
			case <-pubDone:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: publish blocked before cancel", a.Impl)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: subscriber did not stop on cancel", a.Impl)
			}
			sub.Unsubscribe()
			sub.Unsubscribe() // idempotent at the adapter level
			b.Close()
			b.Close() // idempotent where Close exists; no-op adapter otherwise
		})
	}
}

func TestScenarioZeroSubscriber(t *testing.T) {
	for _, a := range attachmentsFor("zero-subscriber-and-edge-lifecycle", broadcastAttachments) {
		t.Run(a.Impl, func(t *testing.T) {
			factory := scenarioBroadcastFactories()[a.Impl]
			b := factory()
			defer b.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				for v := 1; v <= 50; v++ {
					b.Publish(v)
				}
			}()
			select {
			case <-done:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: Publish blocked with zero subscribers", a.Impl)
			}
			// A late subscriber must receive subsequent values per policy.
			sub := b.Subscribe()
			defer sub.Unsubscribe()
			ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
			defer cancel()
			pubDone := make(chan struct{})
			go func() { defer close(pubDone); b.Publish(999) }()
			v, ok := sub.Receive(ctx)
			if !ok || v != 999 {
				t.Fatalf("%s: late subscriber got (%d,%v) want (999,true)", a.Impl, v, ok)
			}
			select {
			case <-pubDone:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: Publish to late subscriber blocked", a.Impl)
			}
		})
	}
}

// --- Caster / scatter / gather facets ---

type casterLike struct {
	send       func(int) int
	add        func(int) int
	c          chan int
	close      func()
	ack        func()
	skipChurn  bool
	rearmEach  bool
	singleOnly bool
}

func casterLikes() map[string]func() casterLike {
	m := map[string]func() casterLike{}
	for name, mk := range casterImpls() {
		mk := mk
		name := name
		m[name] = func() casterLike {
			a := mk()
			return casterLike{a.send, a.add, a.c, a.close, a.ack, a.skipChurn, a.rearmEach, a.singleOnly}
		}
	}
	m["BaselineRendezvousCaster"] = func() casterLike {
		x := NewBaselineRendezvousCaster[int]()
		return casterLike{x.Send, x.Add, x.C, nil, nil, false, true, false}
	}
	return m
}

func TestScenarioCaster(t *testing.T) {
	for _, a := range casterAttachments {
		if a.ExposureLimited {
			t.Run(a.Impl+"/exposure-limited", func(t *testing.T) {
				mk := casterLikes()[a.Impl]
				l := mk()
				if l.close != nil {
					defer l.close()
				}
				runSingleAckCasterFacet(t, casterAPI{l.send, l.add, l.c, l.close, l.ack, l.skipChurn, l.rearmEach, l.singleOnly})
			})
			continue
		}
		t.Run(a.Impl+"/steady", func(t *testing.T) {
			mk, ok := casterLikes()[a.Impl]
			if !ok {
				t.Fatalf("no caster factory %q", a.Impl)
			}
			runCasterStress(t, func() casterAPI {
				l := mk()
				return casterAPI{l.send, l.add, l.c, l.close, l.ack, l.skipChurn, l.rearmEach, l.singleOnly}
			}, 16, 1000, false)
		})
		t.Run(a.Impl+"/churn", func(t *testing.T) {
			mk := casterLikes()[a.Impl]
			runCasterStress(t, func() casterAPI {
				l := mk()
				return casterAPI{l.send, l.add, l.c, l.close, l.ack, l.skipChurn, l.rearmEach, l.singleOnly}
			}, 16, 1000, true)
		})
	}
}

type scatterLike struct {
	send func(int) int
	add  func(int) int
	recv func() (int, bool) // non-blocking receive from the shared channel
}

func scatterLikes() map[string]func() scatterLike {
	return map[string]func() scatterLike{
		"ChanScatter": func() scatterLike {
			x := NewChanScatter(make(chan int))
			return scatterLike{x.Send, x.Add, func() (int, bool) {
				select {
				case v := <-x.C:
					return v, true
				default:
					return 0, false
				}
			}}
		},
		"BaselineScatterQueue": func() scatterLike {
			x := NewBaselineScatterQueue[int]()
			return scatterLike{x.Send, x.Add, func() (int, bool) {
				select {
				case v := <-x.C:
					return v, true
				default:
					return 0, false
				}
			}}
		},
	}
}

func TestScenarioScatter(t *testing.T) {
	for _, a := range scatterAttachments {
		t.Run(a.Impl, func(t *testing.T) {
			mk := scatterLikes()[a.Impl]
			x := mk()
			if got := x.send(1); got != 0 {
				t.Fatalf("send with no receivers = %d, want 0", got)
			}
			const receivers, sends = 8, 500
			// The shared channel is unbuffered, so receivers cannot poll it
			// non-blockingly; drive the pool through the raw channels instead.
			_ = x
			runScatterFacet(t, a.Impl, receivers, sends)
		})
	}
}

// runScatterFacet uses blocking receives on the shared channel (the documented
// consumption pattern) and asserts exactly-once delivery globally.
func runScatterFacet(t *testing.T, impl string, receivers, sends int) {
	t.Helper()
	var ch chan int
	var send func(int) int
	var add func(int) int
	switch impl {
	case "ChanScatter":
		x := NewChanScatter(make(chan int))
		ch, send, add = x.C, x.Send, x.Add
	case "BaselineScatterQueue":
		x := NewBaselineScatterQueue[int]()
		ch, send, add = x.C, x.Send, x.Add
	default:
		t.Fatalf("unknown scatter impl %q", impl)
	}
	counts := make([]sync.Map, 0) // placeholder to keep structure simple
	_ = counts
	seen := make(map[int]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Pool membership pattern: each worker registers once, then receives
	// repeatedly. (Per-receive re-registration is the caster pattern, not
	// the scatter pattern: a scatter Send holds the dispatch lock while a
	// re-registering worker would be trying to join.)
	for r := 0; r < receivers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			add(1)
			defer add(-1)
			for {
				select {
				case v := <-ch:
					mu.Lock()
					seen[v]++
					mu.Unlock()
				case <-stop:
					return
				}
			}
		}()
	}
	// Deterministic start: do not dispatch until the whole pool has
	// registered. Add(0) is the documented no-op count probe.
	for add(0) < receivers {
		runtime.Gosched()
	}
	for v := 1; v <= sends; v++ {
		done := make(chan int, 1)
		go func(v int) { done <- send(v) }(v)
		select {
		case n := <-done:
			if n != 1 {
				t.Fatalf("%s: Send(%d)=%d want 1", impl, v, n)
			}
		case <-time.After(scenarioGuard):
			t.Fatalf("%s: Send(%d) blocked", impl, v)
		}
	}
	close(stop)
	wg.Wait()
	if len(seen) != sends {
		t.Fatalf("%s: %d distinct values received, want %d", impl, len(seen), sends)
	}
	for v := 1; v <= sends; v++ {
		if seen[v] != 1 {
			t.Fatalf("%s: value %d received %d times, want exactly 1", impl, v, seen[v])
		}
	}
	// Last-receiver withdrawal cannot hang a Send: register, start a Send,
	// withdraw before any receive.
	for i := 0; i < 200; i++ {
		add(1)
		done := make(chan int, 1)
		go func() { done <- send(777) }()
		add(-1)
		select {
		case <-done:
		case <-time.After(scenarioGuard):
			t.Fatalf("%s: Send hung after last receiver withdrew (iteration %d)", impl, i)
		}
		// Drain a possible real delivery of 777 racing the withdrawal.
		select {
		case <-ch:
		default:
		}
	}
}

type gatherLike struct {
	wait func() int
	add  func(int) int
	send func() // one signal send on the shared channel
}

func gatherLikes() map[string]func() gatherLike {
	return map[string]func() gatherLike{
		"ChanGatherer": func() gatherLike {
			x := NewChanGatherer(make(chan struct{}))
			return gatherLike{x.Wait, x.Add, func() { x.C <- struct{}{} }}
		},
		"BaselineGatherBarrier": func() gatherLike {
			x := NewBaselineGatherBarrier[struct{}]()
			return gatherLike{x.Wait, x.Add, func() { x.C <- struct{}{} }}
		},
	}
}

func TestScenarioGather(t *testing.T) {
	for _, a := range gatherAttachments {
		t.Run(a.Impl, func(t *testing.T) {
			mk := gatherLikes()[a.Impl]
			x := mk()
			if got := x.wait(); got != 0 {
				t.Fatalf("Wait with no senders = %d, want 0", got)
			}
			const senders = 8
			for i := 0; i < senders; i++ {
				x.add(1)
			}
			var wg sync.WaitGroup
			for i := 0; i < senders; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); x.send() }()
			}
			done := make(chan int, 1)
			go func() { done <- x.wait() }()
			select {
			case n := <-done:
				if n != senders {
					t.Fatalf("Wait = %d, want %d", n, senders)
				}
			case <-time.After(scenarioGuard):
				t.Fatal("Wait blocked with all senders signalling")
			}
			wg.Wait()
			// Withdrawal compensation: register a sender that never signals,
			// withdraw it during Wait, Wait must still return.
			x.add(1)
			done2 := make(chan int, 1)
			go func() { done2 <- x.wait() }()
			x.add(-1)
			select {
			case <-done2:
			case <-time.After(scenarioGuard):
				t.Fatal("Wait hung after sender withdrew without signalling")
			}
		})
	}
}
