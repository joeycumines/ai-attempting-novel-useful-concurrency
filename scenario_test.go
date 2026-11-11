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
	KindPayload  // multi-word payload integrity, driven via asyncAPI
	KindFanin    // M producers, one batch-oriented consumer, bounded lossy lanes
	KindExchange // two parties swap one value each (symmetric rendezvous)
	KindBarrier  // broadcast barrier with a sender deadline, departures and late acknowledgements
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

	{ID: "nonblocking-lossless-overflow", Title: "Nonblocking lossless fan-out with private unbounded overflow",
		UseCase:   "Event notification where a publisher must never wait for any consumer and every active consumer must eventually receive every value, accepting per-consumer memory growth as the price",
		Tradeoffs: "Slow consumers are isolated in private FIFO queues and cannot apply backpressure or cause loss; retained memory grows with lag and can exhaust the process, unlike bounded rings or backpressured buffers.",
		Kind:      KindBroadcast, Loss: Lossless, Baseline: "BaselineQueuedFanout (baseline)",
		Invariants: []string{"every active subscription receives every accepted value exactly once in FIFO order", "Send does not wait for consumer readiness", "a slow consumer's backlog does not reorder or block another consumer", "withdrawal stops future admission without losing already accepted backlog"}},

	{ID: "work-distribution-scatter", Title: "Work distribution to exactly one receiver (scatter / competing consumers)",
		UseCase:   "Task dispatch for a dynamic worker pool where each item must be processed by exactly one worker and workers join/leave freely",
		Tradeoffs: "No broadcast cost and natural load balancing (whichever worker is ready wins); in exchange delivery is exactly-one, so this is useless for notification, and a withdrawing worker must never orphan an in-flight dispatch.",
		Kind:      KindScatter, Loss: ExactlyOne, Baseline: "BaselineScatterQueue",
		Invariants: []string{"every dispatched value is received by exactly one receiver", "Send returns 0 iff no receiver was registered, 1 iff exactly one received (including via steal compensation)", "withdrawal of the last receiver never hangs a Send"}},
	{ID: "bounded-async-fanout-dispatch", Title: "Bounded asynchronous fan-out dispatch with detectable overload",
		UseCase:   "Bursty event publication where the publisher should enqueue quickly and a dispatcher fans out to independent subscriber mailboxes, accepting bounded loss when either stage overloads",
		Tradeoffs: "A central input queue absorbs bursts and decouples publication from fan-out, but adds dispatch latency and a second loss stage; pooled channels bound allocation while slow subscribers lose oldest mail with observable counters.",
		Kind:      KindBroadcast, Loss: LossyDetectable, Baseline: "BaselineAsyncFanout (baseline)",
		Invariants: []string{"Send never blocks on subscriber readiness", "input and dispatch eviction are separately counted", "each subscriber observes retained values in increasing order without duplicates", "the final value of a burst is eventually observable after overload clears"}},
	{ID: "bounded-competing-consumer-overflow", Title: "Bounded competing-consumer dispatch with explicit overload eviction",
		UseCase:   "Work queues where admission must remain nonblocking and memory must stay bounded, but each accepted item is processed by exactly one worker and overload may evict the oldest pending item with an observable counter",
		Tradeoffs: "Unlike rendezvous scatter, admission does not wait for a worker; unlike lossless work distribution, a full queue evicts pending work. The drop counter makes overload visible and the retained suffix remains FIFO.",
		Kind:      KindScatter, Loss: LossyDetectable, Baseline: "BaselineBoundedScatterQueue (baseline)",
		Invariants: []string{"each retained value is received by at most one worker", "accepted values that are not evicted are received exactly once in FIFO order", "admission never blocks on worker readiness", "overwrite-oldest eviction increments an exact dropped count"}},

	{ID: "signal-gather-barrier", Title: "Signal gathering from N senders (N-to-1 barrier)",
		UseCase:   "Phase/completion barriers for a dynamic set of producers whose signals must all be collected before a coordinator proceeds (the dual of casting)",
		Tradeoffs: "Wait performs one rendezvous per registered sender; a sender that withdraws without signalling must be compensated or Wait hangs, which is the central correctness hazard of the family.",
		Kind:      KindGather, Loss: GatherAll, Baseline: "BaselineGatherBarrier",
		Invariants: []string{"Wait returns after exactly one signal per registered sender", "a withdrawing sender is compensated so Wait cannot hang", "Wait's return equals the number of real (non-compensated) signals"}},
	{ID: "bounded-fanin-merge", Title: "Bounded fan-in merge (M producers, one batch-oriented consumer)",
		UseCase:   "Ingestion and telemetry lanes where many independent producers stream into a single batch-oriented consumer (log pipelines, metrics fan-in, agent work completion queues) with bounded memory and overload that must drop, never block",
		Tradeoffs: "The consumer is a single serialization point, unlike broadcast; producers must never block behind a slow consumer, so each lane is bounded and drops newest values at push time (counted at push), trading completeness for liveness and bounded memory. Ordering is FIFO per producer only; there is deliberately no global order, which removes any temptation for cross-lane coordination.",
		Kind:      KindFanin, Loss: LossyDetectable, Baseline: "BaselineFanIn (baseline)",
		Invariants: []string{"every accepted value is observed by the consumer exactly once, in per-producer FIFO order", "TryPush/TryPushBatch report refusal (false/short count) exactly when the value was not accepted — loss is observable at push time", "total buffered values never exceed the sum of lane capacities", "after Close returns, all accepted values are drainable and Next eventually reports closure"}},
	{ID: "symmetric-pairwise-exchange", Title: "Symmetric pairwise exchange (two parties swap one value each)",
		UseCase:   "Hand-off points where two goroutines must meet and trade — buffer swapping between a filler and a drainer, pairing a request with a responder, elimination back-off for a contended structure — with cancellation on either side",
		Tradeoffs: "There is no buffering and no fan-out: a caller waits until exactly one partner arrives. The design question is whether spreading callers over an arena of meeting slots beats a single lock; the correctness hazard is cancellation racing the match, which must either withdraw the offer or complete the exchange, never both and never neither.",
		Kind:      KindExchange, Loss: ExactlyOne, Baseline: "BaselineExchanger (baseline)",
		Invariants: []string{"a value offered is received by exactly one partner, or the offering caller is told the exchange failed — never both, never neither", "exchanges are symmetric: if A received B's value then B received A's", "two callers that are both waiting are eventually paired", "a caller released by cancellation or close gets an error and its value is never delivered"}},
	{ID: "deadline-bounded-barrier-broadcast", Title: "Barrier broadcast with a sender deadline, mid-broadcast departure and late acknowledgement",
		UseCase:   "Coordinated reconfiguration and epoch changes where the publisher needs every current member to receive and acknowledge a value, but must be able to give up at a deadline and carry on, and where members may leave at any moment",
		Tradeoffs: "As for the lock-step barrier, publisher latency is bounded by the slowest member — until the deadline, after which the value is abandoned and members may or may not have seen it. The implementation has to keep its accounting exact across an abandoned barrier: an acknowledgement that arrives late must not be credited to the next value, and a departing member must release the publisher.",
		Kind:      KindBarrier, Loss: Lossless, Baseline: "BaselineDeadlineBarrier (baseline)",
		Invariants: []string{"a send that returns nil was received (and, where separate, acknowledged) by every member that was joined throughout it", "a member that leaves releases a sender waiting on it", "a send returns its context's error once the context is done, however many members are stalled", "after an abandoned send the next send completes normally: late acknowledgements are not credited to it", "members that keep participating observe every completed value, in order"}},
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
	{"lockstep/fuzz/op-stream", "lockstep-barrier-lossless", CaseFuzz, "Fuzzed stream of open-consumer / publish / await-publishers / probe / retire-consumer ops against one implementation per input", "FuzzScenarioBroadcast"},
	{"lockstep/test/op-stream", "lockstep-barrier-lossless", CaseTest, "Fixed-seed op streams (300 per implementation) plus every recorded regression stream, over all broadcast attachments", "TestScenarioBroadcastOpStreams"},
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
	{"buffered/fuzz/op-stream", "buffered-lossless-backpressured", CaseFuzz, "Op-stream fuzz target (one implementation per input, chosen by the input); subjects are the attachments not marked exposure-limited, the buffered ones among them", "FuzzScenarioBroadcast"},
	{"fanout56/test/buffer-close", "buffered-lossless-backpressured", CaseTest, "Independent queues, full-queue backpressure and idempotent unsubscribe", "TestFanout56BufferedAndClose"},
	{"fanout56/test/publisher-order", "buffered-lossless-backpressured", CaseTest, "Concurrent Send callers preserve the same total order for all subscribers", "TestFanout56ConcurrentOrder"},
	{"fanout56/bench/send", "buffered-lossless-backpressured", CaseBenchmark, "Eight-subscriber buffered fan-out Send cost", "BenchmarkFanout56Send"},
	{"lockstep57/test/phase-ack", "lockstep-barrier-lossless", CaseTest, "Eight subscribers receive ordered values through the phase gate and atomic acknowledgement plane", "TestChanPubSub57FanoutAndLateWaiters"},
	{"lockstep57/test/withdraw-release", "lockstep-barrier-lossless", CaseTest, "Withdrawing an undelivered subscriber compensates the active ping and releases Send", "TestChanPubSub57UnsubscribeReleasesPendingSend"},
	{"lockstep57/test/context-iterator", "lockstep-barrier-lossless", CaseTest, "Context-backed iterator acknowledges delivery and unsubscribes on cancellation", "TestChanPubSub57ContextIterator"},
	{"lockstep57/bench/eight-subscriber-send", "lockstep-barrier-lossless", CaseBenchmark, "Eight-subscriber rendezvous and phase-acknowledgement cost", "BenchmarkChanPubSub57Send"},
	{"ring-lossy/test/monotonic-final", "bounded-ring-lossy-detectable", CaseTest, "Monotonic delivery, final value always observed, sends bounded to capacity where exposure-limited", "TestScenarioBroadcast"},
	{"ring-lossy/bench/fanout", "bounded-ring-lossy-detectable", CaseBenchmark, "Ring publish throughput at 8 subscribers", "BenchmarkScenarioBroadcast"},
	{"ring-lossy/fuzz/op-stream", "bounded-ring-lossy-detectable", CaseFuzz, "Op-stream fuzz target (one implementation per input, chosen by the input); subjects are the attachments not marked exposure-limited, the ring ones among them", "FuzzScenarioBroadcast"},
	{"latest/test/conflated-final", "latest-value-conflating-watch", CaseTest, "Strictly increasing observations, latest always observed, Send non-blocking", "TestScenarioBroadcast"},
	{"latest/bench/fanout", "latest-value-conflating-watch", CaseBenchmark, "Conflating publish throughput at 8 subscribers", "BenchmarkScenarioBroadcast"},
	{"latest/fuzz/op-stream", "latest-value-conflating-watch", CaseFuzz, "Op-stream fuzz target (one implementation per input, chosen by the input); subjects are the attachments not marked exposure-limited, the conflating ones among them", "FuzzScenarioBroadcast"},
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
	{"fanin68/test/bounded-lanes", "bounded-fanin-merge", CaseTest, "Bounded lanes drop-newest with push-time accounting; per-lane FIFO; Close-then-drain contract; exact accepted/refused counts", "TestScenarioFanin"},
	{"fanin-baseline/test/shared-channel", "bounded-fanin-merge", CaseTest, "Vacuum shared-channel baseline passes the same bounded-loss FIFO contracts", "TestScenarioFanin"},
	{"fanin/bench/merge", "bounded-fanin-merge", CaseBenchmark, "Merged throughput at producers=4 with a concurrent consumer and exact accepted/drained accounting", "BenchmarkScenarioFaninMerge"},
	{"fanin/fuzz/model", "bounded-fanin-merge", CaseFuzz, "Bounded-loss per-lane FIFO model against a random push/drain/close op stream", "FuzzScenarioFaninModel"},
	{"exchange/test/pairing-and-cancellation", "symmetric-pairwise-exchange", CaseTest, "Pairing, conservation and symmetry for 2/4/16 parties; withdrawn offers; cancellation racing the match; close", "TestScenarioExchange"},
	{"exchange/bench/contended-pairs", "symmetric-pairwise-exchange", CaseBenchmark, "Completed exchanges per caller with 2/8/64 contending parties", "BenchmarkScenarioExchange"},
	{"exchange/fuzz/patient-and-cancelling", "symmetric-pairwise-exchange", CaseFuzz, "Fuzzed mix of patient and cancelling callers; conservation and symmetry", "FuzzScenarioExchange"},
	{"barrier/test/deadline-departure-late-ack", "deadline-bounded-barrier-broadcast", CaseTest, "All-acknowledge ordering; acknowledgement (not receipt) releases the sender; departure releases the sender; sender deadline then normal service", "TestScenarioBarrier"},
	{"barrier/bench/send-with-acks", "deadline-bounded-barrier-broadcast", CaseBenchmark, "Send cost with 1/8/64 acknowledging members", "BenchmarkScenarioBarrier"},
	{"barrier/fuzz/op-stream", "deadline-bounded-barrier-broadcast", CaseFuzz, "Fuzzed join/leave/stall/deadline stream against the delivery model", "FuzzScenarioBarrier"},
	{"edge/test/standard-no-subscribers", "zero-subscriber-and-edge-lifecycle", CaseTest, "Standard harness: publish with no subscribers never blocks or panics, for every Broadcaster adapter", "TestStandardHarness_NoSubscribers"},
	{"churn/test/standard-concurrent-churn", "ephemeral-churn-membership", CaseTest, "Standard harness: bounded subscribe/receive/unsubscribe cycles against a free-running publisher, for every Broadcaster adapter that does not opt out", "TestStandardHarness_ConcurrentChurn"},
	{"churn/bench/unified-churn", "ephemeral-churn-membership", CaseBenchmark, "Subscribe/unsubscribe cost for every Broadcaster adapter", "BenchmarkUnified_Churn"},
	{"churn/test/membership-churn", "ephemeral-churn-membership", CaseTest, "Continuous subscribe/unsubscribe while publishing; no corruption/stall", "TestScenarioChurn"},
	{"churn/bench/subscribe-unsubscribe", "ephemeral-churn-membership", CaseBenchmark, "Subscribe/unsubscribe cost (BenchmarkUnified_Churn + scenario bench)", "BenchmarkScenarioBroadcast"},
	{"fanout/test/32-subscribers", "large-fanout-scalability", CaseTest, "Correct fan-out at 32 subscribers", "TestScenarioLargeFanout"},
	{"fanout/bench/up-to-512", "large-fanout-scalability", CaseBenchmark, "Publish scaling at 1/8/64/512 subscribers", "BenchmarkUnified_Broadcast"},
	{"payload/test/torn-read", "wide-payload-integrity", CaseTest, "Multi-word payload never torn under steady + churn load", "TestScenarioPayload"},
	{"payload/bench/broadcast", "wide-payload-integrity", CaseBenchmark, "Wide-payload publish throughput", "BenchmarkAsync"},
	{"payload/fuzz/model", "wide-payload-integrity", CaseFuzz, "Ring model fuzz (existing) + scenario op streams", "FuzzRingPubSubModel"},
	{"lifecycle/test/cancel-close", "cancellation-and-close-lifecycle", CaseTest, "Context cancellation and idempotent Close where documented", "TestScenarioLifecycle"},
	{"lifecycle/fuzz/op-stream", "cancellation-and-close-lifecycle", CaseFuzz, "Op-stream fuzz target (one implementation per input, chosen by the input); streams retire consumers mid-publish and close on teardown", "FuzzScenarioBroadcast"},
	{"edge/test/zero-subscriber", "zero-subscriber-and-edge-lifecycle", CaseTest, "Publish with zero subscribers never blocks; late subscriber gets later values", "TestScenarioZeroSubscriber"},
	{"edge/bench/no-subscribers", "zero-subscriber-and-edge-lifecycle", CaseBenchmark, "Publish cost with zero subscribers", "BenchmarkScenarioBroadcast"},
	{"ring-lossy/test/seqcaster-loss-accounting", "bounded-ring-lossy-detectable", CaseTest, "SeqCaster23Meta reports exact overwrite loss and shuts down its workers", "TestSeqCaster23MetaLossAndClose"},
	{"ring-lossy/bench/seqcaster-empty-send", "bounded-ring-lossy-detectable", CaseBenchmark, "SeqCaster23Meta publication cost with no subscriptions", "BenchmarkSeqCaster23MetaSendNoSubscribers"},
	{"ringcaster/test/lap-resume", "bounded-ring-lossy-detectable", CaseTest, "Retained-window catch-up, exact lapping loss, resume gap and conflation semantics", "TestRingCaster55LapResumeAndConflation"},
	{"ringcaster/test/wake-close", "bounded-ring-lossy-detectable", CaseTest, "Arm/re-check wake protocol, close drain and closed-arm behavior", "TestRingCaster55SelectWakeAndClose"},
	{"ringcaster/test/allocations", "bounded-ring-lossy-detectable", CaseTest, "One slab per 256 publications and allocation-free cursor fast paths", "TestRingCaster55AllocationProfile"},
	{"ringcaster/bench/publish", "bounded-ring-lossy-detectable", CaseBenchmark, "Slab-backed ring publish cost with no parked consumers", "BenchmarkRingCaster55Publish"},
	{"ringcaster/fuzz/model", "bounded-ring-lossy-detectable", CaseFuzz, "TryRecv/TryRecvLatest compared with a retained-window model", "FuzzRingCaster55Model"},
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
	{"queued24/test/backlog-isolation", "nonblocking-lossless-overflow", CaseTest, "One stalled consumer queues while another drains; exact FIFO and accepted counts", "TestQueuedFanout24BacklogIsolation"},
	{"queued24/test/withdraw-leases", "nonblocking-lossless-overflow", CaseTest, "Concurrent Send/Unsubscribe admits no value after withdrawal and drains accepted backlog", "TestQueuedFanout24WithdrawLeases"},
	{"queued24/test/cancel-and-pump", "nonblocking-lossless-overflow", CaseTest, "Context escape, select Pump contract and idempotent withdrawal", "TestQueuedFanout24CancelAndPump"},
	{"queued24/bench/mailbox-and-overflow", "nonblocking-lossless-overflow", CaseBenchmark, "Mailbox fast path versus exact accounted private overflow", "BenchmarkQueuedFanout24Paths"},
	{"queued24/fuzz/fifo-model", "nonblocking-lossless-overflow", CaseFuzz, "Bounded publish/drain/withdraw stream compared with a per-subscriber FIFO model", "FuzzQueuedFanout24FIFO"},
	{"queued24/test/concurrent-publishers", "nonblocking-lossless-overflow", CaseTest, "Concurrent publishers preserve each producer's FIFO order and exact accepted count", "TestQueuedFanout24ConcurrentPublisherFIFO"},
	{"nonblocking-overflow/test/baseline-drain", "nonblocking-lossless-overflow", CaseTest, "Novel mailbox and ordinary queued baseline both drain accepted backlog after withdrawal", "TestNonblockingOverflowBaseline"},
	{"stable32/test/competing-drain", "work-distribution-scatter", CaseTest, "Bounded ring dispatches each value to exactly one waiter and reports no-receiver drops", "TestStableRing32CompetingDrain"},
	{"stable32/test/cancel-bounds", "bounded-competing-consumer-overflow", CaseTest, "Context cancellation, receiver accounting bounds and capacity overwrite policy", "TestStableRing32CancelAndBounds"},
	{"stable32/bench/dispatch", "bounded-competing-consumer-overflow", CaseBenchmark, "Condvar ring admission plus dispatch round trip with eight competing waiters", "BenchmarkStableRing32Dispatch"},
	{"stable32/fuzz/queue-model", "bounded-competing-consumer-overflow", CaseFuzz, "Serial queue operations against an overwrite-oldest bounded FIFO model", "FuzzStableRing32QueueModel"},
	{"select38/test/cancel-close", "cancellation-and-close-lifecycle", CaseTest, "Cancellable send and lease-draining retirement at membership boundaries", "TestSelectFanout38Cancellation"},
	{"select38/bench/churn", "ephemeral-churn-membership", CaseBenchmark, "Persistent-tree membership churn versus snapshot publication", "BenchmarkSelectFanout38Churn"},
	{"fanout45/test/lease-close", "cancellation-and-close-lifecycle", CaseTest, "Buffered and synchronous node leases close without send-on-closed panic", "TestFanout45LeaseClose"},
	{"fanout45/bench/buffered", "buffered-lossless-backpressured", CaseBenchmark, "Intrusive-list buffered publication with deterministic draining", "BenchmarkFanout45Buffered"},
	{"ring45/test/replay-overrun", "bounded-catchup-replay", CaseTest, "Retained suffix replay and exact detectable overrun accounting", "TestRingFanout45ReplayAndOverrun"},
	{"ring45/bench/publish", "bounded-ring-lossy-detectable", CaseBenchmark, "Shared-ring publication with eight draining cursors", "BenchmarkRingFanout45Publish"},
	{"caster11/test/cas-boundaries", "rendezvous-cast-no-ack", CaseTest, "Lock-free CAS registration, zero-subscriber fast path and withdrawal compensation", "TestChanCaster11MetaCASBoundaries"},
	{"caster58/test/helpers", "rendezvous-cast-no-ack", CaseTest, "Padding layout, TryRecv gate behavior and context withdrawal compensation", "TestChanCaster58Helpers"},
	{"caster58/bench/try-recv", "rendezvous-cast-no-ack", CaseBenchmark, "Empty TryRecv and successful rendezvous helper cost", "BenchmarkChanCaster58Helpers"},
	{"lockstep58/test/atomic-pong", "lockstep-barrier-lossless", CaseTest, "Pre-armed atomic countdown, final signal and late join boundaries", "TestChanPubSub58AtomicPong"},
	{"lockstep58/test/cancel-ownership", "cancellation-and-close-lifecycle", CaseTest, "Callback/iterator ownership transfer during active rendezvous", "TestChanPubSub58CancellationOwnership"},
	{"lockstep58/bench/send", "lockstep-barrier-lossless", CaseBenchmark, "Eight-subscriber atomic-pong Send cost", "BenchmarkChanPubSub58Send"},
	{"lockstep50/test/parity", "lockstep-barrier-lossless", CaseTest, "Parity WaitGroup generations, completion gate and withdrawal accounting", "TestGenPubSub50Parity"},
	{"lockstep50/test/cancel-ownership", "cancellation-and-close-lifecycle", CaseTest, "Generation-aware callback/iterator ownership transfer", "TestGenPubSub50CancellationOwnership"},
	{"lockstep50/bench/send", "lockstep-barrier-lossless", CaseBenchmark, "Eight-subscriber parity WaitGroup Send cost", "BenchmarkGenPubSub50Send"},
	{"broadcast58/test/ring-generations", "bounded-ring-lossy-detectable", CaseTest, "Exact missed counts, retained suffix, wide payloads and close drain", "TestBroadcaster58RingAndClose"},
	{"broadcast58/test/iterator-edges", "cancellation-and-close-lifecycle", CaseTest, "Load-drain-select wake protocol, nil yield and cancellation cleanup", "TestBroadcaster58IteratorEdges"},
	{"broadcast58/test/wide-payload", "wide-payload-integrity", CaseTest, "Concurrent multi-word ring copies are coherent and monotonic under overwrite", "TestBroadcaster58WidePayload"},
	{"broadcast58/bench/send", "bounded-ring-lossy-detectable", CaseBenchmark, "Ring copy plus swap-then-close generation cost", "BenchmarkBroadcaster58Send"},
	{"broadcast58/fuzz/ring-model", "bounded-ring-lossy-detectable", CaseFuzz, "Cursor operations compared with an exact bounded-ring model", "FuzzBroadcaster58RingModel"},
	{"ring14/test/burst-eviction", "bounded-async-fanout-dispatch", CaseTest, "Paused two-stage model checks exact input/dispatch drop counters and retained suffix", "TestRingFanOut14BurstEviction"},
	{"ring14/test/pool-reuse", "bounded-async-fanout-dispatch", CaseTest, "Remove drains pooled channels before reuse and pool exhaustion is explicit", "TestRingFanOut14PoolReuse"},
	{"ring14/test/close", "cancellation-and-close-lifecycle", CaseTest, "Idempotent Close rejects admission and closes pooled channels after dispatcher exit", "TestRingFanOut14Close"},
	{"ring14/bench/admission", "bounded-async-fanout-dispatch", CaseBenchmark, "Full input-queue and full subscriber-mailbox drop-oldest admission cost", "BenchmarkRingFanOut14Admission"},
	{"ring14/fuzz/two-stage-model", "bounded-async-fanout-dispatch", CaseFuzz, "Paused dispatcher compared with an exact two-stage bounded FIFO model", "FuzzRingFanOut14TwoStageModel"},
	{"async-baseline/test/final-value", "bounded-async-fanout-dispatch", CaseTest, "Ordinary async baseline retains the final burst value under bounded overload", "TestBaselineAsyncFanoutFinalValue"},
	{"bounded-scatter/test/evict-exactly-one", "bounded-competing-consumer-overflow", CaseTest, "Full-queue eviction count and exactly-once retained suffix across competing waiters", "TestBoundedScatterEviction"},
	{"bounded-scatter/bench/admission", "bounded-competing-consumer-overflow", CaseBenchmark, "Nonblocking admission into a full bounded queue", "BenchmarkBoundedScatterAdmission"},
	{"bounded-scatter/fuzz/model", "bounded-competing-consumer-overflow", CaseFuzz, "Ring and ordinary baseline admission/drain operations compared with a bounded FIFO eviction model", "FuzzBoundedScatterModel"},
	{"bounded-scatter/test/competing-exactly-one", "bounded-competing-consumer-overflow", CaseTest, "Non-overflow dispatch to competing workers is exactly once for ring and baseline", "TestBoundedScatterCompetingExactlyOne"},
	{"beacon08/test/call-point", "latest-value-conflating-watch", CaseTest, "Beacon and RingBeacon capture the subscription point at SubscribeContext call time", "TestBeaconSubscribeContextCapturesCallPoint"},
	{"beacon08/test/ring-call-point", "bounded-ring-lossy-detectable", CaseTest, "RingBeacon iterator observes values published before first invocation", "TestRingBeaconSubscribeContextCapturesCallPoint"},
	{"beacon08/test/ring-wakeup", "bounded-ring-lossy-detectable", CaseTest, "RingBeacon re-checks after Ready capture so a concurrent Send cannot strand the iterator", "TestRingBeaconSubscribeContextNoLostWakeup"},
	{"beacon08/test/reliable-armed-round", "lockstep-barrier-lossless", CaseTest, "ReliableBeacon registration captures the armed round so churn cannot leave an unacknowledged subscription", "TestReliableBeaconSubscribeContextCapturesArmedRound"},
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
