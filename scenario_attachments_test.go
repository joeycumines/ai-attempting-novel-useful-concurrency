package bigbuff

// Attachment tables and the init-time registration that validates them.

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

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
	bRingBeacon  = "RingBeacon (08-MuseSpark)"
	bEpochGemini = "EpochCasterGemini (09-Gemini)"
	bEpochPubSub = "EpochPubSub (11-meta)"
	bReliable    = "ReliableBeacon (08-MuseSpark)"
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
	att("zero-subscriber-and-edge-lifecycle", "Caster:ChanCaster11Meta", "Lock-free dual-counter caster variant edge behaviour.", true),
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
	att("rendezvous-cast-no-ack", "ChanCaster11Meta", "Completed lock-free CAS dual-counter sketch: positive Add spins only while armed, negative Add compensates the shared rendezvous.", true),
	att("rendezvous-cast-no-ack", "ChanCaster14Xing", "Caster variant.", true),
	att("rendezvous-cast-no-ack", "ChanCaster15Nemotron", "Caster variant.", true),
	att("rendezvous-cast-no-ack", "ChanCaster17FastPath", "Caster variant with fast-path send.", true),
	att("rendezvous-cast-no-ack", "ChanCaster17Pumped", "Caster variant with pumped send.", true),
	att("rendezvous-cast-no-ack", "BaselineRendezvousCaster", "Vacuum baseline: mutex-guarded count + one unbuffered channel + compensating dummy sends on withdrawal.", true),
	att("rendezvous-cast-no-ack", "AdaptiveCaster23Meta", "Adaptive hybrid retains ChanCaster's synchronous rendezvous path and provides a separate SeqCaster path for registered ring subscribers; this attachment measures its shared-channel caster mode.", true),
	attX("rendezvous-cast-no-ack", "ChanPubSub35ChatJimmy", "Shared ChanCaster channel with a condition-variable pong barrier; the registered receive count is acknowledged before Send returns.", false, "Exposure limited: no concurrent receiver withdrawal or cancellation during a Send/Wait round; the response counts acknowledgements globally and cannot reclaim a missing acknowledgement, so no churn/abandonment facet may run."),
	att("rendezvous-cast-no-ack", "ChanCaster58Spark", "The reference dual-counter rendezvous protocol with cache-line isolation and RecvContext/TryRecv helpers; withdrawal still compensates an active Send.", true),
	att("rendezvous-cast-no-ack", "ChanCaster47Gemini", "Atomic packed-state one-shot receiver accounting; Add and Send coordinate through CAS and a parked positive-add barrier.", true),
}

var scatterAttachments = []Attachment{
	att("work-distribution-scatter", "ChanScatter", "Single-channel scatter with steal compensation on last withdrawal: the novel design under test.", true),
	att("bounded-competing-consumer-overflow", "StableRing32Nano (32-Nano)", "Condvar ring admits without waiting, evicts the oldest pending item at capacity and counts eviction; one shared cursor dispatches to one waiter.", true),
	att("bounded-competing-consumer-overflow", "BaselineBoundedScatterQueue (baseline)", "Ordinary buffered channel with serialized make-room-then-enqueue admission; no novel ring or wakeup machinery.", true),
	att("work-distribution-scatter", "StableRing32Nano (32-Nano)", "Explicit poor-fit comparison: the condvar ring admits before a worker receives and can evict under overload, so it does not provide the lossless receipt-coupled scatter invariant; its good-fit scenario is bounded-competing-consumer-overflow.", false),
	att("work-distribution-scatter", "BaselineScatterQueue", "Vacuum baseline: an ordinary unbuffered channel already dispatches to exactly one ready receiver.", true),
}

var gatherAttachments = []Attachment{
	att("signal-gather-barrier", "ChanGatherer", "Dual of the reference caster: Wait receives once per sender, Add(-1) compensates.", true),
	att("signal-gather-barrier", "BaselineGatherBarrier", "Vacuum baseline: mutex-guarded count + one unbuffered channel + compensating sends.", true),
}

var faninAttachments = []Attachment{
	att("bounded-fanin-merge", "EdgeFanIn68Chatgpt (68-ChatGPT)", "Per-producer bounded SPSC lanes with an MPSC ready-stack and amortised batch drain; drop-newest with push-time accounting (TryPush false / TryPushBatch short count); FIFO per lane, no global ordering; Close drains accepted values before reporting closure. The scenario's intended design.", true),
	att("bounded-fanin-merge", "BaselineFanIn (baseline)", "Vacuum baseline: one shared buffered channel behind one mutex (push path), select-default drops counted in Drops(); single lock domain, no lane isolation or batching — the ordinary design the port must beat.", true),
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
	att("wide-payload-integrity", "RingFanOut14Xing_c64", "Pooled dispatcher mailboxes carry multi-word payloads; drop-oldest admission must not tear retained values.", true),
	att("wide-payload-integrity", "BaselineAsyncFanout_c64", "Ordinary async dispatcher baseline carrying multi-word payloads.", true),
	att("wide-payload-integrity", "Broadcaster58Spark_c64", "Per-cell locks make multi-word payload copies coherent while atomic sequence stamps detect cursor overrun.", true),
}

func allAttachments() []Attachment {
	out := make([]Attachment, 0, len(broadcastAttachments)+len(casterAttachments)+len(scatterAttachments)+len(gatherAttachments)+len(faninAttachments))
	out = append(out, broadcastAttachments...)
	out = append(out, casterAttachments...)
	out = append(out, scatterAttachments...)
	out = append(out, gatherAttachments...)
	out = append(out, faninAttachments...)
	out = append(out, payloadAttachments...)
	out = append(out, exchangeAttachments...)
	out = append(out, barrierAttachments...)
	return out
}

func init() {
	// Keep response-derived implementations declarative while ensuring the
	// same strict init-time reference and metadata validation applies to them.
	broadcastAttachments = append(broadcastAttachments, extractedBroadcastAttachments()...)
	broadcastAttachments = append(broadcastAttachments, gapBroadcastAttachments()...)
	broadcastAttachments = append(broadcastAttachments, portBroadcastAttachments()...)
	broadcastAttachments = append(broadcastAttachments, apexBroadcastAttachments()...)
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
	var duplicateAttachments []string
	attachmentKeys := make(map[string]bool)
	for _, a := range allAttachments() {
		key := string(a.Scenario) + "\x00" + a.Impl
		if attachmentKeys[key] {
			duplicateAttachments = append(duplicateAttachments, string(a.Scenario)+"/"+a.Impl)
		}
		attachmentKeys[key] = true
	}
	if len(duplicateAttachments) != 0 {
		panic("duplicate attachments: " + strings.Join(duplicateAttachments, ", "))
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
	// Every scenario's declared baseline must itself be attached to it: a
	// scenario without its yardstick is not a comparison.
	for _, s := range Scenarios {
		if !attachmentKeys[string(s.ID)+"\x00"+s.Baseline] {
			panic(fmt.Sprintf("bigbuff: scenario %q declares baseline %q, which is not attached to it", s.ID, s.Baseline))
		}
	}
	// Every attachment must resolve to a factory of the family that runs its
	// scenario's kind. The key-indexed families are checked here; broadcast
	// factories are indexed by Name(), which exists only on a constructed
	// instance, so that family is resolved by TestScenarioRegistryComplete
	// rather than by constructing every broadcaster at package init.
	keySets := map[ScenarioKind]map[string]bool{
		KindCaster:   {"BaselineRendezvousCaster": true},
		KindScatter:  {},
		KindGather:   {},
		KindPayload:  {},
		KindFanin:    {},
		KindExchange: {},
		KindBarrier:  {},
	}
	for name := range casterImpls() {
		keySets[KindCaster][name] = true
	}
	for name := range scatterLikes() {
		keySets[KindScatter][name] = true
	}
	for name := range gatherLikes() {
		keySets[KindGather][name] = true
	}
	for name := range asyncImpls() {
		keySets[KindPayload][name] = true
	}
	for name := range faninLikes() {
		keySets[KindFanin][name] = true
	}
	for name := range exchangeLikes() {
		keySets[KindExchange][name] = true
	}
	for name := range barrierLikes() {
		keySets[KindBarrier][name] = true
	}
	for _, a := range allAttachments() {
		kind := scenarioByID[a.Scenario].Kind
		if keys, ok := keySets[kind]; ok && !keys[a.Impl] {
			panic(fmt.Sprintf("bigbuff: attachment %q/%q does not resolve to a %s factory", a.Scenario, a.Impl, scenarioKindName(kind)))
		}
	}
}

// scenarioBroadcastFactories indexes every Broadcaster factory by Name().
// It is AllBroadcasters plus the caster-family adapters: casters share one
// channel across all subscribers, so they cannot honour the independent-
// subscriber assumptions of the generic harness (a fast subscriber could
// take another's copy of a round), but through this adapter they are sound
// for the single-subscriber / zero-subscriber scenario facets that use it.
var broadcastFactoryOnce sync.Once
var broadcastFactoryMap map[string]func() Broadcaster

func scenarioBroadcastFactories() map[string]func() Broadcaster {
	broadcastFactoryOnce.Do(func() {
		broadcastFactoryMap = make(map[string]func() Broadcaster)
		add := func(f func() Broadcaster) {
			probe := f()
			name := probe.Name()
			// Naming probes must not retain worker goroutines (notably SeqCaster).
			probe.Close()
			if _, dup := broadcastFactoryMap[name]; dup {
				panic("bigbuff: duplicate broadcaster factory identity " + name)
			}
			broadcastFactoryMap[name] = f
		}
		for _, f := range AllBroadcasters() {
			add(f)
		}
		for _, key := range []string{"ChanCaster", "ChanCaster2", "ChanCasterV2_06meta", "ChanCaster09Gemini", "ChanCaster11Meta", "ChanCaster14Xing", "ChanCaster15Nemotron", "ChanCaster17FastPath", "ChanCaster17Pumped", "ChanCaster58Spark"} {
			add(casterBroadcasterFactory(key))
		}
	})
	return broadcastFactoryMap
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
