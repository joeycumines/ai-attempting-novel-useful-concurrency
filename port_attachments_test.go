package bigbuff

// Adapters for the rawLLM response extracts ported in this branch
// (candidates 59-70). Adapters only: public APIs are adapted, loss, blocking
// and acknowledgement semantics are documented per attachment, never changed.
//
// All of these implementations are lossy by design (overwrite rings or
// conflating latest-value cells), so every adapter reports IsCoalescing() ==
// true and is attached to the scenarios whose invariants tolerate loss, with
// honest fit remarks.

import (
	"context"
	"sync"
	"sync/atomic"
)

// cancelledAdapterCtx is a permanently cancelled context for adapters whose
// candidate offers only a context-bounded blocking read: such reads check
// for data first, so this turns them into deterministic non-blocking reads.
var cancelledAdapterCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// adapterWakeGate is a test-side wake path for candidates that are polling
// registers by design (no wait primitive of their own). It is a single
// buffered token with relay: waiters arm before their final emptiness check
// and a woken waiter re-posts the token while other waiters remain armed, so
// one notify reaches every armed waiter. notify is O(1) and allocation-free
// (one non-blocking send) so that publish-side measurements of the adapted
// candidate are perturbed by a constant, documented amount; receive-side
// wake latency through this gate belongs to the adapter, not the candidate.
type adapterWakeGate struct {
	waiters atomic.Int64
	ch      chan struct{}
}

func newAdapterWakeGate() *adapterWakeGate {
	return &adapterWakeGate{ch: make(chan struct{}, 1)}
}

func (g *adapterWakeGate) notify() {
	select {
	case g.ch <- struct{}{}:
	default:
	}
}

func (g *adapterWakeGate) arm()    { g.waiters.Add(1) }
func (g *adapterWakeGate) disarm() { g.waiters.Add(-1) }

// park blocks an armed waiter until a token arrives (true) or ctx is done.
func (g *adapterWakeGate) park(ctx context.Context) bool {
	select {
	case <-g.ch:
		if g.waiters.Add(-1) > 0 {
			g.notify()
		}
		return true
	case <-ctx.Done():
		g.waiters.Add(-1)
		return false
	}
}

// --- HorizonRing59Gemini (59-Gemini) ---

type horizonRing59Adapter struct{ r *HorizonRing59Gemini[int] }

func newHorizonRing59Adapter() Broadcaster {
	return &horizonRing59Adapter{r: NewHorizonRing59Gemini[int](64)}
}
func (a *horizonRing59Adapter) Name() string       { return "HorizonRing59Gemini (59-Gemini)" }
func (a *horizonRing59Adapter) Publish(v int) int  { a.r.Publish(v); return 1 }
func (a *horizonRing59Adapter) Close()             { a.r.Close() }
func (a *horizonRing59Adapter) IsCoalescing() bool { return true }
func (a *horizonRing59Adapter) Subscribe() BroadcastSubscriber {
	return &horizonRing59Sub{c: a.r.NewCursor59Gemini(StartLatest59Gemini)}
}

type horizonRing59Sub struct{ c *Cursor59Gemini[int] }

func (s *horizonRing59Sub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	v, _, _, err := s.c.ReadContext(ctx)
	if err != nil {
		return 0, false
	}
	return v, true
}
func (s *horizonRing59Sub) TryReceive() (int, bool) {
	v, _, _, ok := s.c.TryRead()
	return v, ok
}
func (s *horizonRing59Sub) Unsubscribe() {} // cursors are stateless; no registration

// --- StripedCombiner59Gemini (59-Gemini, fronting HorizonRing59Gemini) ---

type stripedCombiner59Adapter struct {
	ring *HorizonRing59Gemini[int]
	c    *StripedCombiner59Gemini[int]
}

func newStripedCombiner59Adapter() Broadcaster {
	ring := NewHorizonRing59Gemini[int](64)
	return &stripedCombiner59Adapter{
		ring: ring,
		c:    NewStripedCombiner59Gemini(ring, 8),
	}
}
func (a *stripedCombiner59Adapter) Name() string       { return "StripedCombiner59Gemini (59-Gemini)" }
func (a *stripedCombiner59Adapter) Publish(v int) int  { a.c.Submit(v); return 1 }
func (a *stripedCombiner59Adapter) Close()             { a.ring.Close() }
func (a *stripedCombiner59Adapter) IsCoalescing() bool { return true }
func (a *stripedCombiner59Adapter) Subscribe() BroadcastSubscriber {
	return &horizonRing59Sub{c: a.ring.NewCursor59Gemini(StartLatest59Gemini)}
}

// --- Latest3260Terra (60-Terra) ---

type latest3260TerraAdapter struct {
	x    *Latest3260Terra
	gate *adapterWakeGate
}

func newLatest3260TerraAdapter() Broadcaster {
	return &latest3260TerraAdapter{x: &Latest3260Terra{}, gate: newAdapterWakeGate()}
}
func (a *latest3260TerraAdapter) Name() string { return "Latest3260Terra (60-Terra)" }
func (a *latest3260TerraAdapter) Publish(v int) int {
	a.x.Publish(uint32(v))
	a.gate.notify()
	return 1
}
func (a *latest3260TerraAdapter) Close() {
	a.x.Close()
	a.gate.notify()
}
func (a *latest3260TerraAdapter) IsCoalescing() bool { return true }
func (a *latest3260TerraAdapter) Subscribe() BroadcastSubscriber {
	// Revision is captured synchronously so no publication can be missed.
	return &latest3260TerraSub{x: a.x, gate: a.gate, rev: a.x.Load().Revision}
}

type latest3260TerraSub struct {
	x    *Latest3260Terra
	gate *adapterWakeGate
	rev  uint32
}

func (s *latest3260TerraSub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	// The candidate is a polling register with no wake primitive by design.
	// The blocking Receive the Broadcaster contract requires is therefore
	// supplied by the adapter's wake gate (signalled by the adapter's
	// Publish / Close after the candidate's own call), not by the candidate:
	// see the exposure note on its attachments.
	for {
		if v, ok, closed := s.poll(); ok || closed {
			return v, ok
		}
		s.gate.arm()
		if v, ok, closed := s.poll(); ok || closed {
			s.gate.disarm()
			return v, ok
		}
		if !s.gate.park(ctx) {
			return 0, false
		}
	}
}

// poll reports a new revision (ok) or a closed register (closed).
func (s *latest3260TerraSub) poll() (v int, ok, closed bool) {
	snap := s.x.Load()
	if snap.Closed {
		return 0, false, true
	}
	if snap.Revision != s.rev {
		s.rev = snap.Revision
		return int(snap.Value), true, false
	}
	return 0, false, false
}
func (s *latest3260TerraSub) TryReceive() (int, bool) {
	snap := s.x.Load()
	if snap.Closed || snap.Revision == s.rev {
		return 0, false
	}
	s.rev = snap.Revision
	return int(snap.Value), true
}
func (s *latest3260TerraSub) Unsubscribe() {}

// --- Pulse61Deepseek (61-DeepSeek) ---

type pulse61Adapter struct{ p *Pulse61Deepseek[uint64] }

func newPulse61Adapter() Broadcaster {
	return &pulse61Adapter{p: NewPulse61Deepseek[uint64]()}
}
func (a *pulse61Adapter) Name() string       { return "Pulse61Deepseek (61-DeepSeek)" }
func (a *pulse61Adapter) Publish(v int) int  { _ = a.p.Set(uint64(v)); return 1 }
func (a *pulse61Adapter) Close()             { _ = a.p.Close() }
func (a *pulse61Adapter) IsCoalescing() bool { return true }
func (a *pulse61Adapter) Subscribe() BroadcastSubscriber {
	return &pulse61Sub{p: a.p, ver: a.p.Version()}
}

type pulse61Sub struct {
	p   *Pulse61Deepseek[uint64]
	ver uint64
}

func (s *pulse61Sub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	v, ver, err := s.p.WaitFor(ctx, s.ver)
	if err != nil {
		return 0, false
	}
	s.ver = ver
	return int(v), true
}
func (s *pulse61Sub) TryReceive() (int, bool) {
	v, ver := s.p.Load()
	if ver == s.ver {
		return 0, false
	}
	s.ver = ver
	return int(v), true
}
func (s *pulse61Sub) Unsubscribe() {}

// --- TelemetryRing63Qwen (63-Qwen; single-producer) ---

type telemetryRing63QwenAdapter struct{ r *TelemetryRing63Qwen[int] }

// SinglePublisher declares the candidate's contract (overlapping Publish
// calls panic); see harnessPublish.
func (a *telemetryRing63QwenAdapter) SinglePublisher() bool { return true }

func newTelemetryRing63QwenAdapter() Broadcaster {
	return &telemetryRing63QwenAdapter{r: NewTelemetryRing63Qwen[int](1024, true)}
}
func (a *telemetryRing63QwenAdapter) Name() string       { return "TelemetryRing63Qwen (63-Qwen)" }
func (a *telemetryRing63QwenAdapter) Publish(v int) int  { a.r.Publish(v); return 1 }
func (a *telemetryRing63QwenAdapter) Close()             {} // no Close by design
func (a *telemetryRing63QwenAdapter) IsCoalescing() bool { return true }
func (a *telemetryRing63QwenAdapter) Subscribe() BroadcastSubscriber {
	// Subscribers replay from the first retained value; zero-subscriber
	// facets are not attached because the ring exposes no published-count
	// accessor to tail-attach cheaply.
	return &telemetryRing63QwenSub{r: a.r, cursor: 0}
}

type telemetryRing63QwenSub struct {
	r      *TelemetryRing63Qwen[int]
	cursor uint64
}

func (s *telemetryRing63QwenSub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	// NextWait arms the relayed notifier before its final emptiness check, so
	// a publication can neither be missed nor wake only one of several
	// subscribers (the raw NotifyChan token wakes a single receiver).
	return s.r.NextWait(ctx, &s.cursor)
}
func (s *telemetryRing63QwenSub) TryReceive() (int, bool) { return s.r.Next(&s.cursor) }
func (s *telemetryRing63QwenSub) Unsubscribe()            {}

// --- Beacon64GLM (64-GLM) ---

type beacon64GLMAdapter struct{ b *Beacon64GLM[int] }

func newBeacon64GLMAdapter() Broadcaster {
	return &beacon64GLMAdapter{b: NewBeacon64GLM[int](1024)}
}
func (a *beacon64GLMAdapter) Name() string       { return "Beacon64GLM (64-GLM)" }
func (a *beacon64GLMAdapter) Publish(v int) int  { _, _ = a.b.Publish(v); return 1 }
func (a *beacon64GLMAdapter) Close()             { a.b.Close() }
func (a *beacon64GLMAdapter) IsCoalescing() bool { return true }
func (a *beacon64GLMAdapter) Subscribe() BroadcastSubscriber {
	return &beacon64GLMSub{w: a.b.WatchLatest()}
}

type beacon64GLMSub struct{ w *Watcher64GLM[int] }

func (s *beacon64GLMSub) Receive(ctx context.Context) (int, bool) {
	v, _, ok := s.w.Next(ModeLatest64GLM, ctx)
	return v, ok
}
func (s *beacon64GLMSub) TryReceive() (int, bool) {
	v, _, ok := s.w.Next(ModeNonBlock64GLM, nil)
	return v, ok
}
func (s *beacon64GLMSub) Unsubscribe() {} // watchers are stateless cursors

// --- Broadcaster65Deepseek (65-DeepSeek) ---

type broadcaster65DeepseekAdapter struct{ b *Broadcaster65Deepseek[int] }

func newBroadcaster65DeepseekAdapter() Broadcaster {
	return &broadcaster65DeepseekAdapter{b: NewBroadcaster65Deepseek[int](1024)}
}
func (a *broadcaster65DeepseekAdapter) Name() string       { return "Broadcaster65Deepseek (65-DeepSeek)" }
func (a *broadcaster65DeepseekAdapter) Publish(v int) int  { _, _ = a.b.Publish(v); return 1 }
func (a *broadcaster65DeepseekAdapter) Close()             { _ = a.b.Close() }
func (a *broadcaster65DeepseekAdapter) IsCoalescing() bool { return true }
func (a *broadcaster65DeepseekAdapter) Subscribe() BroadcastSubscriber {
	// The subscription's own context is unused by the adapter; facets pass
	// their context to Receive instead.
	return &broadcaster65DeepseekSub{sub: a.b.Subscribe(context.Background())}
}

type broadcaster65DeepseekSub struct{ sub *Subscription65Deepseek[int] }

func (s *broadcaster65DeepseekSub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	// NextContext parks on the subscription's own notify channel and honours
	// this call's context without closing the subscription.
	return s.sub.NextContext(ctx)
}
func (s *broadcaster65DeepseekSub) TryReceive() (int, bool) {
	v, r := s.sub.TryNext()
	return v, r == ReadOK65Deepseek
}
func (s *broadcaster65DeepseekSub) Unsubscribe() { _ = s.sub.Close() }

// --- Horizon66Gemini (66-Gemini) ---

type horizon66GeminiAdapter struct{ h *Horizon66Gemini[int] }

func newHorizon66GeminiAdapter() Broadcaster {
	return &horizon66GeminiAdapter{h: NewHorizon66Gemini[int](64)}
}
func (a *horizon66GeminiAdapter) Name() string       { return "Horizon66Gemini (66-Gemini)" }
func (a *horizon66GeminiAdapter) Publish(v int) int  { a.h.Publish(v); return 1 }
func (a *horizon66GeminiAdapter) Close()             { a.h.Close() }
func (a *horizon66GeminiAdapter) IsCoalescing() bool { return true }
func (a *horizon66GeminiAdapter) Subscribe() BroadcastSubscriber {
	// Tail attach: first unread sequence is PublishedSeq()+1.
	return &horizon66GeminiSub{h: a.h, cursor: a.h.PublishedSeq() + 1}
}

type horizon66GeminiSub struct {
	h      *Horizon66Gemini[int]
	cursor uint64
}

func (s *horizon66GeminiSub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	v, _, st := s.h.ReadNext(ctx, &s.cursor, CatchUpSkipToOldest66Gemini)
	if st != StatusOK66Gemini {
		return 0, false
	}
	return v, true
}
func (s *horizon66GeminiSub) TryReceive() (int, bool) {
	if s.h.PublishedSeq() < s.cursor {
		return 0, false
	}
	v, st := s.h.TryRead(s.cursor)
	switch st {
	case StatusOK66Gemini:
		s.cursor++
		return v, true
	case StatusOverrun66Gemini:
		s.cursor = s.h.OldestAvailableSeq()
		return 0, false
	default:
		return 0, false
	}
}
func (s *horizon66GeminiSub) Unsubscribe() {}

// --- Ring67Haiku (67-Haiku) ---

type ring67HaikuAdapter struct{ r *Ring67Haiku[int] }

func newRing67HaikuAdapter() Broadcaster {
	return &ring67HaikuAdapter{r: NewRing67Haiku[int](64)}
}
func (a *ring67HaikuAdapter) Name() string       { return "Ring67Haiku (67-Haiku)" }
func (a *ring67HaikuAdapter) Publish(v int) int  { _, _ = a.r.Publish(v); return 1 }
func (a *ring67HaikuAdapter) Close()             { a.r.Close() }
func (a *ring67HaikuAdapter) IsCoalescing() bool { return true }
func (a *ring67HaikuAdapter) Subscribe() BroadcastSubscriber {
	return &ring67HaikuSub{c: a.r.Subscribe()}
}

type ring67HaikuSub struct{ c *RingCursor67Haiku[int] }

func (s *ring67HaikuSub) Receive(ctx context.Context) (int, bool) {
	v, _, err := s.c.Next(ctx)
	if err != nil {
		return 0, false
	}
	return v, true
}
func (s *ring67HaikuSub) TryReceive() (int, bool) {
	// Next checks for data before it consults the context, so an already
	// cancelled context makes it a deterministic non-blocking read.
	v, _, err := s.c.Next(cancelledAdapterCtx)
	if err != nil {
		return 0, false
	}
	return v, true
}
func (s *ring67HaikuSub) Unsubscribe() {} // cursors are stateless

// --- WatchCell69Deepseek (69-DeepSeek) ---

type watchCell69DeepseekAdapter struct{ x *WatchCell69Deepseek[int] }

func newWatchCell69DeepseekAdapter() Broadcaster {
	return &watchCell69DeepseekAdapter{x: NewWatchCell69Deepseek[int]()}
}
func (a *watchCell69DeepseekAdapter) Name() string       { return "WatchCell69Deepseek (69-DeepSeek)" }
func (a *watchCell69DeepseekAdapter) Publish(v int) int  { a.x.Store(v); return 1 }
func (a *watchCell69DeepseekAdapter) Close()             { a.x.Close() }
func (a *watchCell69DeepseekAdapter) IsCoalescing() bool { return true }
func (a *watchCell69DeepseekAdapter) Subscribe() BroadcastSubscriber {
	// Tail-attach anchored synchronously at subscribe time: the watcher
	// goroutine only delivers observations whose epoch is strictly newer
	// than the captured one, so nothing published before the subscribe is
	// ever delivered. Delivery re-anchors to the latest Load (the cell is
	// conflating by design), which keeps the Watch catch-up yield safe to
	// consume without ever dropping a post-subscribe publication.
	s := &watchCell69DeepseekSub{
		x:    a.x,
		ch:   make(chan int, 1),
		done: make(chan struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	go s.watch(a.x.Epoch())
	return s
}

type watchCell69DeepseekSub struct {
	x      *WatchCell69Deepseek[int]
	ch     chan int
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func (s *watchCell69DeepseekSub) watch(anchor uint64) {
	defer close(s.done)
	for range s.x.Watch(s.ctx) {
		// The yielded value is deliberately ignored: re-anchor to the
		// latest observation so a coalesced publication can never leave
		// the subscriber stranded on a superseded value.
		v, ep := s.x.Load()
		if ep <= anchor {
			continue // pre-subscribe boundary, nothing new yet
		}
		anchor = ep
		// Conflate into the single-slot channel: replace a pending
		// (undelivered) value with the newest.
		select {
		case s.ch <- v:
		default:
			select {
			case <-s.ch:
			default:
			}
			s.ch <- v
		}
	}
}

func (s *watchCell69DeepseekSub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case v := <-s.ch:
		return v, true
	case <-s.done:
		// The cell was closed: drain any pending observation first.
		select {
		case v := <-s.ch:
			return v, true
		default:
			return 0, false
		}
	case <-ctx.Done():
		return 0, false
	case <-s.ctx.Done():
		return 0, false
	}
}
func (s *watchCell69DeepseekSub) TryReceive() (int, bool) {
	select {
	case v := <-s.ch:
		return v, true
	default:
		return 0, false
	}
}
func (s *watchCell69DeepseekSub) Unsubscribe() {
	s.once.Do(func() { s.cancel() })
	<-s.done
}

// --- Horizon70Meta (70-Meta) ---

type horizon70MetaAdapter struct{ h *Horizon70Meta[int] }

func newHorizon70MetaAdapter() Broadcaster {
	return &horizon70MetaAdapter{h: NewHorizon70Meta[int](64)}
}
func (a *horizon70MetaAdapter) Name() string       { return "Horizon70Meta (70-Meta)" }
func (a *horizon70MetaAdapter) Publish(v int) int  { a.h.Publish(v); return 1 }
func (a *horizon70MetaAdapter) Close()             { a.h.Close() }
func (a *horizon70MetaAdapter) IsCoalescing() bool { return true }
func (a *horizon70MetaAdapter) Subscribe() BroadcastSubscriber {
	return &horizon70MetaSub{c: a.h.SubscribeFromLatest()}
}

type horizon70MetaSub struct{ c *Cursor70Meta[int] }

func (s *horizon70MetaSub) Receive(ctx context.Context) (int, bool) {
	return s.c.WaitNextContext(ctx)
}
func (s *horizon70MetaSub) TryReceive() (int, bool) { return s.c.Next() }
func (s *horizon70MetaSub) Unsubscribe()            {} // cursors are stateless

func portBroadcastAttachments() []Attachment {
	return []Attachment{
		att("bounded-ring-lossy-detectable", "HorizonRing59Gemini (59-Gemini)", "Overwritable seqlock ring with independent cursors; overwritten values are reported as dropped counts per cursor (TryRead/ReadContext). Attached ahead of 59's combiner front-end.", true),
		att("bounded-ring-lossy-detectable", "StripedCombiner59Gemini (59-Gemini)", "Flat-combining publish front-end over HorizonRing59Gemini; delivery semantics are the ring's (lossy overwrite, drop accounting at the cursor). Single publisher only, like the ring.", true),
		att("latest-value-conflating-watch", "Latest3260Terra (60-Terra)", "Atomic packed value+revision word; watch semantics by re-reading the word. No wake primitive exists by design (the candidate is a non-parking polling register), so the adapter supplies one on the test side: Publish posts to a wake gate after the word swap and Receive parks on it. The measured publish cost therefore includes one non-blocking channel send that is the adapter's, not the candidate's; conflation is the whole point of the cell.", true),
		att("latest-value-conflating-watch", "Pulse61Deepseek (61-DeepSeek)", "Latest-value cell with version-gated WaitFor wakeups; watchers coalesce arbitrarily many sets into one read. Exactly the conflating-watch envelope.", true),
		att("latest-value-conflating-watch", "WatchCell69Deepseek (69-DeepSeek)", "Latest-value cell with coalesced generation-gated wakeups; this adapter tail-attaches by a subscribe-time epoch anchor and blocks on the cell's condvar-gated Watch path (no polling). Delivery is conflated under load: only the newest pending value is retained.", true),
		att("bounded-ring-lossy-detectable", "TelemetryRing63Qwen (63-Qwen)", "SPSC seqlock telemetry ring; publishers overwrite the oldest entry and readers observe seqlock-validated values with lag detectable via advertised head. Parked readers wake through an armed-waiter count and a relayed token (completion-phase repair: the single shared token used to wake one reader of many). Not attachable to zero-subscriber facets (no published-count accessor); single publisher only, so the concurrent test drivers serialise publishes for this adapter.", true),
		att("bounded-ring-lossy-detectable", "Beacon64GLM (64-GLM)", "Epoch-gated seqlock broadcast ring with self-serve cursors; lapped cursors skip to the oldest retained value and report Skip counts (the scenario's detectable-loss criterion).", true),
		att("bounded-ring-lossy-detectable", "Broadcaster65Deepseek (65-DeepSeek)", "Bounded ring with per-subscription skip accounting (Subscription65Deepseek.Skipped); overwritten generations are counted, never delivered torn.", true),
		att("bounded-ring-lossy-detectable", "Horizon66Gemini (66-Gemini)", "Bounded horizon ring; lapped readers recover per CatchUpPolicy (SkipToOldest attached) with capability status reporting and lag accounting.", true),
		att("bounded-ring-lossy-detectable", "Ring67Haiku (67-Haiku)", "Bounded ring with per-cursor lap detection; Next reports the exact number of overwritten tickets (skipped) with each return.", true),
		att("bounded-ring-lossy-detectable", "Horizon70Meta (70-Meta)", "Bounded ring with per-cursor skip accounting (Lag/Missed) and lossless ordered delivery while the cursor keeps up.", true),

		att("ephemeral-churn-membership", "HorizonRing59Gemini (59-Gemini)", "Stateless cursors; subscription churn never mutates ring state.", true),
		att("ephemeral-churn-membership", "StripedCombiner59Gemini (59-Gemini)", "Subscription is a stateless ring cursor; publish-side combining lanes are unaffected by subscriber churn.", true),
		att("ephemeral-churn-membership", "Latest3260Terra (60-Terra)", "Watchers are stateless; attach/detach is a free read of the atomic word.", true),
		att("ephemeral-churn-membership", "Pulse61Deepseek (61-DeepSeek)", "Watchers register only while parked in WaitFor; churn costs one wait-free slot per active waiter.", true),
		att("ephemeral-churn-membership", "WatchCell69Deepseek (69-DeepSeek)", "Watchers are stateless cursors over an epoch counter; churn never mutates publisher state.", true),
		att("ephemeral-churn-membership", "TelemetryRing63Qwen (63-Qwen)", "Readers replay from retained history independently; no membership registry exists.", true),
		att("ephemeral-churn-membership", "Beacon64GLM (64-GLM)", "Watchers hold private cursors; no registry, no fan-out state.", true),
		att("ephemeral-churn-membership", "Broadcaster65Deepseek (65-DeepSeek)", "Subscriptions are independent cursors; churn does not touch the publish path.", true),
		att("ephemeral-churn-membership", "Horizon66Gemini (66-Gemini)", "Stateless cursors; subscription churn never mutates ring state.", true),
		att("ephemeral-churn-membership", "Ring67Haiku (67-Haiku)", "Cursors are caller-owned values; attach/detach is local to each session.", true),
		att("ephemeral-churn-membership", "Horizon70Meta (70-Meta)", "Cursors are caller-owned with no publisher-side registry.", true),

		att("zero-subscriber-and-edge-lifecycle", "HorizonRing59Gemini (59-Gemini)", "Publish with no cursors is one slot claim plus a fenced write.", true),
		att("zero-subscriber-and-edge-lifecycle", "StripedCombiner59Gemini (59-Gemini)", "Submit with no cursors still completes synchronously through the combining lanes.", true),
		att("zero-subscriber-and-edge-lifecycle", "Latest3260Terra (60-Terra)", "Publish with no watchers is a single atomic word swap.", true),
		att("zero-subscriber-and-edge-lifecycle", "Pulse61Deepseek (61-DeepSeek)", "Publish with no watchers is an atomic store and a no-op broadcast.", true),
		att("zero-subscriber-and-edge-lifecycle", "WatchCell69Deepseek (69-DeepSeek)", "Publish with no watchers is a store plus an O(1) wake broadcast.", true),
		att("zero-subscriber-and-edge-lifecycle", "Beacon64GLM (64-GLM)", "Publish with no watchers arms no gate; one claim CAS and a fenced slot write.", true),
		att("zero-subscriber-and-edge-lifecycle", "Broadcaster65Deepseek (65-DeepSeek)", "Publish with no subscriptions writes the slot and advances head.", true),
		att("zero-subscriber-and-edge-lifecycle", "Horizon66Gemini (66-Gemini)", "Publish with no readers is one sequence claim and a slot write.", true),
		att("zero-subscriber-and-edge-lifecycle", "Ring67Haiku (67-Haiku)", "Publish with no cursors is a striped CAS and a slot store.", true),
		att("zero-subscriber-and-edge-lifecycle", "Horizon70Meta (70-Meta)", "Publish with no cursors is one slot-claim CAS and a fenced write.", true),

		att("cancellation-and-close-lifecycle", "HorizonRing59Gemini (59-Gemini)", "ReadContext returns on ctx cancellation; Close is idempotent and wakes parked readers.", true),
		att("cancellation-and-close-lifecycle", "StripedCombiner59Gemini (59-Gemini)", "ReadContext honours ctx; adapter Close closes the backing ring (Combiner itself has no separate Close).", true),
		att("cancellation-and-close-lifecycle", "Latest3260Terra (60-Terra)", "Receive exits on ctx cancellation; Close flips the closed bit in the same packed word.", true),
		att("cancellation-and-close-lifecycle", "Pulse61Deepseek (61-DeepSeek)", "WaitFor returns on cancellation or close; Close is idempotent.", true),
		att("cancellation-and-close-lifecycle", "WatchCell69Deepseek (69-DeepSeek)", "Watch iterators end on ctx cancellation; Close is idempotent and wakes parked watchers.", true),
		att("cancellation-and-close-lifecycle", "TelemetryRing63Qwen (63-Qwen)", "Receive exits on ctx cancellation; no global Close exists by design (the notifier never closes), so adapter Close is a no-op.", true),
		att("cancellation-and-close-lifecycle", "Beacon64GLM (64-GLM)", "Next honours ctx; Close is idempotent and releases every parked watcher.", true),
		att("cancellation-and-close-lifecycle", "Broadcaster65Deepseek (65-DeepSeek)", "Receive blocks in NextContext (added in the completion phase next to the candidate's Next: the same wait, plus the caller's context); Close is idempotent.", true),
		att("cancellation-and-close-lifecycle", "Horizon66Gemini (66-Gemini)", "ReadNext returns on ctx; Close is idempotent and wakes parked readers.", true),
		att("cancellation-and-close-lifecycle", "Ring67Haiku (67-Haiku)", "Next returns on ctx; Close releases parked cursors.", true),
		att("cancellation-and-close-lifecycle", "Horizon70Meta (70-Meta)", "WaitNextContext returns on ctx; Close wakes every parked waiter; idempotent.", true),

		att("large-fanout-scalability", "HorizonRing59Gemini (59-Gemini)", "Publish is one sequence claim plus a fenced slot write regardless of cursor count.", true),
		att("large-fanout-scalability", "StripedCombiner59Gemini (59-Gemini)", "Publish amortizes combining cost under contention; fan-out cost stays O(1) for the write path.", true),
		att("large-fanout-scalability", "Latest3260Terra (60-Terra)", "Publish is a single atomic word swap regardless of watcher count.", true),
		att("large-fanout-scalability", "Pulse61Deepseek (61-DeepSeek)", "Publish is one atomic store plus an O(1) wake broadcast regardless of waiter count.", true),
		att("large-fanout-scalability", "WatchCell69Deepseek (69-DeepSeek)", "Publish is one store plus an O(1) generation flip regardless of watcher count.", true),
		att("large-fanout-scalability", "Beacon64GLM (64-GLM)", "Publish is one claim CAS plus a fenced write regardless of watcher count; no per-watcher work.", true),
		att("large-fanout-scalability", "Horizon66Gemini (66-Gemini)", "Publish is one sequence claim plus a slot write regardless of reader count.", true),
		att("large-fanout-scalability", "Ring67Haiku (67-Haiku)", "Publish is one striped CAS plus a slot store regardless of cursor count.", true),
		att("large-fanout-scalability", "Horizon70Meta (70-Meta)", "Publish is one slot-claim CAS plus a fenced write regardless of cursor count.", true),
	}
}
