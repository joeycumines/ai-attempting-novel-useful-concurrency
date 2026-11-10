package bigbuff

// Adapters and attachments for the outputs integrated in the completion
// phase: Ring62Haiku (62, previously probe-only), ChanRing71Fable (71) and
// the three "apex" prompt responses PhaseCaster72Gemini /
// SymExchanger72Gemini (72), Cohort73Meta (73) and Fanout74Deepseek (74).
//
// Adapters only map public APIs onto the harness contracts. Where a
// candidate separates receipt from acknowledgement, the Broadcaster adapter
// acknowledges at receipt (the Broadcaster contract has no acknowledgement
// hook); the separate acknowledgement, sender deadlines and mid-flight
// departure are exercised through the deadline-barrier kind (barrierLikes).

import (
	"context"
	"sync"
)

const (
	bRing62Haiku       = "Ring62Haiku (62-Haiku)"
	bChanRing71        = "ChanRing71Fable (71-Fable)"
	bPhaseCaster72     = "PhaseCaster72Gemini (72-Gemini)"
	bPhaseCaster72Ack  = "PhaseCaster72Gemini_phased (72-Gemini)"
	bCohort73          = "Cohort73Meta (73-Meta)"
	bFanout74          = "Fanout74Deepseek (74-DeepSeek)"
	xSymExchanger72    = "SymExchanger72Gemini (72-Gemini)"
	xSymExchanger72a   = "SymExchanger72Gemini_a1 (72-Gemini)"
	xBaselineExchanger = "BaselineExchanger (baseline)"
	dBaselineBarrier   = "BaselineDeadlineBarrier (baseline)"
)

// phaseCaster72Slots is the adapter's subscriber capacity. Capacity is
// fixed at construction by design; 1024 covers the largest fan-out any
// harness profile requests (512) with headroom for churn.
const phaseCaster72Slots = 1024

// --- Ring62Haiku (62-Haiku; single-publisher) ---

type ring62HaikuAdapter struct{ r *Ring62Haiku[int] }

// SinglePublisher declares the candidate's contract (overlapping Publish
// calls panic); see harnessPublish.
func (a *ring62HaikuAdapter) SinglePublisher() bool { return true }

func newRing62HaikuAdapter() Broadcaster {
	return &ring62HaikuAdapter{r: NewRing62Haiku[int](64)}
}
func (a *ring62HaikuAdapter) Name() string { return bRing62Haiku }
func (a *ring62HaikuAdapter) Publish(v int) int {
	if !a.r.Publish(v) {
		return 0
	}
	return 1
}
func (a *ring62HaikuAdapter) Close()             { a.r.Close() }
func (a *ring62HaikuAdapter) IsCoalescing() bool { return true }
func (a *ring62HaikuAdapter) Subscribe() BroadcastSubscriber {
	return &ring62HaikuSub{c: a.r.Subscribe()}
}

type ring62HaikuSub struct{ c *Cursor62Haiku[int] }

func (s *ring62HaikuSub) Receive(ctx context.Context) (int, bool) {
	v, _, err := s.c.Next(ctx)
	return v, err == nil
}
func (s *ring62HaikuSub) TryReceive() (int, bool) {
	v, _, ok := s.c.TryNext()
	return v, ok
}
func (s *ring62HaikuSub) Unsubscribe() {} // cursors are caller-owned values

// --- ChanRing71Fable (71-Fable) ---

type chanRing71Adapter struct{ r *ChanRing71Fable[int] }

func newChanRing71Adapter() Broadcaster         { return &chanRing71Adapter{r: NewChanRing71Fable[int](64)} }
func (a *chanRing71Adapter) Name() string       { return bChanRing71 }
func (a *chanRing71Adapter) Close()             { a.r.Close() }
func (a *chanRing71Adapter) IsCoalescing() bool { return true }
func (a *chanRing71Adapter) Publish(v int) int {
	// Send on a closed ring panics by design (channel semantics). The
	// harness never closes concurrently with a publish, so this check is
	// sufficient for its publish-after-close facets.
	if a.r.Closed() {
		return 0
	}
	a.r.Send(v)
	return 1
}
func (a *chanRing71Adapter) Subscribe() BroadcastSubscriber {
	return &chanRing71Sub{c: a.r.Cursor()}
}

type chanRing71Sub struct{ c ChanRingCursor71Fable[int] }

func (s *chanRing71Sub) Receive(ctx context.Context) (int, bool) {
	v, _, err := s.c.Recv(ctx)
	return v, err == nil
}
func (s *chanRing71Sub) TryReceive() (int, bool) {
	v, _, ok := s.c.TryRecv()
	return v, ok
}
func (s *chanRing71Sub) Unsubscribe() {} // a cursor is a position; it is only armed inside Recv

// --- PhaseCaster72Gemini (72-Gemini) ---

type phaseCaster72Adapter struct {
	c      *PhaseCaster72Gemini[int]
	phased bool
}

func newPhaseCaster72Adapter() Broadcaster {
	return &phaseCaster72Adapter{c: NewPhaseCaster72Gemini[int](phaseCaster72Slots)}
}
func newPhaseCaster72PhasedAdapter() Broadcaster {
	return &phaseCaster72Adapter{c: NewPhaseCaster72Gemini[int](phaseCaster72Slots), phased: true}
}
func (a *phaseCaster72Adapter) Name() string {
	if a.phased {
		return bPhaseCaster72Ack
	}
	return bPhaseCaster72
}
func (a *phaseCaster72Adapter) Publish(v int) int {
	var n int
	if a.phased {
		n, _ = a.c.BroadcastPhased(context.Background(), v)
	} else {
		n, _ = a.c.Broadcast(v)
	}
	return n
}
func (a *phaseCaster72Adapter) Close()             { _ = a.c.Close() }
func (a *phaseCaster72Adapter) IsCoalescing() bool { return false }
func (a *phaseCaster72Adapter) Subscribe() BroadcastSubscriber {
	sub, ch, err := a.c.SubscribeManaged()
	if err != nil {
		// Closed caster or exhausted fixed capacity: the batteries convert
		// this into a recorded skip, tests fail loudly.
		panic("phaseCaster72Adapter: Subscribe: " + err.Error())
	}
	return &phaseCaster72Sub{a: a, sub: sub, ch: ch}
}

type phaseCaster72Sub struct {
	a    *phaseCaster72Adapter
	sub  Subscription72Gemini[int]
	ch   <-chan int
	once sync.Once
}

func (s *phaseCaster72Sub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case v := <-s.ch:
		if s.a.phased {
			s.sub.Ack()
		}
		return v, true
	case <-ctx.Done():
	case <-s.a.c.Done():
	}
	return 0, false
}
func (s *phaseCaster72Sub) TryReceive() (int, bool) {
	// The channel is unbuffered: a value is available only while the
	// broadcaster is parked on this subscriber.
	select {
	case v := <-s.ch:
		if s.a.phased {
			s.sub.Ack()
		}
		return v, true
	default:
		return 0, false
	}
}
func (s *phaseCaster72Sub) Unsubscribe() {
	// The candidate panics (and poisons itself) on a second Unsubscribe;
	// the harness contract requires idempotence.
	s.once.Do(s.sub.Unsubscribe)
}

// --- Cohort73Meta (73-Meta) ---

type cohort73Adapter struct{ c *Cohort73Meta[int] }

func newCohort73Adapter() Broadcaster         { return &cohort73Adapter{c: NewCohort73Meta[int]()} }
func (a *cohort73Adapter) Name() string       { return bCohort73 }
func (a *cohort73Adapter) Close()             { a.c.Close() }
func (a *cohort73Adapter) IsCoalescing() bool { return false }
func (a *cohort73Adapter) Publish(v int) int {
	n, _ := a.c.Broadcast(context.Background(), v)
	return n
}
func (a *cohort73Adapter) Subscribe() BroadcastSubscriber {
	m, ok := a.c.Join()
	if !ok {
		panic("cohort73Adapter: Join on closed cohort")
	}
	return &cohort73Sub{m: m}
}

type cohort73Sub struct{ m *Member73Meta[int] }

func (s *cohort73Sub) Receive(ctx context.Context) (int, bool) {
	v, _, err := s.m.Next(ctx) // Wait, then Ack the epoch
	return v, err == nil
}
func (s *cohort73Sub) TryReceive() (int, bool) {
	v, e, ok := s.m.TryWait()
	if ok {
		s.m.Ack(e)
	}
	return v, ok
}
func (s *cohort73Sub) Unsubscribe() { s.m.Leave() }

// --- Fanout74Deepseek (74-DeepSeek) ---

type fanout74Adapter struct{ f *Fanout74Deepseek[int] }

func newFanout74Adapter() Broadcaster         { return &fanout74Adapter{f: NewFanout74Deepseek[int]()} }
func (a *fanout74Adapter) Name() string       { return bFanout74 }
func (a *fanout74Adapter) Close()             { _ = a.f.Close() }
func (a *fanout74Adapter) IsCoalescing() bool { return false }
func (a *fanout74Adapter) Publish(v int) int {
	n := a.f.Len()
	if err := a.f.Send(context.Background(), v); err != nil {
		return 0
	}
	return n
}
func (a *fanout74Adapter) Subscribe() BroadcastSubscriber {
	s, err := a.f.Subscribe()
	if err != nil {
		panic("fanout74Adapter: Subscribe: " + err.Error())
	}
	return &fanout74Sub{s: s}
}

type fanout74Sub struct{ s *Sub74Deepseek[int] }

func (s *fanout74Sub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	return s.s.Next(ctx)
}
func (s *fanout74Sub) TryReceive() (int, bool) { return s.s.Take() }
func (s *fanout74Sub) Unsubscribe()            { _ = s.s.Close() }

func apexBroadcastAttachments() []Attachment {
	return []Attachment{
		// 62 — Ring62Haiku
		att("bounded-ring-lossy-detectable", bRing62Haiku, "Single-publisher seqlock ring of plain-data values stored as whole 64-byte lines; cursors are caller-owned and every read reports the exact number of positions skipped. Overlapping Publish calls panic by contract; the concurrent test drivers serialise publishes for this adapter (measurements publish from one goroutine and are unaffected).", true),
		att("ephemeral-churn-membership", bRing62Haiku, "Cursors are plain values with no registry: attach/detach costs nothing on the publish path.", true),
		att("zero-subscriber-and-edge-lifecycle", bRing62Haiku, "Publish with no cursors is two stamped stores and a head store; Subscribe tail-attaches so a late cursor sees only later values.", true),
		att("cancellation-and-close-lifecycle", bRing62Haiku, "Next returns on ctx; Close is idempotent, unparks every cursor and leaves committed entries readable.", true),
		att("large-fanout-scalability", bRing62Haiku, "Publish cost is independent of cursor count; wake is skipped entirely when no cursor is parked.", true),

		// 71 — ChanRing
		att("bounded-ring-lossy-detectable", bChanRing71, "Multi-sender ring with a one-word reader/writer pin per slot; a lapped cursor skips forward and the loss is visible as sequence gaps and in Lost(). Parked cursors are woken by a counted token cascade on one shared unbuffered channel: the sender sends one token and the receivers forward the rest as a binary tree.", true),
		att("ephemeral-churn-membership", bChanRing71, "A cursor is a position with no registry: subscribe and unsubscribe write no shared state and allocate nothing.", true),
		att("zero-subscriber-and-edge-lifecycle", bChanRing71, "Send with nothing armed is a mutex round-trip, two slot-lock adds, a tail store and one atomic load; Cursor tail-attaches.", true),
		att("cancellation-and-close-lifecycle", bChanRing71, "Recv disarms on ctx (taking and forwarding its wake token if a round already claimed it); Close wakes every parked cursor and retained values stay readable. Send after Close panics by design; the adapter checks Closed() first.", true),
		att("large-fanout-scalability", bChanRing71, "The sender's wake cost is one channel send regardless of the number of parked cursors; the remaining N-1 sends are performed by the woken subscribers.", true),

		// 72 — PhaseCaster (1-phase Broadcast)
		att("synchronous-handoff-no-barrier", bPhaseCaster72, "Per-subscriber unbuffered channels driven from a slot arena: Broadcast returns after one rendezvous per subscriber (try-send, bounded spin, then park). Fixed subscriber capacity chosen at construction (adapter: 1024 slots).", true),
		att("ephemeral-churn-membership", bPhaseCaster72, "Unsubscribe during an in-flight send hands the slot over through a drain protocol so the broadcaster is never stalled by a departing subscriber; slots are generation-tagged against reuse.", true),
		att("cancellation-and-close-lifecycle", bPhaseCaster72, "Receivers select on the caster's Done channel; Close waits for, and does not interrupt, a broadcast already in flight (by design).", true),
		att("zero-subscriber-and-edge-lifecycle", bPhaseCaster72, "Broadcast with no subscribers is a turnstile round-trip and one coordination CAS.", true),
		att("large-fanout-scalability", bPhaseCaster72, "Delivery is one sequential rendezvous per subscriber: cost grows linearly with fan-out and every subscriber waits for those ahead of it. Attached to document the mismatch.", false),

		// 72 — PhaseCaster (2-phase BroadcastPhased, acknowledged at receipt by the adapter)
		att("lockstep-barrier-lossless", bPhaseCaster72Ack, "BroadcastPhased delivers to every subscriber and then waits for each to Ack or Unsubscribe; the adapter acknowledges immediately after receipt. The separate acknowledgement and the sender deadline are exercised in deadline-bounded-barrier-broadcast.", true),
		att("cancellation-and-close-lifecycle", bPhaseCaster72Ack, "Same lifecycle as the 1-phase mode; Close during the acknowledgement wait makes BroadcastPhased return an error (integration repair).", true),

		// 73 — Cohort
		att("lockstep-barrier-lossless", bCohort73, "Epoch-published value plus an explicit pending-acknowledgement count; Broadcast returns when every member joined at publish time has acked the epoch or left. One barrier channel is allocated per broadcast.", true),
		att("ephemeral-churn-membership", bCohort73, "Join and Leave take the same mutex as Broadcast's publish step; Leave decrements the pending count for an unacked member so churn cannot stall the publisher.", true),
		att("cancellation-and-close-lifecycle", bCohort73, "Wait returns on ctx; Close releases every waiter and an in-flight Broadcast, which reports the closure (integration repair).", true),
		att("zero-subscriber-and-edge-lifecycle", bCohort73, "Broadcast with no members publishes the epoch and returns without allocating a barrier.", true),
		att("large-fanout-scalability", bCohort73, "Publish notifies members with one non-blocking send each under the cohort mutex, and every acknowledgement takes that same mutex: correct at any size, but the barrier serialises all members. Attached to document the mismatch.", false),

		// 74 — Fanout
		att("synchronous-handoff-no-barrier", bFanout74, "Per-subscriber single-slot mailbox with offer and acknowledgement epochs; Send returns once every subscriber has read the value (reading is the acknowledgement) or closed.", true),
		att("ephemeral-churn-membership", bFanout74, "The subscriber list is copy-on-write; a subscriber closing mid-send wakes the waiting sender.", true),
		att("cancellation-and-close-lifecycle", bFanout74, "Next returns on ctx; Close closes every subscriber, releasing an in-flight Send (which then reports success: a closed subscriber counts as departed).", true),
		att("zero-subscriber-and-edge-lifecycle", bFanout74, "Send with no subscribers is a mutex round-trip and an epoch increment.", true),
		att("large-fanout-scalability", bFanout74, "Offers are posted to all subscribers before any acknowledgement is awaited, so subscribers proceed in parallel, but the sender still performs O(N) offers and waits. Attached to document the mismatch.", false),
	}
}

// exchangeAttachments bind exchangeLikes factories to the exchange scenario.
var exchangeAttachments = []Attachment{
	att("symmetric-pairwise-exchange", xSymExchanger72, "Slot arena (default 32 slots): a caller first tries to claim a waiting offer, otherwise publishes its own in a free slot, rescans for a concurrent offer, spins briefly and parks on the slot's wake channel; cancellation rolls the offer back with one CAS or completes the match if it lost the race.", true),
	att("symmetric-pairwise-exchange", xSymExchanger72a, "The same exchanger with a one-slot arena: no elimination spread, every caller contends on one state word. Attached to expose the arena's contribution and the rescan/withdraw paths under maximum collision.", false),
	att("symmetric-pairwise-exchange", xBaselineExchanger, "Vacuum baseline: one mutex, one waiting-offer pointer, a buffered reply channel allocated per parked offer; cancellation withdraws under the lock.", true),
}

// barrierAttachments bind barrierLikes factories to the deadline-bounded
// barrier scenario.
var barrierAttachments = []Attachment{
	att("deadline-bounded-barrier-broadcast", bPhaseCaster72Ack, "BroadcastPhased(ctx): sequential rendezvous delivery, then a wait for every Ack; a deadline during the acknowledgement wait marks each unacknowledged slot as owing one late Ack, which is absorbed. A second consecutive abandoned broadcast to the same unacknowledging subscriber is outside what the marker can represent (recorded limit; the facets acknowledge every received value before receiving again).", true),
	att("deadline-bounded-barrier-broadcast", bCohort73, "Broadcast(ctx): the value is published under an epoch and the publisher waits on a pending-acknowledgement count; a deadline abandons the barrier (the value stays published), acknowledgements carry their epoch so a late one is ignored, and Leave compensates. A member that missed an abandoned value is handed the latest one.", true),
	att("deadline-bounded-barrier-broadcast", bFanout74, "Send(ctx): reading is the acknowledgement, so there is no separate barrier step and no late acknowledgement to tolerate; a deadline leaves the value in each unread mailbox to be overwritten by the next Send.", true),
	att("deadline-bounded-barrier-broadcast", dBaselineBarrier, "Vacuum baseline: per-member unbuffered channel, a fresh one-slot acknowledgement channel per member per send, and a select on the sender's context and the member's departure at every blocking point. Allocates per member per send.", true),
}
