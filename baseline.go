package bigbuff

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// This file holds the standard-practice BASELINE implementations, one per
// major behavioral variant in this repository. Each baseline is the
// simplest sound design that is ordinary "standard practice" - deliberately
// NOT optimised and NOT novel - so the novel implementations can be compared
// apples-to-apples:
//
//   - Lock-step pub-sub (lossless broadcast, Send waits for all receivers):
//     baseline is ChanPubSub (chanpubsub.go) - a mutex/cond barrier plus one
//     unbuffered rendezvous channel shared by all subscribers.
//   - Rendezvous caster (Send hands the value to exactly N receivers):
//     baseline is ChanCaster (chancaster.go) - the reference implementation.
//   - Latest-value coalescing broadcast: baseline is ChanLatest
//     (chanlatest.go) - an atomic slot plus a swapped signal channel.
//   - Async fan-out (Send never blocks on slow subscribers):
//     baseline is BaselineFanout (below) - a mutex-guarded subscriber set of
//     buffered channels with non-blocking drop-oldest delivery.

// BaselineFanout is the standard-practice async broadcast: a mutex-guarded
// set of subscribers, each with its own buffered channel; Send performs a
// non-blocking send to every subscriber, dropping the OLDEST buffered value
// when a subscriber's buffer is full (so the newest value is always kept).
// Sound, simple, and lossy under subscriber lag.
type BaselineFanout[V any] struct {
	mu     sync.RWMutex
	subs   map[uint64]chan V
	nextID atomic.Uint64
	cap    int
	closed atomic.Bool
}

func NewBaselineFanout[V any](capacity int) *BaselineFanout[V] {
	if capacity < 1 {
		capacity = 1
	}
	return &BaselineFanout[V]{subs: make(map[uint64]chan V), cap: capacity}
}

// Send delivers v to every current subscriber without ever blocking on a
// slow one: if a subscriber's buffer is full, its oldest buffered value is
// dropped to make room.
func (b *BaselineFanout[V]) Send(v V) int {
	if b.closed.Load() {
		return 0
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for _, ch := range b.subs {
		select {
		case ch <- v:
		default:
			select {
			case <-ch: // drop oldest
			default:
			}
			select {
			case ch <- v:
			default:
			}
		}
		n++
	}
	return n
}

// Subscribe registers a new buffered channel and returns its id.
func (b *BaselineFanout[V]) Subscribe() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.nextID.Add(1)
	b.subs[id] = make(chan V, b.cap)
	return id
}

// Unsubscribe removes a subscription and closes its channel.
func (b *BaselineFanout[V]) Unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subs[id]; ok {
		delete(b.subs, id)
		close(ch)
	}
}

// C returns the channel for a subscription id.
func (b *BaselineFanout[V]) C(id uint64) <-chan V {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.subs[id]
}

// Close shuts down the broadcaster, closing all subscriber channels.
func (b *BaselineFanout[V]) Close() {
	if b.closed.CompareAndSwap(false, true) {
		b.mu.Lock()
		defer b.mu.Unlock()
		for id, ch := range b.subs {
			delete(b.subs, id)
			close(ch)
		}
	}
}

// SubscribeContext yields values delivered to a fresh subscription until ctx
// is cancelled or the broadcaster is closed.
func (b *BaselineFanout[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	id := b.Subscribe()
	return func(yield func(V) bool) {
		defer b.Unsubscribe(id)
		ch := b.C(id)
		if ch == nil {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-ch:
				if !ok || !yield(v) {
					return
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Additional standard-practice baselines.
//
// Each type below is the textbook, staff-engineer-in-a-vacuum solution to one
// scenario family: the simplest sound design using only mutexes, ordinary
// buffered/unbuffered channels and (where unavoidable) a WaitGroup. None of
// them uses any novel mechanism from this repository. They exist purely as
// comparison points and correctness oracles for their scenario.
// ---------------------------------------------------------------------------

// BaselineRendezvousCaster is the vacuum solution to rendezvous casting: a
// mutex-guarded receiver count plus one unbuffered channel; Send performs
// exactly one rendezvous per registered receiver. A receiver that withdraws
// mid-send performs a compensating dummy send so Send cannot hang.
type BaselineRendezvousCaster[V any] struct {
	C chan V
	// sendMu serialises Send (write) against positive Add (read): a receiver
	// that re-registers after receiving blocks until the current round ends,
	// so it can never take two copies of one Send.
	sendMu         sync.RWMutex
	mu             sync.Mutex
	n              int
	sending        bool
	remain         int
	steals         int
	activeStealers int
}

func NewBaselineRendezvousCaster[V any]() *BaselineRendezvousCaster[V] {
	return &BaselineRendezvousCaster[V]{C: make(chan V)}
}

func (x *BaselineRendezvousCaster[V]) Add(delta int) int {
	if delta > 0 {
		x.sendMu.RLock()
		defer x.sendMu.RUnlock()
	}
	x.mu.Lock()
	x.n += delta
	steal := delta < 0 && x.sending && x.remain > 0
	if steal {
		x.activeStealers++
	}
	n := x.n
	x.mu.Unlock()
	if steal {
		defer func() {
			x.mu.Lock()
			x.activeStealers--
			x.mu.Unlock()
		}()
		// Compensate: this registration will never receive, so consume one
		// of the in-flight Send's rendezvous on its behalf. Retried
		// non-blockingly until it lands or the round ends (a real receiver
		// may have taken the value first); bounded by the round's own
		// progress, not by time.
		for range -delta {
			for {
				x.mu.Lock()
				sending := x.sending
				x.mu.Unlock()
				if !sending {
					break
				}
				select {
				case <-x.C:
					x.mu.Lock()
					x.steals++
					x.mu.Unlock()
					goto stolen
				default:
					runtime.Gosched()
				}
			}
			break
		stolen:
		}
	}
	return n
}

func (x *BaselineRendezvousCaster[V]) Send(v V) int {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	x.mu.Lock()
	x.steals = 0
	n := x.n
	if n == 0 {
		x.mu.Unlock()
		return 0
	}
	x.sending, x.remain = true, n
	x.mu.Unlock()
	for i := 0; i < n; i++ {
		x.C <- v
		x.mu.Lock()
		x.remain--
		x.mu.Unlock()
	}
	x.mu.Lock()
	x.sending = false
	x.mu.Unlock()
	// Let any in-flight steal finish accounting before reading steals: a
	// steal that consumed the final rendezvous increments steals just after
	// its receive returns. Bounded by the stealers' own progress.
	for {
		x.mu.Lock()
		active := x.activeStealers
		x.mu.Unlock()
		if active == 0 {
			break
		}
		runtime.Gosched()
	}
	x.mu.Lock()
	// Registrations are per-round (mirroring the caster contract): the n
	// registrations this Send consumed are retired; any receiver that
	// re-registered after receiving is counted towards the next round.
	x.n -= n
	if x.n < 0 {
		x.n = 0
	}
	ret := n - x.steals // stolen rendezvous were not real receives
	x.mu.Unlock()
	return ret
}

// BaselineBarrierPubSub is the vacuum lock-step pub/sub: a mutex-guarded set
// of per-subscriber channels (buffered by one round, so a Send can always
// deposit its value) plus a per-round WaitGroup that every receiver must Done
// before Send may return. A subscriber that abandons a round without acking
// compensates the WaitGroup when it unsubscribes, so Send can never hang on
// a departed subscriber.
type BaselineBarrierPubSub[V any] struct {
	sendMu   sync.Mutex
	mu       sync.Mutex
	subs     map[uint64]*baselineBarrierSub[V]
	next     uint64
	round    uint64
	wg       *sync.WaitGroup
	inFlight bool
}

type baselineBarrierSub[V any] struct {
	ch         chan V
	ackedRound uint64
}

func NewBaselineBarrierPubSub[V any]() *BaselineBarrierPubSub[V] {
	return &BaselineBarrierPubSub[V]{subs: make(map[uint64]*baselineBarrierSub[V])}
}

func (x *BaselineBarrierPubSub[V]) Send(v V) int {
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	x.mu.Lock()
	subs := make([]*baselineBarrierSub[V], 0, len(x.subs))
	for _, sub := range x.subs {
		subs = append(subs, sub)
	}
	if len(subs) == 0 {
		x.mu.Unlock()
		return 0
	}
	x.round++
	wg := &sync.WaitGroup{}
	wg.Add(len(subs))
	x.wg = wg
	x.inFlight = true
	x.mu.Unlock()
	for _, sub := range subs {
		sub.ch <- v // cap-1 buffer holds the single in-flight round
	}
	wg.Wait()
	x.mu.Lock()
	x.inFlight = false
	x.mu.Unlock()
	return len(subs)
}

// ack acknowledges the current round on behalf of one subscriber.
func (x *BaselineBarrierPubSub[V]) ack(id uint64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	sub, ok := x.subs[id]
	if !ok {
		return
	}
	if x.inFlight && sub.ackedRound < x.round {
		sub.ackedRound = x.round
		x.wg.Done()
	}
}

func (x *BaselineBarrierPubSub[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := &baselineBarrierSub[V]{ch: make(chan V, 1)}
	x.mu.Lock()
	x.next++
	id := x.next
	x.subs[id] = sub
	x.mu.Unlock()
	return func(yield func(V) bool) {
		defer func() {
			x.mu.Lock()
			delete(x.subs, id)
			// Compensate an un-acked in-flight round so a blocked Send
			// is released when this subscriber departs mid-round.
			if x.inFlight && sub.ackedRound < x.round {
				sub.ackedRound = x.round
				x.wg.Done()
			}
			x.mu.Unlock()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-sub.ch:
				x.ack(id)
				if !yield(v) {
					return
				}
			}
		}
	}
}

// BaselineLatestBroadcast is the vacuum latest-value broadcaster: a
// mutex-guarded set of capacity-1 mailboxes; Send overwrites any stale
// pending value so the newest value is always the one observed.
type BaselineLatestBroadcast[V any] struct {
	mu     sync.RWMutex
	subs   map[uint64]chan V
	next   uint64
	closed bool
}

func NewBaselineLatestBroadcast[V any]() *BaselineLatestBroadcast[V] {
	return &BaselineLatestBroadcast[V]{subs: make(map[uint64]chan V)}
}

func (x *BaselineLatestBroadcast[V]) Send(v V) int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	n := 0
	for _, ch := range x.subs {
		select {
		case ch <- v:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- v:
			default:
			}
		}
		n++
	}
	return n
}

func (x *BaselineLatestBroadcast[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	ch := make(chan V, 1)
	x.mu.Lock()
	if x.closed {
		x.mu.Unlock()
		return func(func(V) bool) {}
	}
	x.next++
	id := x.next
	x.subs[id] = ch
	x.mu.Unlock()
	return func(yield func(V) bool) {
		defer func() {
			x.mu.Lock()
			if c, ok := x.subs[id]; ok {
				delete(x.subs, id)
				close(c)
			}
			x.mu.Unlock()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-ch:
				if !ok || !yield(v) {
					return
				}
			}
		}
	}
}

func (x *BaselineLatestBroadcast[V]) Close() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return
	}
	x.closed = true
	for id, ch := range x.subs {
		delete(x.subs, id)
		close(ch)
	}
}

// BaselineBufferedBroadcast is the vacuum buffered lossless broadcaster: a
// buffered channel per subscriber, with deliberate textbook backpressure.
// Membership is snapshotted under RLock and released before blocking sends;
// unsubscribe/close signal per-subscriber done channels instead of closing the
// data channel underneath an in-flight sender.
type baselineBufferedSub[V any] struct {
	ch   chan V
	done chan struct{}
	once sync.Once
}

func (s *baselineBufferedSub[V]) close() { s.once.Do(func() { close(s.done) }) }

type BaselineBufferedBroadcast[V any] struct {
	mu        sync.RWMutex
	subs      map[uint64]*baselineBufferedSub[V]
	next      uint64
	cap       int
	done      chan struct{}
	closeOnce sync.Once
}

func NewBaselineBufferedBroadcast[V any](capacity int) *BaselineBufferedBroadcast[V] {
	if capacity < 1 {
		capacity = 1
	}
	return &BaselineBufferedBroadcast[V]{subs: make(map[uint64]*baselineBufferedSub[V]), cap: capacity, done: make(chan struct{})}
}

func (x *BaselineBufferedBroadcast[V]) Send(v V) int {
	x.mu.RLock()
	select {
	case <-x.done:
		x.mu.RUnlock()
		return 0
	default:
	}
	subs := make([]*baselineBufferedSub[V], 0, len(x.subs))
	for _, sub := range x.subs {
		subs = append(subs, sub)
	}
	x.mu.RUnlock()
	n := 0
	for _, sub := range subs {
		select {
		case <-x.done:
			return n
		case <-sub.done:
		case sub.ch <- v:
			n++
		}
	}
	return n
}

func (x *BaselineBufferedBroadcast[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	sub := &baselineBufferedSub[V]{ch: make(chan V, x.cap), done: make(chan struct{})}
	x.mu.Lock()
	select {
	case <-x.done:
		x.mu.Unlock()
		return func(func(V) bool) {}
	default:
	}
	x.next++
	id := x.next
	x.subs[id] = sub
	x.mu.Unlock()
	return func(yield func(V) bool) {
		defer func() {
			x.mu.Lock()
			delete(x.subs, id)
			x.mu.Unlock()
			sub.close()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-x.done:
				return
			case <-sub.done:
				return
			case v := <-sub.ch:
				if !yield(v) {
					return
				}
			}
		}
	}
}

func (x *BaselineBufferedBroadcast[V]) Close() {
	x.closeOnce.Do(func() { close(x.done) })
	x.mu.Lock()
	defer x.mu.Unlock()
	for id, sub := range x.subs {
		sub.close()
		delete(x.subs, id)
	}
}

// BaselineScatterQueue is the vacuum work-distribution queue: one ordinary
// channel; Send dispatches to exactly whichever receiver is ready, which is
// simply what an unbuffered channel already does.
type BaselineScatterQueue[V any] struct {
	C  chan V
	mu sync.Mutex
	n  int
}

func NewBaselineScatterQueue[V any]() *BaselineScatterQueue[V] {
	return &BaselineScatterQueue[V]{C: make(chan V)}
}

func (x *BaselineScatterQueue[V]) Add(delta int) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.n += delta
	if x.n < 0 {
		x.n = 0
		panic(`bigbuff: baseline scatter: receiver count underflow`)
	}
	return x.n
}

func (x *BaselineScatterQueue[V]) Send(v V) int {
	x.mu.Lock()
	n := x.n
	x.mu.Unlock()
	if n == 0 {
		return 0
	}
	x.C <- v
	return 1
}

// BaselineGatherBarrier is the vacuum N-to-1 signal gatherer: a mutex-guarded
// sender count plus one unbuffered channel; Wait receives exactly once per
// registered sender, with a compensating send for any sender that withdraws.
type BaselineGatherBarrier[V any] struct {
	C       chan V
	mu      sync.Mutex
	n       int
	waiting bool
	remain  int
}

func NewBaselineGatherBarrier[V any]() *BaselineGatherBarrier[V] {
	return &BaselineGatherBarrier[V]{C: make(chan V)}
}

func (x *BaselineGatherBarrier[V]) Add(delta int) int {
	x.mu.Lock()
	if delta < 0 && x.waiting && x.remain > 0 {
		x.n += delta
		x.mu.Unlock()
		var zero V
		for range -delta {
			x.C <- zero
		}
		x.mu.Lock()
		defer x.mu.Unlock()
		return x.n
	}
	x.n += delta
	defer x.mu.Unlock()
	return x.n
}

func (x *BaselineGatherBarrier[V]) Wait() int {
	x.mu.Lock()
	n := x.n
	if n == 0 {
		x.mu.Unlock()
		return 0
	}
	x.waiting, x.remain = true, n
	x.mu.Unlock()
	for i := 0; i < n; i++ {
		<-x.C
		x.mu.Lock()
		x.remain--
		x.mu.Unlock()
	}
	x.mu.Lock()
	x.waiting = false
	x.n -= n // this round's registrations are consumed by the gather
	if x.n < 0 {
		x.n = 0
	}
	x.mu.Unlock()
	return n
}

// Subscribers reports the current number of registered subscriptions.
func (b *BaselineFanout[V]) Subscribers() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}
