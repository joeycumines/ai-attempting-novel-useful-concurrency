package bigbuff

import (
	"context"
	"iter"
	"sync"
)

// Adapters for iterator- and beacon-style broadcasters: the pump helper, the
// latest-value and ring beacons, the epoch casters, and ReliableBeacon.

// --- Adapters for iterator/beacon-style implementations ---
//
// Each adapter registers (captures its read cursor) synchronously inside
// Subscribe, so a Publish issued after Subscribe returns can never be
// missed due to goroutine start-up delay.

// pumpSub delivers values from a background goroutine through a buffered channel.
type pumpSub struct {
	ch     chan int
	cancel context.CancelFunc
	once   sync.Once
	done   chan struct{}
}

func newPumpSub(pump func(ctx context.Context, ch chan<- int)) BroadcastSubscriber {
	ctx, cancel := context.WithCancel(context.Background())
	s := &pumpSub{ch: make(chan int, 16), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.ch)
		defer close(s.done)
		pump(ctx, s.ch)
	}()
	return s
}

func (s *pumpSub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.ch:
		return v, ok
	}
}
func (s *pumpSub) TryReceive() (int, bool) {
	select {
	case v, ok := <-s.ch:
		return v, ok
	default:
		return 0, false
	}
}
func (s *pumpSub) Unsubscribe() {
	s.once.Do(func() { s.cancel(); <-s.done })
}

// pumpSubscribe adapts an iterator-based subscription whose registration
// happens synchronously inside the subscribe call (the seq func is obtained
// before the pump goroutine starts, so no publish can be missed).
func pumpSubscribe(subscribe func(ctx context.Context) iter.Seq[int]) BroadcastSubscriber {
	ctx, cancel := context.WithCancel(context.Background())
	seq := subscribe(ctx) // registers synchronously
	s := &pumpSub{ch: make(chan int, 16), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.ch)
		defer close(s.done)
		for v := range seq {
			select {
			case s.ch <- v:
			case <-ctx.Done():
				return
			}
		}
	}()
	return s
}

// 16. BaselineFanout - standard-practice async fan-out (drop-on-full)
type baselineFanoutAdapter struct{ b *BaselineFanout[int] }

func newBaselineFanoutAdapter() Broadcaster {
	return &baselineFanoutAdapter{b: NewBaselineFanout[int](64)}
}

func (a *baselineFanoutAdapter) Name() string        { return "BaselineFanout (standard)" }
func (a *baselineFanoutAdapter) Publish(val int) int { return a.b.Send(val) }
func (a *baselineFanoutAdapter) IsCoalescing() bool  { return true }
func (a *baselineFanoutAdapter) Close()              { a.b.Close() }
func (a *baselineFanoutAdapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.b.SubscribeContext)
}

// 17. Beacon (01-baseline) - latest-value, coalescing
type beaconAdapter struct{ b *Beacon[int] }

func newBeaconAdapter() Broadcaster { return &beaconAdapter{b: NewBeacon[int]()} }

func (a *beaconAdapter) Name() string        { return "Beacon (01-baseline)" }
func (a *beaconAdapter) Publish(val int) int { a.b.Send(val); return 1 }
func (a *beaconAdapter) IsCoalescing() bool  { return true }
func (a *beaconAdapter) Close()              {}
func (a *beaconAdapter) Subscribe() BroadcastSubscriber {
	next := a.b.Seq() + 1 // captured synchronously
	return newPumpSub(func(ctx context.Context, ch chan<- int) {
		for {
			if v, s := a.b.Load(); s >= next {
				next = s + 1
				select {
				case ch <- v:
				case <-ctx.Done():
					return
				}
				continue
			}
			sig := a.b.Signal()
			// re-check after grabbing sig: Send may have swapped+closed the
			// old channel between Load and Signal
			if _, s := a.b.Load(); s >= next {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-sig:
			}
		}
	})
}

// 17. RingBeacon (08-MuseSpark) - bounded ring, drops on overrun
type ringBeaconAdapter struct{ b *RingBeacon[int] }

func newRingBeaconAdapter() Broadcaster { return &ringBeaconAdapter{b: NewRingBeacon[int](64)} }

func (a *ringBeaconAdapter) Name() string        { return "RingBeacon (08-MuseSpark)" }
func (a *ringBeaconAdapter) Publish(val int) int { a.b.Send(val); return 1 }
func (a *ringBeaconAdapter) IsCoalescing() bool  { return true }
func (a *ringBeaconAdapter) Close()              {}
func (a *ringBeaconAdapter) Subscribe() BroadcastSubscriber {
	cur := a.b.Cursor() // captured synchronously
	return newPumpSub(func(ctx context.Context, ch chan<- int) {
		for {
			if v, _, ok := cur.Next(); ok {
				select {
				case ch <- v:
				case <-ctx.Done():
					return
				}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-cur.Ready():
			}
		}
	})
}

// 18. EpochCasterGemini (09-Gemini) - seqlock ring, drops on overrun
type epochCasterGeminiAdapter struct{ e *EpochCasterGemini[int] }

func newEpochCasterGeminiAdapter() Broadcaster {
	return &epochCasterGeminiAdapter{e: NewEpochCasterGemini[int](64)}
}

func (a *epochCasterGeminiAdapter) Name() string { return "EpochCasterGemini (09-Gemini)" }
func (a *epochCasterGeminiAdapter) Publish(val int) int {
	return a.e.Send(val)
}
func (a *epochCasterGeminiAdapter) IsCoalescing() bool { return true }
func (a *epochCasterGeminiAdapter) Close()             { a.e.Close() }
func (a *epochCasterGeminiAdapter) Subscribe() BroadcastSubscriber {
	sub := a.e.Subscribe() // captures cursor synchronously
	return newPumpSub(func(ctx context.Context, ch chan<- int) {
		defer sub.Disarm()
		for {
			if v, _, ok := sub.TryRecv(); ok {
				select {
				case ch <- v:
				case <-ctx.Done():
					return
				}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-a.e.closed:
				return
			case <-sub.Arm():
				// The pulse registration was consumed by this receive; ack it
				// (mirrors SubscribeContext) so the caster state stays consistent.
				sub.AckPulse()
			}
		}
	})
}

// 19. EpochPubSub (11-meta) - epoch-published latest value with ack barrier
type epochPubSubAdapter struct{ x *EpochPubSub[int] }

func newEpochPubSubAdapter() Broadcaster { return &epochPubSubAdapter{x: NewEpochPubSub[int]()} }

func (a *epochPubSubAdapter) Name() string        { return "EpochPubSub (11-meta)" }
func (a *epochPubSubAdapter) Publish(val int) int { return a.x.Send(val) }
func (a *epochPubSubAdapter) IsCoalescing() bool  { return true }
func (a *epochPubSubAdapter) Close()              {}
func (a *epochPubSubAdapter) Subscribe() BroadcastSubscriber {
	a.x.Subscribe() // registered synchronously
	_, seq, _ := a.x.Current()
	next := seq + 1
	return newPumpSub(func(ctx context.Context, ch chan<- int) {
		defer a.x.Unsubscribe()
		for {
			if v, s, ok := a.x.Current(); ok && s >= next {
				next = s + 1
				a.x.Ack(s)
				select {
				case ch <- v:
				case <-ctx.Done():
					return
				}
				continue
			}
			notify := a.x.Notify()
			if _, s, ok := a.x.Current(); ok && s >= next {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-notify:
			}
		}
	})
}

// 20. ReliableBeacon (08-MuseSpark) - latest value with reliable ack barrier
type reliableBeaconAdapter struct{ x *ReliableBeacon[int] }

func newReliableBeaconAdapter() Broadcaster {
	return &reliableBeaconAdapter{x: NewReliableBeacon[int]()}
}

func (a *reliableBeaconAdapter) Name() string        { return "ReliableBeacon (08-MuseSpark)" }
func (a *reliableBeaconAdapter) Publish(val int) int { return a.x.Send(val) }
func (a *reliableBeaconAdapter) IsCoalescing() bool  { return true }
func (a *reliableBeaconAdapter) Close()              {}
func (a *reliableBeaconAdapter) Subscribe() BroadcastSubscriber {
	a.x.Subscribe() // registered synchronously
	_, seq := a.x.Load()
	next := seq + 1
	return newPumpSub(func(ctx context.Context, ch chan<- int) {
		defer a.x.Unsubscribe()
		for {
			if v, s := a.x.Load(); s >= next {
				next = s + 1
				// Only Wait when a round is actually armed; otherwise the
				// round already completed (e.g. this subscriber was
				// compensated) and Wait would park forever.
				if a.x.pendingLoad() > 0 {
					a.x.Wait()
				}
				select {
				case ch <- v:
				case <-ctx.Done():
					return
				}
				continue
			}
			sig := a.x.C()
			if _, s := a.x.Load(); s >= next {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-sig:
			}
		}
	})
}
