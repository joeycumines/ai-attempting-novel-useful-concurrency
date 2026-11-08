package bigbuff

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Broadcaster is the unified adapter interface for standardizing across
// all broadcast implementations in the repository.
type Broadcaster interface {
	Name() string
	Publish(val int) int
	Subscribe() BroadcastSubscriber
	Close()
	IsCoalescing() bool
}

// BroadcastSubscriber is the unified adapter interface for receivers.
type BroadcastSubscriber interface {
	Receive(ctx context.Context) (val int, ok bool)
	TryReceive() (val int, ok bool)
	Unsubscribe()
}

// AllBroadcasters returns a slice of factory functions that create
// each broadcaster implementation configured for standard testing.
func AllBroadcasters() []func() Broadcaster {
	return []func() Broadcaster{
		func() Broadcaster { return newChanPubSubAdapter() },
		func() Broadcaster { return newChanPubSub57Adapter() },
		func() Broadcaster { return newRingPubSubAdapter() },
		func() Broadcaster { return newPhasePubSubAdapter() },
		func() Broadcaster { return newRelayPubSubAdapter() },
		func() Broadcaster { return newChanRelayAdapter() },
		func() Broadcaster { return newChanLatestAdapter() },
		func() Broadcaster { return newEpochTapeAdapter() },
		func() Broadcaster { return newGenCasterAdapter() },
		func() Broadcaster { return newChanFanoutAdapter() },
		func() Broadcaster { return newEpochCaster13Adapter() },
		func() Broadcaster { return newChanBeaconAdapter() },
		func() Broadcaster { return newChanFeedAdapter() },
		func() Broadcaster { return newChanRotorAdapter() },
		func() Broadcaster { return newChanGathererAdapter() },
		func() Broadcaster { return newChanHubAdapter() },
		func() Broadcaster { return newBaselineFanoutAdapter() },
		func() Broadcaster { return newBeaconAdapter() },
		func() Broadcaster { return newRingBeaconAdapter() },
		func() Broadcaster { return newEpochCasterGeminiAdapter() },
		func() Broadcaster { return newEpochPubSubAdapter() },
		func() Broadcaster { return newReliableBeaconAdapter() },
		pubsubBroadcaster("ChanPubSubV2_06meta"),
		pubsubBroadcaster("ChanPubSub09Gemini"),
		pubsubBroadcaster("ChanPubSub10Gemini"),
		pubsubBroadcaster("ChanPubSub14Xing"),
		pubsubBroadcaster("ChanPubSub16DeepSeek"),
		pubsubBroadcaster("ChanPubSub17Gemini"),
		func() Broadcaster { return newBaselineBarrierAdapter() },
		func() Broadcaster { return newBaselineLatestAdapter() },
		func() Broadcaster { return newBaselineBufferedAdapter() },
		func() Broadcaster { return newChanRelayTrySendAdapter() },
		func() Broadcaster { return newChanHubTrySendAdapter() },
		func() Broadcaster { return newRingPubSubTrySendAdapter() },
		func() Broadcaster { return newCarillon21Adapter() },
		func() Broadcaster { return newChanPubSubPacked22Adapter() },
		func() Broadcaster { return newSeqCaster23Adapter() },
		func() Broadcaster { return newSenseCaster25Adapter() },
		func() Broadcaster { return newChanRing26Adapter() },
		func() Broadcaster { return newMulticast27Adapter() },
		func() Broadcaster { return newBroadcast28Adapter() },
		func() Broadcaster { return newRelay29Adapter() },
		func() Broadcaster { return newArcadia30Adapter() },
		func() Broadcaster { return newVecast31Adapter() },
		func() Broadcaster { return newChanMulticast33Adapter() },
		func() Broadcaster { return newChanMultiCast34Adapter() },
		func() Broadcaster { return newShardCaster36Adapter() },
		func() Broadcaster { return newSelectCaster37Adapter() },
		func() Broadcaster { return newShardCast39Adapter() },
		func() Broadcaster { return newSlabCaster40Adapter() },
		func() Broadcaster { return newEpochCast41Adapter() },
		func() Broadcaster { return newBroadcast42Adapter() },
		func() Broadcaster { return newSlotPubSub43Adapter() },
		func() Broadcaster { return newPhantomCast44Adapter() },
		func() Broadcaster { return newChanHub46Adapter() },
		func() Broadcaster { return newChanPubSub47Adapter() },
		func() Broadcaster { return newChanRing47Adapter() },
		func() Broadcaster { return newWatchCaster48Adapter() },
		func() Broadcaster { return newChanReplay51Adapter() },
		func() Broadcaster { return newBaselineBoundedReplayAdapter() },
		func() Broadcaster { return newChanCascade52Adapter() },
		func() Broadcaster { return newReplayCast53Adapter() },
		func() Broadcaster { return newEpochBroadcast54Adapter() },
		func() Broadcaster { return newRingCaster55Adapter() },
		func() Broadcaster { return newFanout56Adapter() },
		func() Broadcaster { return newSelectBroadcaster49Adapter() },
		func() Broadcaster { return newQueuedFanout24Adapter() },
		func() Broadcaster { return newBaselineQueuedAdapter() },
		func() Broadcaster { return newSelectFanout38Adapter() },
		func() Broadcaster { return newFanout45Adapter() },
		func() Broadcaster { return newFanout45BufferedAdapter() },
		func() Broadcaster { return newRingFanout45Adapter() },
		func() Broadcaster { return newChanPubSub58Adapter() },
		func() Broadcaster { return newGenPubSub50Adapter() },
		func() Broadcaster { return newBroadcaster58Adapter() },
		func() Broadcaster { return newRingFanOut14Adapter() },
		func() Broadcaster { return newBaselineAsyncAdapter() },
	}
}

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

// 1. Baseline ChanPubSub
type chanPubSubAdapter struct {
	ch chan int
	ps *ChanPubSub[chan int, int]
}

func newChanPubSubAdapter() Broadcaster {
	ch := make(chan int)
	return &chanPubSubAdapter{ch: ch, ps: NewChanPubSub(ch)}
}

func (a *chanPubSubAdapter) Name() string        { return "ChanPubSub (Baseline)" }
func (a *chanPubSubAdapter) Publish(val int) int { return a.ps.Send(val) }
func (a *chanPubSubAdapter) IsCoalescing() bool  { return false }
func (a *chanPubSubAdapter) Close()              { close(a.ch) }
func (a *chanPubSubAdapter) Subscribe() BroadcastSubscriber {
	a.ps.Subscribe()
	return &chanPubSubSub{a: a}
}

type chanPubSubSub struct {
	a      *chanPubSubAdapter
	closed bool
}

func (s *chanPubSubSub) Receive(ctx context.Context) (int, bool) {
	if s.closed {
		return 0, false
	}
	select {
	case <-ctx.Done():
		return 0, false
	case val, ok := <-s.a.ch:
		if !ok {
			s.closed = true
			return 0, false
		}
		s.a.ps.Wait()
		return val, true
	}
}

func (s *chanPubSubSub) TryReceive() (int, bool) {
	select {
	case val, ok := <-s.a.ch:
		if !ok {
			s.closed = true
			return 0, false
		}
		s.a.ps.Wait()
		return val, true
	default:
		return 0, false
	}
}

func (s *chanPubSubSub) Unsubscribe() {
	if !s.closed {
		s.closed = true
		s.a.ps.Unsubscribe()
	}
}

// 2. RingPubSub (18-chatgpt)
type ringPubSubAdapter struct {
	hub *RingPubSub[int]
}

func newRingPubSubAdapter() Broadcaster {
	return &ringPubSubAdapter{hub: NewRingPubSub[int](64)}
}

func (a *ringPubSubAdapter) Name() string { return "RingPubSub (18-ChatGPT)" }
func (a *ringPubSubAdapter) Publish(val int) int {
	n, _ := a.hub.Send(context.Background(), val)
	return n
}
func (a *ringPubSubAdapter) IsCoalescing() bool { return false }
func (a *ringPubSubAdapter) Close()             { a.hub.Close() }
func (a *ringPubSubAdapter) Subscribe() BroadcastSubscriber {
	sub := a.hub.Subscribe()
	return &ringPubSubSub{sub: &sub}
}

type ringPubSubSub struct {
	sub *RingSubscription[int]
}

func (s *ringPubSubSub) Receive(ctx context.Context) (int, bool) {
	v, err := s.sub.Recv(ctx)
	if err != nil {
		return 0, false
	}
	return v, true
}
func (s *ringPubSubSub) TryReceive() (int, bool) {
	v, ok := s.sub.TryRecv()
	return v, ok
}
func (s *ringPubSubSub) Unsubscribe() { s.sub.Close() }

// 3. PhasePubSub (19-chatgpt)
type phasePubSubAdapter struct {
	bus *PhasePubSub[int]
}

func newPhasePubSubAdapter() Broadcaster {
	return &phasePubSubAdapter{bus: NewPhasePubSub[int]()}
}

func (a *phasePubSubAdapter) Name() string        { return "PhasePubSub (19-ChatGPT)" }
func (a *phasePubSubAdapter) Publish(val int) int { return a.bus.Send(val) }
func (a *phasePubSubAdapter) IsCoalescing() bool  { return false }
func (a *phasePubSubAdapter) Close()              {}
func (a *phasePubSubAdapter) Subscribe() BroadcastSubscriber {
	sub := a.bus.Subscribe()
	return &phasePubSubSub{sub: sub}
}

type phasePubSubSub struct {
	sub PhaseSubscription[int]
}

func (s *phasePubSubSub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.C():
		return s.sub.Read(), true
	}
}
func (s *phasePubSubSub) TryReceive() (int, bool) {
	select {
	case <-s.sub.C():
		return s.sub.Read(), true
	default:
		return 0, false
	}
}
func (s *phasePubSubSub) Unsubscribe() { s.sub.Unsubscribe() }

// 4. RelayPubSub (19-chatgpt)
type relayPubSubAdapter struct {
	bus *RelayPubSub[int]
}

func newRelayPubSubAdapter() Broadcaster {
	return &relayPubSubAdapter{bus: NewRelayPubSub[int](8)}
}

func (a *relayPubSubAdapter) Name() string        { return "RelayPubSub (19-ChatGPT)" }
func (a *relayPubSubAdapter) Publish(val int) int { return a.bus.Send(val) }
func (a *relayPubSubAdapter) IsCoalescing() bool  { return false }
func (a *relayPubSubAdapter) Close()              {}
func (a *relayPubSubAdapter) Subscribe() BroadcastSubscriber {
	sub := a.bus.Subscribe()
	return &relayPubSubSub{sub: sub}
}

type relayPubSubSub struct {
	sub RelayPubSubSubscription[int]
}

func (s *relayPubSubSub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.C():
		return s.sub.Read(), true
	}
}
func (s *relayPubSubSub) TryReceive() (int, bool) {
	select {
	case <-s.sub.C():
		return s.sub.Read(), true
	default:
		return 0, false
	}
}
func (s *relayPubSubSub) Unsubscribe() { s.sub.Unsubscribe() }

// 5. ChanRelay (20-arena-agent)
type chanRelayAdapter struct {
	relay *ChanRelay[int]
}

func newChanRelayAdapter() Broadcaster {
	return &chanRelayAdapter{relay: NewChanRelay[int]()}
}

func (a *chanRelayAdapter) Name() string { return "ChanRelay (20-Arena)" }
func (a *chanRelayAdapter) Publish(val int) int {
	return a.relay.Send(val)
}
func (a *chanRelayAdapter) IsCoalescing() bool { return false }
func (a *chanRelayAdapter) Close()             { a.relay.Close() }
func (a *chanRelayAdapter) Subscribe() BroadcastSubscriber {
	sub := a.relay.Subscribe()
	return &chanRelaySub{sub: sub}
}

type chanRelaySub struct {
	sub *RelaySubscription[int]
}

func (s *chanRelaySub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.Done():
		return 0, false
	case v := <-s.sub.C():
		return v, true
	}
}
func (s *chanRelaySub) TryReceive() (int, bool) {
	val, ok := s.sub.TryNext()
	return val, ok
}
func (s *chanRelaySub) Unsubscribe() { s.sub.Close() }

// 6. ChanLatest (03-chatgpt)
type chanLatestAdapter struct {
	latest *ChanLatest[int]
}

func newChanLatestAdapter() Broadcaster {
	return &chanLatestAdapter{latest: NewChanLatest[int]()}
}

func (a *chanLatestAdapter) Name() string        { return "ChanLatest (03-ChatGPT)" }
func (a *chanLatestAdapter) Publish(val int) int { return a.latest.Send(val) }
func (a *chanLatestAdapter) IsCoalescing() bool  { return true }
func (a *chanLatestAdapter) Close()              { a.latest.Close() }
func (a *chanLatestAdapter) Subscribe() BroadcastSubscriber {
	sub := a.latest.Subscribe()
	return &chanLatestSub{sub: sub}
}

type chanLatestSub struct {
	sub *ChanLatestSubscription[int]
}

func (s *chanLatestSub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.sub.C():
		return v, ok
	}
}
func (s *chanLatestSub) TryReceive() (int, bool) {
	select {
	case v, ok := <-s.sub.C():
		return v, ok
	default:
		return 0, false
	}
}
func (s *chanLatestSub) Unsubscribe() { s.sub.Unsubscribe() }

// 7. EpochTape (10-gemini)
type epochTapeAdapter struct {
	tape *EpochTape[int]
}

func newEpochTapeAdapter() Broadcaster {
	return &epochTapeAdapter{tape: NewEpochTape[int](64)}
}

func (a *epochTapeAdapter) Name() string { return "EpochTape (10-Gemini)" }
func (a *epochTapeAdapter) Publish(val int) int {
	a.tape.Publish(val)
	return 1
}
func (a *epochTapeAdapter) IsCoalescing() bool { return false }
func (a *epochTapeAdapter) Close()             { a.tape.Close() }
func (a *epochTapeAdapter) Subscribe() BroadcastSubscriber {
	sub := a.tape.Subscribe()
	return &epochTapeSub{sub: sub}
}

type epochTapeSub struct {
	sub EpochSub[int]
}

func (s *epochTapeSub) Receive(ctx context.Context) (int, bool) {
	for {
		ready := s.sub.Ready()
		v, _, closed, ok := s.sub.TryNext()
		if ok {
			return v, true
		}
		if closed {
			return 0, false
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-ready:
		}
	}
}
func (s *epochTapeSub) TryReceive() (int, bool) {
	v, _, _, ok := s.sub.TryNext()
	return v, ok
}
func (s *epochTapeSub) Unsubscribe() {}

// 8. GenCaster (06-meta)
type genCasterAdapter struct {
	caster *GenCaster[int]
}

func newGenCasterAdapter() Broadcaster {
	return &genCasterAdapter{caster: NewGenCaster[int]()}
}

func (a *genCasterAdapter) Name() string { return "GenCaster (06-Meta)" }
func (a *genCasterAdapter) Publish(val int) int {
	a.caster.Send(val)
	return 1
}
func (a *genCasterAdapter) IsCoalescing() bool { return false }
func (a *genCasterAdapter) Close()             {}
func (a *genCasterAdapter) Subscribe() BroadcastSubscriber {
	sub := a.caster.Subscribe()
	return &genCasterSub{sub: sub}
}

type genCasterSub struct {
	sub GenCasterSub[int]
}

func (s *genCasterSub) Receive(ctx context.Context) (int, bool) {
	ch := s.sub.C()
	if val, ok := s.sub.TryNext(); ok {
		return val, true
	}
	select {
	case <-ctx.Done():
		return 0, false
	case <-ch:
		return s.sub.Next()
	}
}
func (s *genCasterSub) TryReceive() (int, bool) {
	return s.sub.TryNext()
}
func (s *genCasterSub) Unsubscribe() {}

// 9. ChanFanout (12-openrouter)
type chanFanoutAdapter struct {
	fanout *ChanFanout[int]
}

func newChanFanoutAdapter() Broadcaster {
	return &chanFanoutAdapter{fanout: NewChanFanout[int](64)}
}

func (a *chanFanoutAdapter) Name() string        { return "ChanFanout (12-OpenRouter)" }
func (a *chanFanoutAdapter) Publish(val int) int { return a.fanout.Send(val) }
func (a *chanFanoutAdapter) IsCoalescing() bool  { return false }
func (a *chanFanoutAdapter) Close()              { a.fanout.Close() }
func (a *chanFanoutAdapter) Subscribe() BroadcastSubscriber {
	sub := a.fanout.Subscribe()
	return &chanFanoutSub{sub: sub}
}

type chanFanoutSub struct {
	sub *FanoutSub[int]
}

func (s *chanFanoutSub) Receive(ctx context.Context) (int, bool) {
	for {
		ready := s.sub.Ready()
		if v, _, ok := s.sub.TryRecv(); ok {
			return v, true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-ready:
		}
	}
}
func (s *chanFanoutSub) TryReceive() (int, bool) {
	v, _, ok := s.sub.TryRecv()
	return v, ok
}
func (s *chanFanoutSub) Unsubscribe() { s.sub.Unsubscribe() }

// 10. EpochCaster13Crouton (13-openrouter)
type epochCaster13Adapter struct {
	caster *EpochCaster13Crouton[int]
}

func newEpochCaster13Adapter() Broadcaster {
	return &epochCaster13Adapter{caster: NewEpochCasterWithRingSize13Crouton[int](64)}
}

func (a *epochCaster13Adapter) Name() string        { return "EpochCaster (13-OpenRouter)" }
func (a *epochCaster13Adapter) Publish(val int) int { return int(a.caster.Send(val)) }
func (a *epochCaster13Adapter) IsCoalescing() bool  { return false }
func (a *epochCaster13Adapter) Close()              { a.caster.Close() }
func (a *epochCaster13Adapter) Subscribe() BroadcastSubscriber {
	sub := a.caster.Subscribe()
	return &epochCaster13Sub{sub: sub}
}

type epochCaster13Sub struct {
	sub *EpochSub13Crouton[int]
}

func (s *epochCaster13Sub) Receive(ctx context.Context) (int, bool) {
	v, err := s.sub.RecvContext(ctx)
	if err != nil {
		return 0, false
	}
	return v, true
}
func (s *epochCaster13Sub) TryReceive() (int, bool) {
	v, ok, _ := s.sub.TryRecv()
	return v, ok
}
func (s *epochCaster13Sub) Unsubscribe() { s.sub.Unsubscribe() }

// 11. ChanBeacon (07-qwen)
type chanBeaconAdapter struct {
	beacon *ChanBeacon[int]
}

func newChanBeaconAdapter() Broadcaster {
	return &chanBeaconAdapter{beacon: NewChanBeacon[int](64)}
}

func (a *chanBeaconAdapter) Name() string        { return "ChanBeacon (07-Qwen)" }
func (a *chanBeaconAdapter) Publish(val int) int { return a.beacon.Send(val) }
func (a *chanBeaconAdapter) IsCoalescing() bool  { return false }
func (a *chanBeaconAdapter) Close()              { a.beacon.Close() }
func (a *chanBeaconAdapter) Subscribe() BroadcastSubscriber {
	sub := a.beacon.Subscribe()
	return &chanBeaconSub{sub: sub}
}

type chanBeaconSub struct {
	sub *RelaySub[int]
}

func (s *chanBeaconSub) Receive(ctx context.Context) (int, bool) {
	for {
		ready := s.sub.Ready()
		v, ok := s.sub.Consume()
		if ok {
			return v, true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-ready:
		}
	}
}
func (s *chanBeaconSub) TryReceive() (int, bool) {
	return s.sub.Consume()
}
func (s *chanBeaconSub) Unsubscribe() {}

// 12. ChanFeed (04-deepseek)
type chanFeedAdapter struct {
	feed *ChanFeed[int]
}

func newChanFeedAdapter() Broadcaster {
	return &chanFeedAdapter{feed: NewChanFeed[int](64, ChanFeedDropOldest)}
}

func (a *chanFeedAdapter) Name() string        { return "ChanFeed (04-DeepSeek)" }
func (a *chanFeedAdapter) Publish(val int) int { return a.feed.Send(val) }
func (a *chanFeedAdapter) IsCoalescing() bool  { return false }
func (a *chanFeedAdapter) Close()              { a.feed.Close() }
func (a *chanFeedAdapter) Subscribe() BroadcastSubscriber {
	sub := a.feed.Subscribe()
	return &chanFeedSub{sub: sub}
}

type chanFeedSub struct {
	sub *ChanFeedSub[int]
}

func (s *chanFeedSub) Receive(ctx context.Context) (int, bool) {
	for {
		ready := s.sub.Ready()
		v, ok := s.sub.Next()
		if ok {
			return v, true
		}
		if s.sub.Closed() {
			return 0, false
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-ready:
		}
	}
}
func (s *chanFeedSub) TryReceive() (int, bool) {
	return s.sub.Next()
}
func (s *chanFeedSub) Unsubscribe() { s.sub.Unsubscribe() }

// 13. ChanRotor (17-gemini)
type chanRotorAdapter struct {
	rotor *ChanRotor[chan int, int]
}

func newChanRotorAdapter() Broadcaster {
	return &chanRotorAdapter{rotor: NewChanRotor[chan int, int](make(chan int), make(chan int))}
}

func (a *chanRotorAdapter) Name() string { return "ChanRotor (17-Gemini)" }
func (a *chanRotorAdapter) Publish(val int) int {
	a.rotor.Send(val)
	return 1
}
func (a *chanRotorAdapter) IsCoalescing() bool { return false }
func (a *chanRotorAdapter) Close()             {}
func (a *chanRotorAdapter) Subscribe() BroadcastSubscriber {
	sub := a.rotor.Subscribe()
	return &chanRotorSub{sub: sub}
}

type chanRotorSub struct {
	sub ChanRotorSub[chan int, int]
}

func (s *chanRotorSub) Receive(ctx context.Context) (int, bool) {
	ch := s.sub.C()
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-ch:
		if !ok {
			return 0, false
		}
		s.sub.Advance()
		return v, true
	}
}
func (s *chanRotorSub) TryReceive() (int, bool) {
	ch := s.sub.C()
	select {
	case v, ok := <-ch:
		if !ok {
			return 0, false
		}
		s.sub.Advance()
		return v, true
	default:
		return 0, false
	}
}
func (s *chanRotorSub) Unsubscribe() { s.sub.Unsubscribe() }

// 14. ChanGathererPubSub (05-qwen)
type chanGathererAdapter struct {
	ping chan int
	pong chan struct{}
	ps   *ChanGathererPubSub[chan int, int, chan struct{}]
}

func newChanGathererAdapter() Broadcaster {
	ping := make(chan int)
	pong := make(chan struct{})
	return &chanGathererAdapter{
		ping: ping,
		pong: pong,
		ps:   NewChanGathererPubSub(ping, pong),
	}
}

func (a *chanGathererAdapter) Name() string        { return "ChanGathererPubSub (05-Qwen)" }
func (a *chanGathererAdapter) Publish(val int) int { return a.ps.Send(val) }
func (a *chanGathererAdapter) IsCoalescing() bool  { return false }
func (a *chanGathererAdapter) Close()              { close(a.ping) }
func (a *chanGathererAdapter) Subscribe() BroadcastSubscriber {
	a.ps.Subscribe()
	return &chanGathererSub{a: a}
}

type chanGathererSub struct {
	a      *chanGathererAdapter
	closed bool
}

func (s *chanGathererSub) Receive(ctx context.Context) (int, bool) {
	if s.closed {
		return 0, false
	}
	select {
	case <-ctx.Done():
		return 0, false
	case val, ok := <-s.a.ping:
		if !ok {
			s.closed = true
			return 0, false
		}
		s.a.ps.Wait()
		return val, true
	}
}
func (s *chanGathererSub) TryReceive() (int, bool) {
	select {
	case val, ok := <-s.a.ping:
		if !ok {
			s.closed = true
			return 0, false
		}
		s.a.ps.Wait()
		return val, true
	default:
		return 0, false
	}
}
func (s *chanGathererSub) Unsubscribe() {
	if !s.closed {
		s.closed = true
		s.a.ps.Unsubscribe()
	}
}

// 15. ChanHub (15-nemotron)
type chanHubAdapter struct {
	hub *ChanHub[int]
}

func newChanHubAdapter() Broadcaster {
	return &chanHubAdapter{hub: NewChanHub[int]()}
}

func (a *chanHubAdapter) Name() string        { return "ChanHub (15-Nemotron)" }
func (a *chanHubAdapter) Publish(val int) int { return a.hub.Send(val) }
func (a *chanHubAdapter) IsCoalescing() bool  { return false }
func (a *chanHubAdapter) Close()              {}
func (a *chanHubAdapter) Subscribe() BroadcastSubscriber {
	ch := make(chan int, 64)
	id := a.hub.Subscribe(ch)
	return &chanHubSub{hub: a.hub, ch: ch, id: id}
}

type chanHubSub struct {
	hub    *ChanHub[int]
	ch     chan int
	id     uint64
	closed bool
}

func (s *chanHubSub) Receive(ctx context.Context) (int, bool) {
	if s.closed {
		return 0, false
	}
	select {
	case <-ctx.Done():
		return 0, false
	case val, ok := <-s.ch:
		if !ok {
			s.closed = true
			return 0, false
		}
		return val, true
	}
}
func (s *chanHubSub) TryReceive() (int, bool) {
	select {
	case val, ok := <-s.ch:
		if !ok {
			s.closed = true
			return 0, false
		}
		return val, true
	default:
		return 0, false
	}
}
func (s *chanHubSub) Unsubscribe() {
	if !s.closed {
		s.closed = true
		s.hub.Unsubscribe(s.id)
	}
}

// --- Standardized Test Suite verifying all Broadcasters under identical conditions ---

func TestStandardHarness_SingleSubscriber(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			sub := b.Subscribe()
			defer sub.Unsubscribe()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			const messages = 5
			for i := 1; i <= messages; i++ {
				go func(val int) {
					b.Publish(val)
				}(i)

				val, ok := sub.Receive(ctx)
				if !ok || val != i {
					t.Fatalf("expected (%d, true), got (%d, %v)", i, val, ok)
				}
			}
		})
	}
}

func TestStandardHarness_NoSubscribers(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			// Publishing with zero subscribers must never block.
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := 1; i <= 100; i++ {
					b.Publish(i)
				}
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("Publish blocked with no subscribers")
			}
		})
	}
}

func TestStandardHarness_MultiSubscriberFanout(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			const numSubs = 8
			const numMsgs = 10

			subs := make([]BroadcastSubscriber, numSubs)
			for i := range subs {
				subs[i] = b.Subscribe()
				defer subs[i].Unsubscribe()
			}

			var wg sync.WaitGroup
			wg.Add(numSubs)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			for i := 0; i < numSubs; i++ {
				go func(s BroadcastSubscriber) {
					defer wg.Done()
					if b.IsCoalescing() {
						for {
							val, ok := s.Receive(ctx)
							if !ok {
								return
							}
							if val == numMsgs {
								return
							}
						}
					} else {
						for m := 1; m <= numMsgs; m++ {
							val, ok := s.Receive(ctx)
							if !ok {
								t.Errorf("expected value, got ok=false")
								return
							}
							if val <= 0 {
								t.Errorf("invalid value %d", val)
								return
							}
						}
					}
				}(subs[i])
			}

			for m := 1; m <= numMsgs; m++ {
				b.Publish(m)
			}

			wg.Wait()
		})
	}
}

func TestStandardHarness_InOrderDelivery(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		if b.IsCoalescing() {
			b.Close()
			continue // skip coalescing broadcasters for strict sequence ordering
		}
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			sub := b.Subscribe()
			defer sub.Unsubscribe()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			const total = 20
			for i := 1; i <= total; i++ {
				go b.Publish(i)
				v, ok := sub.Receive(ctx)
				if !ok {
					t.Fatalf("receive failed at %d", i)
				}
				if v != i {
					t.Fatalf("out of order: expected %d, got %d", i, v)
				}
			}
		})
	}
}

func TestStandardHarness_ConcurrentChurn(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		if limited, ok := b.(interface{ SkipStandardConcurrentChurn() bool }); ok && limited.SkipStandardConcurrentChurn() {
			b.Close()
			continue
		}
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			var stop atomic.Bool
			var wg, subWg sync.WaitGroup

			// Publisher runs until the bounded subscriber churn completes.
			wg.Add(1)
			go func() {
				defer wg.Done()
				m := 1
				for !stop.Load() {
					b.Publish(m)
					m++
				}
			}()

			// Fixed churn cycles per subscriber; no readiness or duration sleep.
			for i := 0; i < 4; i++ {
				subWg.Add(1)
				go func() {
					defer subWg.Done()
					for j := 0; j < 20; j++ {
						func() {
							s := b.Subscribe()
							defer s.Unsubscribe()
							s.Receive(ctx)
						}()
					}
				}()
			}

			subWg.Wait()
			stop.Store(true)
			cancel()
			wg.Wait()
		})
	}
}

func TestStandardHarness_TryReceive(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		t.Run(b.Name(), func(t *testing.T) {
			defer b.Close()
			if limited, ok := b.(interface{ SkipStandardTryReceive() bool }); ok && limited.SkipStandardTryReceive() {
				t.Skip("exposure-limited TryReceive is covered by a dedicated safe facet")
			}
			sub := b.Subscribe()
			defer sub.Unsubscribe()

			// Initially without publication, TryReceive returns false
			val, ok := sub.TryReceive()
			if ok {
				t.Fatalf("expected ok=false when empty, got val=%d", val)
			}

			// A barrier publication may need its receiver to participate in the
			// operation being polled. Completion, not elapsed time, is the bound.
			published := make(chan int, 1)
			go func() { published <- b.Publish(999) }()
			for {
				val, ok = sub.TryReceive()
				if ok {
					if val != 999 {
						t.Fatalf("expected 999, got %d", val)
					}
					break
				}
				// Publication completion is not delivery completion for async
				// adapters. If it finished first, one blocking Receive supplies
				// the deterministic completion boundary (and still checks value).
				select {
				case <-published:
					ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
					val, ok = sub.Receive(ctx)
					cancel()
					if !ok || val != 999 {
						t.Fatalf("post-publish Receive=(%d,%v), want (999,true)", val, ok)
					}
					return
				default:
					runtime.Gosched()
				}
			}
			select {
			case <-published:
			case <-time.After(scenarioGuard):
				t.Fatal("publication did not finish after receipt")
			}
		})
	}
}

// --- Generic adapters for pubsub-family, caster-family and baselines ---

// genericPubSubBroadcaster adapts any pubsubAPI (SubscribeContext-style) to Broadcaster.
type genericPubSubBroadcaster struct {
	name string
	api  pubsubAPI
}

func (a *genericPubSubBroadcaster) Name() string { return a.name }
func (a *genericPubSubBroadcaster) SkipStandardConcurrentChurn() bool {
	return a.name == "ChanPubSubV2_06meta" // manual Wait cannot be interrupted mid-round
}
func (a *genericPubSubBroadcaster) Publish(v int) int  { return a.api.send(v) }
func (a *genericPubSubBroadcaster) IsCoalescing() bool { return !a.api.lossless }
func (a *genericPubSubBroadcaster) Close()             {}
func (a *genericPubSubBroadcaster) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(func(ctx context.Context) iter.Seq[int] { return a.api.subscribe(ctx) })
}

func pubsubBroadcaster(name string) func() Broadcaster {
	return func() Broadcaster {
		mk, ok := pubsubImpls()[name]
		if !ok {
			panic("unknown pubsub impl " + name)
		}
		return &genericPubSubBroadcaster{name: name, api: mk()}
	}
}

// casterBroadcaster adapts a ChanCaster-family value: Subscribe registers one
// pending receive; each successful Receive re-registers for the next round;
// Unsubscribe withdraws the pending registration.
type casterBroadcaster struct {
	name string
	api  casterAPI
}

func (a *casterBroadcaster) Name() string       { return a.name }
func (a *casterBroadcaster) Publish(v int) int  { return a.api.send(v) }
func (a *casterBroadcaster) IsCoalescing() bool { return false }
func (a *casterBroadcaster) Close() {
	if a.api.close != nil {
		a.api.close()
	}
}
func (a *casterBroadcaster) Subscribe() BroadcastSubscriber {
	a.api.add(1)
	return &casterSub{api: a.api}
}

type casterSub struct {
	api  casterAPI
	done bool
}

func (s *casterSub) Receive(ctx context.Context) (int, bool) {
	if s.done {
		return 0, false
	}
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.api.c:
		if !ok {
			return 0, false
		}
		if s.api.ack != nil {
			s.api.ack()
		}
		if s.api.rearmEach {
			s.api.add(1)
		} // ChanCaster intent is one-shot
		return v, true
	}
}
func (s *casterSub) TryReceive() (int, bool) {
	select {
	case v, ok := <-s.api.c:
		if !ok {
			return 0, false
		}
		if s.api.ack != nil {
			s.api.ack()
		}
		if s.api.rearmEach {
			s.api.add(1)
		}
		return v, true
	default:
		return 0, false
	}
}
func (s *casterSub) Unsubscribe() {
	if !s.done {
		s.done = true
		s.api.add(-1)
	}
}

func casterBroadcasterFactory(key string) func() Broadcaster {
	return func() Broadcaster {
		mk, ok := casterImpls()[key]
		if !ok {
			panic("unknown caster impl " + key)
		}
		return &casterBroadcaster{name: "Caster:" + key, api: mk()}
	}
}

// Baseline adapters (staff-engineer-in-a-vacuum baselines from baseline.go).
type baselineBarrierAdapter struct{ x *BaselineBarrierPubSub[int] }

func newBaselineBarrierAdapter() Broadcaster {
	return &baselineBarrierAdapter{x: NewBaselineBarrierPubSub[int]()}
}
func (a *baselineBarrierAdapter) Name() string       { return "BaselineBarrierPubSub (baseline)" }
func (a *baselineBarrierAdapter) Publish(v int) int  { return a.x.Send(v) }
func (a *baselineBarrierAdapter) IsCoalescing() bool { return false }
func (a *baselineBarrierAdapter) Close()             {}
func (a *baselineBarrierAdapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type baselineLatestAdapter struct{ x *BaselineLatestBroadcast[int] }

func newBaselineLatestAdapter() Broadcaster {
	return &baselineLatestAdapter{x: NewBaselineLatestBroadcast[int]()}
}
func (a *baselineLatestAdapter) Name() string       { return "BaselineLatestBroadcast (baseline)" }
func (a *baselineLatestAdapter) Publish(v int) int  { return a.x.Send(v) }
func (a *baselineLatestAdapter) IsCoalescing() bool { return true }
func (a *baselineLatestAdapter) Close()             { a.x.Close() }
func (a *baselineLatestAdapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type baselineBufferedAdapter struct {
	x *BaselineBufferedBroadcast[int]
}

func newBaselineBufferedAdapter() Broadcaster {
	return &baselineBufferedAdapter{x: NewBaselineBufferedBroadcast[int](64)}
}
func (a *baselineBufferedAdapter) Name() string       { return "BaselineBufferedBroadcast (baseline)" }
func (a *baselineBufferedAdapter) Publish(v int) int  { return a.x.Send(v) }
func (a *baselineBufferedAdapter) IsCoalescing() bool { return false }
func (a *baselineBufferedAdapter) Close()             { a.x.Close() }
func (a *baselineBufferedAdapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

// Variant adapters: same implementation, distinct operational mode.
type chanRelayTrySendAdapter struct{ x *ChanRelay[int] }

func newChanRelayTrySendAdapter() Broadcaster {
	return &chanRelayTrySendAdapter{x: NewChanRelay[int](WithRelayWindow(1))}
}
func (a *chanRelayTrySendAdapter) Name() string       { return "ChanRelay TrySend (20-Arena)" }
func (a *chanRelayTrySendAdapter) Publish(v int) int  { return a.x.TrySend(v) }
func (a *chanRelayTrySendAdapter) IsCoalescing() bool { return true }
func (a *chanRelayTrySendAdapter) Close()             { a.x.Close() }
func (a *chanRelayTrySendAdapter) Subscribe() BroadcastSubscriber {
	sub := a.x.Subscribe()
	return &chanRelaySub{sub: sub}
}

type chanHubTrySendAdapter struct{ hub *ChanHub[int] }

func newChanHubTrySendAdapter() Broadcaster {
	return &chanHubTrySendAdapter{hub: NewChanHub[int]()}
}
func (a *chanHubTrySendAdapter) Name() string       { return "ChanHub TrySend (15-Nemotron)" }
func (a *chanHubTrySendAdapter) Publish(v int) int  { return a.hub.TrySend(v) }
func (a *chanHubTrySendAdapter) IsCoalescing() bool { return true }
func (a *chanHubTrySendAdapter) Close()             {}
func (a *chanHubTrySendAdapter) Subscribe() BroadcastSubscriber {
	ch := make(chan int, 64)
	id := a.hub.Subscribe(ch)
	return &chanHubSub{hub: a.hub, ch: ch, id: id}
}

type ringPubSubTrySendAdapter struct{ hub *RingPubSub[int] }

func newRingPubSubTrySendAdapter() Broadcaster {
	return &ringPubSubTrySendAdapter{hub: NewRingPubSub[int](64)}
}
func (a *ringPubSubTrySendAdapter) Name() string      { return "RingPubSub TrySend (18-ChatGPT)" }
func (a *ringPubSubTrySendAdapter) Publish(v int) int { n, _ := a.hub.TrySend(v); return n }
func (a *ringPubSubTrySendAdapter) IsCoalescing() bool {
	return false
}
func (a *ringPubSubTrySendAdapter) Close() { a.hub.Close() }
func (a *ringPubSubTrySendAdapter) Subscribe() BroadcastSubscriber {
	sub := a.hub.Subscribe()
	return &ringPubSubSub{sub: &sub}
}
