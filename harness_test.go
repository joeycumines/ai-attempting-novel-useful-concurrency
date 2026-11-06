package bigbuff

import (
	"context"
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
	}
}

// --- Adapters for each implementation ---

// 1. Baseline ChanPubSub
type chanPubSubAdapter struct {
	ch chan int
	ps *ChanPubSub[chan int, int]
}

func newChanPubSubAdapter() Broadcaster {
	ch := make(chan int)
	return &chanPubSubAdapter{ch: ch, ps: NewChanPubSub(ch)}
}

func (a *chanPubSubAdapter) Name() string { return "ChanPubSub (Baseline)" }
func (a *chanPubSubAdapter) Publish(val int) int { return a.ps.Send(val) }
func (a *chanPubSubAdapter) IsCoalescing() bool { return false }
func (a *chanPubSubAdapter) Close() { close(a.ch) }
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
func (a *ringPubSubAdapter) Close() { a.hub.Close() }
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

func (a *phasePubSubAdapter) Name() string { return "PhasePubSub (19-ChatGPT)" }
func (a *phasePubSubAdapter) Publish(val int) int { return a.bus.Send(val) }
func (a *phasePubSubAdapter) IsCoalescing() bool { return false }
func (a *phasePubSubAdapter) Close() {}
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

func (a *relayPubSubAdapter) Name() string { return "RelayPubSub (19-ChatGPT)" }
func (a *relayPubSubAdapter) Publish(val int) int { return a.bus.Send(val) }
func (a *relayPubSubAdapter) IsCoalescing() bool { return false }
func (a *relayPubSubAdapter) Close() {}
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
func (a *chanRelayAdapter) Close() { a.relay.Close() }
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

func (a *chanLatestAdapter) Name() string { return "ChanLatest (03-ChatGPT)" }
func (a *chanLatestAdapter) Publish(val int) int { return a.latest.Send(val) }
func (a *chanLatestAdapter) IsCoalescing() bool { return true }
func (a *chanLatestAdapter) Close() { a.latest.Close() }
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
func (a *epochTapeAdapter) Close() { a.tape.Close() }
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
func (a *genCasterAdapter) Close() {}
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

func (a *chanFanoutAdapter) Name() string { return "ChanFanout (12-OpenRouter)" }
func (a *chanFanoutAdapter) Publish(val int) int { return a.fanout.Send(val) }
func (a *chanFanoutAdapter) IsCoalescing() bool { return false }
func (a *chanFanoutAdapter) Close() { a.fanout.Close() }
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

func (a *epochCaster13Adapter) Name() string { return "EpochCaster (13-OpenRouter)" }
func (a *epochCaster13Adapter) Publish(val int) int { return int(a.caster.Send(val)) }
func (a *epochCaster13Adapter) IsCoalescing() bool { return false }
func (a *epochCaster13Adapter) Close() { a.caster.Close() }
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

func (a *chanBeaconAdapter) Name() string { return "ChanBeacon (07-Qwen)" }
func (a *chanBeaconAdapter) Publish(val int) int { return a.beacon.Send(val) }
func (a *chanBeaconAdapter) IsCoalescing() bool { return false }
func (a *chanBeaconAdapter) Close() { a.beacon.Close() }
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

func (a *chanFeedAdapter) Name() string { return "ChanFeed (04-DeepSeek)" }
func (a *chanFeedAdapter) Publish(val int) int { return a.feed.Send(val) }
func (a *chanFeedAdapter) IsCoalescing() bool { return false }
func (a *chanFeedAdapter) Close() { a.feed.Close() }
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
func (a *chanRotorAdapter) Close() {}
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

func (a *chanGathererAdapter) Name() string { return "ChanGathererPubSub (05-Qwen)" }
func (a *chanGathererAdapter) Publish(val int) int { return a.ps.Send(val) }
func (a *chanGathererAdapter) IsCoalescing() bool { return false }
func (a *chanGathererAdapter) Close() { close(a.ping) }
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

func (a *chanHubAdapter) Name() string { return "ChanHub (15-Nemotron)" }
func (a *chanHubAdapter) Publish(val int) int { return a.hub.Send(val) }
func (a *chanHubAdapter) IsCoalescing() bool { return false }
func (a *chanHubAdapter) Close() {}
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

func TestStandardHarness_MultiSubscriberFanout(t *testing.T) {
	for _, factory := range AllBroadcasters() {
		b := factory()
		t.Run(b.Name(), func(t *testing.T) {
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
			continue // skip coalescing broadcasters for strict sequence ordering
		}
		t.Run(b.Name(), func(t *testing.T) {
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
		t.Run(b.Name(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			var stop atomic.Bool
			var wg sync.WaitGroup

			// Publisher goroutine
			wg.Add(1)
			go func() {
				defer wg.Done()
				m := 1
				for !stop.Load() {
					b.Publish(m)
					m++
					time.Sleep(100 * time.Microsecond)
				}
			}()

			// Churning subscriber goroutines
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for !stop.Load() {
						func() {
							s := b.Subscribe()
							defer s.Unsubscribe()
							s.Receive(ctx)
						}()
						time.Sleep(50 * time.Microsecond)
					}
				}()
			}

			time.Sleep(50 * time.Millisecond)
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
			sub := b.Subscribe()
			defer sub.Unsubscribe()

			// Initially without publication, TryReceive returns false
			val, ok := sub.TryReceive()
			if ok {
				t.Fatalf("expected ok=false when empty, got val=%d", val)
			}

			// Background publish
			go b.Publish(999)

			// Poll TryReceive with a short timeout
			deadline := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				val, ok = sub.TryReceive()
				if ok {
					if val != 999 {
						t.Errorf("expected 999, got %d", val)
					}
					return
				}
				time.Sleep(1 * time.Millisecond)
			}
		})
	}
}

