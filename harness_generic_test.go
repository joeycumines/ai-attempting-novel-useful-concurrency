package bigbuff

import (
	"context"
	"iter"
)

// Generic adapters for pubsub-family and caster-family broadcasters, the
// baselines, and the TrySend variants.

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
