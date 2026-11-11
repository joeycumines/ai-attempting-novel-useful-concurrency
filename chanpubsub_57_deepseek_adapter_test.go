package bigbuff

import (
	"context"
	"sync"
	"sync/atomic"
)

type chanPubSub57Adapter struct {
	x *ChanPubSub57DeepSeek[chan int, int]
	// subsMu lets TryReceive hold the membership still. Subscribe and
	// Unsubscribe share it (read side): the primitive's Subscribe blocks for
	// the length of an in-flight Send, and that Send may be waiting for a
	// subscriber to withdraw, so one membership change must never exclude
	// another. TryReceive takes the write side without waiting.
	subsMu sync.RWMutex
}

func newChanPubSub57Adapter() Broadcaster {
	return &chanPubSub57Adapter{x: NewChanPubSub57DeepSeek[chan int, int](make(chan int))}
}

func (b *chanPubSub57Adapter) Name() string      { return "ChanPubSub57DeepSeek (57-DeepSeek)" }
func (b *chanPubSub57Adapter) Publish(v int) int { return b.x.Send(v) }

// The extracted primitive has no hub-wide Close operation. Unsubscribe is the
// supported lifecycle operation and is exercised by its dedicated facets.
func (b *chanPubSub57Adapter) Close()             {}
func (b *chanPubSub57Adapter) IsCoalescing() bool { return false }

func (b *chanPubSub57Adapter) Subscribe() BroadcastSubscriber {
	b.subsMu.RLock()
	b.x.Subscribe()
	b.subsMu.RUnlock()
	return &chanPubSub57Sub{b: b}
}

type chanPubSub57Sub struct {
	b      *chanPubSub57Adapter
	closed atomic.Bool
}

func (s *chanPubSub57Sub) Receive(ctx context.Context) (int, bool) {
	if s.closed.Load() {
		return 0, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.b.x.C():
		if !ok {
			return 0, false
		}
		s.b.x.Wait()
		return v, true
	}
}

func (s *chanPubSub57Sub) TryReceive() (int, bool) {
	if s.closed.Load() {
		return 0, false
	}
	// A successful read is immediately followed by the primitive's phase
	// acknowledgement. If there are other subscribers, that acknowledgement
	// may block until all of them receive; TryReceive must remain safe in the
	// serialized fuzz/harness path, so it polls only when this is the sole
	// registered receiver. subsMu keeps membership stable through Wait; if a
	// membership change is in progress the poll reports a miss.
	if !s.b.subsMu.TryLock() {
		return 0, false
	}
	defer s.b.subsMu.Unlock()
	if s.closed.Load() || s.b.x.subscribers.Load() != 1 {
		return 0, false
	}
	select {
	case v, ok := <-s.b.x.C():
		if !ok {
			return 0, false
		}
		s.b.x.Wait()
		return v, true
	default:
		return 0, false
	}
}

func (s *chanPubSub57Sub) Unsubscribe() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.b.subsMu.RLock()
	s.b.x.Unsubscribe()
	s.b.subsMu.RUnlock()
}
