package bigbuff

import (
	"context"
	"sync/atomic"
)

type slabCaster40Adapter struct{ x *SlabCaster40Gemini[int] }

func newSlabCaster40Adapter() Broadcaster {
	return &slabCaster40Adapter{NewSlabCaster40Gemini[int](WithSlabCaster40BufferSize(16), WithSlabCaster40Policy(SlabPolicy40DropOldest))}
}
func (a *slabCaster40Adapter) Name() string       { return "SlabCaster40Gemini/drop-oldest (40-Gemini)" }
func (a *slabCaster40Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *slabCaster40Adapter) Close()             { _ = a.x.Close() }
func (a *slabCaster40Adapter) IsCoalescing() bool { return true }
func (a *slabCaster40Adapter) Subscribe() BroadcastSubscriber {
	sub := a.x.Subscribe()
	return &slabCaster40Sub{sub: sub, ch: sub.C()}
}

// slabCaster40Sub holds the channel it was given at Subscribe. A handle is a
// pointer into the slab that the slot's next subscriber shares, so it is
// closed exactly once here, and its channel is not read again after that.
type slabCaster40Sub struct {
	sub    *SlabSubscription40Gemini[int]
	ch     <-chan int
	closed atomic.Bool
}

func (s *slabCaster40Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.ch:
		return v, ok
	}
}
func (s *slabCaster40Sub) TryReceive() (int, bool) {
	select {
	case v, ok := <-s.ch:
		return v, ok
	default:
		return 0, false
	}
}
func (s *slabCaster40Sub) Unsubscribe() {
	if s.closed.CompareAndSwap(false, true) {
		s.sub.Close()
	}
}
