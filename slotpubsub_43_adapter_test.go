package bigbuff

import "context"

type slotPubSub43Adapter struct{ x *SlotPubSub43Grok[int] }

func newSlotPubSub43Adapter() Broadcaster         { return &slotPubSub43Adapter{NewSlotPubSub43Grok[int](64)} }
func (a *slotPubSub43Adapter) Name() string       { return "SlotPubSub43Grok (43-Grok)" }
func (a *slotPubSub43Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *slotPubSub43Adapter) Close()             {}
func (a *slotPubSub43Adapter) IsCoalescing() bool { return false }
func (a *slotPubSub43Adapter) Subscribe() BroadcastSubscriber {
	return &slotPubSub43Sub{sub: a.x.Subscribe()}
}
func (a *slotPubSub43Adapter) SkipStandardConcurrentChurn() bool { return true }

type slotPubSub43Sub struct{ sub *SlotSubscription43Grok[int] }

func (s *slotPubSub43Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case v := <-s.sub.C():
		s.sub.Wait()
		return v, true
	}
}
func (s *slotPubSub43Sub) TryReceive() (int, bool) {
	select {
	case v := <-s.sub.C():
		s.sub.Wait()
		return v, true
	default:
		return 0, false
	}
}
func (s *slotPubSub43Sub) Unsubscribe() { s.sub.Unsubscribe() }
