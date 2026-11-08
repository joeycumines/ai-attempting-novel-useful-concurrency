package bigbuff

import "context"

type selectCaster37Adapter struct{ x *SelectCaster37Gemini[int] }

func newSelectCaster37Adapter() Broadcaster {
	return &selectCaster37Adapter{NewSelectCaster37Gemini[int]()}
}
func (a *selectCaster37Adapter) Name() string       { return "SelectCaster37Gemini (37-Gemini)" }
func (a *selectCaster37Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *selectCaster37Adapter) Close()             { a.x.Close() }
func (a *selectCaster37Adapter) IsCoalescing() bool { return true }
func (a *selectCaster37Adapter) Subscribe() BroadcastSubscriber {
	return &selectCaster37Sub{sub: a.x.Subscribe()}
}

type selectCaster37Sub struct {
	sub *SelectSubscription37Gemini[int]
}

func (s *selectCaster37Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.Done():
		return 0, false
	case v, ok := <-s.sub.C():
		return v, ok
	}
}
func (s *selectCaster37Sub) TryReceive() (int, bool) {
	select {
	case <-s.sub.Done():
		return 0, false
	case v, ok := <-s.sub.C():
		return v, ok
	default:
		return 0, false
	}
}
func (s *selectCaster37Sub) Unsubscribe() { s.sub.Close() }
