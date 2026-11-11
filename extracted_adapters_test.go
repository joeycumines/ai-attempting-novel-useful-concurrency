package bigbuff

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
)

// Adapters for extracted response implementations. These intentionally adapt
// public APIs only; they do not change loss, blocking, or acknowledgement semantics.

type carillon21Adapter struct{ x *Carillon[int] }

func newCarillon21Adapter() Broadcaster         { return &carillon21Adapter{NewCarillon[int](64)} }
func (a *carillon21Adapter) Name() string       { return "Carillon (21-ClaudeHaiku)" }
func (a *carillon21Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *carillon21Adapter) Close()             {}
func (a *carillon21Adapter) IsCoalescing() bool { return false }
func (a *carillon21Adapter) Subscribe() BroadcastSubscriber {
	return &carillon21Sub{sub: a.x.Subscribe()}
}

type carillon21Sub struct{ sub CarillonSubscription[int] }

func (s *carillon21Sub) Receive(ctx context.Context) (int, bool) {
	v, e := s.sub.Recv(ctx)
	return v, e == nil
}
func (s *carillon21Sub) TryReceive() (int, bool) { return s.sub.TryRecv() }
func (s *carillon21Sub) Unsubscribe()            { s.sub.Unsubscribe() }

type chanPubSubPacked22Adapter struct {
	x *ChanPubSubPacked[chan int, int]
}

func newChanPubSubPacked22Adapter() Broadcaster {
	return &chanPubSubPacked22Adapter{NewChanPubSubPacked(make(chan int))}
}
func (a *chanPubSubPacked22Adapter) Name() string       { return "ChanPubSubPacked (22-ChatGPT)" }
func (a *chanPubSubPacked22Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *chanPubSubPacked22Adapter) Close()             {}
func (a *chanPubSubPacked22Adapter) IsCoalescing() bool { return false }
func (a *chanPubSubPacked22Adapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type seqCaster23Adapter struct{ x *SeqCaster23Meta[int] }

func newSeqCaster23Adapter() Broadcaster         { return &seqCaster23Adapter{NewSeqCaster23Meta[int](6, 4)} }
func (a *seqCaster23Adapter) Name() string       { return "SeqCaster23Meta (23-MetaAI)" }
func (a *seqCaster23Adapter) Publish(v int) int  { a.x.Send(v); return 1 }
func (a *seqCaster23Adapter) Close()             { a.x.Close() }
func (a *seqCaster23Adapter) IsCoalescing() bool { return true }
func (a *seqCaster23Adapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(func(ctx context.Context) iter.Seq[int] {
		seq := a.x.SubscribeContext(ctx)
		return func(yield func(int) bool) { seq(yield) }
	})
}

type senseCaster25Adapter struct{ x *SenseCaster[int] }

func newSenseCaster25Adapter() Broadcaster         { return &senseCaster25Adapter{NewSenseCaster[int]()} }
func (a *senseCaster25Adapter) Name() string       { return "SenseCaster (25-ClaudeHaiku)" }
func (a *senseCaster25Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *senseCaster25Adapter) Close()             {}
func (a *senseCaster25Adapter) IsCoalescing() bool { return false }
func (a *senseCaster25Adapter) Subscribe() BroadcastSubscriber {
	return &senseCaster25Sub{sub: a.x.Subscribe()}
}

type senseCaster25Sub struct{ sub SenseSubscription[int] }

func (s *senseCaster25Sub) Receive(ctx context.Context) (int, bool) {
	v, e := s.sub.Recv(ctx)
	return v, e == nil
}
func (s *senseCaster25Sub) TryReceive() (int, bool) { return s.sub.TryRecv() }
func (s *senseCaster25Sub) Unsubscribe()            { s.sub.Close() }

type chanRing26Adapter struct{ x *ChanRing26Kimi[int] }

func newChanRing26Adapter() Broadcaster         { return &chanRing26Adapter{NewChanRing26Kimi[int](64, 64)} }
func (a *chanRing26Adapter) Name() string       { return "ChanRing26Kimi (26-Kimi)" }
func (a *chanRing26Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *chanRing26Adapter) Close()             {}
func (a *chanRing26Adapter) IsCoalescing() bool { return true }
func (a *chanRing26Adapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type multicast27Adapter struct {
	x *Multicast27Mistral[chan int, int]
}

func newMulticast27Adapter() Broadcaster {
	return &multicast27Adapter{NewMulticast27Mistral(make(chan int))}
}
func (a *multicast27Adapter) Name() string       { return "Multicast27Mistral (27-Mistral)" }
func (a *multicast27Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *multicast27Adapter) Close()             { a.x.Close() }
func (a *multicast27Adapter) IsCoalescing() bool { return false }
func (a *multicast27Adapter) Subscribe() BroadcastSubscriber {
	// SubscribeContext registers lazily when its sequence is ranged. The
	// unified harness requires eager registration before Publish, so use the
	// explicit subscription handle here.
	return &multicast27Sub{sub: a.x.Subscribe()}
}

type multicast27Sub struct {
	sub *Multicast27Subscription[chan int, int]
}

func (s *multicast27Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.Done():
		return 0, false
	case v := <-s.sub.C():
		return v, true
	}
}
func (s *multicast27Sub) TryReceive() (int, bool) {
	select {
	case <-s.sub.Done():
		return 0, false
	case v := <-s.sub.C():
		return v, true
	default:
		return 0, false
	}
}
func (s *multicast27Sub) Unsubscribe() { s.sub.Unsubscribe() }

type broadcast28Adapter struct{ x *Broadcast28Flint[int] }

func newBroadcast28Adapter() Broadcaster         { return &broadcast28Adapter{NewBroadcast28Flint[int]()} }
func (a *broadcast28Adapter) Name() string       { return "Broadcast28Flint (28-Flint)" }
func (a *broadcast28Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *broadcast28Adapter) Close()             {}
func (a *broadcast28Adapter) IsCoalescing() bool { return false }
func (a *broadcast28Adapter) Subscribe() BroadcastSubscriber {
	ctx, cancel := context.WithCancel(context.Background())
	s, e := a.x.Subscribe(ctx)
	if e != nil {
		cancel()
		panic(e)
	}
	return &broadcast28Sub{sub: s, cancel: cancel}
}

type broadcast28Sub struct {
	sub    *Subscription28Flint[int]
	cancel context.CancelFunc
}

func (s *broadcast28Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case v := <-s.sub.Ch():
		return v, true
	}
}
func (s *broadcast28Sub) TryReceive() (int, bool) {
	select {
	case v := <-s.sub.Ch():
		return v, true
	default:
		return 0, false
	}
}
func (s *broadcast28Sub) Unsubscribe() { s.cancel(); _ = s.sub.Stop() }

type relay29Adapter struct{ x *Relay29Nemotron[int] }

func newRelay29Adapter() Broadcaster                     { return &relay29Adapter{NewRelay29Nemotron[int](64)} }
func (a *relay29Adapter) Name() string                   { return "Relay29Nemotron (29-Nemotron)" }
func (a *relay29Adapter) Publish(v int) int              { a.x.Publish(v); return 1 }
func (a *relay29Adapter) Close()                         {}
func (a *relay29Adapter) IsCoalescing() bool             { return true }
func (a *relay29Adapter) Subscribe() BroadcastSubscriber { return &relay29Sub{sub: a.x.Subscribe()} }

type relay29Sub struct{ sub *Relay29NemotronLink[int] }

func (s *relay29Sub) Receive(ctx context.Context) (int, bool) { return s.sub.WaitNext(ctx) }
func (s *relay29Sub) TryReceive() (int, bool)                 { return s.sub.Next() }
func (s *relay29Sub) Unsubscribe()                            {}

type arcadia30Adapter struct{ x *Arcadia[int] }

func newArcadia30Adapter() Broadcaster         { return &arcadia30Adapter{NewArcadia[int](64)} }
func (a *arcadia30Adapter) Name() string       { return "Arcadia (30-dots3)" }
func (a *arcadia30Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *arcadia30Adapter) Close()             {}
func (a *arcadia30Adapter) IsCoalescing() bool { return true }
func (a *arcadia30Adapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type vecast31Adapter struct{ x *Vecast[int] }

func newVecast31Adapter() Broadcaster         { return &vecast31Adapter{NewVecast[int]()} }
func (a *vecast31Adapter) Name() string       { return "Vecast (31-Nemotron)" }
func (a *vecast31Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *vecast31Adapter) Close()             { _ = a.x.Close() }
func (a *vecast31Adapter) IsCoalescing() bool { return false }

func (a *vecast31Adapter) Subscribe() BroadcastSubscriber {
	sub := a.x.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	p := &pumpSub{ch: make(chan int, 16), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(p.ch)
		defer close(p.done)
		defer a.x.Unsubscribe(sub)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-sub.C():
				if !ok {
					return
				}
				select {
				case p.ch <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return p
}

type chanMulticast33Adapter struct {
	x         *ChanMulticast33North[chan int, int]
	closeOnce sync.Once
}

func newChanMulticast33Adapter() Broadcaster {
	return &chanMulticast33Adapter{x: NewChanMulticast33North(make(chan int))}
}
func (a *chanMulticast33Adapter) Name() string       { return "ChanMulticast33North (33-North)" }
func (a *chanMulticast33Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *chanMulticast33Adapter) Close()             { a.closeOnce.Do(func() { close(a.x.root) }) }
func (a *chanMulticast33Adapter) IsCoalescing() bool { return false }
func (a *chanMulticast33Adapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type chanMultiCast34Adapter struct {
	x *ChanMultiCast34Lightning[int]
}

func newChanMultiCast34Adapter() Broadcaster {
	return &chanMultiCast34Adapter{NewChanMultiCast34Lightning[int](64)}
}
func (a *chanMultiCast34Adapter) Name() string       { return "ChanMultiCast34Lightning (34-Lightning)" }
func (a *chanMultiCast34Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *chanMultiCast34Adapter) Close()             { a.x.Close() }
func (a *chanMultiCast34Adapter) IsCoalescing() bool { return false }

// SkipStandardConcurrentChurn marks the source's known architectural limit:
// Send holds the reader lock while waiting, whereas membership removal needs
// the writer lock. The generic churn stress intentionally avoids that deadlock
// path; focused tests exercise only the documented bounded exposure.
func (a *chanMultiCast34Adapter) SkipStandardConcurrentChurn() bool { return true }
func (a *chanMultiCast34Adapter) Subscribe() BroadcastSubscriber {
	return &chanMultiCast34Sub{a: a, ch: a.x.Subscribe()}
}

type chanMultiCast34Sub struct {
	a    *chanMultiCast34Adapter
	ch   <-chan int
	done bool
}

func (s *chanMultiCast34Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case v := <-s.ch:
		return v, true
	}
}
func (s *chanMultiCast34Sub) TryReceive() (int, bool) {
	select {
	case v := <-s.ch:
		return v, true
	default:
		return 0, false
	}
}
func (s *chanMultiCast34Sub) Unsubscribe() {
	if !s.done {
		s.done = true
		s.a.x.Unsubscribe()
	}
}

// StableRing32Nano is a competing-consumer queue, not a broadcaster. It is
// intentionally registered with the scatter-family facets rather than this API.

type chanHub46Adapter struct {
	x         *ChanHub46Grok[int]
	closeOnce sync.Once
}

func newChanHub46Adapter() Broadcaster         { return &chanHub46Adapter{x: NewChanHub46Grok[int](64)} }
func (a *chanHub46Adapter) Name() string       { return "ChanHub46Grok (46-Grok)" }
func (a *chanHub46Adapter) Publish(v int) int  { a.x.Publish(v); return 1 }
func (a *chanHub46Adapter) Close()             { a.closeOnce.Do(a.x.Close) }
func (a *chanHub46Adapter) IsCoalescing() bool { return false }
func (a *chanHub46Adapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type chanPubSub47Adapter struct {
	x *ChanPubSub47Gemini[chan int, int]
}

func newChanPubSub47Adapter() Broadcaster {
	return &chanPubSub47Adapter{x: NewChanPubSub47Gemini(make(chan int))}
}
func (a *chanPubSub47Adapter) Name() string       { return "ChanPubSub47Gemini (47-Gemini)" }
func (a *chanPubSub47Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *chanPubSub47Adapter) Close()             {} // This response variant has no hub Close API.
func (a *chanPubSub47Adapter) IsCoalescing() bool { return false }
func (a *chanPubSub47Adapter) Subscribe() BroadcastSubscriber {
	a.x.Subscribe()
	return &chanPubSub47Sub{x: a.x, ch: a.x.C()}
}

type chanPubSub47Sub struct {
	x    *ChanPubSub47Gemini[chan int, int]
	ch   chan int
	once sync.Once
}

func (s *chanPubSub47Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case value, ok := <-s.ch:
		if !ok {
			return 0, false
		}
		s.x.Wait()
		return value, true
	}
}
func (s *chanPubSub47Sub) TryReceive() (int, bool) {
	select {
	case value, ok := <-s.ch:
		if !ok {
			return 0, false
		}
		s.x.Wait()
		return value, true
	default:
		return 0, false
	}
}
func (s *chanPubSub47Sub) Unsubscribe() { s.once.Do(s.x.Unsubscribe) }

type chanRing47Adapter struct{ x *ChanRing47Gemini[int] }

func newChanRing47Adapter() Broadcaster {
	return &chanRing47Adapter{x: NewChanRing47Gemini[int](64)}
}
func (a *chanRing47Adapter) Name() string       { return "ChanRing47Gemini (47-Gemini)" }
func (a *chanRing47Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *chanRing47Adapter) Close()             { a.x.Close() }
func (a *chanRing47Adapter) IsCoalescing() bool { return false }
func (a *chanRing47Adapter) Subscribe() BroadcastSubscriber {
	return &chanRing47Sub{sub: a.x.Subscribe()}
}

type chanRing47Sub struct{ sub RingReader47Gemini[int] }

func (s *chanRing47Sub) Receive(ctx context.Context) (int, bool) {
	for {
		if value, ok := s.sub.TryRecv(); ok {
			return value, true
		}
		if s.sub.Closed() {
			return 0, false
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-s.sub.Ready():
		}
	}
}
func (s *chanRing47Sub) TryReceive() (int, bool) { return s.sub.TryRecv() }
func (s *chanRing47Sub) Unsubscribe()            { s.sub.Close() }

type watchCaster48Adapter struct{ x *WatchCaster48Grok[int] }

func newWatchCaster48Adapter() Broadcaster {
	return &watchCaster48Adapter{x: NewWatchCaster48Grok[int]()}
}
func (a *watchCaster48Adapter) Name() string { return "WatchCaster48Grok (48-Grok)" }
func (a *watchCaster48Adapter) Publish(v int) int {
	a.x.Send(v)
	return 1
}
func (a *watchCaster48Adapter) Close()             {}
func (a *watchCaster48Adapter) IsCoalescing() bool { return true }
func (a *watchCaster48Adapter) Subscribe() BroadcastSubscriber {
	return &watchCaster48Sub{x: a.x, last: a.x.Generation()}
}

type watchCaster48Sub struct {
	x    *WatchCaster48Grok[int]
	last uint64
}

func (s *watchCaster48Sub) Receive(ctx context.Context) (int, bool) {
	for {
		notify := s.x.Notify()
		value, generation := s.x.Load()
		if generation > s.last {
			s.last = generation
			return value, true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-notify:
		}
	}
}
func (s *watchCaster48Sub) TryReceive() (int, bool) {
	value, generation := s.x.Load()
	if generation <= s.last {
		return 0, false
	}
	s.last = generation
	return value, true
}
func (s *watchCaster48Sub) Unsubscribe() {}

type selectBroadcaster49Adapter struct{ x *SelectBroadcaster49Qwen[int] }

func newSelectBroadcaster49Adapter() Broadcaster {
	return &selectBroadcaster49Adapter{x: NewSelectBroadcaster49Qwen[int](64)}
}
func (a *selectBroadcaster49Adapter) Name() string { return "SelectBroadcaster49Qwen (49-Qwen)" }
func (a *selectBroadcaster49Adapter) Publish(v int) int {
	a.x.Send(v)
	return 1
}
func (a *selectBroadcaster49Adapter) Close()             {}
func (a *selectBroadcaster49Adapter) IsCoalescing() bool { return true }
func (a *selectBroadcaster49Adapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type chanReplay51Adapter struct {
	x *ChanReplay51DeepSeek[chan int, int]
}

func newChanReplay51Adapter() Broadcaster {
	return &chanReplay51Adapter{x: NewChanReplay51DeepSeek[chan int, int](make(chan int), 64)}
}
func (a *chanReplay51Adapter) Name() string       { return "ChanReplay51DeepSeek (51-DeepSeek)" }
func (a *chanReplay51Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *chanReplay51Adapter) Close()             {} // No hub Close operation is provided.
func (a *chanReplay51Adapter) IsCoalescing() bool { return false }
func (a *chanReplay51Adapter) Subscribe() BroadcastSubscriber {
	a.x.Subscribe()
	return &chanReplay51Sub{x: a.x, ch: a.x.C()}
}

type chanReplay51Sub struct {
	x    *ChanReplay51DeepSeek[chan int, int]
	ch   chan int
	once sync.Once
}

func (s *chanReplay51Sub) Receive(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case value, ok := <-s.ch:
		if !ok {
			return 0, false
		}
		s.x.Wait()
		return value, true
	}
}
func (s *chanReplay51Sub) TryReceive() (int, bool) {
	select {
	case value, ok := <-s.ch:
		if !ok {
			return 0, false
		}
		s.x.Wait()
		return value, true
	default:
		return 0, false
	}
}
func (s *chanReplay51Sub) Unsubscribe() { s.once.Do(s.x.Unsubscribe) }

type baselineBoundedReplayAdapter struct{ x *BaselineBoundedReplay[int] }

func newBaselineBoundedReplayAdapter() Broadcaster {
	return &baselineBoundedReplayAdapter{x: NewBaselineBoundedReplay[int](64)}
}
func (a *baselineBoundedReplayAdapter) Name() string {
	return "BaselineBoundedReplay (baseline)"
}
func (a *baselineBoundedReplayAdapter) Publish(v int) int  { return a.x.Send(v) }
func (a *baselineBoundedReplayAdapter) Close()             { a.x.Close() }
func (a *baselineBoundedReplayAdapter) IsCoalescing() bool { return false }
func (a *baselineBoundedReplayAdapter) Subscribe() BroadcastSubscriber {
	return &baselineBoundedReplaySubAdapter{sub: a.x.Subscribe()}
}

type baselineBoundedReplaySubAdapter struct {
	sub *baselineBoundedReplaySub[int]
}

func (s *baselineBoundedReplaySubAdapter) Receive(ctx context.Context) (int, bool) {
	return s.sub.Receive(ctx)
}
func (s *baselineBoundedReplaySubAdapter) TryReceive() (int, bool) {
	return s.sub.TryReceive()
}
func (s *baselineBoundedReplaySubAdapter) Unsubscribe() { s.sub.Close() }

func extractedBroadcastAttachments() []Attachment {
	return []Attachment{
		att("lockstep-barrier-lossless", "Carillon (21-ClaudeHaiku)", "Per-subscriber ring cursors; Send waits until each registered subscriber has advanced, providing synchronous lossless fan-out.", true),
		att("lockstep-barrier-lossless", "ChanPubSubPacked (22-ChatGPT)", "Packed-state version of the shared-channel pub/sub protocol with explicit receive acknowledgements.", true),
		att("bounded-ring-lossy-detectable", "SeqCaster23Meta (23-MetaAI)", "Sharded wakeups over a bounded sequence ring; lag is explicitly counted by SeqSub23Meta.Lost.", true),
		att("lockstep-barrier-lossless", "SenseCaster (25-ClaudeHaiku)", "Sense-flip publication waits until every subscriber receives or closes; this is a lossless lockstep fit.", true),
		att("bounded-ring-lossy-detectable", "ChanRing26Kimi (26-Kimi)", "Fixed ring with independent cursors; overwritten values are skipped and detectable through cursor/loss accounting.", true),
		att("synchronous-handoff-no-barrier", "Multicast27Mistral (27-Mistral)", "Intrusive subscriber list and synchronous per-mailbox delivery; send is backpressured but has no processing acknowledgement barrier.", true),
		att("synchronous-handoff-no-barrier", "Broadcast28Flint (28-Flint)", "Lock-free linked subscribers with quit-select retirement; handoff does not wait for post-receive acknowledgements.", true),
		att("bounded-ring-lossy-detectable", "Relay29Nemotron (29-Nemotron)", "Per-link cursors read from a bounded sequence ring; subscribers can fall behind and skip overwritten publications.", true),
		att("bounded-ring-lossy-detectable", "Arcadia (30-dots3)", "Bounded atomic-pointer ring with independent subscriber cursors; late/slow readers may skip overwritten values.", true),
		att("synchronous-handoff-no-barrier", "Vecast (31-Nemotron)", "Sharded subscriber-vector fan-out with per-subscriber channel handoff and a slow-receiver phase; a withdrawing subscriber releases a Send that is waiting for it.", true),
		att("buffered-lossless-backpressured", "ChanMulticast33North (33-North)", "Single distributor relays to private one-value channels: lossless and ordered, one value of slack per subscriber, a stalled subscriber stalls the publisher.", true),
		attX("synchronous-handoff-no-barrier", "ChanMultiCast34Lightning (34-Lightning)", "RWMutex-protected subscriber snapshot and synchronous fan-out.", true, "Exposure limited: exercise only deterministic LIFO Unsubscribe after the steady publish facet. This design has no per-handle unsubscribe identity and no subscriber-channel close; do not exercise arbitrary concurrent removal or lifecycle closure of subscriber channels."),
		att("synchronous-handoff-no-barrier", "ShardCaster36Meta (36-MetaAI)", "Immutable subscriber snapshots keep membership churn off the Send path; each subscriber has an unbuffered rendezvous channel and a teardown signal.", true),
		att("latest-value-conflating-watch", "SelectCaster37Gemini (37-Gemini)", "Per-subscriber selectable buffered channels with sliding overflow preserve the newest state without blocking Send.", true),
		att("latest-value-conflating-watch", "ShardCast39Mistral/latest (39-Mistral)", "Latest policy evicts stale buffered values for each subscriber and keeps Send nonblocking.", true),
		attX("latest-value-conflating-watch", "SlabCaster40Gemini/drop-oldest (40-Gemini)", "DropOldest per-subscriber channels supply a nonblocking latest-oriented broadcast.", true, "Exposure limited: adapter selects only DropOldest and tests never race Close/unsubscribe or slot reuse with an active Send. The source stores a mutable channel pointer without atomic/lease protection, and Block policy can race channel closure; those concurrency/lifecycle paths are structurally unsafe in this version."),
		att("bounded-ring-lossy-detectable", "EpochCast41Sonnet/lossy (41-Sonnet)", "Lossy bounded ring reports overwrite counts and supports independent subscriber cursors; this adapter consumes lag notifications and is covered by direct accounting tests.", true),
		att("bounded-ring-lossy-detectable", "Broadcast42Spark (42-MuseSpark)", "Immutable sequence-stamped ring nodes allow stateless independent cursors; overrun is counted and SendBatch coalesces wakeups.", true),
		attX("lockstep-barrier-lossless", "SlotPubSub43Grok (43-Grok)", "Preallocated page slots plus a shared pong barrier provide acknowledged synchronous fan-out.", true, "Exposure limited: the adapter acknowledges every successful receive before returning, and facets do not cancel/unsubscribe in the middle of a Send round. The source's global acknowledgement count can strand a publisher if a registered receiver exits before its matching Wait."),
		att("bounded-ring-lossy-detectable", "PhantomCast44Qwen (44-Qwen)", "Bounded Disruptor-style ring uses independent subscriber cursors and explicit Missed accounting; each publication coalesces wake signals.", true),
		att("buffered-lossless-backpressured", "ChanHub46Grok (46-Grok)", "Power-of-two sequence ring with independent cursors; Publish blocks at capacity until every attached cursor advances, preserving each value in order.", true),
		att("ephemeral-churn-membership", "ChanHub46Grok (46-Grok)", "Independent cursor attach/detach supports changing subscriber membership while publication continues; each new cursor begins at the current head.", true),
		att("cancellation-and-close-lifecycle", "ChanHub46Grok (46-Grok)", "Context-bound cursor iterators detach on cancellation, and Close wakes receivers while allowing already-published values to drain.", true),
		att("zero-subscriber-and-edge-lifecycle", "ChanHub46Grok (46-Grok)", "With no cursors, publication is immediately available; later cursors start at the current head and observe subsequent values only.", true),
		att("lockstep-barrier-lossless", "ChanPubSub47Gemini (47-Gemini)", "Response 47's atomic-state caster plus pong barrier sends once to each registered receiver and waits for each receive acknowledgement.", true),
		att("ephemeral-churn-membership", "ChanPubSub47Gemini (47-Gemini)", "Context iterators register synchronously and withdraw between publication rounds; Send includes the current subscriber set.", true),
		att("zero-subscriber-and-edge-lifecycle", "ChanPubSub47Gemini (47-Gemini)", "Send returns immediately with no subscribers and later subscriptions participate in later sends.", true),
		attX("cancellation-and-close-lifecycle", "ChanPubSub47Gemini (47-Gemini)", "Context cancellation withdraws an iterator subscription.", false, "The candidate has no hub Close API; lifecycle facets cancel the active iterator first and do not rely on the adapter's no-op Close."),
		att("buffered-lossless-backpressured", "ChanRing47Gemini (47-Gemini)", "Native-select cursor ring pipelines up to its bounded capacity, blocks Send rather than overwriting unread values, and advances each subscriber independently.", true),
		att("ephemeral-churn-membership", "ChanRing47Gemini (47-Gemini)", "Value-held readers support dynamic membership; Close releases unread slot references so a slow cursor does not permanently pin the ring.", true),
		att("cancellation-and-close-lifecycle", "ChanRing47Gemini (47-Gemini)", "Context cancellation closes the reader cursor, while ring Close wakes select-ready readers and stops publishers.", true),
		att("latest-value-conflating-watch", "WatchCaster48Grok (48-Grok)", "Generation-tracked latest value coalesces intermediates; a closed generation channel wakes all current observers and Load returns a coherent snapshot.", true),
		att("ephemeral-churn-membership", "WatchCaster48Grok (48-Grok)", "Observers are per-cursor generation snapshots with no registration in the publisher; churn does not mutate fan-out state.", true),
		att("zero-subscriber-and-edge-lifecycle", "WatchCaster48Grok (48-Grok)", "Send is independent of observer count; each observer begins at the current generation and observes later updates only.", true),
		att("lockstep-barrier-lossless", "ChanReplay51DeepSeek (51-DeepSeek)", "The response-51 extension preserves synchronous receive acknowledgements for live subscribers while retaining a bounded history for late joins.", true),
		att("synchronous-handoff-no-barrier", "ChanCascade52Opus (52-ClaudeOpus)", "Two phase-alternating unbuffered transports carry cooperative relays; Send waits for all receipt/relay handoffs, but not for application processing after Received.", true),
		att("lockstep-barrier-lossless", "EpochBroadcast54Terra (54-GPT-6.1-Terra)", "Every subscriber in the membership snapshot receives each value before Send returns; Close of an outstanding subscription releases its share without counting delivery.", true),
		att("ephemeral-churn-membership", "EpochBroadcast54Terra (54-GPT-6.1-Terra)", "Membership is a locked count rather than a subscriber registry; independent subscription cursors can join or close around publication snapshots.", true),
		att("zero-subscriber-and-edge-lifecycle", "EpochBroadcast54Terra (54-GPT-6.1-Terra)", "Send with no registered subscriptions returns zero without allocating an epoch; later subscriptions participate only in later sends.", true),
		attX("cancellation-and-close-lifecycle", "EpochBroadcast54Terra (54-GPT-6.1-Terra)", "Per-subscription Close and context cancellation release an outstanding receive obligation.", false, "The primitive has no broadcaster-wide Close; lifecycle facets exercise per-subscription cancellation only, and the adapter's Close is intentionally a no-op."),
		att("bounded-ring-lossy-detectable", "RingCaster55Haiku (55-ClaudeHaiku)", "Slab-allocated immutable cells plus per-subscriber cursors retain a bounded window; TryRecv reports every overwritten sequence and SubscribeFrom reports an initial retention gap.", true),
		att("large-fanout-scalability", "RingCaster55Haiku (55-ClaudeHaiku)", "Publish writes one immutable cell and signals a shared armed generation rather than traversing subscribers; parked readers share the wake.", true),
		att("ephemeral-churn-membership", "RingCaster55Haiku (55-ClaudeHaiku)", "Subscriber cursors are caller-owned values and never enter publisher state; attach/detach churn is local to each session.", true),
		att("cancellation-and-close-lifecycle", "RingCaster55Haiku (55-ClaudeHaiku)", "Context-aware Recv and idempotent caster Close wake parked readers, allow retained values to drain, then return ErrRingClosed.", true),
		att("buffered-lossless-backpressured", "Fanout56MaiCode (56-MaiCode)", "Each subscription has an independent bounded channel; publications are lossless in order and a full queue backpressures Send until read or per-subscription Close.", true),
		att("lockstep-barrier-lossless", "Fanout56MaiCode (56-MaiCode)", "The one-element queue can accept a value before the subscriber reads it, so this is explicitly not a receive barrier; attached as a measured mismatch.", false),
		att("large-fanout-scalability", "Fanout56MaiCode (56-MaiCode)", "Correct exact delivery, but Send walks and synchronously enqueues to every cursor; this is a deliberate poor-fit measurement for O(1) high fan-out.", false),
		att("ephemeral-churn-membership", "Fanout56MaiCode (56-MaiCode)", "Membership uses immutable copy-on-write snapshots; churn performs O(N) copy work but never mutates a snapshot being traversed by Send.", true),
		att("zero-subscriber-and-edge-lifecycle", "Fanout56MaiCode (56-MaiCode)", "Send with an empty snapshot returns zero; new subscriptions receive subsequent publications only.", true),
		attX("cancellation-and-close-lifecycle", "Fanout56MaiCode (56-MaiCode)", "Per-subscription Close is idempotent and unblocks an in-flight send.", false, "There is no broadcaster-wide Close. Lifecycle facets cancel readers and close individual subscriptions; the adapter's hub Close is a no-op."),
		att("zero-subscriber-and-edge-lifecycle", "RingCaster55Haiku (55-ClaudeHaiku)", "Publish remains nonblocking with no cursors; a late subscriber starts at the current tail and only sees future publications.", true),
		att("bounded-ring-lossy-detectable", "ReplayCast53Gpt6Sol (53-GPT-6-Sol)", "O(1)-with-fanout sequence ring retains bounded history; each Next reports exact overwritten publications while Publish only replaces one shared generation signal.", true),
		att("large-fanout-scalability", "ReplayCast53Gpt6Sol (53-GPT-6-Sol)", "Publication does not sweep subscriber state; a shared generation notification wakes independently paced cursors, with scheduling work paid by actual awakened readers.", true),
		att("ephemeral-churn-membership", "ReplayCast53Gpt6Sol (53-GPT-6-Sol)", "Subscriptions own independent cursors and Done channels; attach/detach does not mutate publication fan-out work.", true),
		att("cancellation-and-close-lifecycle", "ReplayCast53Gpt6Sol (53-GPT-6-Sol)", "Unsubscribe is idempotent and signals Done; Close wakes selectors, permits retained values to drain, then reports closed.", true),
		att("zero-subscriber-and-edge-lifecycle", "ReplayCast53Gpt6Sol (53-GPT-6-Sol)", "With no active cursors Publish advances sequence without copying; later subscriptions start at the next publication and cannot read stale ring slots.", true),
		att("bounded-catchup-replay", "ChanReplay51DeepSeek (51-DeepSeek)", "Atomic replay-snapshot plus live registration yields the retained suffix without a gap or duplicate; capacity is fixed and older history is intentionally unavailable.", true),
		att("bounded-catchup-replay", "BaselineBoundedReplay (baseline)", "Textbook mutex-protected bounded history plus per-subscriber buffered channels; snapshot and registration share one lock, with send backpressure on slow readers.", true),
		att("buffered-lossless-backpressured", "BaselineBoundedReplay (baseline)", "Mutex-protected per-subscriber bounded queues provide ordinary lossless backpressure; replay history is an additional bounded retention cost.", true),
		att("ephemeral-churn-membership", "ChanReplay51DeepSeek (51-DeepSeek)", "Subscriber registration and removal participate in the replay/send lock protocol; live readers withdraw by compensating the active one-shot send count.", true),
		att("zero-subscriber-and-edge-lifecycle", "ChanReplay51DeepSeek (51-DeepSeek)", "Send retains history while no live subscribers exist; the harness adapter intentionally uses live-only Subscribe, then tests future delivery.", true),
		attX("cancellation-and-close-lifecycle", "ChanReplay51DeepSeek (51-DeepSeek)", "A cancelled live iterator withdraws its receiver intent.", false, "The implementation has no hub Close API; lifecycle facets cancel and unsubscribe first and do not require adapter Close to stop it."),
		att("ephemeral-churn-membership", "BaselineBoundedReplay (baseline)", "The baseline removes per-subscriber channels under a mutex; a done signal releases sends already targeting a removed subscriber.", true),
		att("zero-subscriber-and-edge-lifecycle", "BaselineBoundedReplay (baseline)", "Zero-listener sends append only to bounded history; live-only new subscriptions receive subsequent publications.", true),
		att("cancellation-and-close-lifecycle", "BaselineBoundedReplay (baseline)", "Close and per-subscription cancellation signal blocked receivers and unblock pending sends.", true),
		att("latest-value-conflating-watch", "SelectBroadcaster49Qwen (49-Qwen)", "Closed notification epochs wake all waiters; a sequence-stamped bounded ring lets observers catch up and eventually see the latest value without blocking publishers.", true),
		att("ephemeral-churn-membership", "SelectBroadcaster49Qwen (49-Qwen)", "No central subscriber registry; each iterator keeps an independent sequence cursor and can be cancelled independently.", true),
		att("zero-subscriber-and-edge-lifecycle", "SelectBroadcaster49Qwen (49-Qwen)", "Publication does not require registered observers; new iterators begin at the generation captured when SubscribeContext is called.", true),
		att("bounded-ring-lossy-detectable", "SelectBroadcaster49Qwen (49-Qwen)", "Bounded ring provides ordered catch-up until overrun and silently snaps past overwritten values.", false),
		attX("cancellation-and-close-lifecycle", "SelectBroadcaster49Qwen (49-Qwen)", "Context cancellation stops iterator consumption.", false, "This response has no broadcaster Close API; lifecycle facets cancel subscriptions and never rely on a global close operation."),
		att("zero-subscriber-and-edge-lifecycle", "ChanRing47Gemini (47-Gemini)", "Publish with no active readers does not consume a slot; readers start at the current tail.", true),
		att("lockstep-barrier-lossless", "ChanPubSub57DeepSeek (57-DeepSeek)", "The phase gate lets receivers acknowledge after the complete rendezvous phase; the atomic ack counter and one-slot signal coordinate the sender.", true),
		att("ephemeral-churn-membership", "ChanPubSub57DeepSeek (57-DeepSeek)", "Atomic subscriber accounting plus the existing ChanCaster ping protocol withdraws a subscriber during an active send without stranding its publisher.", true),
		attX("cancellation-and-close-lifecycle", "ChanPubSub57DeepSeek (57-DeepSeek)", "Context iterators withdraw their subscriber count on cancellation.", false, "The primitive has no hub-wide Close API; lifecycle coverage uses context cancellation and Unsubscribe only, never adapter Close as a global shutdown."),
		att("zero-subscriber-and-edge-lifecycle", "ChanPubSub57DeepSeek (57-DeepSeek)", "Send returns immediately at zero subscribers; subscriptions added later participate in subsequent rendezvous rounds.", true),
		att("large-fanout-scalability", "ChanPubSub57DeepSeek (57-DeepSeek)", "Each publication rendezvous and phase-acknowledges all receivers; attached to measure its intentionally costly O(N) synchronization at high fan-out.", false),
	}
}

type chanCascade52Adapter struct {
	x *ChanCascade52Opus[chan int, int]
}

func newChanCascade52Adapter() Broadcaster {
	return &chanCascade52Adapter{x: MakeChanCascade52Opus[int]()}
}
func (a *chanCascade52Adapter) Name() string       { return "ChanCascade52Opus (52-ClaudeOpus)" }
func (a *chanCascade52Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *chanCascade52Adapter) Close()             {}
func (a *chanCascade52Adapter) IsCoalescing() bool { return false }
func (a *chanCascade52Adapter) Subscribe() BroadcastSubscriber {
	return &chanCascade52Sub{r: a.x.Subscribe()}
}

type chanCascade52Sub struct {
	r      ChanCascadeReceiver52Opus[chan int, int]
	closed bool
}

func (s *chanCascade52Sub) Receive(ctx context.Context) (int, bool) {
	if s.closed {
		return 0, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.r.C():
		if !ok {
			s.Unsubscribe()
			return 0, false
		}
		s.r.Received(v)
		return v, true
	}
}
func (s *chanCascade52Sub) TryReceive() (int, bool) {
	if s.closed {
		return 0, false
	}
	select {
	case v, ok := <-s.r.C():
		if !ok {
			s.Unsubscribe()
			return 0, false
		}
		s.r.Received(v)
		return v, true
	default:
		return 0, false
	}
}
func (s *chanCascade52Sub) Unsubscribe() {
	if !s.closed {
		s.closed = true
		s.r.Unsubscribe()
	}
}

type replayCast53Adapter struct{ x *ReplayCast53Gpt6Sol[int] }

func newReplayCast53Adapter() Broadcaster {
	return &replayCast53Adapter{x: NewReplayCast53Gpt6Sol[int](64)}
}
func (a *replayCast53Adapter) Name() string { return "ReplayCast53Gpt6Sol (53-GPT-6-Sol)" }
func (a *replayCast53Adapter) Publish(v int) int {
	if err := a.x.Publish(v); err != nil {
		return 0
	}
	return 1 // accepted publication; this API does not synchronously count readers
}
func (a *replayCast53Adapter) Close()             { a.x.Close() }
func (a *replayCast53Adapter) IsCoalescing() bool { return false }
func (a *replayCast53Adapter) Subscribe() BroadcastSubscriber {
	sub, err := a.x.Subscribe()
	if err != nil {
		return &replayCast53Sub{}
	}
	return &replayCast53Sub{sub: sub}
}

type replayCast53Sub struct {
	sub          *ReplaySubscription53Gpt6Sol[int]
	unsubscribed atomic.Bool
}

func (s *replayCast53Sub) Receive(ctx context.Context) (int, bool) {
	if s.sub == nil || s.unsubscribed.Load() {
		return 0, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		value, _, err := s.sub.Next()
		if err == nil {
			return value, true
		}
		switch {
		case errors.Is(err, ErrReplayEmpty53Gpt6Sol):
			select {
			case <-ctx.Done():
				return 0, false
			case <-s.sub.Done():
				return 0, false
			case <-s.sub.Ready():
			}
		case errors.Is(err, ErrReplayClosed53Gpt6Sol), errors.Is(err, ErrReplayUnsubscribed53Gpt6Sol):
			return 0, false
		default:
			return 0, false
		}
	}
}
func (s *replayCast53Sub) TryReceive() (int, bool) {
	if s.sub == nil || s.unsubscribed.Load() {
		return 0, false
	}
	value, _, err := s.sub.Next()
	return value, err == nil
}
func (s *replayCast53Sub) Unsubscribe() {
	if s.sub != nil && s.unsubscribed.CompareAndSwap(false, true) {
		s.sub.Unsubscribe()
	}
}

type epochBroadcast54Adapter struct{ x *EpochBroadcast54Terra[int] }

func newEpochBroadcast54Adapter() Broadcaster {
	return &epochBroadcast54Adapter{x: NewEpochBroadcast54Terra[int]()}
}
func (a *epochBroadcast54Adapter) Name() string       { return "EpochBroadcast54Terra (54-GPT-6.1-Terra)" }
func (a *epochBroadcast54Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *epochBroadcast54Adapter) Close()             {} // this primitive has no broadcaster-wide Close
func (a *epochBroadcast54Adapter) IsCoalescing() bool { return false }
func (a *epochBroadcast54Adapter) Subscribe() BroadcastSubscriber {
	return &epochBroadcast54Sub{sub: a.x.Subscribe()}
}

type epochBroadcast54Sub struct {
	sub    *EpochSubscription54Terra[int]
	closed bool
}

func (s *epochBroadcast54Sub) Receive(ctx context.Context) (int, bool) {
	if s.closed {
		return 0, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.Ready():
		return s.sub.Next(), true
	}
}
func (s *epochBroadcast54Sub) TryReceive() (int, bool) {
	if s.closed {
		return 0, false
	}
	select {
	case <-s.sub.Ready():
		return s.sub.Next(), true
	default:
		return 0, false
	}
}
func (s *epochBroadcast54Sub) Unsubscribe() {
	if !s.closed {
		s.closed = true
		s.sub.Close()
	}
}

type ringCaster55Adapter struct{ x *RingCaster55Haiku[int] }

func newRingCaster55Adapter() Broadcaster {
	return &ringCaster55Adapter{x: NewRingCaster55Haiku[int](64)}
}
func (a *ringCaster55Adapter) Name() string       { return "RingCaster55Haiku (55-ClaudeHaiku)" }
func (a *ringCaster55Adapter) Publish(v int) int  { a.x.Publish(v); return 1 }
func (a *ringCaster55Adapter) Close()             { a.x.Close() }
func (a *ringCaster55Adapter) IsCoalescing() bool { return false }
func (a *ringCaster55Adapter) Subscribe() BroadcastSubscriber {
	sub := a.x.Subscribe(new(RingSub55Haiku[int]))
	return &ringCaster55Sub{sub: sub}
}

type ringCaster55Sub struct {
	sub    *RingSub55Haiku[int]
	closed bool
}

func (s *ringCaster55Sub) Receive(ctx context.Context) (int, bool) {
	if s.closed || s.sub == nil {
		return 0, false
	}
	v, err := s.sub.Recv(ctx)
	return v, err == nil
}
func (s *ringCaster55Sub) TryReceive() (int, bool) {
	if s.closed || s.sub == nil {
		return 0, false
	}
	return s.sub.TryRecv()
}
func (s *ringCaster55Sub) Unsubscribe() {
	s.closed = true
	s.sub = nil
}

type fanout56Adapter struct{ x *Fanout56MaiCode[int] }

func newFanout56Adapter() Broadcaster {
	return &fanout56Adapter{x: NewFanout56MaiCode[int]()}
}
func (a *fanout56Adapter) Name() string       { return "Fanout56MaiCode (56-MaiCode)" }
func (a *fanout56Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *fanout56Adapter) Close()             {} // response 56 has per-subscription Close only
func (a *fanout56Adapter) IsCoalescing() bool { return false }
func (a *fanout56Adapter) Subscribe() BroadcastSubscriber {
	return &fanout56AdapterSub{sub: a.x.Subscribe()}
}

type fanout56AdapterSub struct{ sub *Subscription56MaiCode[int] }

func (s *fanout56AdapterSub) Receive(ctx context.Context) (int, bool) {
	if s.sub == nil {
		return 0, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return 0, false
	case <-s.sub.Done():
		return 0, false
	case value := <-s.sub.Chan():
		return value, true
	}
}
func (s *fanout56AdapterSub) TryReceive() (int, bool) {
	if s.sub == nil {
		return 0, false
	}
	select {
	case value := <-s.sub.Chan():
		return value, true
	case <-s.sub.Done():
		return 0, false
	default:
		return 0, false
	}
}
func (s *fanout56AdapterSub) Unsubscribe() {
	if s.sub != nil {
		s.sub.Close()
	}
}
