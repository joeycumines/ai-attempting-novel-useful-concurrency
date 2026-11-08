package bigbuff

// Adapters for implementations whose source existed (or was delivered) before
// the AGENTS.md integration rules. Each adapter preserves the source's loss,
// blocking and lifecycle semantics; scenario attachments state the fit.

import (
	"context"
	"sync"
)

type queuedFanout24Adapter struct{ x *QueuedFanout24Meta[int] }

func newQueuedFanout24Adapter() Broadcaster {
	return &queuedFanout24Adapter{NewQueuedFanout24Meta[int]()}
}
func (a *queuedFanout24Adapter) Name() string       { return "QueuedFanout24Meta (24-MetaAI)" }
func (a *queuedFanout24Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *queuedFanout24Adapter) IsCoalescing() bool { return false }
func (a *queuedFanout24Adapter) Close()             {} // source has per-subscription withdrawal only
func (a *queuedFanout24Adapter) Subscribe() BroadcastSubscriber {
	return &queuedFanout24AdapterSub{a.x.Subscribe()}
}

type queuedFanout24AdapterSub struct {
	s *QueuedFanout24Subscription[int]
}

func (s *queuedFanout24AdapterSub) Receive(ctx context.Context) (int, bool) {
	return s.s.RecvContext(ctx)
}
func (s *queuedFanout24AdapterSub) TryReceive() (int, bool) { return s.s.TryRecv() }
func (s *queuedFanout24AdapterSub) Unsubscribe()            { s.s.Unsubscribe() }

type baselineQueuedAdapter struct{ x *BaselineQueuedFanout[int] }

func newBaselineQueuedAdapter() Broadcaster {
	return &baselineQueuedAdapter{NewBaselineQueuedFanout[int]()}
}
func (a *baselineQueuedAdapter) Name() string       { return "BaselineQueuedFanout (baseline)" }
func (a *baselineQueuedAdapter) Publish(v int) int  { return a.x.Send(v) }
func (a *baselineQueuedAdapter) IsCoalescing() bool { return false }
func (a *baselineQueuedAdapter) Close()             { a.x.Close() }
func (a *baselineQueuedAdapter) Subscribe() BroadcastSubscriber {
	return &baselineQueuedAdapterSub{a.x.Subscribe()}
}

type baselineQueuedAdapterSub struct{ s *baselineQueuedSub[int] }

func (s *baselineQueuedAdapterSub) Receive(ctx context.Context) (int, bool) { return s.s.Receive(ctx) }
func (s *baselineQueuedAdapterSub) TryReceive() (int, bool) {
	return s.s.TryReceive()
}
func (s *baselineQueuedAdapterSub) Unsubscribe() { s.s.Unsubscribe() }

type selectFanout38Adapter struct{ x *SelectFanout[int] }

func newSelectFanout38Adapter() Broadcaster   { return &selectFanout38Adapter{NewSelectFanout[int]()} }
func (a *selectFanout38Adapter) Name() string { return "SelectFanout (38-ChatGPT)" }
func (a *selectFanout38Adapter) Publish(v int) int {
	n, err := a.x.Send(v)
	if err != nil {
		return 0
	}
	return n
}
func (a *selectFanout38Adapter) IsCoalescing() bool { return false }
func (a *selectFanout38Adapter) Close()             { a.x.Close() }
func (a *selectFanout38Adapter) Subscribe() BroadcastSubscriber {
	s, err := a.x.Subscribe()
	if err != nil {
		panic(err)
	}
	return &selectFanout38Sub{s: s}
}

type selectFanout38Sub struct {
	s    *SelectSubscription[int]
	once sync.Once
}

func (s *selectFanout38Sub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.s.C():
		return v, ok
	}
}
func (s *selectFanout38Sub) TryReceive() (int, bool) {
	select {
	case v, ok := <-s.s.C():
		return v, ok
	default:
		return 0, false
	}
}
func (s *selectFanout38Sub) Unsubscribe() { s.once.Do(s.s.Unsubscribe) }

type fanout45Adapter struct {
	x        *Fanout[int]
	capacity int
}

func newFanout45Adapter() Broadcaster { return &fanout45Adapter{x: NewFanout[int](), capacity: 0} }
func newFanout45BufferedAdapter() Broadcaster {
	return &fanout45Adapter{x: NewFanout[int](), capacity: 16}
}
func (a *fanout45Adapter) Name() string {
	if a.capacity == 0 {
		return "Fanout45ChatGPT/sync (45-ChatGPT)"
	}
	return "Fanout45ChatGPT/buffered (45-ChatGPT)"
}
func (a *fanout45Adapter) Publish(v int) int  { return a.x.Broadcast(v) }
func (a *fanout45Adapter) IsCoalescing() bool { return false }
func (a *fanout45Adapter) Close()             { a.x.Close() }
func (a *fanout45Adapter) Subscribe() BroadcastSubscriber {
	s, err := a.x.SubscribeBuffered(a.capacity)
	if err != nil {
		panic(err)
	}
	return &fanout45Sub{s: s}
}

type fanout45Sub struct{ s *FanoutSubscription[int] }

func (s *fanout45Sub) Receive(ctx context.Context) (int, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if v, ok := s.TryReceive(); ok {
		return v, true
	}
	select {
	case <-ctx.Done():
		return 0, false
	case v, ok := <-s.s.C():
		return v, ok
	}
}
func (s *fanout45Sub) TryReceive() (int, bool) {
	select {
	case v, ok := <-s.s.C():
		return v, ok
	default:
		return 0, false
	}
}
func (s *fanout45Sub) Unsubscribe() { s.s.Close() }

type ringFanout45Adapter struct{ x *RingFanout[int] }

func newRingFanout45Adapter() Broadcaster {
	x, err := NewRingFanout[int](64)
	if err != nil {
		panic(err)
	}
	return &ringFanout45Adapter{x}
}
func (a *ringFanout45Adapter) Name() string { return "RingFanout (45-ChatGPT)" }
func (a *ringFanout45Adapter) Publish(v int) int {
	_, err := a.x.Publish(v)
	if err != nil {
		return 0
	}
	return a.x.SubscriberCount()
}
func (a *ringFanout45Adapter) IsCoalescing() bool { return true }
func (a *ringFanout45Adapter) Close()             { a.x.Close() }
func (a *ringFanout45Adapter) Subscribe() BroadcastSubscriber {
	s, err := a.x.Subscribe()
	if err != nil {
		panic(err)
	}
	return &ringFanout45Sub{s: s}
}

type ringFanout45Sub struct{ s *RingFanoutSubscription[int] }

func (s *ringFanout45Sub) Receive(ctx context.Context) (int, bool) {
	v, _, ok, err := s.s.Next(ctx)
	return v, ok && err == nil
}
func (s *ringFanout45Sub) TryReceive() (int, bool) { v, _, ok := s.s.TryNext(); return v, ok }
func (s *ringFanout45Sub) Unsubscribe()            { s.s.Close() }

type broadcaster58Adapter struct{ x *Broadcaster58Spark[int] }

func newBroadcaster58Adapter() Broadcaster {
	return &broadcaster58Adapter{NewBroadcaster58Spark[int](64)}
}
func (a *broadcaster58Adapter) Name() string       { return "Broadcaster58Spark (58-MuseSpark)" }
func (a *broadcaster58Adapter) Publish(v int) int  { a.x.Send(v); return a.x.Subscribers() }
func (a *broadcaster58Adapter) IsCoalescing() bool { return true }
func (a *broadcaster58Adapter) Close()             { a.x.Close() }
func (a *broadcaster58Adapter) Subscribe() BroadcastSubscriber {
	return &broadcaster58Sub{s: a.x.Subscribe()}
}

type broadcaster58Sub struct {
	s      *Broadcaster58Subscription[int]
	closed bool
}

func (s *broadcaster58Sub) Receive(ctx context.Context) (int, bool) {
	if s.closed {
		return 0, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if v, ok, _ := s.s.Next(); ok {
			return v, true
		}
		if s.s.b.IsClosed() {
			return 0, false
		}
		notify := s.s.Notify()
		if v, ok, _ := s.s.Next(); ok {
			return v, true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-s.s.b.Done():
		case <-notify:
		}
	}
}
func (s *broadcaster58Sub) TryReceive() (int, bool) {
	if s.closed {
		return 0, false
	}
	v, ok, _ := s.s.Next()
	return v, ok
}
func (s *broadcaster58Sub) Unsubscribe() {
	if !s.closed {
		s.closed = true
		s.s.Unsubscribe()
	}
}

type chanPubSub58Adapter struct {
	x *ChanPubSub58Spark[chan int, int]
}

func newChanPubSub58Adapter() Broadcaster {
	return &chanPubSub58Adapter{NewChanPubSub58Spark(make(chan int))}
}
func (a *chanPubSub58Adapter) Name() string       { return "ChanPubSub58Spark (58-MuseSpark)" }
func (a *chanPubSub58Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *chanPubSub58Adapter) IsCoalescing() bool { return false }
func (a *chanPubSub58Adapter) Close()             {}
func (a *chanPubSub58Adapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type genPubSub50Adapter struct {
	x *GenPubSub50Meta[chan int, int]
}

func newGenPubSub50Adapter() Broadcaster {
	return &genPubSub50Adapter{NewGenPubSub50Meta(make(chan int))}
}
func (a *genPubSub50Adapter) Name() string       { return "GenPubSub50Meta (50-MetaAI)" }
func (a *genPubSub50Adapter) Publish(v int) int  { return a.x.Send(v) }
func (a *genPubSub50Adapter) IsCoalescing() bool { return false }
func (a *genPubSub50Adapter) Close()             {}
func (a *genPubSub50Adapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

type ringFanOut14Adapter struct {
	x *RingFanOut14Xing[chan int, int]
}

func newRingFanOut14Adapter() Broadcaster {
	return &ringFanOut14Adapter{NewRingFanOut14Xing[chan int, int](64, 64, 16)}
}
func (a *ringFanOut14Adapter) Name() string       { return "RingFanOut14Xing (14-Xing)" }
func (a *ringFanOut14Adapter) Publish(v int) int  { _ = a.x.Send(v); return a.x.Len() }
func (a *ringFanOut14Adapter) IsCoalescing() bool { return true }
func (a *ringFanOut14Adapter) Close()             { a.x.Close() }
func (a *ringFanOut14Adapter) Subscribe() BroadcastSubscriber {
	ch, err := a.x.Add()
	if err != nil {
		panic(err)
	}
	return &ringFanOut14Sub{x: a.x, ch: ch}
}

type ringFanOut14Sub struct {
	x    *RingFanOut14Xing[chan int, int]
	ch   chan int
	once sync.Once
}

func (s *ringFanOut14Sub) Receive(ctx context.Context) (int, bool) {
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
func (s *ringFanOut14Sub) TryReceive() (int, bool) {
	select {
	case v, ok := <-s.ch:
		return v, ok
	default:
		return 0, false
	}
}
func (s *ringFanOut14Sub) Unsubscribe() { s.once.Do(func() { s.x.Remove(s.ch) }) }

type baselineAsyncAdapter struct{ x *BaselineAsyncFanout[int] }

func newBaselineAsyncAdapter() Broadcaster {
	return &baselineAsyncAdapter{NewBaselineAsyncFanout[int](64, 16)}
}
func (a *baselineAsyncAdapter) Name() string       { return "BaselineAsyncFanout (baseline)" }
func (a *baselineAsyncAdapter) Publish(v int) int  { return a.x.Send(v) }
func (a *baselineAsyncAdapter) IsCoalescing() bool { return true }
func (a *baselineAsyncAdapter) Close()             { a.x.Close() }
func (a *baselineAsyncAdapter) Subscribe() BroadcastSubscriber {
	return pumpSubscribe(a.x.SubscribeContext)
}

func gapBroadcastAttachments() []Attachment {
	return []Attachment{
		att("nonblocking-lossless-overflow", "BaselineQueuedFanout (baseline)", "Ordinary mutex plus private unbounded slice queues and a coalescing wake channel; sender never waits for a consumer.", true),
		att("nonblocking-lossless-overflow", "QueuedFanout24Meta (24-MetaAI)", "COW membership, lock-free mailbox fast path and receiver-pumped private overflow preserve every value without sender backpressure; lagging consumers grow memory.", true),
		att("ephemeral-churn-membership", "QueuedFanout24Meta (24-MetaAI)", "Membership edits copy a snapshot rather than locking the publish traversal; per-subscriber leases make withdrawal safe.", true),
		att("zero-subscriber-and-edge-lifecycle", "QueuedFanout24Meta (24-MetaAI)", "An empty snapshot accepts no delivery and returns zero; later subscriptions see future publications.", true),
		attX("cancellation-and-close-lifecycle", "QueuedFanout24Meta (24-MetaAI)", "RecvContext and Done provide a cancellation escape without closing the data channel.", false, "The response has no hub-wide Close; lifecycle facets use cancellation and per-subscription Unsubscribe, never adapter Close as global shutdown."),
		att("bounded-ring-lossy-detectable", "QueuedFanout24Meta (24-MetaAI)", "Deliberate mismatch: private overflow is lossless and unbounded, not a bounded detectable-loss ring.", false),
		att("synchronous-handoff-no-barrier", "SelectFanout (38-ChatGPT)", "Persistent AVL membership snapshots feed cancellable per-subscriber rendezvous leases; Send completes at receipt, not processing.", true),
		att("ephemeral-churn-membership", "SelectFanout (38-ChatGPT)", "Subscribe/unsubscribe clone an O(log N) tree path while publishers traverse an immutable snapshot without a membership lock.", true),
		att("cancellation-and-close-lifecycle", "SelectFanout (38-ChatGPT)", "SendContext, per-subscription retirement and hub Close cancel pending deliveries and drain leases before closing channels.", true),
		att("zero-subscriber-and-edge-lifecycle", "SelectFanout (38-ChatGPT)", "An empty tree publishes immediately with count zero; closed instances reject new subscriptions.", true),
		att("large-fanout-scalability", "SelectFanout (38-ChatGPT)", "Churn is logarithmic but delivery still performs N rendezvous; attached as an explicit poor-fit scaling measurement.", false),
		att("synchronous-handoff-no-barrier", "Fanout45ChatGPT/sync (45-ChatGPT)", "Intrusive atomic links and one delivery lease per node provide unbuffered handoff without a post-receive barrier.", true),
		att("buffered-lossless-backpressured", "Fanout45ChatGPT/buffered (45-ChatGPT)", "Per-subscription bounded channels absorb independent lag and then backpressure the serialized publisher.", true),
		att("ephemeral-churn-membership", "Fanout45ChatGPT/buffered (45-ChatGPT)", "O(1) intrusive-list edits and node leases avoid copying a subscriber slice on churn.", true),
		att("cancellation-and-close-lifecycle", "Fanout45ChatGPT/buffered (45-ChatGPT)", "Node and hub Close abort blocked sends, wait for leases and close data channels exactly once.", true),
		att("zero-subscriber-and-edge-lifecycle", "Fanout45ChatGPT/sync (45-ChatGPT)", "Empty head returns zero without taking the publication mutex; later nodes receive future values.", true),
		att("bounded-ring-lossy-detectable", "RingFanout (45-ChatGPT)", "Shared overwrite-oldest ring plus independent cursors reports exact missed counts and never waits for readers.", true),
		att("bounded-catchup-replay", "RingFanout (45-ChatGPT)", "SubscribeReplay atomically captures the retained suffix and a live cursor; older history is intentionally unavailable.", true),
		att("large-fanout-scalability", "RingFanout (45-ChatGPT)", "Publish stores once and signals coalesced per-node notifications; publisher work still walks nodes but does not copy payloads per reader.", true),
		att("ephemeral-churn-membership", "RingFanout (45-ChatGPT)", "Intrusive node links and reader leases support attach/detach without rebuilding a membership slice.", true),
		att("cancellation-and-close-lifecycle", "RingFanout (45-ChatGPT)", "Next accepts context cancellation; subscription/hub Close retire notification and reader leases safely.", true),
		att("zero-subscriber-and-edge-lifecycle", "RingFanout (45-ChatGPT)", "Publish with no nodes still advances the bounded log; live-only subscriptions start at the current tail.", true),
		att("lockstep-barrier-lossless", "ChanPubSub58Spark (58-MuseSpark)", "Atomic pong countdown is armed before the shared-channel ping snapshot; the sender parks on a Cond and a per-round close gate prevents fast receivers from stealing another copy of the same round.", true),
		att("ephemeral-churn-membership", "ChanPubSub58Spark (58-MuseSpark)", "The write gate fixes round membership while iterator ownership transfer makes cancellation/withdrawal compensation exact.", true),
		att("cancellation-and-close-lifecycle", "ChanPubSub58Spark (58-MuseSpark)", "Context iterators transfer withdrawal ownership from callback to consumer before receiving, avoiding a steal/receipt race.", true),
		att("zero-subscriber-and-edge-lifecycle", "ChanPubSub58Spark (58-MuseSpark)", "Zero participants return immediately; subscriptions admitted after a round observe later rounds.", true),
		att("large-fanout-scalability", "ChanPubSub58Spark (58-MuseSpark)", "The atomic pong plane removes a mutex convoy but the ping still performs N rendezvous; explicit poor-fit scaling attachment.", false),
		att("lockstep-barrier-lossless", "GenPubSub50Meta (50-MetaAI)", "Parity-flipped WaitGroups park the sender on one futex; a per-round close gate prevents same-round stealing after the source's Done-only repair.", true),
		att("ephemeral-churn-membership", "GenPubSub50Meta (50-MetaAI)", "Membership is separate from generation arming, so Adds during pong wait never race WaitGroup.Wait.", true),
		att("cancellation-and-close-lifecycle", "GenPubSub50Meta (50-MetaAI)", "Iterator/callback ownership transfer compensates exactly one withdrawal or acknowledgement.", true),
		att("zero-subscriber-and-edge-lifecycle", "GenPubSub50Meta (50-MetaAI)", "Empty snapshots return zero; late subscribers join subsequent generations.", true),
		att("large-fanout-scalability", "GenPubSub50Meta (50-MetaAI)", "Sender wakeup is O(1), but the shared ping still costs N rendezvous and the completion gate wakes N receivers.", false),
		att("bounded-ring-lossy-detectable", "Broadcaster58Spark (58-MuseSpark)", "Power-of-two sequence ring with protected payload copies, wrap-safe cursors and exact missed counts; one shared close-generation wakes readers.", true),
		att("latest-value-conflating-watch", "Broadcaster58Spark (58-MuseSpark)", "A lagging cursor can skip to the retained window and eventually observe the newest value; publication never waits for readers.", true),
		att("ephemeral-churn-membership", "Broadcaster58Spark (58-MuseSpark)", "Subscription is a counter plus private cursor; attach/detach does not coordinate with the publication path.", true),
		att("cancellation-and-close-lifecycle", "Broadcaster58Spark (58-MuseSpark)", "Close swaps in a pre-closed generation and signals Done; cursors drain retained values and context cancellation unsubscribes once.", true),
		att("zero-subscriber-and-edge-lifecycle", "Broadcaster58Spark (58-MuseSpark)", "Send advances the ring with no subscribers; a late cursor starts live and cannot read stale idle-period slots.", true),
		att("bounded-async-fanout-dispatch", "RingFanOut14Xing (14-Xing)", "Completed prose specification: bounded input burst queue plus pooled per-subscriber mailboxes and one nonblocking drop-oldest dispatcher.", true),
		att("bounded-async-fanout-dispatch", "BaselineAsyncFanout (baseline)", "Ordinary buffered input channel, dispatcher goroutine and standard drop-oldest fan-out; no channel pool.", true),
		att("bounded-async-fanout-dispatch", "BaselineFanout (standard)", "Explicit poor-fit comparison: standard drop-oldest fan-out has no asynchronous input burst queue.", false),
		att("ephemeral-churn-membership", "RingFanOut14Xing (14-Xing)", "Fixed channel pool leases and drains slots on Remove; churn does not allocate new data channels after construction.", true),
		att("zero-subscriber-and-edge-lifecycle", "RingFanOut14Xing (14-Xing)", "Empty pool membership dispatches nowhere; closed instances reject Add/Send and pooled channels become drainable then closed.", true),
		att("cancellation-and-close-lifecycle", "RingFanOut14Xing (14-Xing)", "SubscribeContext owns Remove; Close stops admission and the dispatcher, then closes pooled channels exactly once.", true),
		att("zero-subscriber-and-edge-lifecycle", "BaselineAsyncFanout (baseline)", "Ordinary async baseline edge behaviour.", true),
		att("cancellation-and-close-lifecycle", "BaselineAsyncFanout (baseline)", "Context cancellation unsubscribes; Close stops the dispatcher and closes subscriber channels.", true),
	}
}
