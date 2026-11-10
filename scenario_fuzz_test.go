package bigbuff

import (
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"
)

// broadcastFuzzSubjects lists the broadcast implementations the op-stream
// fuzz target drives, once each however many scenarios they are attached to.
// It excludes attachments whose metadata forbids mid-send withdrawal/close or
// concurrent slot lifecycle; those implementations have dedicated model fuzz
// targets instead of being silently exercised here.
var broadcastFuzzSubjects = sync.OnceValue(func() []string {
	seen := map[string]bool{}
	var subjects []string
	for _, a := range broadcastAttachments {
		if a.ExposureLimited || seen[a.Impl] {
			continue
		}
		probe := scenarioBroadcastFactories()[a.Impl]()
		limited, skip := probe.(interface{ SkipBroadcastFuzz() bool })
		skip = skip && limited.SkipBroadcastFuzz()
		probe.Close()
		if skip {
			continue
		}
		seen[a.Impl] = true
		subjects = append(subjects, a.Impl)
	}
	return subjects
})

// FuzzScenarioBroadcast drives one broadcast implementation, chosen by the
// input, through an op stream derived from the input: at most 24 ops and at
// most 4 live consumers.
//
// The stream is a legal usage pattern for every attachment, including the
// lock-step ones, where a subscriber that neither receives nor leaves stalls
// every publisher and every later subscriber by contract. So a subscriber
// here is always either consuming (its worker loops on a blocking Receive) or
// gone. The ops:
//
//	0  open a consumer (Subscribe, then receive in a loop)
//	1  publish the next value, on its own goroutine
//	2  wait until every publish started so far has returned
//	3  probe: Subscribe, one TryReceive, Unsubscribe
//	4  retire the newest consumer (cancel its context, then Unsubscribe)
//
// Nothing waits on a clock; the only timer is the deadlock guard.
//
// Assertions: no panic; with every subscriber consuming, every publish
// returns (op 2 and the end of the stream); cancelling a consumer's context
// releases its Receive; Receive does not fail while its context is live and
// the broadcaster is open; no subscriber receives the same value twice (every
// published value is distinct). Order is not asserted: publishes are
// concurrent, so their order is not defined.
func FuzzScenarioBroadcast(f *testing.F) {
	streams := [][]byte{
		{0, 1, 2, 3, 4, 1, 2},
		{1, 1, 1, 0, 2, 4},
		{3, 2, 1, 0, 4, 4, 1},
		{0, 0, 1, 1, 0, 1, 2, 4, 1, 3, 0, 1, 4, 1, 2},
	}
	subjects := broadcastFuzzSubjects()
	for i := range subjects {
		for _, stream := range streams {
			f.Add(uint16(i), stream)
		}
	}
	for _, r := range broadcastOpStreamRegressions {
		if i := slices.Index(subjects, r.impl); i >= 0 {
			f.Add(uint16(i), r.ops)
		}
	}
	f.Fuzz(func(t *testing.T, which uint16, ops []byte) {
		if len(ops) == 0 || len(ops) > 24 {
			return
		}
		subjects := broadcastFuzzSubjects()
		fuzzBroadcastStream(t, subjects[int(which)%len(subjects)], ops)
	})
}

// broadcastOpStreamRegressions are op streams that exposed a defect when
// they were first generated. Each defect has a dedicated regression test next
// to the implementation; the streams are kept as fuzz seeds and replayed by
// TestScenarioBroadcastOpStreams.
var broadcastOpStreamRegressions = []struct {
	impl string
	ops  []byte
	what string
}{
	{"Broadcast28Flint (28-Flint)", []byte{2, 3, 2, 0, 2, 2, 1, 1, 2, 3, 1, 0, 3, 1, 1, 3, 1, 0},
		"Send restarted its walk at a retired subscriber: duplicate deliveries, then no return"},
	{"ChanPubSub47Gemini (47-Gemini)", []byte{4, 4, 0, 2, 2, 2, 0, 3, 2, 3, 3, 0, 0, 1, 3, 4, 4, 3, 0, 2, 1},
		"Unsubscribe straight after Wait parked on a send that was never coming"},
	{"BaselineBarrierPubSub (baseline)", []byte{3, 0, 2, 1, 3, 2, 1, 0, 1, 4, 3, 0, 2, 3},
		"a subscriber that joined mid-round and left drove the round's WaitGroup negative"},
	{"EpochPubSub (11-meta)", []byte{3, 1, 1, 2, 3, 2, 2, 1, 1, 0, 3, 0, 3, 1, 3, 1},
		"adapter read its starting sequence after registering and skipped a round it was counted in"},
	{"EpochPubSub (11-meta)", []byte{0, 4, 1, 1, 0, 0, 2, 0, 4, 4, 1, 3, 1, 2, 0, 4, 0, 4, 4, 2},
		"as above, reached through a consumer instead of a probe"},
}

// TestScenarioBroadcastOpStreams replays, without the fuzzing engine, the
// recorded regression streams and a fixed pseudo-random set of streams for
// every subject of FuzzScenarioBroadcast. The plain `go test` run of a fuzz
// target executes only its seeds, which are four hand-written streams per
// implementation; this gives each implementation a few hundred distinct
// streams on every run, at every GOMAXPROCS the suite is run with. The
// streams are a pure function of the subject list, so a failure is
// reproducible from the implementation name and ops it reports.
func TestScenarioBroadcastOpStreams(t *testing.T) {
	subjects := broadcastFuzzSubjects()
	for _, r := range broadcastOpStreamRegressions {
		if !slices.Contains(subjects, r.impl) {
			t.Errorf("regression stream names %q, which is not a subject of the op-stream fuzz target", r.impl)
			continue
		}
		// The defects were interleaving-dependent; replay each stream enough
		// times for the scheduler to vary.
		for range 200 {
			fuzzBroadcastStream(t, r.impl, r.ops)
		}
	}
	perSubject := 300
	if testing.Short() {
		perSubject = 30
	}
	for i, impl := range subjects {
		rng := rand.New(rand.NewPCG(uint64(i), 0x6f7073747265616d))
		for range perSubject {
			ops := make([]byte, 1+rng.IntN(24))
			for j := range ops {
				ops[j] = byte(rng.IntN(5))
			}
			fuzzBroadcastStream(t, impl, ops)
		}
	}
}

func fuzzBroadcastStream(t *testing.T, impl string, ops []byte) {
	b := scenarioBroadcastFactories()[impl]()
	publish := harnessPublish(b)

	type consumer struct {
		sub    BroadcastSubscriber
		cancel context.CancelFunc
		done   chan struct{}
	}
	await := func(done <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(scenarioGuard):
			t.Fatalf("%s: %s did not finish (deadlock); ops %v", impl, what, ops)
		}
	}
	// guarded runs one lifecycle call under the deadlock guard, so that a
	// call that never returns is reported against the implementation and the
	// op stream instead of surfacing as a bare test timeout.
	guarded := func(what string, fn func()) {
		t.Helper()
		done := make(chan struct{})
		go func() { defer close(done); fn() }()
		await(done, what)
	}
	subscribe := func() (sub BroadcastSubscriber) {
		t.Helper()
		guarded("Subscribe", func() { sub = b.Subscribe() })
		return sub
	}
	open := func() *consumer {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		c := &consumer{sub: subscribe(), cancel: cancel, done: make(chan struct{})}
		go func() {
			defer close(c.done)
			seen := map[int]bool{}
			for {
				v, ok := c.sub.Receive(ctx)
				if !ok {
					if ctx.Err() == nil {
						t.Errorf("%s: Receive failed with a live context on an open broadcaster; ops %v", impl, ops)
					}
					return
				}
				if seen[v] {
					t.Errorf("%s: a subscriber received value %d twice; ops %v", impl, v, ops)
				}
				seen[v] = true
			}
		}()
		return c
	}
	retire := func(c *consumer) {
		t.Helper()
		c.cancel()
		await(c.done, "a cancelled consumer")
		guarded("Unsubscribe of a cancelled consumer", c.sub.Unsubscribe)
	}

	var consumers []*consumer
	var publishers sync.WaitGroup
	awaitPublishers := func() {
		t.Helper()
		done := make(chan struct{})
		go func() { publishers.Wait(); close(done) }()
		await(done, "publishers (every subscriber is consuming)")
	}
	val := 0
	for _, op := range ops {
		switch op % 5 {
		case 0:
			if len(consumers) < 4 {
				consumers = append(consumers, open())
			}
		case 1:
			val++
			v := val
			publishers.Add(1)
			go func() { defer publishers.Done(); publish(v) }()
		case 2:
			awaitPublishers()
		case 3:
			probe := subscribe()
			guarded("TryReceive then Unsubscribe of a probe", func() {
				probe.TryReceive()
				probe.Unsubscribe()
			})
		case 4:
			if len(consumers) > 0 {
				last := consumers[len(consumers)-1]
				consumers = consumers[:len(consumers)-1]
				retire(last)
			}
		}
	}
	awaitPublishers()
	for _, c := range consumers {
		retire(c)
	}
	guarded("Close", b.Close)
}

// FuzzScenarioCasterScatterGather drives caster, scatter and gather
// attachments through bounded op streams in their documented patterns.
func FuzzScenarioCasterScatterGather(f *testing.F) {
	f.Add([]byte{0, 1, 2, 1, 0})
	f.Add([]byte{1, 1, 0, 2, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 24 {
			return
		}
		// Scatter: serial operations through each implementation's own wait
		// abstraction, so queued rings and unbuffered channels are both legal.
		// Values are unique and each accepted value must be consumed once.
		for _, a := range scatterAttachments {
			if a.ExposureLimited {
				continue
			}
			x := scatterLikes()[a.Impl]()
			registered := 0
			next, expected := 1, 1
			drain := func() {
				for {
					v, ok := x.try()
					if !ok {
						return
					}
					if v != expected {
						t.Fatalf("scatter %s: got %d want %d", a.Impl, v, expected)
					}
					expected++
				}
			}
			for _, op := range ops {
				switch op % 3 {
				case 0:
					if registered < 4 {
						x.add(1)
						registered++
					}
				case 1:
					if registered == 0 {
						if n := x.send(next); n != 0 {
							t.Fatalf("scatter %s: no receivers but Send=%d", a.Impl, n)
						}
						continue
					}
					sendDone := make(chan int, 1)
					go func(v int) { sendDone <- x.send(v) }(next)
					ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
					v, ok := x.wait(ctx)
					cancel()
					if !ok || v != next {
						t.Fatalf("scatter %s: wait=(%d,%v), want %d", a.Impl, v, ok, next)
					}
					select {
					case n := <-sendDone:
						if n != 1 {
							t.Fatalf("scatter %s: Send=%d want 1", a.Impl, n)
						}
					case <-time.After(scenarioGuard):
						t.Fatalf("scatter %s: Send did not complete after receipt", a.Impl)
					}
					next++
					drain()
				case 2:
					if registered > 0 {
						x.add(-1)
						registered--
					}
					drain()
				}
			}
			for registered > 0 {
				x.add(-1)
				registered--
			}
			drain()
		}
		// Gather: ops only register/withdraw; signals happen exclusively
		// inside a proper Wait round (a lone send on the unbuffered channel
		// with no Wait in flight would park forever by design).
		for _, a := range gatherAttachments {
			x := gatherLikes()[a.Impl]()
			registered := 0
			doWait := func() {
				if registered == 0 {
					return
				}
				var wg sync.WaitGroup
				for i := 0; i < registered; i++ {
					wg.Add(1)
					go func() { defer wg.Done(); x.send() }()
				}
				done := make(chan int, 1)
				go func() { done <- x.wait() }()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatalf("gather %s: Wait blocked", a.Impl)
				}
				wg.Wait()
				registered = 0
			}
			for _, op := range ops {
				switch op % 3 {
				case 0:
					x.add(1)
					registered++
				case 1:
					doWait()
				case 2:
					if registered > 0 {
						// Withdraw one sender, then complete the round for
						// the rest so no registration leaks into the next op.
						registered--
						withdrawDone := make(chan int, 1)
						go func() { withdrawDone <- x.wait() }()
						// Compensation for the withdrawn sender is supplied
						// by Add(-1) itself in both implementations.
						x.add(-1)
						var wg sync.WaitGroup
						for i := 0; i < registered; i++ {
							wg.Add(1)
							go func() { defer wg.Done(); x.send() }()
						}
						select {
						case <-withdrawDone:
						case <-time.After(5 * time.Second):
							t.Fatalf("gather %s: Wait after withdraw blocked", a.Impl)
						}
						wg.Wait()
						registered = 0
					}
				}
			}
			doWait()
		}
		// Casters are covered op-by-op by TestScenarioCaster churn facets;
		// here a single bounded round per caster per input keeps fuzz fast.
		for _, a := range casterAttachments {
			if a.ExposureLimited {
				continue
			}
			x := casterLikes()[a.Impl]()
			x.add(1)
			done := make(chan int, 1)
			go func() { done <- x.send(1) }()
			select {
			case v := <-x.c:
				if x.ack != nil {
					x.ack()
				}
				if v != 1 {
					t.Fatalf("caster %s: got %d want 1", a.Impl, v)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("caster %s: receive blocked", a.Impl)
			}
			select {
			case n := <-done:
				if n != 1 {
					t.Fatalf("caster %s: Send=%d want 1", a.Impl, n)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("caster %s: Send blocked", a.Impl)
			}
		}
	})
}
