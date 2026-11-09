package bigbuff

// Broadcast facets: the per-attachment runners and the broadcast scenario tests.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// All synchronisation below is via channels/WaitGroups. Timeouts exist only
// as deadlock guards that fail the test (bounded, verifiable), never as a
// retry/sleep mechanism.

const scenarioGuard = 15 * time.Second

func runBroadcastFacet(t *testing.T, a Attachment, subs, msgs int) {
	t.Helper()
	sc := scenarioByID[a.Scenario]
	factory, ok := scenarioBroadcastFactories()[a.Impl]
	if !ok {
		t.Fatalf("no factory for %q", a.Impl)
	}
	b := factory()
	defer b.Close()
	subscribers := make([]BroadcastSubscriber, subs)
	for i := range subscribers {
		subscribers[i] = b.Subscribe()
		defer subscribers[i].Unsubscribe()
	}
	ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
	defer cancel()
	results := make([][]int, subs)
	var wg sync.WaitGroup
	for i, s := range subscribers {
		wg.Add(1)
		go func(i int, s BroadcastSubscriber) {
			defer wg.Done()
			for {
				v, ok := s.Receive(ctx)
				if !ok {
					return
				}
				results[i] = append(results[i], v)
				// Terminal-value termination works for every loss policy:
				// even a conflating attachment to a lossless scenario must
				// eventually surface the final value. Strict per-value
				// assertions below only apply to expected-good-fit lossless
				// attachments.
				if v == msgs {
					return
				}
			}
		}(i, s)
	}
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for v := 1; v <= msgs; v++ {
			b.Publish(v)
		}
	}()
	select {
	case <-pubDone:
	case <-time.After(scenarioGuard):
		t.Fatalf("%s/%s: publisher blocked (guard timeout)", a.Scenario, a.Impl)
	}
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(scenarioGuard):
		t.Fatalf("%s/%s: subscribers did not observe terminal value (guard timeout)", a.Scenario, a.Impl)
	}
	for i, got := range results {
		if len(got) == 0 {
			t.Fatalf("%s/%s: subscriber %d received nothing", a.Scenario, a.Impl, i)
		}
		for j := 1; j < len(got); j++ {
			if got[j] <= got[j-1] {
				t.Fatalf("%s/%s: subscriber %d duplicate/reorder: %v", a.Scenario, a.Impl, i, got)
			}
		}
		if got[len(got)-1] != msgs {
			t.Fatalf("%s/%s: subscriber %d final=%d want %d (%v)", a.Scenario, a.Impl, i, got[len(got)-1], msgs, got)
		}
		if sc.Loss == Lossless && a.ExpectedGoodFit {
			if len(got) != msgs {
				t.Fatalf("%s/%s: subscriber %d got %d values want %d (%v)", a.Scenario, a.Impl, i, len(got), msgs, got)
			}
			for v := 1; v <= msgs; v++ {
				if got[v-1] != v {
					t.Fatalf("%s/%s: subscriber %d gap at %d: %v", a.Scenario, a.Impl, i, v, got)
				}
			}
		}
	}
}

func TestScenarioBroadcast(t *testing.T) {
	for _, sc := range Scenarios {
		if sc.Kind != KindBroadcast {
			continue
		}
		// Churn / large-fanout / lifecycle / zero-subscriber scenarios have
		// dedicated facets below (their invariants differ from steady fan-out).
		switch sc.ID {
		case "ephemeral-churn-membership", "large-fanout-scalability", "cancellation-and-close-lifecycle", "zero-subscriber-and-edge-lifecycle":
			continue
		}
		for _, a := range attachmentsFor(sc.ID, broadcastAttachments) {
			t.Run(string(sc.ID)+"/"+a.Impl, func(t *testing.T) {
				msgs := 20
				// Exposure-limited ring facets: never publish past capacity.
				runBroadcastFacet(t, a, 4, msgs)
			})
		}
	}
}

func TestScenarioChurn(t *testing.T) {
	// Bounded, deterministic churn: a fixed number of subscription sessions,
	// each receiving with a context that the test itself cancels after the
	// publisher finishes. No sleeps: completion is signalled by channels.
	for _, a := range attachmentsFor("ephemeral-churn-membership", broadcastAttachments) {
		t.Run(a.Impl, func(t *testing.T) {
			factory := scenarioBroadcastFactories()[a.Impl]
			b := factory()
			defer b.Close()
			const sessions = 24
			const msgs = 60
			ctx, cancel := context.WithCancel(context.Background())
			var violations sync.Map // impl -> first error string
			var wg sync.WaitGroup
			for s := 0; s < sessions; s++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sub := b.Subscribe()
					defer sub.Unsubscribe()
					last := 0
					for {
						v, ok := sub.Receive(ctx)
						if !ok {
							return
						}
						if v <= last {
							violations.Store(a.Impl, fmt.Sprintf("duplicate/reorder %d after %d", v, last))
							return
						}
						last = v
						if v == msgs {
							return
						}
					}
				}()
			}
			pubDone := make(chan struct{})
			go func() {
				defer close(pubDone)
				for v := 1; v <= msgs; v++ {
					b.Publish(v)
				}
			}()
			select {
			case <-pubDone:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: publisher blocked during churn", a.Impl)
			}
			cancel()
			waitDone := make(chan struct{})
			go func() { wg.Wait(); close(waitDone) }()
			select {
			case <-waitDone:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: churn sessions did not terminate", a.Impl)
			}
			if v, ok := violations.Load(a.Impl); ok {
				t.Fatalf("%s: %v", a.Impl, v)
			}
		})
	}
}

func TestScenarioLargeFanout(t *testing.T) {
	for _, a := range attachmentsFor("large-fanout-scalability", broadcastAttachments) {
		t.Run(a.Impl, func(t *testing.T) { runBroadcastFacet(t, a, 32, 10) })
	}
}

func TestScenarioPayload(t *testing.T) {
	// Wide-payload integrity is driven through the existing async pv harness:
	// steady (lossless variants must show no gaps) — deterministic sizes.
	for name, mk := range asyncImpls() {
		t.Run(name, func(t *testing.T) { runAsyncStress(t, mk, 8, 800, false, false) })
	}
}

func TestScenarioLifecycle(t *testing.T) {
	for _, a := range attachmentsFor("cancellation-and-close-lifecycle", broadcastAttachments) {
		t.Run(a.Impl, func(t *testing.T) {
			factory := scenarioBroadcastFactories()[a.Impl]
			b := factory()
			sub := b.Subscribe()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					if _, ok := sub.Receive(ctx); !ok {
						return
					}
				}
			}()
			pubDone := make(chan struct{})
			go func() { defer close(pubDone); b.Publish(1) }()
			select {
			case <-pubDone:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: publish blocked before cancel", a.Impl)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: subscriber did not stop on cancel", a.Impl)
			}
			sub.Unsubscribe()
			sub.Unsubscribe() // idempotent at the adapter level
			b.Close()
			b.Close() // idempotent where Close exists; no-op adapter otherwise
		})
	}
}

func TestScenarioZeroSubscriber(t *testing.T) {
	for _, a := range attachmentsFor("zero-subscriber-and-edge-lifecycle", broadcastAttachments) {
		t.Run(a.Impl, func(t *testing.T) {
			factory := scenarioBroadcastFactories()[a.Impl]
			b := factory()
			defer b.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				for v := 1; v <= 50; v++ {
					b.Publish(v)
				}
			}()
			select {
			case <-done:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: Publish blocked with zero subscribers", a.Impl)
			}
			// A late subscriber must receive subsequent values per policy.
			sub := b.Subscribe()
			defer sub.Unsubscribe()
			ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
			defer cancel()
			pubDone := make(chan struct{})
			go func() { defer close(pubDone); b.Publish(999) }()
			v, ok := sub.Receive(ctx)
			if !ok || v != 999 {
				t.Fatalf("%s: late subscriber got (%d,%v) want (999,true)", a.Impl, v, ok)
			}
			select {
			case <-pubDone:
			case <-time.After(scenarioGuard):
				t.Fatalf("%s: Publish to late subscriber blocked", a.Impl)
			}
		})
	}
}
