package bigbuff

// Fan-in family: bounded merge of producer lanes into a single batch
// consumer. Lossy-detectable by design: TryPush reports overload at push
// time, values are never silently altered, and after Close every accepted
// value is drainable before Next reports closure.

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

const faninSenders = 4
const faninMergePerLane = 256
const faninDrainTotal = 4 * 64 // both faninLikes fixtures hold exactly this many values
const faninBatch = 32

// measureFaninMerge measures end-to-end merge cost per value: 4 producers
// push tagged values concurrently (retrying the non-blocking TryPush on full,
// which is backpressure, never loss) while the measuring goroutine drains via
// blocking Next calls until every accepted value is observed. dims senders=4.
func measureFaninMerge(ctx *measureContext, name string, mk func() faninLike) {
	scen, goodFit, expLimit := faninAttachmentMeta(name)
	const senders = faninSenders
	const perLane = faninMergePerLane
	total := senders * perLane
	samples := make([]float64, 0, ctx.profile.Reps)
	lastOps := 0
	var violations int64
	for r := 0; r < ctx.profile.Reps; r++ {
		x := mk()
		consumeCtx, cancel := context.WithCancel(context.Background())
		var producers sync.WaitGroup
		start := make(chan struct{})
		for l := 0; l < senders; l++ {
			producers.Add(1)
			l := l
			go func() {
				defer producers.Done()
				<-start
				for i := uint64(0); i < perLane; i++ {
					v := uint64(l)<<32 | i
					for !x.push(l, v) {
						runtime.Gosched()
					}
				}
			}()
		}
		next := make([]uint64, senders)
		got := 0
		t0 := time.Now()
		close(start)
		for got < total {
			v, ok, err := x.next(consumeCtx)
			if err != nil {
				ctx.fail("%s: Next failed: %v", name, err)
				break
			}
			if !ok {
				ctx.fail("%s: Next reported closure after %d of %d values", name, got, total)
				break
			}
			l := int(v >> 32)
			seq := v & 0xffffffff
			if l >= senders || seq != next[l] {
				violations++
				if l < senders && seq+1 > next[l] {
					next[l] = seq + 1
				}
			} else {
				next[l]++
			}
			got++
		}
		d := time.Since(t0)
		cancel()
		producers.Wait()
		x.close()
		lastOps = total
		samples = append(samples, float64(d)/float64(total))
	}
	ctx.writeMetric(map[string]any{
		"metric":                  "merge_ns_op",
		"family":                  "fanin",
		"impl":                    name,
		"unit":                    "ns",
		"dims":                    map[string]any{"senders": senders},
		"ops":                     lastOps,
		"reps":                    len(samples),
		"lower_is_better":         true,
		"scenarios":               scen,
		"expected_good_fit":       goodFit,
		"exposure_limited":        expLimit,
		"exactly_once_violations": violations,
	}, samples)
}

// measureFaninBatchDrain prefills the fixture to its capacity (256 values,
// which both faninLikes implementations hold) and measures the cost of each
// Drain(32) call. Producer-side cost is excluded from the samples.
func measureFaninBatchDrain(ctx *measureContext, name string, mk func() faninLike) {
	scen, goodFit, expLimit := faninAttachmentMeta(name)
	total := faninDrainTotal
	samples := make([]float64, 0, ctx.profile.Reps*(total/faninBatch))
	var violations int64
	lastOps := 0
	for r := 0; r < ctx.profile.Reps; r++ {
		x := mk()
		for l := 0; l < faninSenders; l++ {
			for i := uint64(0); i < 64; i++ {
				v := uint64(l)<<32 | i
				for !x.push(l, v) {
					runtime.Gosched()
				}
			}
		}
		next := make([]uint64, faninSenders)
		drained := 0
		tmp := make([]uint64, faninBatch)
		for drained < total {
			t0 := time.Now()
			n := x.drain(tmp)
			d := time.Since(t0)
			if n == 0 {
				ctx.fail("%s: Drain returned 0 with %d of %d prefilled values remaining", name, total-drained, total)
				break
			}
			for i := 0; i < n; i++ {
				v := tmp[i]
				l := int(v >> 32)
				seq := v & 0xffffffff
				if l >= faninSenders || seq != next[l] {
					violations++
					if l < faninSenders && seq+1 > next[l] {
						next[l] = seq + 1
					}
				} else {
					next[l]++
				}
			}
			drained += n
			samples = append(samples, float64(d))
		}
		lastOps += drained
		x.close()
	}
	ctx.writeMetric(map[string]any{
		"metric":                  "drain_ns_op",
		"family":                  "fanin",
		"impl":                    name,
		"unit":                    "ns",
		"dims":                    map[string]any{"batch": faninBatch},
		"ops":                     lastOps / faninBatch,
		"reps":                    len(samples),
		"lower_is_better":         true,
		"scenarios":               scen,
		"expected_good_fit":       goodFit,
		"exposure_limited":        expLimit,
		"exactly_once_violations": violations,
	}, samples)
}

func runFaninBatteries(ctx *measureContext) {
	for name, mk := range faninLikes() {
		name := name
		mk := mk
		if !ctx.implAllowed(name) {
			continue
		}
		fmt.Fprintf(os.Stderr, "  fanin: %s\n", name)
		if ctx.batteryAllowed("fanin.merge") {
			measureFaninMerge(ctx, name, mk)
		}
		if ctx.batteryAllowed("fanin.batch_drain") {
			measureFaninBatchDrain(ctx, name, mk)
		}
	}
}
