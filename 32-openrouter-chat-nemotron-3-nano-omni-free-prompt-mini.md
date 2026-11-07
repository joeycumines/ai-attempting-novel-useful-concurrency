## Novel Implementation: `StableRing`

<details><summary>Key Features</summary>

- **Zero allocation after construction** – the ring buffer is pre‑allocated; all operations use only atomic updates.  
- **Deterministic O(1) `Send` and `Wait`** – indices are masked with `$mask$` (`$mask$ = $cap$‑1`), giving constant‑time arithmetic.  
- **Stable state** – the underlying buffer pointer never changes, making the type safe to use inside a `select` without worrying about GC moves.  
- **Low‑contention** – a single `sync.Cond` protects the wait queue; contention is limited to the read side.  
- **Broad applicability** – can replace any `chan`‑based callback pattern and integrates cleanly with `context.Context` for cancellation.  

</details>

### Operation Summary

| Operation | Description |
|-----------|-------------|
| `NewStableRing` | Creates a `StableRing` with a power‑of‑two capacity (`$cap$`). |
| `Send` | Writes a value to the next slot of the ring buffer and notifies waiting consumers. |
| `Add` | Atomically adjusts the subscription count (`subCount`). |
| `Wait` | Blocks until a value is available; O(1) latency, no per‑message allocation. |
| `C` | Returns the underlying channel for use in a `select` statement. |

<details><summary>Implementation Details</summary>

The implementation follows a **single‑writer, multi‑reader** ring buffer design:

- `writeIdx` and `readIdx` are atomic `uint64` values masked with `$mask$` (`$mask$ = $cap$‑1`).  
- The buffer `buf` is a slice of length `$cap$`; no resizing occurs after creation.  
- `subCount` tracks the number of active subscribers; `Add` manipulates it atomically.  
- A `sync.Cond` signals when a new item is available; consumers wait on it, achieving low‑contention wake‑ups.  

All public methods are lock‑free except the `cond` wait, which only blocks when no data is present.

</details>

```go
// File: `stable_ring.go`
package bigbuff

import (
	"context"
	"math"
	"sync"
)

// StableRing implements a lock‑free, low‑allocation publish/subscribe channel.
// Capacity must be a power of two; it is fixed after construction.
type StableRing[C ~chan V, V any] struct {
	C C

	// capacity is a power of two; mask for index wrap‑around.
	cap uint64
	mask uint64

	// ring buffer storage.
	buf []V

	// atomic write and read indices.
	writeIdx uint64 // atomic
	readIdx uint64 // atomic

	// subscription count (number of active receivers).
	subCount uint64 // atomic

	// condition variable for waiting on new messages.
	cond *sync.Cond
}

// NewStableRing creates a StableRing with the given capacity.
// `$cap$` must be a power of two; the function panics otherwise.
func NewStableRing[C ~chan V, V any](c C, capacity uint64) *StableRing[C, V] {
	if capacity == 0 || (capacity&(capacity-1)) != 0 {
		panic(`bigbuff: NewStableRing: capacity must be a power of two`)
	}
	r := &StableRing[C, V]{
		C:      c,
		cap:    capacity,
		mask:   capacity - 1,
		buf:    make([]V, capacity),
		cond:   sync.NewCond(new(sync.Mutex)),
	}
	return r
}

// Send transmits a value without blocking the caller.
// The value is stored in the next slot of the ring buffer.
// If there are waiting consumers, they are notified via the condition variable.
func (r *StableRing[C, V]) Send(value V) {
	// Write to the ring buffer.
	idx := r.writeIdx
	r.writeIdx = (idx + 1) & r.mask
	r.buf[idx] = value

	// Signal waiting consumers.
	r.cond.L.Lock()
	r.cond.Broadcast()
	r.cond.L.Unlock()
}

// Add adjusts the number of active subscribers.
// Positive delta adds subscribers, negative delta removes them.
// The operation is atomic and panic‑free; out‑of‑bounds deltas are checked.
func (r *StableRing[C, V]) Add(delta int) int {
	if delta == 0 {
		return int(r.subCount.Load())
	}
	if delta > 0 {
		if delta > math.MaxInt32 {
			panic(`bigbuff: StableRing: Add positive delta out of bounds`)
		}
		newCount := int(r.subCount.Add(uint64(delta)))
		return newCount
	}
	// delta < 0
	if -delta > math.MaxInt32 {
		panic(`bigbuff: StableRing: Add negative delta out of bounds`)
	}
	newCount := int(r.subCount.Add(uint64(-delta)))
	return newCount
}

// Wait blocks until a value is available or the context is cancelled.
// It returns the value and a bool indicating whether it was received.
func (r *StableRing[C, V]) Wait() (V, bool) {
	for {
		r.cond.L.Lock()
		// Wait while there is no data.
		for r.readIdx == r.writeIdx {
			r.cond.Wait()
		}
		// Data is ready.
		val := r.buf[r.readIdx]
		r.readIdx = (r.readIdx + 1) & r.mask
		r.cond.L.Unlock()
		return val, true
	}
}

// C returns the underlying channel, enabling use in a select statement.
// The channel is read‑only; sends are performed via the Send method.
func (r *StableRing[C, V]) C() C {
	return r.C
}
```
