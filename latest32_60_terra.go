package bigbuff

import "sync/atomic"

const (
	latest3260TerraValueMask   uint64 = 1<<32 - 1
	latest3260TerraRevisionMax        = 1<<31 - 1
	latest3260TerraClosedBit   uint64 = 1 << 63
)

// Latest3260Terra is a nonblocking, lossy latest-value register.
//
// Its zero value is ready to use. It must not be copied after first use.
//
// A successful Publish replaces the current value and increments its revision.
// Every successful publication has a distinct revision. Readers may skip any
// number of intermediate publications.
//
// Close is linearizable with Publish. A publication that wins the CAS before
// Close succeeds; one that loses to Close returns PublishClosed60Terra. Close retains
// the final value and revision. No goroutines or resources require teardown.
//
// The finite revision space is intentional: after 2^31-1 successful publishes,
// further publishes return PublishExhausted60Terra instead of wrapping and creating
// an ABA ambiguity. Replace the register when this limit is reached.
//
// Latest3260Terra makes no fairness or bounded-latency guarantee for competing
// publishers. Its polling API does not park goroutines or support select.
type Latest3260Terra struct {
	word atomic.Uint64
}

// PublishResult60Terra describes the outcome of a publication.
type PublishResult60Terra uint8

const (
	Published60Terra PublishResult60Terra = iota
	PublishClosed60Terra
	PublishExhausted60Terra
)

// Snapshot60Terra is one coherent observation of a Latest3260Terra.
//
// Revision zero means that no value has yet been published. Value is zero in
// that initial state, but a published value of zero is valid.
type Snapshot60Terra struct {
	Value    uint32
	Revision uint32
	Closed   bool
}

// Load returns a linearizable snapshot.
func (x *Latest3260Terra) Load() Snapshot60Terra {
	return decodeLatest3260Terra(x.word.Load())
}

// Publish attempts to publish value.
//
// On success it returns the revision assigned to the publication. On failure,
// the revision is zero. A register that is both closed and exhausted reports
// PublishClosed60Terra.
func (x *Latest3260Terra) Publish(value uint32) (revision uint32, result PublishResult60Terra) {
	old := x.word.Load()
	for {
		if old&latest3260TerraClosedBit != 0 {
			return 0, PublishClosed60Terra
		}

		rev := uint32(old >> 32)
		if rev == latest3260TerraRevisionMax {
			return 0, PublishExhausted60Terra
		}

		nextRev := rev + 1
		next := uint64(nextRev)<<32 | uint64(value)
		if x.word.CompareAndSwap(old, next) {
			return nextRev, Published60Terra
		}
		old = x.word.Load()
	}
}

// Close prevents subsequent publications and preserves the last value.
// It returns true only for the call that transitions the register to closed.
func (x *Latest3260Terra) Close() bool {
	old := x.word.Load()
	for {
		if old&latest3260TerraClosedBit != 0 {
			return false
		}
		if x.word.CompareAndSwap(old, old|latest3260TerraClosedBit) {
			return true
		}
		old = x.word.Load()
	}
}

func decodeLatest3260Terra(word uint64) Snapshot60Terra {
	return Snapshot60Terra{
		Value:    uint32(word & latest3260TerraValueMask),
		Revision: uint32((word &^ latest3260TerraClosedBit) >> 32),
		Closed:   word&latest3260TerraClosedBit != 0,
	}
}

// Latest32Cursor60Terra tracks observations for one reader. It must not be used
// concurrently without external synchronization.
//
// A fresh cursor reports an initial closed state, but not the initial open,
// unpublished state. Poll reports a publication or closure once, then reports
// it again only if the revision or closed state changes. The caller can compute
// skipped publications from the difference between successive revisions.
//
// A cursor is tied by the caller to one Latest3260Terra for its lifetime; do not reuse
// it with another register without resetting it to its zero value.
type Latest32Cursor60Terra struct {
	revision uint32
	closed   bool
}

// Poll returns the current snapshot and whether it differs from the cursor's
// previous observation in revision or closed state.
func (c *Latest32Cursor60Terra) Poll(x *Latest3260Terra) (snapshot Snapshot60Terra, changed bool) {
	snapshot = x.Load()
	changed = snapshot.Revision != c.revision || snapshot.Closed != c.closed
	c.revision = snapshot.Revision
	c.closed = snapshot.Closed
	return
}
