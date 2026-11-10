//go:build !race

package bigbuff

// slotBuffer65Deepseek is the seqlock-protected value cell used in the non-race build.
// Direct plain stores/loads; the surrounding slot.seq (acquire/release
// atomics) provides the happens-before ordering required by the seqlock
// protocol.
type slotBuffer65Deepseek[V any] struct {
	v V
}

//go:nosplit
func (b *slotBuffer65Deepseek[V]) load() V { return b.v }

//go:nosplit
func (b *slotBuffer65Deepseek[V]) store(v V) { b.v = v }
