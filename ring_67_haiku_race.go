//go:build race

/*
   Copyright 2026 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bigbuff

import (
	"sync/atomic"
	"unsafe"
)

// ringRaceEnabled67Haiku reports whether the -race payload path is compiled in.
const ringRaceEnabled67Haiku = true

// ringStore67Haiku writes v into the slot payload one 8-byte word at a time with atomic stores.
// Every access to the payload is then atomic, so the seqlock is data-race free under the
// memory model and the race detector can verify the rest of the protocol. The algorithm is
// the same as in the fast build.
//
// The payload starts at offset 8 in ringSlot67Haiku and the slot size is a multiple of 8. The
// words written here therefore lie inside the slot, and the seq word is never touched.
func ringStore67Haiku[V any](sl *ringSlot67Haiku[V], v V) {
	var buf [ringMaxElemBytes67Haiku / 8]uint64
	*(*V)(unsafe.Pointer(&buf)) = v
	n := (unsafe.Sizeof(v) + 7) / 8
	base := unsafe.Pointer(&sl.val)
	for i := uintptr(0); i < n; i++ {
		atomic.StoreUint64((*uint64)(unsafe.Add(base, i*8)), buf[i])
	}
}

// ringLoad67Haiku returns a copy of the slot payload, read one 8-byte word at a time with atomic
// loads. The caller must validate it with the seqlock before using it.
func ringLoad67Haiku[V any](sl *ringSlot67Haiku[V]) V {
	var buf [ringMaxElemBytes67Haiku / 8]uint64
	var zero V
	n := (unsafe.Sizeof(zero) + 7) / 8
	base := unsafe.Pointer(&sl.val)
	for i := uintptr(0); i < n; i++ {
		buf[i] = atomic.LoadUint64((*uint64)(unsafe.Add(base, i*8)))
	}
	return *(*V)(unsafe.Pointer(&buf))
}
