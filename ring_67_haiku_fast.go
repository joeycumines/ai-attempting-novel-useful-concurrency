//go:build !race

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

// ringRaceEnabled67Haiku reports whether the -race payload path is compiled in.
const ringRaceEnabled67Haiku = false

// ringStore67Haiku writes v into the slot payload with a plain copy. The seqlock in Ring.Publish
// brackets this write.
//
// Under the Go memory model a plain write racing with a plain read is a data race. The
// protocol tolerates it because a reader keeps a copy only after validating seq on both
// sides, and the element is pointer-free, so a discarded torn copy is invisible to the
// garbage collector. The -race build replaces this with atomic word accesses so the race
// detector can check the rest of the protocol. The algorithm is the same in both builds.
func ringStore67Haiku[V any](sl *ringSlot67Haiku[V], v V) {
	sl.val = v
}

// ringLoad67Haiku returns a copy of the slot payload. The caller must validate it with the seqlock
// before using it.
func ringLoad67Haiku[V any](sl *ringSlot67Haiku[V]) V {
	return sl.val
}
