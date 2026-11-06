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
	"fmt"
	"testing"
)

// The same complete-delivery benchmark at larger subscriber counts. Keep it
// separate so ordinary benchmark runs can choose the smaller, cheaper matrix.
func BenchmarkBroadcastLarge(b *testing.B) {
	for _, n := range []int{4096, 16384} {
		b.Run(fmt.Sprintf("ChanPubSub/N=%d", n), func(b *testing.B) {
			benchmarkChanPubSub(b, n)
		})
		for _, capacity := range []int{1, 64} {
			b.Run(fmt.Sprintf("RingFast/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkRingPubSub(b, n, capacity, false)
			})
			b.Run(fmt.Sprintf("RingSelect/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkRingPubSub(b, n, capacity, true)
			})
		}
		b.Run(fmt.Sprintf("LinearChannels/B=64/N=%d", n), func(b *testing.B) {
			benchmarkLinearChannels(b, n, 64)
		})
	}
}
