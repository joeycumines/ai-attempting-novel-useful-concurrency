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

// Provenance: 23-meta-ai.md (response #23, Meta AI), second delivered snippet.
//
// The original AdaptiveCaster is described as "ChanCaster for small N,
// SeqCaster for large N", switching at 32 subscribers. As delivered it (a)
// always published to both paths, duplicating every value for any receiver
// registered on both, and (b) never consulted the threshold at all. The repair
// below keeps the described mechanism — an adaptive hybrid that preserves exact
// ChanCaster semantics for the rendezvous path and delegates large-N, churn-heavy
// delivery to SeqCaster — while making the routing coherent:
//
//   - Add(d) registers on the rendezvous path; SubscribeSeq registers on the
//     ring path. The two paths are documented as mutually exclusive per
//     receiver, which is what "adaptive" must mean if values are not to be
//     duplicated.
//   - Send publishes to the ring path when it has any subscriber, and always
//     performs the rendezvous fan-out when the rendezvous path has any
//     receivers; the summed count is returned.

import "sync/atomic"

// AdaptiveCaster23Meta keeps the single-channel, zero-allocation, select-native
// rendezvous contract of [ChanCaster] for small fan-outs, while offering the
// shared-ring [SeqCaster23Meta] path for large fan-outs and heavy churn.
//
// A receiver must choose exactly one path: [AdaptiveCaster23Meta.Add] for the
// rendezvous path (receive from [AdaptiveCaster23Meta.C]) or
// [AdaptiveCaster23Meta.SubscribeSeq] for the ring path. Registering the same
// receiver on both paths would deliver the value twice, which is the caller's
// error rather than a property of the caster.
type AdaptiveCaster23Meta[V any] struct {
	small       *ChanCaster[chan V, V]
	big         *SeqCaster23Meta[V]
	bigSubs     atomic.Int64
	redirectAt  int
	redirecting atomic.Bool
}

// NewAdaptiveCaster23Meta constructs an adaptive caster that redirects
// rendezvous receivers to the ring path once more than redirectAt of them are
// registered. A redirectAt <= 0 means [DefaultAdaptiveRedirect23Meta].
func NewAdaptiveCaster23Meta[V any](redirectAt int) *AdaptiveCaster23Meta[V] {
	if redirectAt <= 0 {
		redirectAt = DefaultAdaptiveRedirect23Meta
	}
	ch := make(chan V)
	return &AdaptiveCaster23Meta[V]{
		small:      NewChanCaster(ch),
		big:        NewSeqCaster23Meta[V](12, 4),
		redirectAt: redirectAt,
	}
}

// DefaultAdaptiveRedirect23Meta is the fan-out at which the hybrid starts
// preferring the ring path (the value used by the original response).
const DefaultAdaptiveRedirect23Meta = 32

// C returns the shared rendezvous channel. Receivers registered with Add read
// from it.
func (a *AdaptiveCaster23Meta[V]) C() chan V { return a.small.C }

// Add registers delta receivers on the rendezvous path.
func (a *AdaptiveCaster23Meta[V]) Add(delta int) int {
	n := a.small.Add(delta)
	if n >= a.redirectAt {
		a.redirecting.Store(true)
	} else if n < a.redirectAt {
		a.redirecting.Store(false)
	}
	return n
}

// Redirecting reports whether the rendezvous path has reached the redirect
// threshold, i.e. whether new receivers should prefer [SeqCaster23Meta].
func (a *AdaptiveCaster23Meta[V]) Redirecting() bool { return a.redirecting.Load() }

// SubscribeSeq registers a ring-path subscriber.
func (a *AdaptiveCaster23Meta[V]) SubscribeSeq() *SeqSub23Meta[V] {
	sub := a.big.Subscribe()
	a.bigSubs.Add(1)
	return sub
}

// UnsubscribeSeq releases a ring-path subscriber.
func (a *AdaptiveCaster23Meta[V]) UnsubscribeSeq(sub *SeqSub23Meta[V]) {
	sub.Unsubscribe()
	a.bigSubs.Add(-1)
}

// Send publishes v to both paths, returning the number of receivers that took
// part in the rendezvous (the ring path is asynchronous, so its receivers are
// not counted, mirroring [SeqCaster23Meta.Send]'s contract of returning a
// sequence rather than a receive count).
func (a *AdaptiveCaster23Meta[V]) Send(v V) int {
	if a.bigSubs.Load() > 0 {
		a.big.Send(v)
	}
	return a.small.Send(v)
}

// Close stops the ring path.
func (a *AdaptiveCaster23Meta[V]) Close() { a.big.Close() }
