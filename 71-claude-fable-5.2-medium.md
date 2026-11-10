I'll add a third, complementary primitive (a buffered, lag-tolerant broadcast ring with select-compatible wake-ups) and test it under the race detector here, since this session isn't linked to your machine.

I added a third primitive, `ChanRing`, in `chanring.go` with tests and benchmarks in `chanring_test.go`; `ChanCaster` and `ChanPubSub` are untouched. It passes its tests under `-race` at GOMAXPROCS 1–8, but only on Go 1.24.7 in a scratch module alongside the two existing files. The 1.26.10 toolchain your `go.mod` requires wasn't downloadable here, so the repo's own test suite has not been run with it. This session isn't linked to your machine, so drop both files into `/Users/joeyc/dev/go-bigbuff` yourself.

## What it is

`ChanRing[V]` is the asynchronous counterpart to `ChanPubSub`'s synchronous rendezvous: a fixed ring of values with per-subscriber cursors and select-compatible parking.

- **Subscribers are positions.** A `ChanRingCursor` is a plain stack value (ring pointer, next sequence number, loss counter). There is no subscriber registry, so subscribing and unsubscribing write no shared state and allocate nothing.
- **Slow subscribers lose data.** A cursor more than one ring-length behind skips forward. The loss shows up as gaps in the sequence numbers and in `cur.Lost()`.
- **Reads are wait-free and race-detector clean.** Each slot has a tiny writer-preferring lock in one atomic word. A reader that sees the writer bit treats the slot as lost (it is being overwritten) and moves on.
- **Send is O(1) in subscriber count.** It writes one slot, publishes the tail, and does at most one channel send, and only if someone is parked. If every subscriber is busy, it does no channel operations.

## The novel part: wake cascade

Parking uses `ChanCaster`'s idea of counted anonymous tokens on one unbuffered channel, but the fan-out is delegated to the receivers. The sender sends one token carrying a budget of N−1. Each receiver splits its budget in half and forwards it with at most two sends on the same channel. Wake-ups spread as a binary tree across the available Ps, and the count stays exact: N armed waiters get exactly N sends. The round's write lock is taken by the sender and released by whichever goroutine receives the last token.

The select contract mirrors `ChanCaster.Add`:

```go
if cur.Arm() {
    select {
    case w := <-ring.C(): cur.Ack(w)   // forwards the wake
    case <-other:         cur.Disarm()
    }
}
```

`Recv(ctx)` and `SubscribeContext(ctx)` (an `iter.Seq2` of sequence number and value) wrap this for simple use.

## Measurements

Two cores, Go 1.24.7, 0.5s per benchmark, so treat these as indicative.

| Benchmark | ChanRing | Comparison |
|---|---|---|
| Send time, 10,000 parked subscribers | 5.6 µs | ChanCaster 4.5 ms |
| Send time, 100,000 parked subscribers | 2.1 µs | ChanCaster 255 ms |
| Lossless broadcast, 8 subscribers (per message) | 179 ns | per-subscriber channels 627 ns |
| Lossless broadcast, 1,000 subscribers | 13 µs | per-subscriber channels 29 µs |
| Lossless broadcast, 10,000 subscribers | 109 µs | per-subscriber channels 1.41 ms |
| Subscribe + unsubscribe with 10,000 parked | 177 ns, 0 allocs | per-subscriber channels 230 ns, 1 alloc; ChanPubSub 278 ns, 0 allocs |
| Send + receive, uncontended | 41 ns, 0 allocs | — |

Free-running `ChanPubSub` broadcast measured 21 ms per message at 10,000 subscribers. In the "lossless" rows the benchmark applies its own batch back-pressure, because the ring has none.

## Limits to know about

- **No built-in back-pressure.** A free-running sender on this box caused 62–99% loss for subscribers that couldn't keep up. If you need lossless delivery, the sender must gate itself, or use `ChanPubSub`.
- **End-to-end wake latency is not better.** Time until all N subscribers have run was about the same as `ChanCaster` (15.8 ms vs 13.0 ms at 10,000); the gain is that the sender isn't held for it.
- **`Ack` is a hard obligation.** A subscriber that arms and then neither receives nor disarms stalls the cascade and blocks all further `Arm` calls.
- **Send can briefly spin.** This happens only when a reader is mid-copy of the exact slot being overwritten, i.e. exactly one lap behind.
- **Values stay referenced until overwritten.** The ring holds up to its capacity in live values.
