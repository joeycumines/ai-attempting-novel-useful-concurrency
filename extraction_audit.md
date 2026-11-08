# Markdown extraction audit (responses 03–20 and later extractions)

This pass cross-checked the pre-`AGENTS.md` extractions against the delivered
Markdown, not merely against the current Go tests. Method:

1. Parse every `.txtar -- filename --` section from each response into a scratch
   tree, and separately capture fenced `go` blocks.
2. Normalize comments/whitespace and diff each delivered implementation/test
   against its extracted Go file. Namespacing, license headers, constructors,
   and completed ellipses are expected divergences; behavioral divergences were
   reviewed individually.
3. Re-run the scenario/implementation registry checks, family harnesses, edge
   cases, churn, and the full ordinary and race suites after repairs.

The scratch comparison tree is not checked in; the durable result is this table
and the per-file `Provenance:` headers. "Equal" below means equal after
comment/whitespace normalization, not necessarily byte-identical.

| Response | Extracted files | Cross-check result / action |
|---|---|---|
| 03 chatgpt | `chanlatest.go`, tests | Source and tests equal. Kept as `ChanLatest03ChatGPT`. |
| 04 deepseek | `chanfeed.go`, tests | Source/tests equal after formatting. |
| 05 qwen | `changatherer.go` (gatherer + fused PubSub), tests | Both fenced implementations retained in the combined file; source tests preserved. |
| 06 meta | `chancaster_06_meta.go`, `chanpubsub_06_meta.go`, `gencaster.go`, tests | The delivered V2 caster/pubsub sketches used unavailable runtime internals and incomplete sharding. Completed them as namespaced portable variants; positive-Add lock-free fast path retained where sound. PubSub unavailable semaphore path completed with Cond and documented exposure. GenCaster renamed internal types to avoid package collisions. |
| 07 qwen | `chanbeacon.go`, tests | Repaired extraction: per-slot sequence/RWMutex publication, cursor/lag accounting and iterator ownership corrected relative to the sketch; tests exercise catch-up and overrun. |
| 08 muse-spark | `beacon.go`, `ring_beacon.go`, `chancaster2.go`, `reliable_beacon.go`, `beacon_subscription_test.go`, tests | Beacon/RingBeacon/ReliableBeacon now capture their subscription point synchronously in SubscribeContext; RingBeacon also re-checks after Ready capture to close a lost-wakeup window. Caster retains atomic arming repairs. ReliableBeacon repaired around atomic pending acks, armed sequence identity, unsubscribe compensation and tracked Wait. Corrected stale registry provenance from “14-Qwen” to response 08. |
| 09 gemini | namespaced caster/pubsub/epochcaster, tests | All three variants retained. Constructor validation, broken-state checks and namespacing reviewed; shared lock-step/payload coverage retained. |
| 10 gemini | `chanpubsub_10_gemini.go`, `epochtape.go`, tests | Distinct 09/10 PubSub variants both retained. EpochTape source/test equal after formatting; zero-epoch edge repaired. |
| 11 meta | `chancaster_11_meta.go`, `epochpubsub_11_meta.go`, tests | Evolution A lock-free dual-counter CAS caster was previously Markdown-only; now extracted (ellipses completed), registered and covered by CAS/withdrawal tests. EpochPubSub retained. |
| 12 crouton | `chanfanout.go`, tests | Iterator ownership/cancellation repaired; source tests preserved and shared broadcast harness runs the adapter. |
| 13 crouton | `epochcaster_13_crouton.go`, tests | Namespaced block-based epoch caster retained; constructor/zero-state and subscriber block accounting reviewed. |
| 14 xing | `chancaster_14_xing.go`, `chanpubsub_14_xing.go`, `ringfanout_14_xing.go`, tests | Caster/PubSub retained. Section 3 was prose-only RingFanOut; now completed with explicit bounded drop-oldest input + subscriber stages, pooled channels, exact drop counters, safe ownership wrapper, baseline and two-stage fuzz model. Interpretations are documented at the extraction header. |
| 15 nemotron | `chancaster_15_nemotron.go`, `chanhub.go`, tests | Caster retained. ChanHub TrySend exposure is bounded and documented; blocking Send and membership semantics tested. |
| 16 deepseek | `chanpubsub_16_deepseek.go`, `chanscatter.go`, tests | PubSub retained with portable unsubscribe spin. Scatter teardown/steal accounting repaired (`stolen` flag and reader fence) so an empty final unsubscribe does not misreport a successful send. |
| 17 gemini | fast-path/pumped casters, `chanpubsub_17_gemini.go`, `chanrotor.go`, tests | Both caster variants retained in one namespaced file; PubSub uses the pumped caster. Rotor equal after formatting; shared harness covers both. |
| 18 chatgpt | `ringpubsub.go`, tests, example | RingPubSub and tests equal; example adapted to package/registry naming. |
| 19 chatgpt | `phasepubsub.go`, `relaypubsub.go`, tests | PhasePubSub and tests equal; RelayPubSub cancellation/cleanup reviewed, low-level unsubscribe/Read contract documented. |
| 20 arena-agent | `chanrelay.go`, tests | Relay source/tests retained with cancellation and subscription-lifecycle repairs; tests equal after formatting. |

Later extractions were also checked for source fidelity:

- **24 queued fanout:** retained lossy nonblocking baseline trade-off; repaired
  concurrent per-producer FIFO admission and withdrawal backlog draining, with
  focused tests and the unbounded-overflow baseline.
- **32 StableRing:** retained competing-consumer ring; explicitly marked
  poor-fit for work-distribution scatter, moved cases to the competing-consumer
  eviction scenario, added exact baseline/fuzz coverage.
- **50 genpubsub:** repaired round arming versus parity epoch identity; retained
  generation wake design and documented provenance.
- **58 spark:** extracted all three implementations. PubSub comments corrected
  to the actual same-round Done gate (the prose “Done alone prevents stealing”
  claim is false for same-round Wait calls); round gate costs one allocation per
  Send and is included in benchmarks. Caster TryRecv steal-block remains an
  explicit source limitation.

No response implementation is intentionally omitted because it overlaps another.
Overlap is represented by separate namespaced types/adapters and scenario
attachments, with poor-fit attachments used instead of forcing incompatible
semantics into a scenario.

## Verification snapshot

Toolchain: Go 1.27.3 (`GOTOOLCHAIN=local`).

- `go vet ./...`: pass.
- `go test ./... -count=1`: pass (~19s).
- `go test -race ./... -count=1`: pass (~61s).
- Final focused 3s fuzz campaigns passed: `FuzzBoundedScatterModel` (~65k execs),
  `FuzzRingFanOut14TwoStageModel` (~68k), `FuzzQueuedFanout24FIFO` (~70k),
  `FuzzBroadcaster58RingModel` (~71k), `FuzzStableRing32QueueModel` (~74k).
  Earlier 5s campaigns also passed (~171k–186k execs each).
- Allocation benchmarks (`-benchtime=200ms`, one sample): `ChanPubSub58Send`
  1 alloc/op, `GenPubSub50Send` 1 alloc/op, `Broadcaster58Send` 1 alloc/op;
  queued mailbox/overflow and RingFanOut14 admission paths 0 alloc/op;
  StableRing and bounded-scatter baseline admission 0 alloc/op.

The readiness-sleep cleanup exposed and repaired two baseline/harness issues:
`BaselineBufferedBroadcast` no longer holds the membership lock while blocking
on a full subscriber (unsubscribe/close cannot deadlock behind Send), and pump
adapter unsubscribe now waits for iterator cleanup so fixed subscriber pools do
not exhaust under tight churn. Manual lock-step adapters that cannot interrupt
Wait mid-round are explicitly skipped by the standard churn facet.

Final deterministic-harness review replaced the shared receipt-queue stress with
an exact ack/ready round handoff: the ack releases lock-step Send, and readiness
confirms the next-round park or churn replacement registration. This removed a
publisher/reader deadlock under `-race` without sleeps or wall-clock churn. The
review also exposed and repaired the 08-MuseSpark subscription-point and
RingBeacon lost-wakeup issues above, with focused regression cases registered in
the case registry. The ChanCaster58 helper test no longer assumes RecvContext
registration wins against Send's zero-receiver fast path; it checks the exact
Send/receive outcome pair instead.
