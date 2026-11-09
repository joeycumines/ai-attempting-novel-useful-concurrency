package bigbuff

// Report body markup: the static skeleton that the client script fills.

const reportHTMLBody = `<body>
<div id="shell">
<nav id="nav" aria-label="Report sections">
  <div id="brand">
    <div class="mark">B</div>
    <div><h1>bigbuff</h1><small>Concurrency Measurement</small></div>
  </div>
  <!-- START AUTOMATED BACKLINKS -->
  <div class="nav-backlinks" style="margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--line);display:flex;flex-direction:column;gap:5px">
    <a href="index.html" style="color:var(--cyan);font-weight:600;display:flex;align-items:center;gap:6px" title="Return to Landing Page">
      <span style="font-size:12px">&larr;</span> <span>Landing Page</span>
    </a>
    <a href="report-01-gemini-flash-3.8-high.html" style="color:var(--ink2);font-size:12px" title="Architectural &amp; Empirical Analysis Report">Architectural Analysis</a>
    <a href="01-readme.md" style="color:var(--ink2);font-size:12px" title="Gist Entry Point &amp; Response Index">Gist Index (01-readme.md)</a>
    <a href="https://github.com/joeycumines/ai-attempting-novel-useful-concurrency" target="_blank" rel="noopener" style="color:var(--ink2);font-size:12px" title="GitHub Repository">GitHub Repository</a>
    <a href="README.md" style="color:var(--ink2);font-size:12px" title="Project README">README.md</a>
  </div>
  <!-- END AUTOMATED BACKLINKS -->
  <a href="#overview" class="active"><span class="dot"></span>Overview</a>
  <a href="#findings"><span class="dot"></span>Key Findings</a>
  <a href="#scaling"><span class="dot"></span>Scaling Curves</a>
  <a href="#leaderboard"><span class="dot"></span>Leaderboard</a>
  <a href="#adoptability"><span class="dot"></span>Adoptability Radar</a>
  <a href="#latency"><span class="dot"></span>Latency Distribution</a>
  <a href="#memory"><span class="dot"></span>Memory &amp; Allocations</a>
  <a href="#churn"><span class="dot"></span>Churn Cost</a>
  <a href="#pathology"><span class="dot"></span>Pathology Lab</a>
  <a href="#correctness"><span class="dot"></span>Correctness Signals</a>
  <a href="#coverage"><span class="dot"></span>Scenario Coverage</a>
  <a href="#methodology"><span class="dot"></span>Methodology &amp; Schema</a>
  <a href="#data"><span class="dot"></span>Data &amp; Exports</a>
  <div id="navfoot">
    <b>bigbuff-measure</b> · deterministic harness<br>
    Every chart regenerates from the embedded JSONL records — nothing is hard-coded. Hover anything for values.
  </div>
</nav>
<nav class="sectionbar" id="sectionbar" aria-label="Report sections">
  <a href="#overview" class="active">Overview</a>
  <a href="#findings">Key Findings</a>
  <a href="#scaling">Scaling Curves</a>
  <a href="#leaderboard">Leaderboard</a>
  <a href="#adoptability">Adoptability Radar</a>
  <a href="#latency">Latency Distribution</a>
  <a href="#memory">Memory &amp; Allocations</a>
  <a href="#churn">Churn Cost</a>
  <a href="#pathology">Pathology Lab</a>
  <a href="#correctness">Correctness Signals</a>
  <a href="#coverage">Scenario Coverage</a>
  <a href="#methodology">Methodology &amp; Schema</a>
  <a href="#data">Data &amp; Exports</a>
</nav>
<main>

<!-- START AUTOMATED MOBILE TOPBAR -->
<div class="mobile-topbar" style="display:none;background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px 16px;margin-bottom:24px;font-size:12.5px;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px">
  <a href="index.html" style="color:var(--cyan);text-decoration:none;font-weight:600;display:inline-flex;align-items:center;gap:6px">
    <span>&larr;</span> <span>Landing Page</span>
  </a>
  <div style="display:flex;align-items:center;gap:14px">
    <a href="01-readme.md" style="color:var(--ink2);text-decoration:none">Gist Index</a>
    <a href="https://github.com/joeycumines/ai-attempting-novel-useful-concurrency" target="_blank" rel="noopener" style="color:var(--ink2);text-decoration:none">GitHub</a>
  </div>
</div>
<!-- END AUTOMATED MOBILE TOPBAR -->

<!-- ══════════════ OVERVIEW ══════════════ -->
<header id="hero">
  <div class="kicker">Empirical concurrency study</div>
  <h1>Measured, not vibes:<br><em><span id="heroCount"></span> concurrent primitives under one harness.</em></h1>
  <p>Every implementation in this repository — hand-rolled broadcasters, caster-likes, scatter dispatchers, gather barriers and async payload lanes — exercised by the same batteries, the same timers, the same GC settings, and the same pass/fail gates. The sections below unpack throughput, latency, allocation, scalability, stall resilience, churn, withdrawal compensation and payload integrity.</p>
  <div class="chips" id="metaChips"></div>
  <div class="kpis" id="kpiRow"></div>
</header>
<section class="block" id="overview"></section>

<!-- ══════════════ FINDINGS ══════════════ -->
<section class="block" id="findings">
  <div class="kicker">Key findings</div>
  <h2 class="title">What the data says</h2>
  <p class="lede">Computed directly from the JSONL records by the report generator — each card links to the section where the underlying measurement lives.</p>
  <div class="findings-grid" id="findingsGrid"></div>
</section>

<!-- ══════════════ SCALING ══════════════ -->
<section class="block" id="scaling">
  <div class="kicker">Throughput under fan-out</div>
  <h2 class="title">Scaling curves, 1 → 64 subscribers</h2>
  <p class="lede">Publish cost per operation as subscriber count grows (log₂ axes). Flat lines scale; rising lines levy per-subscriber work. Toggle implementations in the legend; baselines are dashed.</p>
  <div class="tabs" id="scalingTabs"></div>
  <div class="card pad">
    <div class="toolbar">
      <label class="toggle"><input type="checkbox" id="scalingTopOnly" checked> show 12 fastest only</label>
      <select class="sel" id="scalingSort"><option value="thr">sort legend by throughput</option><option value="ratio">sort legend by 64/1 scaling ratio</option><option value="alpha">sort legend alphabetically</option></select>
    </div>
    <div id="scalingChart"></div>
    <div class="lgnd" id="scalingLegend"></div>
  </div>
</section>

<!-- ══════════════ LEADERBOARD ══════════════ -->
<section class="block" id="leaderboard">
  <div class="kicker">Composite verdict</div>
  <h2 class="title">Leaderboard &amp; adoptability scores</h2>
  <p class="lede">Percentile-ranked dimensions (throughput 28%, stall resilience 20%, scaling 16%, latency 14%, allocations 14%, churn 8%). Integrity failures cap the score at 50. Click a column to sort; filter to find your candidate.</p>
  <div class="tabs" id="lbTabs"></div>
  <div class="toolbar">
    <input class="search" id="lbSearch" placeholder="Filter implementations…" aria-label="Filter implementations">
    <label class="toggle"><input type="checkbox" id="lbBaselinesOnly"> baselines only</label>
    <label class="toggle"><input type="checkbox" id="lbGoodFitOnly"> expected-good-fit only</label>
  </div>
  <div class="tbl-wrap"><table class="tbl" id="lbTable"><thead><tr>
    <th data-k="impl">Implementation</th><th data-k="score">Adoptability</th><th data-k="verdict">Verdict</th>
    <th data-k="thr">ns/op @8</th><th data-k="ci">95% CI</th><th data-k="lat">p99 latency</th><th data-k="alloc">allocs/op</th><th data-k="scale">64/1 ratio</th><th data-k="stall">stall ops</th><th data-k="baseline">kind</th>
  </tr></thead><tbody id="lbBody"></tbody></table></div>
  <p class="small" id="lbFoot" style="margin-top:8px"></p>
</section>

<!-- ══════════════ ADOPTABILITY ══════════════ -->
<section class="block" id="adoptability">
  <div class="kicker">Trade-off fingerprints</div>
  <h2 class="title">Radar: pick your fighter</h2>
  <p class="lede">Six-axis profile, normalised 0–100 within family. Select up to 4 implementations (or press a shortcut below). Baselines render dashed.</p>
  <div class="tabs" id="radarTabs"></div>
  <div class="card pad">
    <div class="toolbar" id="radarShortcuts"></div>
    <div class="radar-flex">
      <div id="radarChart"></div>
      <div>
        <div class="toolbar" style="margin-top:0"><select class="sel" id="radarImpl"></select><button class="sbtn" id="radarAdd" title="Add to radar">＋</button></div>
        <div class="score-rows" id="radarRows"></div>
        <p class="small" style="margin-top:10px">Stall resilience uses log₁₀(ops completed by a publisher whose sibling stalled forever, shared deadline) — higher means it kept delivering. Scaling is the 64/1 relative cost ratio (lower ratio → higher score). Every axis is percentile within the measured family.</p>
      </div>
    </div>
  </div>
</section>

<!-- ══════════════ LATENCY ══════════════ -->
<section class="block" id="latency">
  <div class="kicker">Tail behaviour</div>
  <h2 class="title">Latency distribution @ 8 subscribers</h2>
  <p class="lede">p50 / p90 / p99 / max from reservoir-sampled publish→delivery latency (ADR-004 harness, warm). Sorted by p99. Whiskers show the 95% confidence interval around the mean where available.</p>
  <div class="tabs" id="latTabs"></div>
  <div class="card pad"><div id="latChart"></div><p class="small" style="margin-top:10px">Values in nanoseconds; lower is better. Histogram buckets in <a href="#methodology" style="color:var(--cyan)">Methodology</a>.</p></div>
</section>

<!-- ══════════════ MEMORY ══════════════ -->
<section class="block" id="memory">
  <div class="kicker">Allocation &amp; retention</div>
  <h2 class="title">What publishing costs the allocator</h2>
  <p class="lede">Left: allocations per publish vs publish cost (log–log) at 8 subscribers. Right: estimated bytes retained per subscriber at steady state (heap before/after GC of 16 idle subscriptions), and cost of publishing into the void.</p>
  <div class="tabs" id="memTabs"></div>
  <div class="card pad mem-grid" id="memGrid">
    <div><h4 style="font-size:12px;color:var(--ink2);margin-bottom:8px">allocations/op vs ns/op</h4><div id="allocScatter"></div></div>
    <div><h4 style="font-size:12px;color:var(--ink2);margin-bottom:8px">bytes retained per idle subscriber</h4><div id="memBars"></div></div>
  </div>
  <div class="card pad" style="margin-top:16px"><h4 style="font-size:12px;color:var(--ink2);margin-bottom:8px">zero-subscriber publish cost (broadcast)</h4><div id="zeroSubChart"></div></div>
</section>

<!-- ══════════════ CHURN ══════════════ -->
<section class="block" id="churn">
  <div class="kicker">Membership dynamics</div>
  <h2 class="title">Subscribe → publish → unsubscribe, per op</h2>
  <p class="lede">Churn cost per membership lifecycle — decisive for per-request or presence topologies where subscriptions are constantly created and torn down.</p>
  <div class="tabs" id="churnTabs"></div>
  <div class="card pad"><div id="churnChart"></div></div>
</section>

<!-- ══════════════ PATHOLOGY ══════════════ -->
<section class="block" id="pathology">
  <div class="kicker">Pathology Lab</div>
  <h2 class="title">A consumer stops reading. What happens?</h2>
  <p class="lede">Three lanes: publisher, two healthy subscribers, one permanently stalled subscriber that never calls Receive. Watch both traces side by side — or open any pair from the grid. Blocked publishers are parked, never killed; the harness unwinds them with watchdogs and GC.</p>
  <div class="card pad">
    <div class="toolbar">
      <select class="sel" id="pathoA" aria-label="Left implementation"></select>
      <span style="color:var(--ink3)">vs</span>
      <select class="sel" id="pathoB" aria-label="Right implementation"></select>
      <button class="sbtn" id="pathoPlay" title="Play both">▶</button>
      <button class="sbtn" id="pathoPause" title="Pause">⏸</button>
      <select class="sel" id="pathoSpeed"><option value="0.5">0.5×</option><option value="1" selected>1×</option><option value="2">2×</option><option value="4">4×</option></select>
      <input type="range" id="pathoScrub" min="0" max="1000" value="0" style="flex:1;min-width:140px;accent-color:var(--cyan)" aria-label="Scrub timeline">
    </div>
    <div class="patho-grid" id="pathoCards"></div>
  </div>
  <div class="card pad" style="margin-top:16px">
    <h4 style="font-size:12px;color:var(--ink2);margin-bottom:4px">Retained bytes after the storm (stalled consumer, post-GC)</h4>
    <p class="small" style="margin-bottom:10px">Publishers that keep running past a stalled lane either drop, coalesce, or buffer. Retained = heap growth attributable to the stalled lane after forcing a GC.</p>
    <div id="retainBars"></div>
  </div>
</section>

<!-- ══════════════ CORRECTNESS ══════════════ -->
<section class="block" id="correctness">
  <div class="kicker">Correctness signals</div>
  <h2 class="title">Zero tolerance, measured explicitly</h2>
  <p class="lede">Three independent gates: multi-word payload integrity (seqlock torn-read probe), rendezvous withdrawal compensation (did Add(-1) unstick a parked Send?), exactly-once scatter accounting, plus the harness verifier.</p>
  <div class="patho-grid" style="grid-template-columns:repeat(auto-fit,minmax(280px,1fr))">
    <div class="card pad"><h4 style="font-size:12px;color:var(--ink2)">Torn reads (payload lanes × 8 subs)</h4><div id="tornTable" style="margin-top:10px"></div></div>
    <div class="card pad"><h4 style="font-size:12px;color:var(--ink2)">Withdrawal compensation — rendezvous casters</h4><div id="cwdChart" style="margin-top:10px"></div></div>
    <div class="card pad"><h4 style="font-size:12px;color:var(--ink2)">Verification verdict</h4><div id="verifyPanel" style="margin-top:10px"></div></div>
  </div>
</section>

<!-- ══════════════ COVERAGE ══════════════ -->
<section class="block" id="coverage">
  <div class="kicker">Registry completeness</div>
  <h2 class="title">Scenario × implementation matrix</h2>
  <p class="lede">Attachment registry: which scenarios declare each implementation a good fit (green), a bad fit (red), or silent (grey). Hover a cell; switch families above.</p>
  <div class="tabs" id="covTabs"></div>
  <div class="cov-wrap"><table class="cov" id="covTable"></table></div>
  <div class="cov-legend"><span><i style="background:#166534"></i>expected good fit</span><span><i style="background:#7f1d1d"></i>declared bad fit</span><span><i style="background:#1b2447"></i>no attachment</span></div>
</section>

<!-- ══════════════ METHODOLOGY ══════════════ -->
<section class="block" id="methodology">
  <div class="kicker">Methodology &amp; schema</div>
  <h2 class="title">How to read every number here</h2>
  <p class="lede">The harness lives in <code class="k">measure_*_test.go</code>; results are line-delimited JSON written incrementally, one record per measurement, then verified by <code class="k">bigbuff-measure verify</code> before this page was rendered.</p>

  <details class="mdetail" open><summary>Design principles</summary><div>
    <ul style="padding-left:18px">
      <li><b>Single harness, all implementations.</b> Every battery constructs the implementation through its registry factory, warms it, measures under identical GC (<code class="k">GOGC=off</code> for alloc batteries) and scheduling conditions, and reports distributional stats — never a bare mean.</li>
      <li><b>Deterministic scheduling pressure.</b> Churn and contention batteries use exact iteration counts, channels and WaitGroups; timeouts are deadlock guards, never readiness probes. No sleeps anywhere.</li>
      <li><b>Pathology probes are honest.</b> A publisher parked by construction is an expected outcome, recorded as <code class="k">publisher_blocked</code>; recover converts panics to <code class="k">publisher_panicked</code>. Blocking cleanup (Unsubscribe/Close) is skipped or watchdog-bounded after a blocked probe — the harness would otherwise deadlock the runtime. This asymmetry is deliberate and documented.</li>
      <li><b>Passability.</b> <code class="k">go test</code> stays green: measure subcommands run under <code class="k">go test -run=NONE -args bigbuff-measure …</code>; batteries assert only what their design can guarantee and record the rest. Verification enforces that every registered implementation was measured or explicitly skipped with a reason.</li>
    </ul>
  </div></details>

  <details class="mdetail"><summary>Batteries &amp; metrics</summary><div>
    <div class="scroll-x"><table class="gloss" id="batteryGloss"></table></div>
  </div></details>

  <details class="mdetail"><summary>Record schema (JSONL v1)</summary><div>
    <p>One record per line. <code class="k">type</code> is one of <code class="k">meta</code>, <code class="k">metric</code>, <code class="k">trace</code>, <code class="k">skipped</code>, <code class="k">summary</code>, <code class="k">registry</code>.</p>
    <table class="gloss">
      <tr><td>metric</td><td>record kind; dims carry the sweep dimension (<code class="k">subs</code> / <code class="k">receivers</code> / <code class="k">senders</code>)</td></tr>
      <tr><td>mean / stddev / cv</td><td>distributional summaries over <code class="k">reps</code> repetitions of <code class="k">ops</code> operations each</td></tr>
      <tr><td>p50 / p90 / p99 / max</td><td>reservoir-sampled latencies (ADR-004 HDR-ish reservoir, per-run)</td></tr>
      <tr><td>ci95_low / ci95_high</td><td>Welch-adjusted 95% half-width on the per-op mean (t-distribution, df = n−1)</td></tr>
      <tr><td>approx_retained_bytes</td><td>post-GC heap growth attributable to a permanently stalled subscriber</td></tr>
      <tr><td>exactly_once_violations</td><td>scatter messages received ≠ 1 time, summed over all receivers incl. warmup</td></tr>
      <tr><td>torn_reads / checked</td><td>payload lanes caught serving mismatched [4]uint64 halves vs loads checked</td></tr>
      <tr><td>withdrawal_compensation_success</td><td>fraction of rounds where Add(-1) unstuck a Send parked on a never-receiving intent</td></tr>
      <tr><td>exposure_limited</td><td>true when the implementation is excluded from batteries it would fail by construction (policy: limit exposure, never mis-measure)</td></tr>
      <tr><td>expected_good_fit / scenarios</td><td>attachment registry: the scenarios this implementation was designed for</td></tr>
    </table>
  </div></details>

  <details class="mdetail"><summary>Adoptability score</summary><div>
    <p>Percentile rank within family, weighted: throughput .28 · stall resilience .20 · scaling .16 · latency .14 · allocations .14 · churn .08. Stall resilience uses log₁₀(completed ops) vs family peers. Any torn read or exactly-once scatter violation caps the score at 50 and marks the implementation “Do not adopt”. A 100 means “best in class on measured axes”, not “use everywhere” — read the radar, not just the number.</p>
  </div></details>

  <details class="mdetail"><summary>Threats to validity</summary><div>
    <ul style="padding-left:18px">
      <li>Shared-hardware noise (this run: GOMAXPROCS=2 Xeon @ 2.60GHz). Use quick for regression gates, standard/full for decisions; compare ratios, not absolutes.</li>
      <li>The stalled-consumer battery is a <i>bound</i>, not a leak: publishers parked mid-probe by construction are excluded from cleanup; retention is post-GC heap attribution, approximate by nature.</li>
      <li>Trace events truncate at the configured event budget; outcomes and ops counters are still exact.</li>
      <li>Torn-read coverage depends on delivery counts; implementations with zero deliveries under the probe report <code class="k">insufficient-data</code>, never a fabricated zero.</li>
      <li>Registry attachments are authored judgments (expected good/bad fit); they are metadata for interpretation, not measurements.</li>
    </ul>
  </div></details>
  <p class="footer-note">bigbuff-measure schema v1 · report generated from embedded records · <span id="footMeta"></span></p>
</section>

<!-- ══════════════ DATA ══════════════ -->
<section class="block" id="data">
  <div class="kicker">Raw materials</div>
  <h2 class="title">Data &amp; exports</h2>
  <p class="lede">Everything on this page is derived from the embedded JSONL. Download the full dataset, flattened CSVs, or the attachment registry.</p>
  <div class="dl">
    <a class="dlbtn" download="bigbuff-results.jsonl" id="dlJSONL"><b>results.jsonl</b><small>every meta/metric/trace/skipped/summary record</small></a>
    <a class="dlbtn" download="bigbuff-metrics-wide.csv" id="dlCSVMetrics"><b>metrics_wide.csv</b><small>one row per metric record, all stats columns</small></a>
    <a class="dlbtn" download="bigbuff-scores.csv" id="dlCSVScores"><b>scores.csv</b><small>family, impl, adoptability score + dimensions + verdict</small></a>
    <a class="dlbtn" download="bigbuff-registry.json" id="dlRegistry"><b>registry.json</b><small>scenarios · cases · attachments (expected fit, exclusions)</small></a>
  </div>
  <div class="card pad" style="margin-top:16px"><h4 style="font-size:12px;color:var(--ink2)">Skipped implementations (recorded, not measured)</h4><div id="skippedList" class="small" style="margin-top:8px"></div></div>
</section>

</main>
</div>
<div id="tip" role="tooltip"></div>
<script>
window.BIGBUFF = __DATA_JSON__;
window.BIGBUFF_REGISTRY = __REGISTRY_JSON__;
</script>
`
