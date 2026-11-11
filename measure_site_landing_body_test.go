package bigbuff

// landingPageBody is the markup of the landing page. It was split out of the
// template that measure_site_test.go assembles.

const landingPageBody = `</head>
<body>

<header class="site-nav">
  <div class="nav-brand">
    <div class="mark">b</div>
    <div>
      <h1>bigbuff</h1>
      <small>AI Concurrency Benchmark</small>
    </div>
  </div>
  <nav class="nav-links">
    <a href="#reports">Reports</a>
    <a href="#leaderboard">Leaderboard</a>
    <a href="#responses">58 Responses</a>
    <a href="#paradigms">Paradigms</a>
    <a href="#baselines">Baselines</a>
    <a href="#pathology">Pathology Lab</a>
    <a href="#downloads">Datasets</a>
  </nav>
  <div class="nav-actions">
    <a href="results-01-03-report.html" class="btn btn-primary" title="Launch Run 01 Measurement Report">Measurement Report (Run 01)</a>
    <a href="report-01-gemini-flash-3.8-high.html" class="btn" title="View Architectural Evolution Analysis">Architectural Analysis</a>
    <a href="__GITHUB_REPO_URL__" target="_blank" rel="noopener" class="btn" title="GitHub Repository">GitHub Repo</a>
  </div>
</header>

<main class="container">

  <!-- HERO -->
  <section class="hero">
    <div class="kicker"><span>Empirical Concurrency Research</span> &middot; <span>Deterministic Harness</span></div>
    <h2>AI Attempting <em>Novel Useful Concurrency</em> in Go</h2>
    <p class="lead">
      We challenged <strong>58 AI model configurations</strong> with a one-shot prompt requiring novel, low-allocation Go concurrency primitives for synchronous channel broadcast. We then extracted, repaired in spirit, and measured <strong>116 concurrent primitives</strong> against 10 staff-engineer baselines across 18 scenarios and 2,013 deterministic benchmark runs.
    </p>
    <div class="hero-cta">
      <a href="results-01-03-report.html" class="btn btn-primary">Measurement Report (Run 01 &middot; Standard)</a>
      <a href="results-02-03-report.html" class="btn">Measurement Report (Run 02 &middot; Quick)</a>
      <a href="report-01-gemini-flash-3.8-high.html" class="btn">Architectural &amp; Empirical Analysis</a>
      <a href="#responses" class="btn">Browse 58 Responses</a>
      <a href="__GITHUB_REPO_URL__" target="_blank" rel="noopener" class="btn">GitHub Repository</a>
    </div>
    <div class="kpis">
      <div class="kpi">
        <div class="v">58</div>
        <div class="l">Model Responses</div>
        <div class="s">OpenAI, Anthropic, Google, DeepSeek, Qwen...</div>
      </div>
      <div class="kpi">
        <div class="v">116</div>
        <div class="l">Evaluated Primitives</div>
        <div class="s">Casters, PubSubs, Rings, Epochs, Fans</div>
      </div>
      <div class="kpi">
        <div class="v">18 / 291</div>
        <div class="l">Scenarios / Attachments</div>
        <div class="s">Strict exhaustiveness invariants</div>
      </div>
      <div class="kpi">
        <div class="v">2,013</div>
        <div class="l">Measurements (Run 01)</div>
        <div class="s">Verified PASS across all batteries</div>
      </div>
      <div class="kpi">
        <div class="v">0</div>
        <div class="l">Test Flakiness</div>
        <div class="s">100% deterministic ack/ready synchronization</div>
      </div>
    </div>
  </section>

  <!-- REPORTS HUB -->
  <section class="block" id="reports">
    <div class="kicker">Interactive Analysis &amp; Data Hub</div>
    <h3 class="section-title">Comprehensive Reports &amp; Artifacts</h3>
    <p class="lede">
      Every report is completely self-contained with embedded data &mdash; no CDNs or external assets. Open them online or locally from disk via <code>file://</code>.
    </p>

    <div class="reports-grid">
      <!-- Card 1: Measurement Report Run 01 -->
      <div class="report-card meas">
        <div>
          <span class="report-badge">Interactive Report &middot; Run 01 (Standard Profile)</span>
          <h4>Measurement Report (Run 01 &middot; Standard)</h4>
          <p>
            The master decision-grade benchmark report computed from 2,013 JSONL records. Features interactive throughput scaling curves (1&rarr;64 subscribers), adoptability radar, latency percentiles (p50/p90/p99), memory allocations, churn costs, and the Pathology Lab.
          </p>
          <ul class="report-features">
            <li><span class="dot"></span> <strong>Interactive Scaling Curves:</strong> Toggle implementations, inspect log&sup2; fan-out scaling</li>
            <li><span class="dot"></span> <strong>Adoptability Leaderboard:</strong> 116 primitives scored by throughput, latency, memory &amp; stability</li>
            <li><span class="dot"></span> <strong>Pathology Lab:</strong> Concrete case studies of races, deadlocks, and silent drops</li>
            <li><span class="dot"></span> <strong>Scenario Coverage Matrix:</strong> Complete 18-scenario attachment and baseline grid</li>
          </ul>
        </div>
        <div class="report-actions">
          <a href="results-01-03-report.html" class="btn btn-primary">Open Run 01 Report &rarr;</a>
          <div style="display:flex;gap:8px">
            <a href="results-01-04-metrics_wide.csv" download class="btn" title="Download Flattened Metrics CSV">Metrics CSV</a>
            <a href="results-01-05-scores.csv" download class="btn" title="Download Scores CSV">Scores CSV</a>
          </div>
        </div>
      </div>

      <!-- Card 2: Measurement Report Run 02 -->
      <div class="report-card meas">
        <div>
          <span class="report-badge">Interactive Report &middot; Run 02 (Quick Profile)</span>
          <h4>Measurement Report (Run 02 &middot; Quick)</h4>
          <p>
            The fast smoke / CI-grade benchmark report computed from 1,479 JSONL records. Provides rapid verification across all registered primitives with identical visual tooling and metric exports.
          </p>
          <ul class="report-features">
            <li><span class="dot"></span> <strong>CI-Grade Verification:</strong> 1,479 verified records across all scenario attachments</li>
            <li><span class="dot"></span> <strong>Identical Analytics:</strong> Full scaling charts, adoptability scorecards, and data downloads</li>
            <li><span class="dot"></span> <strong>Run Comparison:</strong> Evaluates consistency across toolchain revisions</li>
          </ul>
        </div>
        <div class="report-actions">
          <a href="results-02-03-report.html" class="btn btn-primary">Open Run 02 Report &rarr;</a>
          <div style="display:flex;gap:8px">
            <a href="results-02-04-metrics_wide.csv" download class="btn" title="Download Flattened Metrics CSV">Metrics CSV</a>
            <a href="results-02-05-scores.csv" download class="btn" title="Download Scores CSV">Scores CSV</a>
          </div>
        </div>
      </div>

      <!-- Card 3: Architectural Analysis Report -->
      <div class="report-card arch">
        <div>
          <span class="report-badge">Structural Analysis &middot; Gemini 3.8 Flash</span>
          <h4>Architectural &amp; Empirical Evolution Report</h4>
          <p>
            An in-depth structural review of Go channel broadcast evolution. Unpacks protocol constructions, explicit registration/receive contracts, safe mid-flight withdrawal mechanics, O(1) control-plane state, and select compatibility.
          </p>
          <ul class="report-features">
            <li><span class="dot"></span> <strong>Protocol Taxonomy:</strong> Lock-step vs Cursor Rings vs Epoch Gates vs RCU Fanout</li>
            <li><span class="dot"></span> <strong>Zero-Allocation Analysis:</strong> Probing runtime slice escapes and interface conversions</li>
            <li><span class="dot"></span> <strong>State Machine Teardown:</strong> Safe cancellation vs deadlock on blocked receivers</li>
          </ul>
        </div>
        <div class="report-actions">
          <a href="report-01-gemini-flash-3.8-high.html" class="btn btn-primary">Read Architectural Analysis &rarr;</a>
          <a href="01-readme.md" class="btn">Extraction Tracker</a>
        </div>
      </div>
    </div>
  </section>

  <!-- LEADERBOARD -->
  <section class="block" id="leaderboard">
    <div class="kicker">Adoptability Leaderboard</div>
    <h3 class="section-title">Empirical Scorecards (116 Primitives)</h3>
    <p class="lede">
      Adoptability scores (0&ndash;100) computed from multi-dimensional statistical metrics: throughput, tail latency, allocation rate, subscriber churn resistance, fan-out scalability, and stall resilience.
    </p>

    <div class="card pad">
      <div class="toolbar">
        <input type="text" id="scoreSearch" class="search-input" placeholder="Search implementation, model, or baseline...">
        <div class="filter-pills" id="familyPills">
          <div class="pill active" data-family="all">All Families</div>
          <div class="pill" data-family="broadcast">Broadcast (Lossless/Lossy)</div>
          <div class="pill" data-family="caster">Casters (Rendezvous)</div>
          <div class="pill" data-family="scatter">Scatter (Queue)</div>
          <div class="pill" data-family="gather">Gather (Barrier)</div>
          <div class="pill" data-family="fanin">FanIn (Bounded Merge)</div>
          <div class="pill" data-family="exchange">Exchange (Pairwise)</div>
          <div class="pill" data-family="barrier">Barrier (Deadline)</div>
          <div class="pill" data-family="payload">Payload (Async)</div>
        </div>
        <div class="filter-pills" id="verdictPills">
          <div class="pill active" data-verdict="all">All Verdicts</div>
          <div class="pill" data-verdict="Strong candidate">Strong Candidate</div>
          <div class="pill" data-verdict="Situational">Situational</div>
          <div class="pill" data-verdict="Niche">Niche</div>
          <div class="pill" data-verdict="Avoid">Avoid</div>
          <div class="pill" data-verdict="Do not adopt">Do Not Adopt</div>
          <div class="pill" data-verdict="Not ranked">Not Ranked</div>
        </div>
      </div>

      <div class="tbl-wrap">
        <table class="tbl" id="scoresTable">
          <thead>
            <tr>
              <th>Family</th>
              <th>Implementation / Origin</th>
              <th>Adoptability Score</th>
              <th>Verdict</th>
              <th>Throughput</th>
              <th>Latency</th>
              <th>Memory</th>
              <th>Scaling</th>
              <th>Churn</th>
            </tr>
          </thead>
          <tbody id="scoresBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- MODEL RESPONSES (58 RESPONSES) -->
  <section class="block" id="responses">
    <div class="kicker">One-Shot Prompt Archive</div>
    <h3 class="section-title">58 AI Model Responses (03 &rarr; 58)</h3>
    <p class="lede">
      Filter, search, and inspect the raw outputs, prompt variants, extracted Go code, and chat links across all tested model providers.
    </p>

    <div class="card pad">
      <div class="toolbar">
        <input type="text" id="respSearch" class="search-input" placeholder="Filter by model, provider, Go file, or mechanism...">
        <div class="filter-pills" id="providerPills">
          <div class="pill active" data-prov="all">All Providers</div>
          <div class="pill" data-prov="OpenAI">OpenAI</div>
          <div class="pill" data-prov="Anthropic">Anthropic</div>
          <div class="pill" data-prov="Google">Google</div>
          <div class="pill" data-prov="DeepSeek">DeepSeek</div>
          <div class="pill" data-prov="Alibaba / Qwen">Qwen</div>
          <div class="pill" data-prov="Meta / Muse">Meta / Muse</div>
          <div class="pill" data-prov="xAI / Grok">Grok</div>
          <div class="pill" data-prov="Mistral AI">Mistral</div>
          <div class="pill" data-prov="NVIDIA / Nemotron">Nemotron</div>
          <div class="pill" data-prov="OpenRouter">OpenRouter</div>
        </div>
        <div class="filter-pills" id="promptPills">
          <div class="pill active" data-prompt="all">All Prompts</div>
          <div class="pill" data-prompt="Standard">Standard (02-prompt.md)</div>
          <div class="pill" data-prompt="Prompt-Mini">Mini (02-prompt-mini.md)</div>
          <div class="pill" data-prompt="Prompt-Safe">Safe (02-prompt-safe.md)</div>
        </div>
      </div>

      <div class="tbl-wrap">
        <table class="tbl" id="respTable">
          <thead>
            <tr>
              <th>#</th>
              <th>Response File</th>
              <th>Provider</th>
              <th>Prompt</th>
              <th>Extracted Primitive(s)</th>
              <th>Go Source</th>
              <th>Status / Mechanism</th>
              <th>Chat Link</th>
            </tr>
          </thead>
          <tbody id="respBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- ARCHITECTURAL PARADIGMS -->
  <section class="block" id="paradigms">
    <div class="kicker">Architectural Taxonomy</div>
    <h3 class="section-title">7 Core Concurrency Paradigms</h3>
    <p class="lede">
      How AI models attempted to evolve beyond linear rendezvous and lock-step barriers.
    </p>

    <div class="cards-grid">
      <div class="f-card info">
        <h4>1. Lock-step PubSub &amp; Barrier Rendezvous</h4>
        <p>
          Zero-allocation synchronous handoff where sender blocks until all registered subscribers acknowledge. Evolved with phase-alternating generations (<code>PhasePubSub</code>, <code>GenPubSub</code>) and atomic acknowledgement planes (<code>ChanPubSub57DeepSeek</code>).
        </p>
      </div>
      <div class="f-card info">
        <h4>2. Synchronous Rendezvous Casters</h4>
        <p>
          Dynamic subscriber registries with explicit registration/receive contracts and safe mid-flight withdrawal (<code>ChanCaster</code>, <code>SeqCaster</code>, <code>SenseCaster</code>). Zero-allocation fast-paths achieve sub-microsecond latency.
        </p>
      </div>
      <div class="f-card info">
        <h4>3. Bounded Lossy &amp; Lossless Rings</h4>
        <p>
          Sequence-stamped ring buffers with atomic cursors (<code>RingPubSub</code>, <code>RingCaster55Haiku</code>, <code>PhantomCast</code>). Eliminates sender blocking while allowing subscribers to catch up or detect overruns.
        </p>
      </div>
      <div class="f-card info">
        <h4>4. Epoch Gates &amp; Generation Signals</h4>
        <p>
          Parity-flipped generation markers and channel-close fan-out (<code>EpochBroadcast</code>, <code>EpochTape</code>, <code>EpochCaster</code>). Wakes unbounded subscribers with O(1) control-plane operations.
        </p>
      </div>
      <div class="f-card info">
        <h4>5. Nonblocking Mailbox &amp; RCU Fanout</h4>
        <p>
          Copy-on-write subscriber arrays paired with per-consumer unbounded FIFO queues (<code>QueuedFanout24Meta</code>, <code>SelectFanout</code>, <code>Fanout56MaiCode</code>). Guarantees sender non-blocking at the cost of heap allocations.
        </p>
      </div>
      <div class="f-card info">
        <h4>6. Work Distribution / Competing Consumers</h4>
        <p>
          Distributes each item to exactly one receiver (<code>ChanScatter</code>, <code>BaselineScatterQueue</code>, <code>StableRing32Nano</code>). Safe mid-flight withdrawal prevents item loss when a consumer cancels.
        </p>
      </div>
      <div class="f-card info">
        <h4>7. Signal Gathering &amp; State Beacons</h4>
        <p>
          Inverted fan-in signal collection (<code>ChanGatherer</code>) and loss-tracking atomic state beacons (<code>ChanBeacon</code>, <code>RingBeacon</code>).
        </p>
      </div>
    </div>
  </section>

  <!-- BASELINES -->
  <section class="block" id="baselines">
    <div class="kicker">Textbook Standard Practice</div>
    <h3 class="section-title">10 Staff-Engineer Baselines</h3>
    <p class="lede">
      Every scenario is anchored by a textbook baseline (see <code>baseline.go</code>) &mdash; the simplest sound design an expert staff engineer would write in a vacuum.
    </p>

    <div class="card pad">
      <div class="tbl-wrap">
        <table class="tbl">
          <thead>
            <tr>
              <th>Variant / Scenario</th>
              <th>Baseline Implementation</th>
              <th>Source File</th>
              <th>Textbook Design Rationale</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>Lock-step Pub-Sub</strong></td>
              <td><code>BaselineBarrierPubSub</code></td>
              <td><code>baseline.go</code></td>
              <td>Lossless synchronous broadcast using slice of subscriber channels with mutex barrier</td>
            </tr>
            <tr>
              <td><strong>Rendezvous Caster</strong></td>
              <td><code>BaselineRendezvousCaster</code></td>
              <td><code>baseline.go</code></td>
              <td>Rendezvous fan-out with explicit registration and per-send slice snapshot</td>
            </tr>
            <tr>
              <td><strong>Latest-Value Coalescing</strong></td>
              <td><code>BaselineLatestBroadcast</code></td>
              <td><code>baseline.go</code></td>
              <td>Single-slot atomic value buffer with rotating notification channel</td>
            </tr>
            <tr>
              <td><strong>Buffered Lossless Broadcast</strong></td>
              <td><code>BaselineBufferedBroadcast</code></td>
              <td><code>baseline.go</code></td>
              <td>Bounded per-subscriber channel buffers with backpressure blocking</td>
            </tr>
            <tr>
              <td><strong>Work Distribution</strong></td>
              <td><code>BaselineScatterQueue</code></td>
              <td><code>baseline.go</code></td>
              <td>Single-item dispatch to exactly one available worker via synchronized queue</td>
            </tr>
            <tr>
              <td><strong>Signal Gathering</strong></td>
              <td><code>BaselineGatherBarrier</code></td>
              <td><code>baseline.go</code></td>
              <td>N senders coordinate to unblock a single waiter via sync.WaitGroup / barrier</td>
            </tr>
            <tr>
              <td><strong>Async Fan-Out</strong></td>
              <td><code>BaselineFanout</code></td>
              <td><code>baseline.go</code></td>
              <td>Non-blocking worker goroutine per subscriber with unbounded channel queue</td>
            </tr>
            <tr>
              <td><strong>Unbounded Queued Fan-Out</strong></td>
              <td><code>BaselineQueuedFanout</code></td>
              <td><code>baseline_queued.go</code></td>
              <td>COW subscriber registry with non-blocking per-consumer FIFO mailboxes</td>
            </tr>
            <tr>
              <td><strong>Bounded Scatter Queue</strong></td>
              <td><code>BaselineBoundedScatterQueue</code></td>
              <td><code>baseline_scatter_bounded.go</code></td>
              <td>Fixed-ring competing consumer queue with exact overflow eviction</td>
            </tr>
            <tr>
              <td><strong>Bounded Catch-up Replay</strong></td>
              <td><code>BaselineBoundedReplay</code></td>
              <td><code>baseline_replay_test.go</code></td>
              <td>Synchronized live handoff with bounded history ring for late joiners</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- PATHOLOGY LAB -->
  <section class="block" id="pathology">
    <div class="kicker">Failure Mode Analysis</div>
    <h3 class="section-title">Pathology Lab: Concurrency Traps in AI Code</h3>
    <p class="lede">
      Testing AI code under rigorous stress, race-detection, and fuzzing uncovered fundamental failure patterns.
    </p>

    <div class="cards-grid">
      <div class="f-card danger">
        <h4>1. Concurrent Mid-Flight Withdrawal Races</h4>
        <p>
          When a receiver abandons a <code>select</code> midway through a publication round, models frequently reused channel slots or completed a stale rendezvous, causing data races or double-deliveries.
        </p>
      </div>
      <div class="f-card danger">
        <h4>2. Deadlock on Unbuffered Hub Teardown</h4>
        <p>
          Calling <code>Close()</code> while a publisher or subscriber was blocked on an unbuffered rendezvous channel routinely deadlocked because the teardown routine waited for active rounds to complete.
        </p>
      </div>
      <div class="f-card warn">
        <h4>3. Hidden Lock Contention in "Non-Blocking" Paths</h4>
        <p>
          Several designs claimed lock-free non-blocking operation, but placed <code>sync.Mutex</code> inside <code>TrySend()</code>, causing catastrophic throughput collapse under 64-subscriber fan-out.
        </p>
      </div>
      <div class="f-card warn">
        <h4>4. Silent Message Loss in Ring Buffers</h4>
        <p>
          Ring buffer outputs frequently failed to stamp items with monotonic sequences, causing slow subscribers to read torn values or skip laps without ever noticing they fell behind.
        </p>
      </div>
      <div class="f-card info">
        <h4>5. Zero-Allocation Illusions</h4>
        <p>
          Over 60% of models claimed "0 allocations per send", but runtime profiling detected hidden allocations from interface boxing (<code>any</code>), slice headers escaping to heap, or ephemeral notification channels.
        </p>
      </div>
    </div>
  </section>

  <!-- RAW DATA & DOWNLOADS -->
  <section class="block" id="downloads">
    <div class="kicker">Raw Data &amp; Verification</div>
    <h3 class="section-title">Committed Runs &amp; Datasets</h3>
    <p class="lede">
      All raw data is committed directly at repository root in the zero-padded run log (<code>results-*-*-*</code>).
    </p>

    <div class="card pad">
      <div class="tbl-wrap">
        <table class="tbl">
          <thead>
            <tr>
              <th>Run</th>
              <th>Profile</th>
              <th>Artifact File</th>
              <th>Description</th>
              <th>Verification</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-03-report.html</code></td>
              <td>Interactive Concurrency Measurement Report (2,013 records)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-03-report.html" class="btn btn-primary">Open Report</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-01-results.jsonl</code></td>
              <td>Raw JSONL benchmark records (2.59 MB)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-01-results.jsonl" download class="btn">Download JSONL</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-04-metrics_wide.csv</code></td>
              <td>Flattened wide metrics CSV (294 KB)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-04-metrics_wide.csv" download class="btn">Download CSV</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-05-scores.csv</code></td>
              <td>Adoptability scores &amp; dimension verdicts CSV</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-05-scores.csv" download class="btn">Download CSV</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-02-registry.json</code></td>
              <td>Scenario/Case/Attachment registry snapshot</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-02-registry.json" download class="btn">Download JSON</a></td>
            </tr>
            <tr>
              <td><strong>Run 01</strong></td>
              <td><span class="tag tag-prompt">standard</span></td>
              <td><code>results-01-00-manifest.json</code></td>
              <td>Run environment fingerprint &amp; SHA-256 manifest</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-01-00-manifest.json" download class="btn">Download JSON</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-03-report.html</code></td>
              <td>Interactive Concurrency Measurement Report (1,479 records)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-03-report.html" class="btn btn-primary">Open Report</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-01-results.jsonl</code></td>
              <td>Smoke / CI profile JSONL (1.53 MB)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-01-results.jsonl" download class="btn">Download JSONL</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-04-metrics_wide.csv</code></td>
              <td>Flattened wide metrics CSV (Run 02)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-04-metrics_wide.csv" download class="btn">Download CSV</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-05-scores.csv</code></td>
              <td>Adoptability scores CSV (Run 02)</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-05-scores.csv" download class="btn">Download CSV</a></td>
            </tr>
            <tr>
              <td><strong>Run 02</strong></td>
              <td><span class="tag tag-prov">quick</span></td>
              <td><code>results-02-00-manifest.json</code></td>
              <td>Run 02 environment fingerprint &amp; SHA-256 manifest</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="results-02-00-manifest.json" download class="btn">Download JSON</a></td>
            </tr>
            <tr>
              <td><strong>Analysis</strong></td>
              <td><span class="tag tag-prov">empirical</span></td>
              <td><code>report-01-gemini-flash-3.8-high.html</code></td>
              <td>Architectural &amp; Empirical Evolution Analysis</td>
              <td><span class="v-strong">PASS</span></td>
              <td><a href="report-01-gemini-flash-3.8-high.html" class="btn">Open Analysis</a></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>

</main>

<footer class="site-foot">
  <div>
    <div style="font-weight:700;color:var(--ink);margin-bottom:4px">bigbuff &middot; AI Concurrency Benchmark</div>
    <div>Copyright &copy; 2026 Joseph Cumines &middot; MIT License</div>
    <div style="margin-top:6px;max-width:520px;color:var(--ink3)">
      Flat layout constraint: all source files, reports, and manifests reside directly at the repository root to maintain sync with the original GitHub Gist.
    </div>
  </div>
  <div style="display:flex;gap:32px;flex-wrap:wrap">
    <div>
      <div style="font-weight:600;color:var(--ink);margin-bottom:8px">Reports &amp; Analysis</div>
      <div style="display:flex;flex-direction:column;gap:5px">
        <a href="results-01-03-report.html">Measurement Report (Run 01 &middot; Standard)</a>
        <a href="results-02-03-report.html">Measurement Report (Run 02 &middot; Quick)</a>
        <a href="report-01-gemini-flash-3.8-high.html">Architectural Evolution Report</a>
        <a href="results-00-00-readme.md">Run Log Documentation</a>
      </div>
    </div>
    <div>
      <div style="font-weight:600;color:var(--ink);margin-bottom:8px">Repository Links</div>
      <div style="display:flex;flex-direction:column;gap:5px">
        <a href="__GITHUB_REPO_URL__" target="_blank" rel="noopener">GitHub Repository</a>
        <a href="01-readme.md">Gist Index (01-readme.md)</a>
        <a href="README.md">README.md</a>
        <a href="measurement.md">Measurement Methodology</a>
        <a href="AGENTS.md">AGENTS.md</a>
      </div>
    </div>
  </div>
</footer>

`
