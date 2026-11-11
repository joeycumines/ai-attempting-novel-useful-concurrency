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
    <a href="#responses">__N_RESPONSES__ Responses</a>
    <a href="#paradigms">Paradigms</a>
    <a href="#baselines">Baselines</a>
    <a href="#pathology">Pathology Lab</a>
    <a href="#downloads">Datasets</a>
  </nav>
  <div class="nav-actions">
    <a href="__LATEST_REPORT__" class="btn btn-primary" title="Open the newest measurement report">Measurement Report (Run __LATEST_RUN__)</a>
    <a href="#reports" class="btn" title="Every report and analysis">All Reports</a>
    <a href="__GITHUB_REPO_URL__" target="_blank" rel="noopener" class="btn" title="GitHub Repository">GitHub Repo</a>
  </div>
</header>

<main class="container">

  <!-- HERO -->
  <section class="hero">
    <div class="kicker"><span>Empirical Concurrency Research</span> &middot; <span>Deterministic Harness</span></div>
    <h2>AI Attempting <em>Novel Useful Concurrency</em> in Go</h2>
    <p class="lead">
      <strong>__N_RESPONSES__ responses</strong> from AI models to a one-shot prompt asking for novel, low-allocation Go concurrency primitives for synchronous channel broadcast. The implementations in them were extracted, repaired where the tracker says so, and measured: <strong>__N_IMPLS__ implementations</strong>, __N_BASELINES__ hand-written baselines among them, across __N_SCENARIOS__ scenarios.
    </p>
    <div class="hero-cta">
__HERO_BUTTONS__      <a href="#responses" class="btn">Browse __N_RESPONSES__ Responses</a>
      <a href="__GITHUB_REPO_URL__" target="_blank" rel="noopener" class="btn">GitHub Repository</a>
    </div>
    <div class="kpis">
      <div class="kpi">
        <div class="v">__N_RESPONSES__</div>
        <div class="l">Model Responses</div>
        <div class="s">Responses __RESP_FIRST__ to __RESP_LAST__, archived verbatim</div>
      </div>
      <div class="kpi">
        <div class="v">__N_IMPLS__</div>
        <div class="l">Implementations Measured</div>
        <div class="s">Run __LATEST_RUN__; __N_RANKED__ of them in families large enough to rank</div>
      </div>
      <div class="kpi">
        <div class="v">__N_SCENARIOS__ / __N_ATTACHMENTS__</div>
        <div class="l">Scenarios / Attachments</div>
        <div class="s">Each implementation is attached to the scenarios it claims</div>
      </div>
      <div class="kpi">
        <div class="v">__LATEST_RECORDS__</div>
        <div class="l">Records (Run __LATEST_RUN__)</div>
        <div class="s">__LATEST_PROFILE__ profile, __LATEST_CPUS__ logical CPUs</div>
      </div>
      <div class="kpi">
        <div class="v">__N_RUNS_VERIFIED__ / __N_RUNS__</div>
        <div class="l">Runs Verified</div>
        <div class="s">Each against the registry dump it was made from</div>
      </div>
    </div>
  </section>

  <!-- REPORTS HUB -->
  <section class="block" id="reports">
    <div class="kicker">Interactive Analysis &amp; Data Hub</div>
    <h3 class="section-title">Comprehensive Reports &amp; Artifacts</h3>
    <p class="lede">
      Every measurement run, newest first, then every written analysis. The HTML reports are self-contained (no CDNs or external assets) and open from disk via <code>file://</code>.
    </p>

    <div class="reports-grid">
__REPORT_CARDS__    </div>
  </section>

  <!-- LEADERBOARD -->
  <section class="block" id="leaderboard">
    <div class="kicker">Adoptability Leaderboard</div>
    <h3 class="section-title">Scorecards (__N_IMPLS__ implementations &middot; Run __LATEST_RUN__)</h3>
    <p class="lede">
      Scores (0&ndash;100) from run __LATEST_RUN__ (__LATEST_PROFILE__ profile). A score is a weighted mean of percentile ranks <em>within a family</em> on throughput, latency, allocations, churn, scaling and stall resilience, over the dimensions that have data and distinguish the members. Broadcast and payload are scored in two <em>delivery classes</em>: the implementations whose subscribers received every published value in the throughput battery, and those that dropped or coalesced some. Publishing is cheaper when the value need not arrive, so a score is comparable only within one class; the Delivered column gives the share that arrived at 8 subscribers. A score places an implementation among its peers on one __LATEST_CPUS__-CPU machine; it is not comparable between families, classes or runs, and it does not say an implementation is correct. Families and classes with fewer than __MIN_RANKED__ implementations are not ranked.
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
              <th>Stall</th>
              <th title="Share of the published values the subscribers received in the throughput battery, at 8 subscribers">Delivered</th>
            </tr>
          </thead>
          <tbody id="scoresBody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- MODEL RESPONSES -->
  <section class="block" id="responses">
    <div class="kicker">One-Shot Prompt Archive</div>
    <h3 class="section-title">__N_RESPONSES__ AI Model Responses (__RESP_FIRST__ &rarr; __RESP_LAST__)</h3>
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
    <h3 class="section-title">9 Core Concurrency Paradigms</h3>
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
      <div class="f-card info">
        <h4>8. Symmetric Pairwise Exchange</h4>
        <p>
          Two parties meet and swap one value each, with no sender or receiver role (<code>SymExchanger72Gemini</code>, <code>BaselineExchanger</code>). A cancelled party gets its own value back; a value is never duplicated or lost.
        </p>
      </div>
      <div class="f-card info">
        <h4>9. Deadline-Bounded Barrier Broadcast</h4>
        <p>
          A send waits for every member, but only until a deadline, and reports who did not acknowledge (<code>Cohort73Meta</code>, <code>Fanout74Deepseek</code>, <code>PhaseCaster72Gemini</code>, <code>BaselineDeadlineBarrier</code>). One stalled member delays a send by at most the deadline.
        </p>
      </div>
    </div>
  </section>

  <!-- BASELINES -->
  <section class="block" id="baselines">
    <div class="kicker">Textbook Standard Practice</div>
    <h3 class="section-title">__N_BASELINES__ Baselines for __N_SCENARIOS__ Scenarios</h3>
    <p class="lede">
      Every scenario names a baseline: the simplest sound design an experienced engineer would write for it with no prompt to be novel. The table is read from the registry dump of run __REGISTRY_RUN__.
    </p>

    <div class="card pad">
      <div class="tbl-wrap">
        <table class="tbl">
          <thead>
            <tr>
              <th>Scenario</th>
              <th>Delivery</th>
              <th>Baseline</th>
              <th>What the registry says of it</th>
            </tr>
          </thead>
          <tbody>
__BASELINE_ROWS__          </tbody>
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
Several responses claim "0 allocations per send" and allocate when measured: a per-publish node or notification channel, interface boxing (<code>any</code>), or a slice header that escapes. The tracker (<code>01-readme.md</code>) records each claim that the allocation tests contradict.
        </p>
      </div>
    </div>
  </section>

  <!-- RAW DATA & DOWNLOADS -->
  <section class="block" id="downloads">
    <div class="kicker">Raw Data &amp; Verification</div>
    <h3 class="section-title">Committed Runs &amp; Datasets</h3>
    <p class="lede">
      Every file of every run is committed at the repository root as <code>results-NN-SS-name</code>. A run is verified against the registry dump it was made from; its manifest records the SHA-256 of each file.
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
__DATASET_ROWS__          </tbody>
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
__FOOTER_REPORT_LINKS__        <a href="results-00-00-readme.md">Run Log Documentation</a>
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
