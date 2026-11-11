package bigbuff

// landingPageScript is the client script of the landing page. It was split out of
// the template that measure_site_test.go assembles.

const landingPageScript = `<script>
(function(){
  var RESPONSES = __RESPONSES_JSON__;
  var SCORES = __SCORES_JSON__;

  function esc(s){
    if (s == null) return '';
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // GitHub Pages builds every root Markdown file into a page at name.html
  // and does not serve name.md. Opened from disk, the .md file is what
  // exists. Links are written as .md and pointed at the page when hosted.
  function hostedMarkdownLinks(){
    if (!/\.github\.io$/.test(location.hostname)) return;
    document.querySelectorAll('a[href$=".md"]').forEach(function(a){
      var href = a.getAttribute('href');
      if (!/^[a-z]+:/i.test(href)) a.setAttribute('href', href.replace(/\.md$/, '.html'));
    });
  }

  // A score is relative to a delivery class where the family has them.
  var CLASS_LABEL = {'delivers-all': 'delivers every value', 'drops': 'drops or coalesces'};
  function share(v){
    if (v === '' || v == null) return '-';
    var f = parseFloat(v);
    if (isNaN(f)) return '-';
    if (f >= 1) return '100%';
    if (f >= 0.9995) return '&lt;100%';
    return (100*f).toFixed(f >= 0.1 ? 0 : f >= 0.01 ? 1 : 2) + '%';
  }

  // --- Render Leaderboard Scores Table ---
  var currentFamily = 'all';
  var currentVerdict = 'all';
  var scoreQuery = '';

  function renderScores(){
    var q = scoreQuery.toLowerCase();
    var filtered = SCORES.filter(function(r){
      if (currentFamily !== 'all' && r.family !== currentFamily) return false;
      if (currentVerdict !== 'all' && r.verdict.toLowerCase().indexOf(currentVerdict.toLowerCase()) === -1) return false;
      if (q) {
        var match = (r.impl||'').toLowerCase().indexOf(q) !== -1 ||
                    (r.family||'').toLowerCase().indexOf(q) !== -1 ||
                    (CLASS_LABEL[r['class']]||'').indexOf(q) !== -1 ||
                    (r.verdict||'').toLowerCase().indexOf(q) !== -1;
        if (!match) return false;
      }
      return true;
    });

    var html = '';
    filtered.forEach(function(r){
      var ranked = r.score !== '' && r.score != null;
      var s = parseInt(r.score, 10) || 0;
      var vClass = 'v-avoid';
      if (r.verdict.indexOf('Strong') !== -1) vClass = 'v-strong';
      else if (r.verdict.indexOf('Situational') !== -1) vClass = 'v-situational';
      else if (r.verdict.indexOf('Niche') !== -1) vClass = 'v-niche';
      else if (r.verdict.indexOf('Not ranked') !== -1) vClass = 'v-unranked';

      html += '<tr>' +
        '<td><span class="tag">' + esc(r.family) + '</span>' + (CLASS_LABEL[r['class']] ? '<div style="color:var(--ink3);font-size:11px;margin-top:3px">' + CLASS_LABEL[r['class']] + '</div>' : '') + '</td>' +
        '<td style="font-weight:600;font-family:var(--mono);font-size:12px">' + esc(r.impl) + '</td>' +
        '<td>' + (ranked ? '<div class="scorebar"><div class="fill" style="width:' + s + '%"></div><span>' + s + '</span></div>' : '<span style="color:var(--ink3)">not ranked</span>') + '</td>' +
        '<td><span class="verdict ' + vClass + '">' + esc(r.verdict) + '</span></td>' +
        '<td>' + (r.throughput || '-') + '</td>' +
        '<td>' + (r.latency || '-') + '</td>' +
        '<td>' + (r.allocations || '-') + '</td>' +
        '<td>' + (r.scaling || '-') + '</td>' +
        '<td>' + (r.churn || '-') + '</td>' +
        '<td>' + (r.stall_resilience || '-') + '</td>' +
        '<td>' + share(r.delivered_at_8) + '</td>' +
        '</tr>';
    });

    if (!filtered.length) {
      html = '<tr><td colspan="11" style="text-align:center;padding:24px;color:var(--ink3)">No implementations matched your filter criteria.</td></tr>';
    }
    document.getElementById('scoresBody').innerHTML = html;
  }

  document.getElementById('scoreSearch').addEventListener('input', function(e){
    scoreQuery = e.target.value;
    renderScores();
  });

  document.getElementById('familyPills').addEventListener('click', function(e){
    var target = e.target.closest('.pill');
    if (!target) return;
    document.querySelectorAll('#familyPills .pill').forEach(function(p){ p.classList.remove('active'); });
    target.classList.add('active');
    currentFamily = target.getAttribute('data-family');
    renderScores();
  });

  document.getElementById('verdictPills').addEventListener('click', function(e){
    var target = e.target.closest('.pill');
    if (!target) return;
    document.querySelectorAll('#verdictPills .pill').forEach(function(p){ p.classList.remove('active'); });
    target.classList.add('active');
    currentVerdict = target.getAttribute('data-verdict');
    renderScores();
  });

  // --- Render Model Responses Table ---
  var currentProv = 'all';
  var currentPrompt = 'all';
  var respQuery = '';

  function renderResponses(){
    var q = respQuery.toLowerCase();
    var filtered = RESPONSES.filter(function(r){
      if (currentProv !== 'all' && r.provider !== currentProv) return false;
      if (currentPrompt !== 'all' && r.prompt_type !== currentPrompt) return false;
      if (q) {
        var match = r.num.indexOf(q) !== -1 ||
                    r.filename.toLowerCase().indexOf(q) !== -1 ||
                    r.provider.toLowerCase().indexOf(q) !== -1 ||
                    r.impls.toLowerCase().indexOf(q) !== -1 ||
                    r.files.toLowerCase().indexOf(q) !== -1 ||
                    r.state.toLowerCase().indexOf(q) !== -1;
        if (!match) return false;
      }
      return true;
    });

    var html = '';
    filtered.forEach(function(r){
      var chatBtn = r.chat_link ? '<a href="' + esc(r.chat_link) + '" target="_blank" rel="noopener" class="tag" style="color:var(--cyan)">Chat &nearr;</a>' : '<span style="color:var(--ink3)">-</span>';
      
      // file_list is the tracker's files cell resolved to files that exist.
      var fileLinks = (r.file_list || []).map(function(f){
        return '<a href="' + esc(f) + '" style="font-family:var(--mono);font-size:11.5px">' + esc(f) + '</a>';
      }).join(', ') || '<span style="color:var(--ink3)">' + esc(r.files || '-') + '</span>';

      html += '<tr>' +
        '<td style="font-family:var(--mono);color:var(--cyan);font-weight:700">' + esc(r.num) + '</td>' +
        '<td><a href="' + esc(r.filename) + '" style="font-weight:600">' + esc(r.filename) + '</a></td>' +
        '<td><span class="tag tag-prov">' + esc(r.provider) + '</span></td>' +
        '<td><span class="tag tag-prompt">' + esc(r.prompt_type) + '</span></td>' +
        '<td style="font-weight:600">' + esc(r.impls) + '</td>' +
        '<td>' + fileLinks + '</td>' +
        '<td style="font-size:12px;color:var(--ink2)">' + esc(r.state) + '</td>' +
        '<td>' + chatBtn + '</td>' +
        '</tr>';
    });

    if (!filtered.length) {
      html = '<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--ink3)">No model responses matched your search criteria.</td></tr>';
    }
    document.getElementById('respBody').innerHTML = html;
    hostedMarkdownLinks();
  }

  document.getElementById('respSearch').addEventListener('input', function(e){
    respQuery = e.target.value;
    renderResponses();
  });

  document.getElementById('providerPills').addEventListener('click', function(e){
    var target = e.target.closest('.pill');
    if (!target) return;
    document.querySelectorAll('#providerPills .pill').forEach(function(p){ p.classList.remove('active'); });
    target.classList.add('active');
    currentProv = target.getAttribute('data-prov');
    renderResponses();
  });

  document.getElementById('promptPills').addEventListener('click', function(e){
    var target = e.target.closest('.pill');
    if (!target) return;
    document.querySelectorAll('#promptPills .pill').forEach(function(p){ p.classList.remove('active'); });
    target.classList.add('active');
    currentPrompt = target.getAttribute('data-prompt');
    renderResponses();
  });

  // Initial renders
  renderScores();
  renderResponses();
  hostedMarkdownLinks();
})();
</script>
`
