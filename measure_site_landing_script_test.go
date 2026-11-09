package bigbuff

// landingPageScript is the client script of the landing page. It was split out of
// the template that measure_site_test.go assembles.

const landingPageScript = `<script>
(function(){
  var RESPONSES = __RESPONSES_JSON__;
  var SCORES = __SCORES_JSON__;
  var REPORTS = __REPORTS_JSON__;

  function esc(s){
    if (s == null) return '';
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
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
                    (r.verdict||'').toLowerCase().indexOf(q) !== -1;
        if (!match) return false;
      }
      return true;
    });

    var html = '';
    filtered.forEach(function(r){
      var s = parseInt(r.score, 10) || 0;
      var vClass = 'v-avoid';
      if (r.verdict.indexOf('Strong') !== -1) vClass = 'v-strong';
      else if (r.verdict.indexOf('Situational') !== -1) vClass = 'v-situational';
      else if (r.verdict.indexOf('Niche') !== -1) vClass = 'v-niche';

      html += '<tr>' +
        '<td><span class="tag">' + esc(r.family) + '</span></td>' +
        '<td style="font-weight:600;font-family:var(--mono);font-size:12px">' + esc(r.impl) + '</td>' +
        '<td><div class="scorebar"><div class="fill" style="width:' + s + '%"></div><span>' + s + '</span></div></td>' +
        '<td><span class="verdict ' + vClass + '">' + esc(r.verdict) + '</span></td>' +
        '<td>' + (r.throughput || '-') + '</td>' +
        '<td>' + (r.latency || '-') + '</td>' +
        '<td>' + (r.allocations || '-') + '</td>' +
        '<td>' + (r.scaling || '-') + '</td>' +
        '<td>' + (r.churn || '-') + '</td>' +
        '</tr>';
    });

    if (!filtered.length) {
      html = '<tr><td colspan="9" style="text-align:center;padding:24px;color:var(--ink3)">No implementations matched your filter criteria.</td></tr>';
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
      
      var fileLinks = r.files.split(',').map(function(f){
        f = f.trim();
        if (f && f !== '-') {
          return '<a href="' + esc(f) + '" style="font-family:var(--mono);font-size:11.5px">' + esc(f) + '</a>';
        }
        return esc(f);
      }).join(', ');

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
})();
</script>
`
