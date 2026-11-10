package bigbuff

// Client script, part 1: data and SVG helpers, overview cards and findings.

const reportJS1 = `
(function(){
'use strict';
var D = window.BIGBUFF, REG = window.BIGBUFF_REGISTRY || {scenarios:[],attachments:[],cases:[]};
var metrics = D.metrics||[], traces = D.traces||[], skipped = D.skipped||[], summary = D.summary||{}, meta = D.meta||{};
var derived = D.derived||{};
var SVGNS = 'http://www.w3.org/2000/svg';
var $ = function(id){ return document.getElementById(id); };
var esc = function(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); };
var isBaselineName = function(n){ return n.indexOf('Baseline') >= 0; };

function fmtNs(v){
  if (v == null || isNaN(v)) return '—';
  var a = Math.abs(v);
  if (a >= 1e6) return (v/1e6).toFixed(2)+' ms';
  if (a >= 1e3) return (v/1e3).toFixed(2)+' µs';
  return v.toFixed(v < 10 ? 2 : 0)+' ns';
}
function fmtBytes(v){
  if (v == null || isNaN(v)) return '—';
  if (v >= (1<<20)) return (v/(1<<20)).toFixed(1)+' MiB';
  if (v >= (1<<10)) return (v/(1<<10)).toFixed(1)+' KiB';
  return v.toFixed(0)+' B';
}
function fmtNum(v){ return v == null || isNaN(v) ? '—' : (Math.abs(v) >= 100 ? Math.round(v).toLocaleString() : v.toFixed(2)); }
function dimsOf(m){ return m.dims||{}; }
function subsOf(m){
  var d = dimsOf(m);
  if (d.subs != null) return d.subs;
  if (d.receivers != null) return d.receivers;
  if (d.senders != null) return d.senders;
  return -1;
}
function meanOf(m){ return typeof m.mean === 'number' ? m.mean : (typeof m.ops_completed === 'number' ? m.ops_completed : NaN); }
function rowsFor(metric, family, subsFilter){
  return metrics.filter(function(m){
    if (m.metric !== metric) return false;
    if (family && m.family !== family) return false;
    if (subsFilter != null && subsOf(m) !== subsFilter) return false;
    return true;
  });
}
function metricByImpl(metric, family, subsFilter){
  var out = {};
  rowsFor(metric, family, subsFilter).forEach(function(m){ out[m.impl] = m; });
  return out;
}
function palette(i){ var h = (i*47 + 187) % 360; return 'hsl('+h+',72%,64%)'; }
function svg(tag, attrs, parent){
  var el = document.createElementNS(SVGNS, tag);
  for (var k in attrs) el.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(el);
  return el;
}
var tip = $('tip');
function bindTip(el, htmlFn){
  var cached = null;
  el.addEventListener('mouseenter', function(){ cached = htmlFn(); });
  el.addEventListener('mousemove', function(ev){
    if (cached === null) cached = htmlFn();
    if (tip._lastFor !== el) { tip.innerHTML = cached; tip._lastFor = el; }
    tip.style.opacity = '1';
    var pad = 14, w = tip.offsetWidth, h = tip.offsetHeight;
    var x = ev.clientX + pad, y = ev.clientY + pad;
    if (x + w > window.innerWidth - 8) x = ev.clientX - w - pad;
    if (y + h > window.innerHeight - 8) y = ev.clientY - h - pad;
    tip.style.left = x+'px'; tip.style.top = y+'px';
  });
  el.addEventListener('mouseleave', function(){ tip.style.opacity='0'; });
}
function containerWidth(el){ return Math.max(el.clientWidth || 900, 320); }
function clear(el){ while (el.firstChild) el.removeChild(el.firstChild); }
function familyMetricName(family, kind){
  if (kind === 'throughput') return family === 'caster' ? 'send_ns_op' : (family === 'scatter' ? 'dispatch_ns_op' : (family === 'gather' ? 'wait_ns_op' : (family === 'fanin' ? 'merge_ns_op' : 'publish_ns_op')));
  if (kind === 'latency') return family === 'caster' ? 'send_latency_ns' : 'publish_latency_ns';
  return null;
}
var familyLabel = { broadcast:'Broadcast', caster:'Caster', scatter:'Scatter', gather:'Gather', payload:'Payload', fanin:'FanIn' };
var familyOrder = ['broadcast','caster','scatter','gather','payload','fanin'];
var familiesPresent = familyOrder.filter(function(f){ return (derived.scores && derived.scores[f] && Object.keys(derived.scores[f]).length) || metrics.some(function(m){return m.family===f;}); });
function derivedFamily(f){ return (derived.scores && derived.scores[f]) || {}; }
function sortedImpls(f, by){
  var rows = Object.keys(derivedFamily(f)).map(function(impl){ return derivedFamily(f)[impl]; });
  rows.sort(function(a,b){
    if (by === 'alpha') return a.impl < b.impl ? -1 : 1;
    return (b.metrics[by]||0) - (a.metrics[by]||0);
  });
  return rows.map(function(r){ return r.impl; });
}

/* ── Overview: meta chips + KPI tiles + verdict distribution ── */
(function renderOverview(){
  var chips = [];
  chips.push('generator <b>'+esc((meta.generator||'bigbuff-measure')+' v'+(meta.generator_version||'1.0.0'))+'</b>');
  chips.push('profile <b>'+esc(meta.profile||'?')+'</b>');
  chips.push('seed <b>'+esc(meta.seed)+'</b>');
  chips.push('commit <b>'+esc(String(meta.commit||'').slice(0,12))+'</b>');
  chips.push('runtime <b>'+esc(meta.go||'')+' · GOMAXPROCS='+esc(meta.gomaxprocs)+'</b>');
  chips.push('host <b>'+esc(meta.cpu||'')+'</b>');
  if (meta.timestamp) chips.push('measured <b>'+esc(String(meta.timestamp).slice(0,19).replace('T',' ')+' UTC')+'</b>');
  if (summary.duration_s != null) chips.push('battery runtime <b>'+esc(Number(summary.duration_s).toFixed(1)+'s')+'</b>');
  $('metaChips').innerHTML = chips.map(function(c){ return '<span class="chip">'+c+'</span>'; }).join('');

  var implSet = {};
  metrics.forEach(function(m){ if (m.impl) implSet[m.impl] = true; });
  var implCount = Object.keys(implSet).length;
  // Registered implementations are those measured plus any that appear only as skip records.
  var skippedOnly = 0, skippedSeen = {};
  skipped.forEach(function(s){ if (s.impl && !implSet[s.impl] && !skippedSeen[s.impl]) { skippedSeen[s.impl] = true; skippedOnly++; } });
  $('heroCount').textContent = implCount + skippedOnly;
  var commaInt = function(n){ return String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ','); };
  var scenarioSet = {};
  (REG.attachments||[]).forEach(function(a){ if (a.scenario) scenarioSet[a.scenario] = true; });
  var torn = 0, tornChecked = 0, violations = 0;
  metrics.forEach(function(m){ if (m.metric==='torn_reads'){ torn += m.violations||0; tornChecked += m.checked||0; } if (typeof m.exactly_once_violations==='number') violations += m.exactly_once_violations; });

  var kpis = [
    { v: implCount, l: 'implementations measured', s: skippedOnly + ' appear only as skip records · ' + familiesPresent.length + ' families' },
    { v: summary.records || metrics.length, l: 'records collected', s: (summary.skipped||0)+' skipped · '+(summary.failures||0)+' failures' },
    { v: Object.keys(scenarioSet).length, l: 'scenarios attached', s: (REG.attachments||[]).length+' good/bad-fit attachments' },
    { v: torn, l: 'torn payload reads', s: commaInt(tornChecked)+' multi-word loads checked' },
    { v: violations, l: 'exactly-once violations', s: 'scatter dispatch, warmup included' }
  ];
  $('kpiRow').innerHTML = kpis.map(function(k){ return '<div class="kpi"><div class="v">'+esc(commaInt(k.v))+'</div><div class="l">'+esc(k.l)+'</div><div class="s">'+esc(k.s)+'</div></div>'; }).join('');

  // Family scoreboard cards + verdict distribution
  var html = '<div class="kicker" style="margin-top:6px">Family scorecards</div><div class="findings-grid" style="margin-top:10px">';
  familiesPresent.forEach(function(f){
    var scores = derivedFamily(f);
    var impls = Object.keys(scores);
    if (!impls.length) return;
    var best = impls.map(function(i){ return scores[i]; }).sort(function(a,b){ return b.score-a.score; })[0];
    var cov = (derived.coverage && derived.coverage[f]) || {expected: impls.length, measured: impls.length, missing: []};
    html += '<div class="finding"><span class="tag">'+esc(familyLabel[f]||f)+' · '+(cov.measured||impls.length)+'/'+(cov.expected||impls.length)+' measured</span>'
      + '<h3>'+esc(best.impl)+'</h3>'
      + '<p>Adoptability <b style="color:var(--cyan)">'+best.score+'</b> — '+esc(best.verdict)+'. '
      + (cov.missing && cov.missing.length ? cov.missing.length+' expected impl(s) skipped (see skip list).' : 'Full family coverage.')
      + '</p></div>';
  });
  html += '</div>';

  // Verdict distribution histogram
  var all = [];
  familiesPresent.forEach(function(f){ Object.keys(derivedFamily(f)).forEach(function(i){ all.push(derivedFamily(f)[i].score); }); });
  var buckets = new Array(10).fill(0);
  all.forEach(function(s){ buckets[Math.min(9, Math.floor(s/10))]++; });
  var maxB = Math.max.apply(null, buckets.concat([1]));
  html += '<div class="card pad" style="margin-top:16px"><h4 style="font-size:12px;color:var(--ink2);margin-bottom:10px">Adoptability score distribution (all families)</h4><div class="histo">';
  buckets.forEach(function(b){ html += '<div style="height:'+Math.round(100*b/maxB)+'%" title="'+b+' impls"></div>'; });
  html += '</div><div class="histo-x"><span>0</span><span>10</span><span>20</span><span>30</span><span>40</span><span>50</span><span>60</span><span>70</span><span>80</span><span>90</span><span>100</span></div></div>';
  $('overview').innerHTML = html;
})();

/* ── Findings cards ── */
(function renderFindings(){
  var grid = $('findingsGrid');
  clear(grid);
  (derived.observations||[]).forEach(function(o){
    var a = document.createElement('article');
    a.className = 'finding ' + (o.kind||'');
    a.innerHTML = '<span class="tag">'+esc(o.kind||'observation')+'</span><h3>'+esc(o.title)+'</h3><p>'+esc(o.detail)+'</p>'
      + (o.anchor ? '<p style="margin-top:8px"><a href="'+esc(o.anchor)+'">See the measurement →</a></p>' : '');
    grid.appendChild(a);
  });
})();
`
