package bigbuff

// Client script, part 3: leaderboard and adoptability radar.

const reportJS3 = `

/* ── Leaderboard ── */
var lbGroup = 0, lbSort = {k:'score', dir:-1};
function lbRows(group){
  var family = group.family;
  var sc = derivedGroup(group);
  var thrName = familyMetricName(family, 'throughput');
  var latName = familyMetricName(family, 'latency');
  var thr = metricByImpl(thrName, family, family === 'fanin' ? 4 : 8);
  var lat = metricByImpl(latName, family, family === 'caster' ? 8 : 8);
  var allocs = family === 'broadcast' ? metricByImpl('publish_allocs_op', family, 8) : {};
  var stalls = metricByImpl('stalled_consumer_ops_completed', family, null);
  var rows = [];
  Object.keys(sc).forEach(function(impl){
    var s = sc[impl];
    var tm = thr[impl], lm = lat[impl], am = allocs[impl], sm = stalls[impl];
    var p99 = lm && typeof lm.p99 === 'number' ? lm.p99 : null;
    var stallOps = sm && typeof sm.ops_completed === 'number' ? sm.ops_completed : (sm ? meanOf(sm) : null);
    rows.push({
      impl: impl, score: s.score, ranked: isRanked(s), verdict: s.verdict, integrity: s.integrity,
      baseline: !!s.baseline, exposureLimited: !!s.exposure_limited, goodFit: !!s.expected_good_fit,
      thr: tm ? meanOf(tm) : null,
      ciLo: tm ? tm.ci95_low : null, ciHi: tm ? tm.ci95_high : null,
      lat: p99,
      alloc: am ? meanOf(am) : null,
      ratio: (s.metrics && typeof s.metrics.scaling_ratio_64_over_1 === 'number') ? s.metrics.scaling_ratio_64_over_1 : null,
      delivered: (s.metrics && typeof s.metrics['delivered_fraction@8'] === 'number') ? s.metrics['delivered_fraction@8'] : null,
      stall: stallOps
    });
  });
  return rows;
}
function lbRender(){
  var tabs = $('lbTabs'); clear(tabs);
  scoreGroups.forEach(function(g, gi){
    var t = document.createElement('button');
    t.className = 'tab' + (gi===lbGroup?' active':'');
    t.textContent = g.label;
    t.onclick = function(){ lbGroup = gi; lbRender(); };
    tabs.appendChild(t);
  });
  if (!scoreGroups.length) return;
  var q = ($('lbSearch').value||'').toLowerCase();
  var baseOnly = $('lbBaselinesOnly').checked, goodOnly = $('lbGoodFitOnly').checked;
  var rows = lbRows(scoreGroups[lbGroup]).filter(function(r){
    if (q && r.impl.toLowerCase().indexOf(q) < 0) return false;
    if (baseOnly && !r.baseline) return false;
    if (goodOnly && !r.goodFit) return false;
    return true;
  });
  var dir = lbSort.dir, key = lbSort.k;
  rows.sort(function(a,b){
    var av = a[key], bv = b[key];
    if (key === 'impl' || key === 'verdict') { av = String(av); bv = String(bv); return dir * (av < bv ? -1 : av > bv ? 1 : 0); }
    if (av == null) return 1; if (bv == null) return -1;
    return dir * (av - bv);
  });
  var tbody = $('lbBody'); clear(tbody);
  rows.forEach(function(r){
    var tr = document.createElement('tr');
    var vclass = 'v-avoid';
    if (r.verdict.indexOf('Strong') === 0) vclass = 'v-strong';
    else if (r.verdict.indexOf('Situational') === 0) vclass = 'v-situational';
    else if (r.verdict.indexOf('Niche') === 0) vclass = 'v-niche';
    else if (r.verdict.indexOf('Not ranked') === 0) vclass = 'v-unranked';
    var fillHue = Math.round(8 + (r.score/100)*130); // red→green
    var scoreCell = '<div class="scorebar"><div class="fill" style="width:'+r.score+'%;background:linear-gradient(90deg,hsl('+fillHue+',65%,28%),hsl('+fillHue+',80%,55%))"></div><span style="color:#eaf6ff">'+r.score+'</span></div>';
    var kind = r.baseline ? '<span class="bl">BASELINE</span>' : '<span style="color:var(--ink3);font-size:10px">measured</span>';
    if (r.exposureLimited) kind += '<span class="xl">EXP-LIMITED</span>';
    if (!r.ranked) scoreCell = '<span class="small" style="color:var(--ink3)">not ranked</span>';
    tr.innerHTML = '<td><span class="impl-name">'+esc(r.impl)+kind+'</span></td>'
      + '<td>'+scoreCell+'</td>'
      + '<td><span class="verdict '+vclass+'">'+esc(r.verdict)+'</span></td>'
      + '<td class="barnum">'+fmtNum(r.thr)+'</td>'
      + '<td class="barnum">'+(r.ciLo!=null?fmtNum(r.ciLo)+'–'+fmtNum(r.ciHi):'—')+'</td>'
      + '<td class="barnum">'+fmtNs(r.lat)+'</td>'
      + '<td class="barnum">'+(r.alloc!=null?r.alloc.toFixed(2):'—')+'</td>'
      + '<td class="barnum">'+(r.ratio!=null?r.ratio.toFixed(2)+'×':'—')+'</td>'
      + '<td class="barnum">'+(r.stall!=null?fmtNum(r.stall):'—')+'</td>'
      + '<td class="barnum">'+fmtShare(r.delivered)+'</td>'
      + '<td>'+kind+'</td>';
    tbody.appendChild(tr);
  });
  $('lbFoot').textContent = rows.length + ' implementation(s) shown · scores are relative to the group in the selected tab' + (scoreGroups[lbGroup].classed ? ' (one delivery class) and are not comparable with the other class' : '') + ' · delivered = share of published values the subscribers received in the throughput battery at 8 subscribers · CI = 95% half-width on the per-op mean · sorted by ' + key + (dir<0 ? ' (desc)' : ' (asc)');
}
document.querySelectorAll('#lbTable th').forEach(function(th){
  th.addEventListener('click', function(){
    var k = th.getAttribute('data-k');
    if (k === 'baseline' || k === 'ci') k = k === 'baseline' ? 'impl' : 'thr';
    if (lbSort.k === k) lbSort.dir *= -1; else { lbSort.k = k; lbSort.dir = -1; }
    lbRender();
  });
});
$('lbSearch').addEventListener('input', lbRender);
$('lbBaselinesOnly').addEventListener('change', lbRender);
$('lbGoodFitOnly').addEventListener('change', lbRender);

/* ── Adoptability radar ── */
var radarGroup = 0, radarSel = [];
var radarAxes = [
  {key:'throughput', label:'Throughput'},
  {key:'latency', label:'Latency'},
  {key:'allocations', label:'Allocations'},
  {key:'churn', label:'Churn'},
  {key:'scaling', label:'Scaling flatness'},
  {key:'stall_resilience', label:'Stall resilience'}
];
function radarRender(){
  var tabs = $('radarTabs'); clear(tabs);
  scoreGroups.forEach(function(g, gi){
    var t = document.createElement('button');
    t.className = 'tab' + (gi===radarGroup?' active':'');
    t.textContent = g.label;
    t.onclick = function(){ radarGroup = gi; radarSel = []; radarRender(); };
    tabs.appendChild(t);
  });
  if (!scoreGroups.length) return;
  var sc = derivedGroup(scoreGroups[radarGroup]);
  var ranked = Object.keys(sc).map(function(i){ return sc[i]; }).sort(function(a,b){ return b.score-a.score; });
  // Default selection: family champion + best baseline
  if (!radarSel.length && ranked.length) {
    radarSel.push(ranked[0].impl);
    var bl = ranked.filter(function(r){ return r.baseline; })[0];
    if (bl) radarSel.push(bl.impl);
  }
  var sel = $('radarImpl'); clear(sel);
  ranked.forEach(function(r){
    var o = document.createElement('option');
    o.value = r.impl; o.textContent = r.impl + '  ·  ' + scoreText(r);
    sel.appendChild(o);
  });
  // shortcuts
  var scBar = $('radarShortcuts'); clear(scBar);
  var mk = function(label, fn){ var b = document.createElement('button'); b.className='tab'; b.textContent = label; b.onclick = function(){ fn(); radarRender(); }; scBar.appendChild(b); };
  mk('Top overall', function(){ radarSel = ranked.length ? [ranked[0].impl] : []; });
  var blb = ranked.filter(function(r){ return r.baseline; });
  if (blb.length) mk('Best baseline', function(){ radarSel = [blb[0].impl]; });
  // A shortcut picks among implementations that were scored on that
  // dimension; one with no data for it is not a candidate.
  var bestOn = function(key){
    return ranked.filter(function(r){ return isRanked(r) && !dimMissing(r, key); })
      .sort(function(a,b){ return (b.dims[key]||0)-(a.dims[key]||0); })[0];
  };
  var flat = bestOn('scaling');
  if (flat) mk('Flattest curve', function(){ radarSel = [flat.impl]; });
  var stall = bestOn('stall_resilience');
  if (stall) mk('Stall-proof', function(){ radarSel = [stall.impl]; });
  mk('Clear', function(){ radarSel = []; });

  var holder = $('radarChart'); clear(holder);
  var W = containerWidth(holder), H = Math.min(430, Math.max(300, W*0.82));
  var cx = W/2, cy = H/2 + 6, R = Math.min(W, H)/2 - 54;
  var svgEl = svg('svg', {width:W, height:H, viewBox:'0 0 '+W+' '+H, role:'img'}, holder);
  var pt = function(i, frac){
    var ang = -Math.PI/2 + i * (2*Math.PI/radarAxes.length);
    return [cx + Math.cos(ang)*R*frac, cy + Math.sin(ang)*R*frac];
  };
  [0.25, 0.5, 0.75, 1].forEach(function(frac){
    var d = '';
    radarAxes.forEach(function(_, i){ var p = pt(i, frac); d += (i ? 'L' : 'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1) + ' '; });
    d += 'Z';
    // The outline is path data (M/L/Z), so it is a path. It used to be given
    // to a polygon's points attribute, which takes bare numbers: browsers
    // reject that and drew neither the rings nor the shapes.
    svg('path', {d: d, fill:'none', stroke:'#26315f', 'stroke-width': frac===1 ? 1.4 : 1, 'stroke-dasharray': frac===1 ? '' : '3 5'}, svgEl);
  });
  radarAxes.forEach(function(axis, i){
    var p = pt(i, 1);
    svg('line', {x1:cx, y1:cy, x2:p[0], y2:p[1], stroke:'#1f2a55', 'stroke-width':1}, svgEl);
    var lp = pt(i, 1.13);
    var t = svg('text', {x:lp[0], y:lp[1]+4, 'text-anchor': i===0||i===3 ? 'middle' : (Math.cos(-Math.PI/2 + i*Math.PI/3) > 0.1 ? 'start' : 'end'), class:'axtxt', style:'font-size:11px;fill:#a9b3d6'}, svgEl);
    t.textContent = axis.label;
  });
  radarSel.forEach(function(impl, si){
    var s = sc[impl]; if (!s) return;
    var color = palette(si + 3);
    // An unranked implementation has no dimension scores, so it gets no
    // shape. A dimension with no data is not a zero: the shape is drawn
    // through the axes that have data, and the missing axis gets a hollow
    // marker on the rim, where it cannot be read as a score.
    if (isRanked(s)) {
      var d = '';
      var first = true;
      radarAxes.forEach(function(axis, i){
        if (dimMissing(s, axis.key)) return;
        var p = pt(i, Math.max(0.02, (s.dims[axis.key]||0)/100));
        d += (first ? 'M' : 'L') + p[0].toFixed(1) + ' ' + p[1].toFixed(1) + ' ';
        first = false;
      });
      if (!first) {
        d += 'Z';
        var poly = svg('path', {d:d, class:'radar-shape', fill:color, 'fill-opacity':0.16, stroke:color, 'stroke-width':2.2, 'stroke-linejoin':'round'}, svgEl);
        if (s.baseline) poly.setAttribute('stroke-dasharray','7 5');
      }
      radarAxes.forEach(function(axis, i){
        if (dimMissing(s, axis.key)) {
          var q = pt(i, 1.0);
          var h0 = svg('circle', {cx:q[0], cy:q[1], r:4.4, fill:'none', stroke:color, 'stroke-width':1.4, 'stroke-dasharray':'2 2', class:'radar-nodata'}, svgEl);
          bindTip(h0, function(){ return '<div class="tt">'+esc(impl)+'</div><div>'+esc(axis.label)+': <b>no data</b> (left out of the score)</div>'; });
          return;
        }
        var p = pt(i, Math.max(0.02, (s.dims[axis.key]||0)/100));
        var c = svg('circle', {cx:p[0], cy:p[1], r:3.4, fill:color}, svgEl);
        bindTip(c, function(){ return '<div class="tt">'+esc(impl)+'</div><div>'+esc(axis.label)+': <b>'+(s.dims[axis.key]||0)+'</b></div>'; });
      });
    }
    // legend chips
    var lg = document.createElement('span');
    lg.innerHTML = '<i style="background:'+color+'"></i>'+esc(impl)+' <b style="color:var(--ink3)">'+scoreText(s)+'</b>';
    lg.onclick = function(){ radarSel = radarSel.filter(function(x){ return x !== impl; }); radarRender(); };
    if (!$('radarLegendHolder')) {
      var holder2 = document.createElement('div');
      holder2.id = 'radarLegendHolder'; holder2.className = 'lgnd';
      $('radarChart').appendChild(holder2);
    }
    $('radarLegendHolder').appendChild(lg);
  });
  // score rows for first selected
  var rowsBox = $('radarRows'); clear(rowsBox);
  var focus = radarSel.length ? sc[radarSel[0]] : null;
  if (focus) {
    var h = '<div class="small" style="margin-bottom:4px">Breakdown — <b style="color:var(--ink)">'+esc(focus.impl)+'</b> · score <b style="color:var(--cyan)">'+scoreText(focus)+'</b></div>';
    if (!isRanked(focus)) h += '<div class="small" style="margin:6px 0">'+(focus.unranked_reason ? 'This family is not ranked in this run: '+esc(focus.unranked_reason)+'. The axes below are empty by design.' : 'This family has '+(focus.peers||'too few')+' measured implementations, too few for percentile dimensions. The axes below are empty by design; use the measurements and their confidence intervals.')+'</div>';
    radarAxes.forEach(function(axis){
      var v = focus.dims[axis.key]||0;
      var none = dimMissing(focus, axis.key) || !isRanked(focus);
      h += '<div class="srow"><div class="dim">'+esc(axis.label)+'</div><div class="strack"><div class="sfill" style="width:'+(none?0:v)+'%"></div></div><div class="mono" style="text-align:right">'+(none?'no data':v)+'</div></div>';
    });
    if (focus.integrity === 'failed') h += '<div class="small" style="color:var(--red);margin-top:6px">Integrity failure — score capped at 50.</div>';
    rowsBox.innerHTML = h;
  } else {
    rowsBox.innerHTML = '<p class="small">Add an implementation to see its dimension breakdown.</p>';
  }
}
$('radarAdd').addEventListener('click', function(){
  var v = $('radarImpl').value;
  if (v && radarSel.indexOf(v) < 0 && radarSel.length < 4) { radarSel.push(v); radarRender(); }
});
`
