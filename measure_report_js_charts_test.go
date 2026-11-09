package bigbuff

// Client script, part 2: scaling, latency, memory and churn charts.

const reportJS2 = `

/* ── Scaling chart ── */
var scalingFamily = 'broadcast';
var scalingHidden = {};
function scalingSeries(family){
  var name = familyMetricName(family, 'throughput');
  var byImpl = {};
  rowsFor(name, family, null).forEach(function(m){
    var s = subsOf(m); if (s < 1) return;
    (byImpl[m.impl] = byImpl[m.impl] || {impl:m.impl, points:[]}).points.push({subs:s, mean:meanOf(m)});
  });
  var out = [];
  Object.keys(byImpl).forEach(function(impl){
    var p = byImpl[impl].points.sort(function(a,b){ return a.subs-b.subs; });
    if (p.length >= 2) out.push({impl:impl, points:p, score:p[p.length-1].mean});
  });
  return out;
}
function renderScaling(){
  var container = $('scalingChart'); clear(container);
  var legend = $('scalingLegend'); clear(legend);
  var tabs = $('scalingTabs'); clear(tabs);
  ['broadcast','caster','payload'].forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab' + (f===scalingFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ scalingFamily = f; scalingHidden = {}; renderScaling(); };
    tabs.appendChild(t);
  });
  var sortMode = $('scalingSort').value;
  var series = scalingSeries(scalingFamily);
  if (sortMode === 'ratio') {
    series.forEach(function(s){ s.ratio = s.points[0].mean > 0 ? s.points[s.points.length-1].mean / s.points[0].mean : 1; });
    series.sort(function(a,b){ return a.ratio - b.ratio; });
  } else if (sortMode === 'alpha') {
    series.sort(function(a,b){ return a.impl < b.impl ? -1 : 1; });
  } else {
    series.sort(function(a,b){ return a.score - b.score; });
  }
  var topOnly = $('scalingTopOnly').checked;
  var fastest = Object.keys(series).length;
  if (topOnly) {
    var byMean = series.slice().sort(function(a,b){ return a.score-b.score; });
    var keep = {}; byMean.slice(0,12).forEach(function(s){ keep[s.impl] = true; });
    series = series.filter(function(s){ return keep[s.impl]; });
  }
  fastest = series.length;
  if (!series.length) { container.innerHTML = '<p class="small">No multi-point sweeps for this family in this profile.</p>'; return; }

  var allSubs = {}, allVals = [];
  series.forEach(function(s){ s.points.forEach(function(p){ allSubs[p.subs] = true; allVals.push(p.mean); }); });
  var subsTicks = Object.keys(allSubs).map(Number).sort(function(a,b){ return a-b; });
  var finVals = allVals.filter(function(v){ return isFinite(v) && v > 0; });
  var minV = finVals.length ? Math.min.apply(null, finVals) : 1e-9;
  var maxV = finVals.length ? Math.max.apply(null, finVals) : 1;
  if (!(minV > 0)) minV = 1e-9;           // log scale cannot see zero/NaN
  if (!(maxV > minV)) maxV = minV * 10;   // degenerate spread
  var minL = Math.log10(minV*0.7), maxL = Math.log10(maxV*1.3);
  var W = containerWidth(container), H = 380, padL = 64, padR = 14, padT = 16, padB = 44;
  var subsSpan = Math.log2(subsTicks[subsTicks.length-1]) - Math.log2(subsTicks[0]);
  var xOf = function(subs){ return subsSpan > 0 ? padL + (Math.log2(subs) - Math.log2(subsTicks[0])) / subsSpan * (W - padL - padR) : (padL + W - padR) / 2; };
  var yOf = function(v){ return padT + (maxL - Math.log10(Math.max(v, 1e-9))) / (maxL - minL) * (H - padT - padB); };

  var svgEl = svg('svg', {width:W, height:H, viewBox:'0 0 '+W+' '+H, role:'img'}, container);
  // y grid at decades — bounds must stay finite (minV may be exactly 0 in the data)
  var dLo = Math.ceil(Math.log10(minV)), dHi = Math.floor(Math.log10(maxV));
  for (var d = dLo; isFinite(d) && d <= dHi && d < dLo + 40; d++) {
    var y = yOf(Math.pow(10, d));
    if (!isFinite(y)) continue;
    svg('line', {x1:padL, y1:y, x2:W-padR, y2:y, class:'grid'}, svgEl);
    var t = svg('text', {x:padL-8, y:y+3, 'text-anchor':'end', class:'axtxt'}, svgEl);
    t.textContent = fmtNs(Math.pow(10, d));
  }
  svg('line', {x1:padL, y1:H-padB, x2:W-padR, y2:H-padB, class:'ax'}, svgEl);
  svg('line', {x1:padL, y1:padT, x2:padL, y2:H-padB, class:'ax'}, svgEl);
  subsTicks.forEach(function(s){
    var x = xOf(s);
    svg('line', {x1:x, y1:H-padB, x2:x, y2:H-padB+5, class:'ax'}, svgEl);
    var t = svg('text', {x:x, y:H-padB+18, 'text-anchor':'middle', class:'axtxt'}, svgEl);
    t.textContent = s;
  });
  var xt = svg('text', {x:(W+padL)/2, y:H-6, 'text-anchor':'middle', class:'axtxt'}, svgEl); xt.textContent = 'fan-out (log₂)';
  var yt = svg('text', {x:14, y:(H)/2, 'text-anchor':'middle', class:'axtxt', transform:'rotate(-90 14 '+(H/2)+')'}, svgEl); yt.textContent = 'ns per operation (log₁₀)';

  series.forEach(function(s, i){
    if (scalingHidden[s.impl]) return;
    var color = palette(i);
    var d = '';
    s.points.forEach(function(p, j){ d += (j ? 'L' : 'M') + xOf(p.subs).toFixed(1) + ' ' + yOf(p.mean).toFixed(1) + ' '; });
    var base = isBaselineName(s.impl);
    var path = svg('path', {d:d, fill:'none', stroke:color, 'stroke-width':2, 'stroke-linejoin':'round', 'stroke-linecap':'round'}, svgEl);
    if (base) path.setAttribute('stroke-dasharray', '7 5');
    s.points.forEach(function(p){
      var c = svg('circle', {cx:xOf(p.subs), cy:yOf(p.mean), r:3.6, fill:color, stroke:'#0b1020', 'stroke-width':1.2}, svgEl);
      bindTip(c, function(){ return '<div class="tt">'+esc(s.impl)+'</div><div class="mono">'+p.subs+' subscribers · '+fmtNs(p.mean)+'/op</div>'; });
    });
  });

  // Legend chips
  series.forEach(function(s, i){
    var chip = document.createElement('span');
    chip.className = scalingHidden[s.impl] ? 'off' : '';
    chip.innerHTML = '<i style="background:'+palette(i)+(isBaselineName(s.impl)?';outline:1px dashed #0b1020':'')+'"></i>'+esc(s.impl);
    chip.onclick = function(){ scalingHidden[s.impl] = !scalingHidden[s.impl]; renderScaling(); };
    legend.appendChild(chip);
  });
}
$('scalingTopOnly').addEventListener('change', renderScaling);
$('scalingSort').addEventListener('change', renderScaling);

/* ── Latency chart ── */
var latFamily = 'broadcast';
function renderLatency(){
  var container = $('latChart'); clear(container);
  var tabs = $('latTabs'); clear(tabs);
  ['broadcast','caster','payload'].forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab' + (f===latFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ latFamily = f; renderLatency(); };
    tabs.appendChild(t);
  });
  var name = familyMetricName(latFamily, 'latency');
  var rows = rowsFor(name, latFamily, 8).map(function(m){
    return {impl: m.impl, p50: m.p50, p90: m.p90, p99: m.p99, max: m.max, mean: m.mean, ciLo: m.ci95_low, ciHi: m.ci95_high};
  }).filter(function(r){ return r.p99 != null; });
  rows.sort(function(a,b){ return a.p99 - b.p99; });
  var top = rows.slice(0, 26);
  if (!top.length) { container.innerHTML = '<p class="small">No latency records for this family in this profile.</p>'; return; }
  var maxV = Math.max.apply(null, top.map(function(r){ return r.max; })) * 1.05;
  var W = containerWidth(container), rowH = 27, padL = Math.min(300, Math.floor(W*0.36)), padR = 84, padT = 8;
  var H = top.length * rowH + padT + 26;
  // Name column: keep every label inside [0, padL-10] so nothing clips at narrow widths
  // (11px mono ≈ 6.6px/char); cap at 34 chars for wide layouts.
  var maxName = Math.max(8, Math.min(34, Math.floor((padL - 14) / 6.6)));
  var svgEl = svg('svg', {width:W, height:H, viewBox:'0 0 '+W+' '+H, role:'img'}, container);
  var xOf = function(v){ return padL + v/maxV * (W - padL - padR); };
  top.forEach(function(r, i){
    var y = padT + i*rowH + rowH/2;
    var t = svg('text', {x:padL-10, y:y+4, 'text-anchor':'end', class:'axtxt', style:'font-size:11px'}, svgEl);
    t.textContent = r.impl.length > maxName ? r.impl.slice(0, maxName-1)+'…' : r.impl;
    // p99 bar
    svg('rect', {x:padL, y:y-8, width:Math.max(2, xOf(r.p99)-padL), height:16, rx:4, fill:'url(#latGrad)'}, svgEl);
    // p50 / p90 markers
    svg('circle', {cx:xOf(r.p50), cy:y, r:4.5, fill:'#34d399', stroke:'#04120a', 'stroke-width':1}, svgEl);
    svg('circle', {cx:xOf(r.p90), cy:y, r:4.5, fill:'#fbbf24', stroke:'#241a04', 'stroke-width':1}, svgEl);
    svg('circle', {cx:xOf(r.p99), cy:y, r:5.5, fill:'#fb7185', stroke:'#2a060d', 'stroke-width':1}, svgEl);
    var v = svg('text', {x:Math.min(xOf(r.max)+6, W-padR+4), y:y+4, class:'axtxt', style:'fill:#e8ecf8'}, svgEl);
    v.textContent = fmtNs(r.p99);
    var hit = svg('rect', {x:0, y:y-rowH/2, width:W, height:rowH, fill:'transparent'}, svgEl);
    bindTip(hit, function(){
      return '<div class="tt">'+esc(r.impl)+'</div><div class="mono">mean '+fmtNs(r.mean)+' · p50 '+fmtNs(r.p50)+' · p90 '+fmtNs(r.p90)+'</div><div class="mono">p99 '+fmtNs(r.p99)+' · max '+fmtNs(r.max)+'</div><div>95% CI '+fmtNs(r.ciLo)+' – '+fmtNs(r.ciHi)+'</div>';
    });
  });
  var defs = svg('defs', {}, svgEl);
  var grad = svg('linearGradient', {id:'latGrad', x1:'0', y1:'0', x2:'1', y2:'0'}, defs);
  svg('stop', {offset:'0%', 'stop-color':'#155e75'}, grad); svg('stop', {offset:'60%', 'stop-color':'#0e7490'}, grad); svg('stop', {offset:'100%', 'stop-color':'#22d3ee'}, grad);
  var lgFull = '● p50   ● p90   ● p99 (bar end)   sorted by p99, best 26 shown';
  // 10px mono ≈ 6px/char; when one line would run past the right edge (narrow viewports),
  // split into two legend lines that both start at x=8 so nothing clips.
  if (padL + lgFull.length * 6 <= W - 4) {
    var lg = svg('text', {x:padL, y:H-6, class:'axtxt'}, svgEl);
    lg.textContent = lgFull;
  } else {
    var lg1 = svg('text', {x:8, y:H-17, class:'axtxt'}, svgEl);
    lg1.textContent = '● p50   ● p90   ● p99 (bar end)';
    var lg2 = svg('text', {x:8, y:H-5, class:'axtxt'}, svgEl);
    lg2.textContent = 'sorted by p99, best 26 shown';
  }
}

/* ── Memory & allocations ── */
var memFamily = 'broadcast';
function renderMemory(){
  var tabs = $('memTabs'); clear(tabs);
  ['broadcast'].forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab active'; t.textContent = familyLabel[f];
    tabs.appendChild(t);
  });
  var grid = $('memGrid');
  var allocName = 'publish_allocs_op';
  var nsName = familyMetricName(memFamily, 'throughput');
  var byImpl = metricByImpl(allocName, memFamily, 8);
  var nsByImpl = metricByImpl(nsName, memFamily, 8);
  var pts = [];
  Object.keys(byImpl).forEach(function(impl){
    var a = meanOf(byImpl[impl]);
    var n = nsByImpl[impl] ? meanOf(nsByImpl[impl]) : NaN;
    if (!isNaN(a) && !isNaN(n)) pts.push({impl:impl, alloc:a, ns:n});
  });
  var scatter = $('allocScatter'); clear(scatter);
  if (pts.length) {
    pts.sort(function(a,b){ return a.alloc-b.alloc; });
    var W = containerWidth(scatter), H = 320, padL = 52, padR = 12, padT = 12, padB = 40;
    var maxA = Math.max.apply(null, pts.map(function(p){return p.alloc;}).concat([1]));
    var minN = Math.min.apply(null, pts.map(function(p){return p.ns;})), maxN = Math.max.apply(null, pts.map(function(p){return p.ns;}));
    if (!isFinite(minN) || minN <= 0) minN = 1;
    if (!isFinite(maxN) || maxN <= minN) maxN = minN * 10;
    var maxA10 = Math.log10(maxA) + 0.4;
    var minL = Math.log10(Math.max(1, minN*0.6)), maxL = Math.log10(maxN*1.4);
    var xOf = function(a){ return padL + (maxA10 === 0 ? 0 : Math.log10(a+1e-9) < 0 ? 0 : Math.log10(a+1e-9)/maxA10) * (W-padL-padR); };
    var yOf = function(v){ return padT + (maxL - Math.log10(Math.max(v, 1e-9))) / (maxL - minL) * (H - padT - padB); };
    var svgEl = svg('svg', {width:W, height:H, viewBox:'0 0 '+W+' '+H, role:'img'}, scatter);
    svg('line', {x1:padL, y1:H-padB, x2:W-padR, y2:H-padB, class:'ax'}, svgEl);
    svg('line', {x1:padL, y1:padT, x2:padL, y2:H-padB, class:'ax'}, svgEl);
    var xt = svg('text', {x:(W+padL)/2, y:H-4, 'text-anchor':'middle', class:'axtxt'}, svgEl); xt.textContent = 'allocations per publish (log)';
    var yt = svg('text', {x:13, y:H/2, 'text-anchor':'middle', class:'axtxt', transform:'rotate(-90 13 '+(H/2)+')'}, svgEl); yt.textContent = 'ns per operation (log₁₀)';
    pts.forEach(function(p, i){
      var c = svg('circle', {cx:xOf(p.alloc), cy:yOf(p.ns), r:5, fill:palette(i), 'fill-opacity':0.85, stroke:'#04070f', 'stroke-width':1}, svgEl);
      bindTip(c, function(){ return '<div class="tt">'+esc(p.impl)+'</div><div class="mono">'+p.alloc.toFixed(2)+' allocs/op · '+fmtNs(p.ns)+'/op</div>'; });
    });
  } else {
    scatter.innerHTML = '<p class="small">Allocation records not present for this family.</p>';
  }
  var memBars = $('memBars'); clear(memBars);
  var memRows = rowsFor('mem_per_subscriber_bytes', 'broadcast', 8).map(function(m){ return {impl:m.impl, bytes:meanOf(m)}; }).filter(function(r){ return !isNaN(r.bytes); });
  memRows.sort(function(a,b){ return b.bytes-a.bytes; });
  var top = memRows.slice(0, 14);
  var maxB = Math.max.apply(null, top.map(function(r){return r.bytes;}).concat([1]));
  top.forEach(function(r){
    var row = document.createElement('div');
    row.className = 'bar-row';
    row.innerHTML = '<div class="mono" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink2)" title="'+esc(r.impl)+'">'+esc(r.impl)+'</div>'
      + '<div style="height:10px;background:#1a2350;border-radius:5px;overflow:hidden"><div style="height:100%;width:'+Math.max(2, 100*r.bytes/maxB).toFixed(1)+'%;background:linear-gradient(90deg,#4c1d95,#a78bfa)"></div></div>'
      + '<div class="mono" style="text-align:right">'+fmtBytes(r.bytes)+'</div>';
    bindTip(row, function(){ return '<div class="tt">'+esc(r.impl)+'</div><div class="mono">'+fmtBytes(r.bytes)+' retained per idle subscriber (post-GC)</div>'; });
    memBars.appendChild(row);
  });
  var zero = $('zeroSubChart'); clear(zero);
  var zeroRows = rowsFor('zero_subscriber_ns_op', 'broadcast', null).map(function(m){ return {impl:m.impl, ns:meanOf(m)}; }).filter(function(r){ return !isNaN(r.ns); });
  zeroRows.sort(function(a,b){ return a.ns-b.ns; });
  var zmax = Math.max.apply(null, zeroRows.map(function(r){return r.ns;}).concat([1]));
  zeroRows.forEach(function(r){
    var row = document.createElement('div');
    row.className = 'bar-row zero';
    row.innerHTML = '<div class="mono" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink2)" title="'+esc(r.impl)+'">'+esc(r.impl)+'</div>'
      + '<div style="height:8px;background:#1a2350;border-radius:4px;overflow:hidden"><div style="height:100%;width:'+Math.max(2, 100*r.ns/zmax).toFixed(1)+'%;background:#155e75"></div></div>'
      + '<div class="mono" style="text-align:right">'+fmtNs(r.ns)+'</div>';
    zero.appendChild(row);
  });
}

/* ── Churn chart ── */
var churnFamily = 'broadcast';
function renderChurn(){
  var container = $('churnChart'); clear(container);
  var tabs = $('churnTabs'); clear(tabs);
  familyOrder.forEach(function(f){
    var has = rowsFor('churn_ns_op', f, null).length > 0;
    if (!has) return;
    var t = document.createElement('button');
    t.className = 'tab' + (f===churnFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ churnFamily = f; renderChurn(); };
    tabs.appendChild(t);
  });
  var rows = rowsFor('churn_ns_op', churnFamily, null).map(function(m){ return {impl:m.impl, ns:meanOf(m)}; }).filter(function(r){ return !isNaN(r.ns); });
  rows.sort(function(a,b){ return a.ns-b.ns; });
  if (!rows.length) { container.innerHTML = '<p class="small">No churn records for this family.</p>'; return; }
  var maxV = Math.max.apply(null, rows.map(function(r){return r.ns;}));
  rows.forEach(function(r){
    var row = document.createElement('div');
    row.className = 'bar-row churn';
    row.innerHTML = '<div class="mono" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink2)" title="'+esc(r.impl)+'">'+esc(r.impl)+'</div>'
      + '<div style="height:11px;background:#1a2350;border-radius:5px;overflow:hidden"><div style="height:100%;width:'+Math.max(2, 100*r.ns/maxV).toFixed(1)+'%;background:linear-gradient(90deg,#134e4a,#2dd4bf)"></div></div>'
      + '<div class="mono" style="text-align:right">'+fmtNs(r.ns)+'</div>';
    container.appendChild(row);
  });
}
`
