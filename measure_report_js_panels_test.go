package bigbuff

// Client script, part 4: pathology lab, correctness, coverage, downloads, methodology and navigation.

const reportJS4 = `

/* ── Pathology Lab ── */
var pathoTraces = {};
traces.forEach(function(t){
  if (t.trace === 'stalled_consumer' && t.family === 'broadcast') {
    (pathoTraces[t.impl] = pathoTraces[t.impl] || {})[t.trace] = t;
  }
});
var pathoImpls = Object.keys(pathoTraces).sort();
var pathoA = 'ChanPubSub (Baseline)', pathoB = 'ChanLatest (03-ChatGPT)';
if (pathoImpls.indexOf(pathoA) < 0) pathoA = pathoImpls[0];
if (pathoImpls.indexOf(pathoB) < 0) pathoB = pathoImpls[1] || pathoImpls[0];
var stallMetrics = {};
rowsFor('stalled_consumer_ops_completed', 'broadcast', null).forEach(function(m){ stallMetrics[m.impl] = m; });
var pathoProgress = 0, pathoPlaying = false, pathoLastTs = 0, pathoSpeed = 1;
var pathoCanvas = {};

function pathoCardHTML(impl, side){
  var t = pathoTraces[impl] && pathoTraces[impl].stalled_consumer;
  var m = stallMetrics[impl] || {};
  var outcome = t ? t.outcome : 'no-trace';
  var ops = t ? t.ops_completed : (typeof m.ops_completed === 'number' ? m.ops_completed : '?');
  var retained = typeof m.approx_retained_bytes === 'number' ? m.approx_retained_bytes : 0;
  var fastMin = typeof m.fast_min_delivered === 'number' ? m.fast_min_delivered : null;
  return '<div class="patho-card"><h4 class="mono" style="font-size:12px">'+esc(impl)+'</h4>'
    + '<span class="outcome outcome-'+esc(outcome === 'completed' ? 'completed' : (outcome === 'publisher_blocked' ? 'publisher_blocked' : 'publisher_panicked'))+'">'+esc(outcome.replace(/_/g,' '))+'</span>'
    + '<canvas id="swim-'+side+'" width="640" height="170" style="width:100%;height:170px;margin-top:10px;background:#080d20;border:1px solid var(--line);border-radius:10px" role="img" aria-label="Swimlane trace for '+esc(impl)+'"></canvas>'
    + '<div class="small" style="margin-top:8px;line-height:1.8">ops completed: <b class="mono" style="color:var(--ink)">'+esc(ops)+'</b> / '+esc(t ? t.ops_planned : (m.ops_planned||'?'))
    + ' · retained: <b class="mono" style="color:'+(retained > 0 ? 'var(--amber)' : 'var(--green)')+'">'+fmtBytes(retained)+'</b><br>'
    + (fastMin != null ? 'healthy lanes delivered ≥ <b class="mono" style="color:var(--ink)">'+esc(fastMin)+'</b> each' : 'no healthy-lane count recorded')
    + '</div></div>';
}
function drawSwim(side){
  var canvas = $('swim-'+side); if (!canvas) return;
  var impl = side === 'A' ? pathoA : pathoB;
  var t = pathoTraces[impl] && pathoTraces[impl].stalled_consumer;
  var ctx = canvas.getContext && canvas.getContext('2d');
  if (!ctx) return;
  var W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);
  if (!t) {
    ctx.fillStyle = '#6b769e'; ctx.font = '12px ui-monospace, monospace';
    ctx.fillText('no trace captured for this implementation in this profile', 20, H/2);
    return;
  }
  var lanes = t.subs || 3; // includes stalled lane as the last lane
  var padL = 64, padR = 46, padT = 18, padB = 22;
  var laneH = (H - padT - padB) / (lanes + 1);
  var dur = Math.max(1, t.duration_ns || 1);
  var tMax = dur; // shared fraction applied by playhead
  // lane labels
  ctx.font = '10px ui-monospace, monospace';
  ctx.textAlign = 'left';
  ctx.fillStyle = '#22d3ee'; ctx.fillText('PUBLISHER', 8, padT + laneH/2 + 3);
  for (var i = 1; i < lanes; i++) {
    ctx.fillStyle = '#a9b3d6';
    ctx.fillText('sub ' + i, 8, padT + (i+0.5)*laneH + 3);
  }
  ctx.fillStyle = '#f87171'; ctx.fillText('STALLED ✕', 8, padT + (lanes+0.5)*laneH + 3);
  // stalled lane hatch
  ctx.strokeStyle = 'rgba(248,113,113,.35)'; ctx.setLineDash([4,4]);
  ctx.beginPath(); ctx.moveTo(padL, padT + (lanes+0.5)*laneH); ctx.lineTo(W-padR, padT + (lanes+0.5)*laneH); ctx.stroke();
  ctx.setLineDash([]);
  // lane separators
  ctx.strokeStyle = 'rgba(31,42,85,.6)';
  for (var li = 0; li <= lanes; li++) {
    var y = padT + li*laneH + laneH/2;
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W-padR, y); ctx.stroke();
  }
  var playT = pathoProgress * tMax;
  // events up to playhead
  var evs = t.events || [];
  for (var i = 0; i < evs.length; i++) {
    var e = evs[i];
    if (e.T > playT) break;
    var x = padL + (e.T / tMax) * (W - padL - padR);
    var lane = e.Lane || 0;
    var y = padT + (lane === 0 ? laneH/2 : (lane+0.5)*laneH);
    ctx.beginPath(); ctx.arc(x, y, e.Kind === 'pub' ? 4 : 3, 0, Math.PI*2);
    ctx.fillStyle = e.Kind === 'pub' ? '#22d3ee' : '#34d399';
    ctx.fill();
  }
  // blocked marker at publisher lane
  if (t.outcome === 'publisher_blocked' && pathoProgress >= 1) {
    var bx = W - padR;
    ctx.strokeStyle = '#f87171'; ctx.lineWidth = 2.4;
    ctx.beginPath(); ctx.moveTo(bx-9, padT + laneH/2 - 7); ctx.lineTo(bx+5, padT + laneH/2 + 7); ctx.moveTo(bx+5, padT + laneH/2 - 7); ctx.lineTo(bx-9, padT + laneH/2 + 7); ctx.stroke();
    ctx.fillStyle = '#fca5a5'; ctx.font = 'bold 10px ui-monospace, monospace'; ctx.textAlign = 'center';
    ctx.fillText('PARKED', bx-2, padT + laneH/2 - 12);
    ctx.textAlign = 'left';
  }
  // playhead line
  var px = padL + pathoProgress * (W - padL - padR);
  ctx.strokeStyle = 'rgba(232,236,248,.35)'; ctx.setLineDash([2,3]);
  ctx.beginPath(); ctx.moveTo(px, padT-4); ctx.lineTo(px, H-padB+4); ctx.stroke();
  ctx.setLineDash([]);
  // axis endpoints
  ctx.fillStyle = '#6b769e'; ctx.font = '9px ui-monospace, monospace';
  ctx.fillText('0', padL, H-6); ctx.textAlign = 'right'; ctx.fillText((dur/1e6).toFixed(1)+' ms', W-padR+42, H-6); ctx.textAlign = 'left';
}
function pathoRenderCards(){
  $('pathoCards').innerHTML = pathoCardHTML(pathoA, 'A') + pathoCardHTML(pathoB, 'B');
  drawSwim('A'); drawSwim('B');
}
function pathoFillSelects(){
  ['pathoA','pathoB'].forEach(function(id){
    var sel = $(id); clear(sel);
    pathoImpls.forEach(function(impl){
      var o = document.createElement('option'); o.value = impl; o.textContent = impl;
      if ((id === 'pathoA' ? pathoA : pathoB) === impl) o.selected = true;
      sel.appendChild(o);
    });
  });
}
function pathoTick(ts){
  if (!pathoPlaying) return;
  if (!pathoLastTs) pathoLastTs = ts;
  var dt = ts - pathoLastTs; pathoLastTs = ts;
  pathoProgress += (dt / 4200) * pathoSpeed; // full trace ≈ 4.2s at 1×
  if (pathoProgress >= 1) { pathoProgress = 1; pathoPlaying = false; }
  $('pathoScrub').value = Math.round(pathoProgress * 1000);
  drawSwim('A'); drawSwim('B');
  if (pathoPlaying) requestAnimationFrame(pathoTick);
}
$('pathoPlay').addEventListener('click', function(){
  if (pathoProgress >= 1) pathoProgress = 0;
  pathoPlaying = true; pathoLastTs = 0;
  requestAnimationFrame(pathoTick);
});
$('pathoPause').addEventListener('click', function(){ pathoPlaying = false; });
$('pathoSpeed').addEventListener('change', function(){ pathoSpeed = parseFloat(this.value); });
$('pathoScrub').addEventListener('input', function(){ pathoProgress = this.value/1000; pathoPlaying = false; drawSwim('A'); drawSwim('B'); });
$('pathoA').addEventListener('change', function(){ pathoA = this.value; pathoProgress = 0; pathoRenderCards(); });
$('pathoB').addEventListener('change', function(){ pathoB = this.value; pathoProgress = 0; pathoRenderCards(); });

/* retained bytes bars */
(function renderRetain(){
  var holder = $('retainBars'); clear(holder);
  var rows = rowsFor('stalled_consumer_ops_completed', 'broadcast', null)
    .map(function(m){ return {impl:m.impl, bytes:(typeof m.approx_retained_bytes==='number'?m.approx_retained_bytes:0), outcome:m.outcome||'?'}; })
    .sort(function(a,b){ return b.bytes - a.bytes; });
  var nonzero = rows.filter(function(r){ return r.bytes > 0; });
  var zeros = rows.length - nonzero.length;
  var maxB = Math.max.apply(null, nonzero.map(function(r){return r.bytes;}).concat([1]));
  nonzero.slice(0, 22).forEach(function(r){
    var row = document.createElement('div');
    row.className = 'bar-row retain';
    var oc = r.outcome === 'completed' ? '<span style="color:var(--green)">completed</span>' : '<span style="color:#fca5a5">blocked</span>';
    row.innerHTML = '<div class="mono" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink2)" title="'+esc(r.impl)+'">'+esc(r.impl)+'</div>'
      + '<div style="height:10px;background:#1a2350;border-radius:5px;overflow:hidden"><div style="height:100%;width:'+Math.max(2, 100*r.bytes/maxB).toFixed(1)+'%;background:linear-gradient(90deg,#7c2d12,#fb923c)"></div></div>'
      + '<div class="mono" style="text-align:right">'+fmtBytes(r.bytes)+'</div><div>'+oc+'</div>';
    holder.appendChild(row);
  });
  var note = document.createElement('p');
  note.className = 'small';
  note.style.marginTop = '8px';
  note.textContent = zeros + ' implementation(s) retained nothing post-GC (drop/coalesce by design). Bars sorted by retained bytes, top ' + Math.min(nonzero.length, 22) + ' shown.';
  holder.appendChild(note);
})();

/* ── Correctness panel ── */
(function renderCorrectness(){
  var torn = rowsFor('torn_reads', 'payload', null).map(function(m){
    return {impl:m.impl, violations:m.violations||0, checked:m.checked||0};
  }).sort(function(a,b){ return b.violations - a.violations || b.checked - a.checked; });
  var totalTorn = torn.reduce(function(s,r){ return s+r.violations; }, 0);
  var totalChecked = torn.reduce(function(s,r){ return s+r.checked; }, 0);
  var tHTML = '<div style="display:flex;align-items:baseline;gap:14px;margin-bottom:8px"><span class="pill '+(totalTorn===0?'pill-pass':'pill-bad')+'">'+(totalTorn===0?'PASS':'FAIL')+'</span><span class="small">'+fmtNum(totalChecked)+' checked loads · '+totalTorn+' torn</span></div>';
  tHTML += '<div style="max-height:260px;overflow:auto">';
  torn.forEach(function(r){
    var ok = r.violations === 0;
    tHTML += '<div style="display:grid;grid-template-columns:1fr auto;gap:8px;font-size:11.5px;padding:3px 0;border-bottom:1px solid var(--line)"><span class="mono" style="color:'+(ok?'var(--ink2)':'var(--red)')+'">'+esc(r.impl)+'</span><span class="mono" style="color:'+(ok?'var(--green)':'var(--red)')+'">'+(ok ? '✓ 0 / '+fmtNum(r.checked) : r.violations+' TORN / '+fmtNum(r.checked))+'</span></div>';
  });
  tHTML += '</div>';
  $('tornTable').innerHTML = tHTML;

  var cwd = rowsFor('withdrawal_compensation_success', 'caster', null).map(function(m){
    return {impl:m.impl, success:meanOf(m), rounds:(m.dims && m.dims.rounds) || 0, medianNs:m.median_round_ns};
  }).sort(function(a,b){ return a.success - b.success; });
  var cHTML = '';
  cwd.forEach(function(r){
    var pct = Math.round(100*r.success);
    var ok = r.success >= 1;
    cHTML += '<div style="margin:5px 0"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px"><span class="mono" style="color:var(--ink2)">'+esc(r.impl)+'</span><span class="mono" style="color:'+(ok?'var(--green)':'var(--red)')+'">'+pct+'% · '+fmtNs(r.medianNs||0)+'/round</span></div>'
      + '<div style="height:8px;background:#1a2350;border-radius:4px;overflow:hidden"><div style="height:100%;width:'+pct+'%;background:'+(ok?'linear-gradient(90deg,#065f46,#34d399)':'#7f1d1d')+'"></div></div></div>';
  });
  $('cwdChart').innerHTML = cHTML || '<p class="small">No withdrawal records.</p>';

  var failures = (summary && typeof summary.failures === 'number') ? summary.failures : 0;
  var records = (summary && summary.records) || metrics.length;
  var skippedCount = (summary && summary.skipped) || skipped.length;
  var vHTML = '<div style="display:flex;align-items:baseline;gap:14px"><span class="pill '+(failures===0?'pill-pass':'pill-bad')+'">'+(failures===0?'PASS':'FAIL')+'</span><div><div style="font-size:13px;font-weight:700">'+esc(failures===0?'Harness verification passed':'Harness verification FAILED')+'</div><div class="small">'+esc(records)+' records · '+esc(skippedCount)+' recorded skips · '+(failures)+' failures</div></div></div>';
  vHTML += '<div class="small" style="margin-top:10px;line-height:1.7">Verification checks that every registered implementation was either measured or skipped with a recorded reason, every record is schema-valid, and the meta record is present. Skips are exposure-limited by policy, never silently dropped.</div>';
  $('verifyPanel').innerHTML = vHTML;
})();

/* ── Coverage matrix ── */
var covFamily = 'broadcast';
function renderCoverage(){
  var tabs = $('covTabs'); clear(tabs);
  familyOrder.forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab' + (f===covFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ covFamily = f; renderCoverage(); };
    tabs.appendChild(t);
  });
  var scenarios = (REG.scenarios||[]).filter(function(s){ return s.kind === covFamily; }).sort(function(a,b){ return a.id < b.id ? -1 : 1; });
  var attach = {};
  (REG.attachments||[]).forEach(function(a){ attach[a.impl+'@@'+a.scenario] = a; });
  var scores = derivedFamily(covFamily);
  var impls = Object.keys(scores).sort(function(a,b){ return (scores[b].score||0) - (scores[a].score||0); });
  var table = $('covTable'); clear(table);
  if (!scenarios.length || !impls.length) { table.innerHTML = '<tr><td class="small" style="padding:12px">No registry data for this family.</td></tr>'; return; }
  var thead = '<thead><tr><th>implementation ↓ · scenario →</th>';
  scenarios.forEach(function(s){ thead += '<th title="'+esc(s.id)+'">'+esc(s.id)+'</th>'; });
  thead += '</tr></thead>';
  var tbody = '<tbody>';
  impls.forEach(function(impl){
    tbody += '<tr><th title="'+esc(impl)+'">'+esc(impl.length > 40 ? impl.slice(0,39)+'…' : impl)+'</th>';
    scenarios.forEach(function(s){
      var a = attach[impl+'@@'+s.id];
      var cls = 'cov-none', label = 'no attachment';
      if (a) {
        if (a.expected_good_fit) { cls = 'cov-good'; label = 'expected good fit'; }
        else { cls = 'cov-bad'; label = 'declared bad fit'; }
        if (a.exposure_limited) label += ' · exposure-limited';
      }
      tbody += '<td class="'+cls+'" data-impl="'+esc(impl)+'" data-scenario="'+esc(s.id)+'"></td>';
    });
    tbody += '</tr>';
  });
  tbody += '</tbody>';
  table.innerHTML = thead + tbody;
  table.querySelectorAll('td').forEach(function(td){
    var impl = td.getAttribute('data-impl'), sid = td.getAttribute('data-scenario');
    var a = attach[impl+'@@'+sid];
    bindTip(td, function(){
      var h = '<div class="tt">'+esc(impl)+'</div><div class="mono">'+esc(sid)+'</div>';
      if (a) {
        h += '<div style="margin-top:4px">'+esc(a.expected_good_fit ? 'Expected good fit' : 'Declared bad fit') + (a.exposure_limited ? ' · <b style="color:var(--red)">exposure-limited</b>' : '') + '</div>';
        if (a.remarks) h += '<div style="margin-top:4px;color:var(--ink2)">'+esc(a.remarks)+'</div>';
      } else {
        h += '<div style="margin-top:4px;color:var(--ink3)">No attachment declared.</div>';
      }
      return h;
    });
  });
}

/* ── Data & exports ── */
function downloadBlob(name, content, mime){
  var blob = new Blob([content], {type: mime || 'application/octet-stream'});
  var url = URL.createObjectURL(blob);
  var a = document.createElement('a');
  a.href = url; a.download = name;
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  setTimeout(function(){ URL.revokeObjectURL(url); }, 5000);
}
(function wireDownloads(){
  var all = [].concat(meta ? [meta] : [], metrics, traces, skipped, (summary && Object.keys(summary).length ? [summary] : []));
  $('dlJSONL').addEventListener('click', function(ev){ ev.preventDefault(); downloadBlob('bigbuff-results.jsonl', all.map(function(r){ return JSON.stringify(r); }).join('\n')+'\n', 'application/x-ndjson'); });
  var csvEscape = function(v){ var s = String(v == null ? '' : v); return /[",\n]/.test(s) ? '"'+s.replace(/"/g,'""')+'"' : s; };
  $('dlCSVMetrics').addEventListener('click', function(ev){
    ev.preventDefault();
    var head = ['impl','family','metric','dims','unit','ops','n','mean','stddev','cv','min','p50','p90','p99','max','ci95_halfwidth','ci95_low','ci95_high','lower_is_better','expected_good_fit','exposure_limited'];
    var lines = [head.join(',')];
    metrics.forEach(function(m){
      lines.push([m.impl, m.family, m.metric, JSON.stringify(m.dims||{}), m.unit, m.ops, m.n, m.mean, m.stddev, m.cv, m.min, m.p50, m.p90, m.p99, m.max, m.ci95_halfwidth, m.ci95_low, m.ci95_high, m.lower_is_better, m.expected_good_fit, m.exposure_limited].map(csvEscape).join(','));
    });
    downloadBlob('bigbuff-metrics-wide.csv', lines.join('\n')+'\n', 'text/csv');
  });
  $('dlCSVScores').addEventListener('click', function(ev){
    ev.preventDefault();
    var head = ['family','impl','score','verdict','baseline','integrity','throughput','latency','allocations','churn','scaling','stall_resilience','expected_good_fit','exposure_limited'];
    var lines = [head.join(',')];
    familyOrder.forEach(function(f){
      Object.keys(derivedFamily(f)).forEach(function(impl){
        var s = derivedFamily(f)[impl];
        lines.push([f, impl, s.score, s.verdict, s.baseline, s.integrity, s.dims.throughput, s.dims.latency, s.dims.allocations, s.dims.churn, s.dims.scaling, s.dims.stall_resilience, s.expected_good_fit, s.exposure_limited].map(csvEscape).join(','));
      });
    });
    downloadBlob('bigbuff-scores.csv', lines.join('\n')+'\n', 'text/csv');
  });
  $('dlRegistry').addEventListener('click', function(ev){ ev.preventDefault(); downloadBlob('bigbuff-registry.json', JSON.stringify(REG, null, 2), 'application/json'); });
  var sl = $('skippedList'); clear(sl);
  if (skipped.length) {
    var grouped = {};
    skipped.forEach(function(s){ (grouped[s.impl] = grouped[s.impl] || []).push(s.battery); });
    sl.innerHTML = Object.keys(grouped).map(function(impl){
      return '<div style="padding:4px 0;border-bottom:1px solid var(--line)"><b class="mono">'+esc(impl)+'</b> — skipped: '+esc(grouped[impl].join(', '))+'<br><span style="color:var(--ink3)">'+esc((skipped.find(function(s){return s.impl===impl;})||{}).reason||'')+'</span></div>';
    }).join('');
  } else sl.textContent = 'None. Every registered implementation was measured.';
})();

/* ── Methodology gloss + footer ── */
(function renderMethodology(){
  var batteries = [
    ['broadcast.throughput', 'publish_ns_op', 'cost per publish vs subscriber count (1…64/256/512)'],
    ['broadcast.latency', 'publish_latency_ns', 'publish→receipt latency, 8 subscribers, p50…p99/max + reservoir histogram'],
    ['broadcast.allocs', 'publish_allocs_op / publish_bytes_op', 'allocator pressure per publish at 8 subscribers, GOGC=off, post-GC measurement'],
    ['broadcast.allocs_zerosubs', 'publish_allocs_op_zerosubs / publish_bytes_op_zerosubs', 'same, publishing into the void'],
    ['broadcast.mem_per_subscriber', 'mem_per_subscriber_bytes', 'post-GC heap retained per idle subscriber (16 idle subscriptions)'],
    ['broadcast.churn', 'churn_ns_op / churn_allocs_op / churn_bytes_op', 'subscribe→publish→unsubscribe lifecycle cost'],
    ['broadcast.stalled_consumer', 'stalled_consumer_ops_completed', 'publisher vs one permanently stalled lane — completed ops + retained bytes'],
    ['broadcast.zero_subscriber', 'zero_subscriber_ns_op', 'cost of publishing with no subscribers'],
    ['broadcast.trace_steady_state / trace_stalled_consumer', 'trace', 'per-lane event timeline (publisher + subscribers), ≤ TraceMaxEvents'],
    ['caster.throughput / latency / churn', 'send_ns_op / send_latency_ns / churn_ns_op', 'rendezvous caster cost model incl. withdrawal-safe churn'],
    ['caster.withdrawal_compensation', 'withdrawal_compensation_success', 'Add(-1) unsticks a Send parked on a never-receiving intent (24 rounds)'],
    ['caster.trace_steady_state', 'trace', 'sender + receiver swimlanes'],
    ['scatter.dispatch', 'dispatch_ns_op / exactly_once_violations', 'work-queue dispatch, receivers 1…N, exactly-once accounting'],
    ['scatter.churn', 'churn_ns_op', 'worker registration lifecycle'],
    ['gather.wait', 'wait_ns_op', 'barrier wait time, 8 senders released on a start barrier'],
    ['gather.churn', 'churn_ns_op', 'sender registration lifecycle'],
    ['payload.throughput / latency', 'publish_ns_op / publish_latency_ns', 'async payload fan-out incl. lossy ring/seqlock designs'],
    ['payload.integrity', 'torn_reads', '[4]uint64 multi-word read integrity under 8-way fan-out'],
    ['payload.stalled_consumer', 'stalled_consumer_ops_completed', 'async lane vs never-ranged subscriber; approximate retained bytes'],
    ['payload.churn', 'churn_ns_op', 'subscribe-with-context cancellation lifecycle']
  ];
  $('batteryGloss').innerHTML = '<thead><tr><td>battery</td><td>metrics</td><td>what it measures</td></tr></thead><tbody>' + batteries.map(function(b){
    return '<tr><td>'+esc(b[0])+'</td><td>'+esc(b[1])+'</td><td>'+esc(b[2])+'</td></tr>';
  }).join('') + '</tbody>';
  $('footMeta').textContent = 'measured '+(meta.timestamp||'')+' · commit '+String(meta.commit||'').slice(0,12)+' · '+(meta.go||'')+' · GOMAXPROCS='+(meta.gomaxprocs||'?');
})();

/* ── Nav scrollspy + initial render ── */
function initNav(){
  var links = document.querySelectorAll('#nav a, #sectionbar a');
  var map = {};
  links.forEach(function(a){ var id = a.getAttribute('href').slice(1); (map[id] = map[id] || []).push(a); });
  var bar = document.getElementById('sectionbar');
  if (typeof window.IntersectionObserver === 'function') {
    var io = new IntersectionObserver(function(entries){
      entries.forEach(function(entry){
        if (entry.isIntersecting) {
          links.forEach(function(a){ a.classList.remove('active'); });
          (map[entry.target.id] || []).forEach(function(a){
            a.classList.add('active');
            if (bar && a.parentElement === bar) {
              bar.scrollTo({left: a.offsetLeft - (bar.clientWidth - a.offsetWidth) / 2, behavior: 'smooth'});
            }
          });
        }
      });
    }, {rootMargin: '-18% 0px -70% 0px', threshold: 0});
    ['overview','findings','scaling','leaderboard','adoptability','latency','memory','churn','pathology','correctness','coverage','methodology','data'].forEach(function(id){
      var el = document.getElementById(id); if (el) io.observe(el);
    });
  }
}
function renderAllCharts(){
  renderScaling(); lbRender(); radarRender(); renderLatency(); renderMemory(); renderChurn(); pathoRenderCards(); renderCoverage();
}
pathoFillSelects();
initNav();
renderAllCharts();
var resizeTimer = null;
window.addEventListener('resize', function(){
  if (resizeTimer) clearTimeout(resizeTimer);
  resizeTimer = setTimeout(renderAllCharts, 160);
});
})();
`
