package bigbuff

// Report stylesheet: CSS tokens and layout, embedded as a template constant.

const reportHTMLCSS = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<title>bigbuff · Concurrency Measurement Report</title>
<style>
:root{
  --bg:#070b14; --bg2:#0b1020; --panel:#0e1530; --panel2:#121a3a; --panel3:#182148;
  --line:#1f2a55; --line2:#2b3a6e;
  --ink:#e8ecf8; --ink2:#a9b3d6; --ink3:#6b769e;
  --cyan:#22d3ee; --indigo:#818cf8; --violet:#a78bfa; --green:#34d399; --amber:#fbbf24;
  --orange:#fb923c; --red:#f87171; --teal:#2dd4bf;
  --radius:14px; --shadow:0 18px 50px rgba(0,0,0,.45);
  --mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
  --sans:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{background:var(--bg);color:var(--ink);font-family:var(--sans);line-height:1.55;-webkit-font-smoothing:antialiased}
::selection{background:rgba(129,140,248,.35)}

/* layout */
#shell{display:grid;grid-template-columns:250px 1fr;min-height:100vh}
nav#nav{position:sticky;top:0;height:100vh;overflow-y:auto;border-right:1px solid var(--line);background:linear-gradient(180deg,#0a0f22,#0b1020 40%,#0a0f22);padding:26px 18px;display:flex;flex-direction:column;gap:4px}
#brand{display:flex;align-items:center;gap:12px;padding:4px 8px 18px;border-bottom:1px solid var(--line);margin-bottom:14px}
#brand .mark{width:38px;height:38px;border-radius:11px;background:conic-gradient(from 210deg,var(--cyan),var(--indigo),var(--violet),var(--cyan));display:grid;place-items:center;font-weight:800;color:#060a17;font-size:19px;box-shadow:0 0 22px rgba(56,189,248,.35)}
#brand h1{font-size:15px;letter-spacing:.02em;line-height:1.25}
#brand small{display:block;color:var(--ink3);font-weight:500;font-size:10px;letter-spacing:.14em;text-transform:uppercase;margin-top:2px}
#nav a{display:flex;align-items:center;gap:10px;color:var(--ink2);text-decoration:none;font-size:13px;padding:7px 10px;border-radius:9px;border:1px solid transparent;transition:all .18s}
#nav a:hover{color:var(--ink);background:var(--panel);border-color:var(--line)}
#nav a .dot{width:6px;height:6px;border-radius:50%;background:var(--line2);flex:none}
#nav a:hover .dot{background:var(--cyan)}
#nav a.active{color:#06121a;background:linear-gradient(90deg,#67e8f9,#a5b4fc);border-color:transparent;font-weight:600}
#nav a.active .dot{background:#06121a}
#navfoot{margin-top:auto;padding:14px 8px 2px;border-top:1px solid var(--line);color:var(--ink3);font-size:11px;line-height:1.7}
#navfoot b{color:var(--ink2)}
main{min-width:0;padding:34px 40px 80px;max-width:1560px}
/* phone section bar: shown only when the sidebar is hidden */
.sectionbar{display:none;position:sticky;top:0;z-index:20;gap:6px;overflow-x:auto;padding:10px 16px;background:#0a0f22;border-bottom:1px solid var(--line);scrollbar-width:thin;scrollbar-color:var(--line2) transparent}
.sectionbar a{flex:none;color:var(--ink2);text-decoration:none;font-size:12.5px;padding:7px 12px;border-radius:999px;border:1px solid var(--line2);background:var(--panel);white-space:nowrap}
.sectionbar a.active{color:#06121a;background:linear-gradient(90deg,#67e8f9,#a5b4fc);border-color:transparent;font-weight:600}
.scroll-x{overflow-x:auto;max-width:100%;scrollbar-width:thin;scrollbar-color:var(--line2) transparent}
/* Visible scroll cues: custom (non-overlay) scrollbars on every intentional scroller, so a
   horizontal overflow is obvious in screenshots and to touch users, not a hidden overlay bar. */
.sectionbar::-webkit-scrollbar,.tbl-wrap::-webkit-scrollbar,.cov-wrap::-webkit-scrollbar,.scroll-x::-webkit-scrollbar,.torn-scroll::-webkit-scrollbar,nav#nav::-webkit-scrollbar{height:8px;width:8px}
.sectionbar::-webkit-scrollbar-track,.tbl-wrap::-webkit-scrollbar-track,.cov-wrap::-webkit-scrollbar-track,.scroll-x::-webkit-scrollbar-track,.torn-scroll::-webkit-scrollbar-track,nav#nav::-webkit-scrollbar-track{background:#0d1330;border-radius:4px}
.sectionbar::-webkit-scrollbar-thumb,.tbl-wrap::-webkit-scrollbar-thumb,.cov-wrap::-webkit-scrollbar-thumb,.scroll-x::-webkit-scrollbar-thumb,.torn-scroll::-webkit-scrollbar-thumb,nav#nav::-webkit-scrollbar-thumb{background:#33407a;border-radius:4px}
.sectionbar::-webkit-scrollbar-thumb:hover,.tbl-wrap::-webkit-scrollbar-thumb:hover,.cov-wrap::-webkit-scrollbar-thumb:hover,.scroll-x::-webkit-scrollbar-thumb:hover,.torn-scroll::-webkit-scrollbar-thumb:hover,nav#nav::-webkit-scrollbar-thumb:hover{background:#46569b}
.tbl-wrap,.cov-wrap{scrollbar-width:thin;scrollbar-color:#33407a #0d1330}
/* ranked bar rows: one template for every bar list, so each reflows at phone width */
.bar-row{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(0,2.4fr) 76px;gap:10px;align-items:center;font-size:11px;margin:3px 0}
.bar-row.retain{grid-template-columns:minmax(0,1.3fr) minmax(0,2.2fr) 74px 84px}
.bar-row.zero{grid-template-columns:minmax(0,1.3fr) minmax(0,2.4fr) 90px;margin:2px 0}
.bar-row.churn{grid-template-columns:minmax(0,1.4fr) minmax(0,2fr) 100px;font-size:11.5px;gap:12px}
@media (max-width:720px){.bar-row,.bar-row.zero,.bar-row.churn{grid-template-columns:minmax(0,1fr) minmax(0,1fr) 64px;gap:8px}.bar-row.retain{grid-template-columns:minmax(0,1fr) minmax(0,1fr) 64px 72px}}
.mem-grid{display:grid;grid-template-columns:1.2fr 1fr;gap:24px}
.mem-grid>*{min-width:0}
/* base scroll-margin must precede the media queries below or the 24px default wins the cascade
   and every tap lands the section under the sticky section bar */
section.block{scroll-margin-top:24px}
@media (max-width:1080px){#shell{grid-template-columns:minmax(0,1fr)}nav#nav{display:none}.sectionbar{display:flex}.mobile-topbar{display:flex!important}section.block{scroll-margin-top:76px}}
@media (max-width:732px){.mem-grid{grid-template-columns:minmax(0,1fr)}}
@media (max-width:720px){main{padding:18px 16px 64px}#hero{padding:24px 20px}#hero h1{font-size:26px}h2.title{font-size:22px}section.block{scroll-margin-top:76px}table.gloss td:first-child{white-space:normal;word-break:break-word}}

/* sections */
section.block{margin-bottom:56px}
.kicker{font-size:10.5px;letter-spacing:.22em;text-transform:uppercase;color:var(--cyan);font-weight:700;margin-bottom:6px}
h2.title{font-size:26px;letter-spacing:-.01em;margin-bottom:6px}
.lede{color:var(--ink2);max-width:880px;font-size:14px}
.card{background:linear-gradient(180deg,var(--panel),var(--bg2));border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow)}
.card.pad{padding:18px 20px}

/* hero */
#hero{position:relative;overflow:hidden;border:1px solid var(--line2);border-radius:20px;padding:34px 36px 30px;background:radial-gradient(1100px 480px at 12% -10%,rgba(99,102,241,.34),transparent 55%),radial-gradient(900px 420px at 95% 0%,rgba(34,211,238,.20),transparent 50%),linear-gradient(180deg,#101736,#0b1020)}
#hero::after{content:"";position:absolute;inset:0;background-image:radial-gradient(rgba(148,163,216,.13) 1px,transparent 1px);background-size:22px 22px;pointer-events:none;mask-image:linear-gradient(180deg,#000 20%,transparent 90%)}
#hero h1{font-size:34px;letter-spacing:-.02em;line-height:1.15;max-width:900px;position:relative}
#hero h1 em{font-style:normal;background:linear-gradient(90deg,#67e8f9,#a5b4fc 55%,#c4b5fd);-webkit-background-clip:text;background-clip:text;color:transparent}
#hero p{color:var(--ink2);margin-top:10px;max-width:820px;position:relative}
.chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:18px;position:relative}
.chip{display:inline-flex;align-items:center;gap:8px;font-family:var(--mono);font-size:11.5px;color:#cfe6ff;background:rgba(14,22,48,.75);border:1px solid var(--line2);border-radius:999px;padding:5px 13px}
.chip b{color:#fff;font-weight:600}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:20px;position:relative}
.kpi{background:rgba(9,13,30,.6);border:1px solid var(--line2);border-radius:14px;padding:13px 15px}
.kpi .v{font-size:24px;font-weight:750;letter-spacing:-.01em;font-family:var(--mono)}
.kpi .l{font-size:10.5px;color:var(--ink2);text-transform:uppercase;letter-spacing:.09em;margin-top:2px}
.kpi .s{font-size:10.5px;color:var(--ink3);margin-top:3px}

/* findings */
.findings-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(326px,1fr));gap:14px;margin-top:16px}
.finding{position:relative;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:16px 17px 14px;overflow:hidden}
.finding::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:linear-gradient(180deg,var(--cyan),var(--indigo))}
.finding.pathology::before{background:linear-gradient(180deg,var(--red),var(--orange))}
.finding.correctness::before{background:linear-gradient(180deg,var(--green),var(--teal))}
.finding.memory::before{background:linear-gradient(180deg,var(--violet),var(--indigo))}
.finding h3{font-size:14.5px;line-height:1.35;margin-bottom:6px}
.finding p{font-size:12.5px;color:var(--ink2)}
.finding .tag{display:inline-block;margin-bottom:8px;font-size:9.5px;letter-spacing:.16em;text-transform:uppercase;color:var(--ink3);border:1px solid var(--line2);padding:2px 8px;border-radius:5px}
.finding a{color:var(--cyan);text-decoration:none}

/* tabs */
.tabs{display:flex;gap:6px;flex-wrap:wrap;margin:14px 0}
.tab{cursor:pointer;border:1px solid var(--line2);background:var(--panel);color:var(--ink2);font-size:12px;font-weight:600;padding:7px 14px;border-radius:9px;transition:all .18s}
.tab:hover{color:var(--ink)}
.tab.active{background:linear-gradient(90deg,#164e63,#312e81);border-color:#3b82f6;color:#e0f2fe}

/* toolbar */
.toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:12px 0}
.search{background:var(--panel);border:1px solid var(--line2);color:var(--ink);border-radius:9px;padding:8px 12px;font-size:13px;min-width:220px;outline:none}
.search:focus{border-color:var(--indigo)}
.toggle{cursor:pointer;user-select:none;font-size:12px;color:var(--ink2);display:flex;align-items:center;gap:6px}
select.sel{background:var(--panel);border:1px solid var(--line2);color:var(--ink);border-radius:9px;padding:7px 10px;font-size:12.5px;min-width:0;max-width:100%}

/* leaderboard table */
.tbl-wrap{overflow:auto;border:1px solid var(--line);border-radius:var(--radius);max-height:640px;overflow-y:auto}
table.tbl{width:100%;border-collapse:collapse;font-size:12px}
table.tbl th{position:sticky;top:0;background:#131b3f;color:var(--ink2);text-align:left;padding:10px 10px;font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;cursor:pointer;user-select:none;white-space:nowrap;border-bottom:1px solid var(--line2)}
table.tbl th:hover{color:var(--ink)}
table.tbl td{padding:8px 10px;border-bottom:1px solid rgba(31,42,85,.5);vertical-align:middle}
table.tbl tr:hover td{background:rgba(99,102,241,.07)}
.impl-name{font-family:var(--mono);font-size:11.5px;white-space:nowrap}
.impl-name .bl{color:var(--amber);font-size:9px;border:1px solid #78560a;background:#2a2008;border-radius:4px;padding:0 4px;margin-left:6px;vertical-align:1px}
.impl-name .xl{color:var(--red);font-size:9px;border:1px solid #7f1d1d;background:#2a0d0d;border-radius:4px;padding:0 4px;margin-left:6px;vertical-align:1px}
.scorebar{position:relative;height:16px;background:#1a2350;border-radius:4px;min-width:120px;overflow:hidden}
.scorebar .fill{position:absolute;left:0;top:0;bottom:0;border-radius:4px;background:linear-gradient(90deg,#0e7490,#22d3ee)}
.scorebar span{position:absolute;left:8px;top:1px;font-size:11px;font-weight:700;color:#04121a;mix-blend-mode:plus-lighter;text-shadow:0 0 4px rgba(34,211,238,.8)}
.verdict{font-size:10.5px;font-weight:700;white-space:nowrap}
.v-strong{color:var(--green)} .v-situational{color:var(--amber)} .v-niche{color:var(--orange)} .v-avoid{color:var(--red)}
.barnum{font-family:var(--mono);color:var(--ink2)}

/* whiskers */
.whisker{display:flex;align-items:center;gap:8px;min-width:150px}
.whisker svg{display:block}

/* svg chart text */
.ax{stroke:#26315f;stroke-width:1}
.grid{stroke:#18224a;stroke-width:1;stroke-dasharray:2 4}
.axtxt{fill:#7c88b3;font-size:10px;font-family:var(--mono)}
.lgnd{display:flex;gap:12px;flex-wrap:wrap;margin-top:10px;font-size:11.5px;color:var(--ink2)}
.lgnd span{display:flex;align-items:center;gap:6px;cursor:pointer;user-select:none}
.lgnd i{width:14px;height:3px;border-radius:2px;display:inline-block}
.lgnd span.off{opacity:.35}

/* pathology */
.patho-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,304px),1fr));gap:16px;margin-top:16px}
.patho-card{background:var(--panel2);border:1px solid var(--line2);border-radius:var(--radius);padding:16px;position:relative}
.patho-card h4{font-size:13px;margin-bottom:2px}
.patho-card .outcome{font-family:var(--mono);font-size:11px;margin-top:6px;padding:4px 8px;border-radius:6px;display:inline-block}
.outcome-completed{background:rgba(52,211,153,.12);color:var(--green);border:1px solid rgba(52,211,153,.3)}
.outcome-publisher_blocked{background:rgba(248,113,113,.12);color:#fda4a4;border:1px solid rgba(248,113,113,.35)}
.outcome-publisher_panicked{background:rgba(251,146,60,.12);color:#fdba74;border:1px solid rgba(251,146,60,.4)}
.swim-controls{display:flex;align-items:center;gap:8px;margin-top:10px}
.sbtn{cursor:pointer;background:var(--panel3);border:1px solid var(--line2);color:var(--ink);border-radius:8px;width:34px;height:30px;font-size:13px}
.sbtn:hover{border-color:var(--indigo)}
.swim-wrap{position:relative}

/* tooltip */
#tip{position:fixed;z-index:60;pointer-events:none;background:#0d1430;border:1px solid var(--line2);border-radius:9px;padding:8px 10px;font-size:11.5px;box-shadow:var(--shadow);opacity:0;transition:opacity .12s;max-width:300px}
#tip .tt{font-weight:700;margin-bottom:2px}
#tip .mono{font-family:var(--mono);color:#a5f3fc}

/* coverage matrix */
.cov-wrap{overflow:auto;border:1px solid var(--line);border-radius:var(--radius);max-height:520px}
table.cov{border-collapse:collapse;font-size:9px}
table.cov th{position:sticky;left:0;background:#131b3f;z-index:2;text-align:right;padding:4px 8px;color:var(--ink2);font-family:var(--mono);font-weight:600;white-space:nowrap;border-bottom:1px solid var(--line2)}
table.cov thead th{position:sticky;top:0;left:auto;z-index:3;writing-mode:vertical-rl;transform:rotate(180deg);padding:6px 4px;border-bottom:1px solid var(--line2)}
table.cov td{width:15px;height:15px;border:1px solid rgba(7,11,20,.6);text-align:center}
.cov-good{background:#166534}.cov-bad{background:#7f1d1d}.cov-none{background:#1b2447}
.cov-legend{display:flex;gap:16px;margin-top:10px;font-size:11.5px;color:var(--ink2)}
.cov-legend i{width:13px;height:13px;border-radius:3px;display:inline-block;vertical-align:-2px;margin-right:6px}

/* radar + verdict */
.radar-flex{display:grid;grid-template-columns:minmax(280px,430px) minmax(0,1fr);gap:22px;align-items:center}
.radar-flex>*{min-width:0}
@media (max-width:900px){.radar-flex{grid-template-columns:minmax(280px,340px) minmax(0,1fr)}}
@media (max-width:755px){.radar-flex{grid-template-columns:1fr}}
.score-rows{display:flex;flex-direction:column;gap:9px}
.srow{display:grid;grid-template-columns:150px 1fr 42px;gap:10px;align-items:center;font-size:12px}
.srow .dim{color:var(--ink2)}
.strack{height:9px;background:#1a2350;border-radius:5px;overflow:hidden}
.sfill{height:100%;border-radius:5px;background:linear-gradient(90deg,#0e7490,#22d3ee)}
.swatch{width:11px;height:11px;border-radius:3px;display:inline-block;margin-right:6px;vertical-align:-1px}

/* downloads */
.dl{display:flex;gap:12px;flex-wrap:wrap;margin-top:14px}
.dlbtn{cursor:pointer;display:flex;flex-direction:column;gap:2px;background:var(--panel2);border:1px solid var(--line2);border-radius:12px;padding:13px 18px;color:var(--ink);text-decoration:none;min-width:200px}
.dlbtn:hover{border-color:var(--cyan);box-shadow:0 0 0 1px var(--cyan) inset}
.dlbtn b{font-size:13px}
.dlbtn small{color:var(--ink3);font-size:11px}

/* methodology */
details.mdetail{border:1px solid var(--line);border-radius:12px;background:var(--panel);padding:14px 18px;margin-top:12px}
details.mdetail summary{cursor:pointer;font-weight:650;font-size:13.5px;outline:none}
details.mdetail div{color:var(--ink2);font-size:12.5px;margin-top:10px}
code.k{font-family:var(--mono);background:#141c40;border:1px solid var(--line2);border-radius:5px;padding:1px 6px;font-size:11px;color:#a5f3fc}
table.gloss{width:100%;border-collapse:collapse;font-size:12px;margin-top:6px}
table.gloss td{padding:6px 10px;border-bottom:1px solid var(--line);vertical-align:top}
table.gloss td:first-child{font-family:var(--mono);white-space:nowrap;color:#a5f3fc}
.small{font-size:11.5px;color:var(--ink3)}
.pill{display:inline-block;font-size:9.5px;font-weight:800;letter-spacing:.1em;padding:2px 8px;border-radius:99px;text-transform:uppercase}
.pill-pass{background:rgba(52,211,153,.15);color:var(--green);border:1px solid rgba(52,211,153,.4)}
.pill-warn{background:rgba(251,191,36,.12);color:var(--amber);border:1px solid rgba(251,191,36,.4)}
.pill-bad{background:rgba(248,113,113,.14);color:var(--red);border:1px solid rgba(248,113,113,.4)}
.histo{display:flex;align-items:flex-end;gap:2px;height:110px}
.histo div{flex:1;background:linear-gradient(180deg,#22d3ee,#155e75);border-radius:3px 3px 0 0;min-height:2px}
.histo-x{display:flex;justify-content:space-between;font-family:var(--mono);font-size:9px;color:var(--ink3);margin-top:3px}
.footer-note{color:var(--ink3);font-size:11px;border-top:1px solid var(--line);padding-top:18px;margin-top:10px}
</style>
</head>
`
