package bigbuff

// landingPageStyle is the stylesheet of the landing page. It was split out of the
// template that measure_site_test.go assembles.

const landingPageStyle = `:root {
  --bg: #070b14;
  --bg2: #0b1020;
  --panel: #0e1530;
  --panel2: #121a3a;
  --panel3: #182148;
  --line: #1f2a55;
  --line2: #2b3a6e;
  --ink: #e8ecf8;
  --ink2: #a9b3d6;
  --ink3: #6b769e;
  --cyan: #22d3ee;
  --indigo: #818cf8;
  --violet: #a78bfa;
  --green: #34d399;
  --amber: #fbbf24;
  --orange: #fb923c;
  --red: #f87171;
  --teal: #2dd4bf;
  --radius: 14px;
  --shadow: 0 18px 50px rgba(0,0,0,.45);
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  --sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; }
body { background: var(--bg); color: var(--ink); font-family: var(--sans); line-height: 1.55; -webkit-font-smoothing: antialiased; }
::selection { background: rgba(129,140,248,.35); }
a { color: var(--cyan); text-decoration: none; transition: color .18s; }
a:hover { color: #a5f3fc; text-decoration: underline; }

/* Top Navbar */
header.site-nav {
  position: sticky; top: 0; z-index: 1000;
  background: rgba(11,16,32,.92); backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--line); padding: 12px 32px;
  display: flex; align-items: center; justify-content: space-between; gap: 16px;
}
.nav-brand { display: flex; align-items: center; gap: 12px; }
.nav-brand .mark {
  width: 36px; height: 36px; border-radius: 10px;
  background: conic-gradient(from 210deg, var(--cyan), var(--indigo), var(--violet), var(--cyan));
  display: grid; place-items: center; font-weight: 800; color: #060a17; font-size: 18px;
  box-shadow: 0 0 20px rgba(56,189,248,.35);
}
.nav-brand h1 { font-size: 15px; letter-spacing: .02em; line-height: 1.2; color: #fff; }
.nav-brand small { display: block; color: var(--ink3); font-size: 10px; letter-spacing: .12em; text-transform: uppercase; }

.nav-links { display: flex; align-items: center; gap: 20px; font-size: 13px; font-weight: 500; }
.nav-links a { color: var(--ink2); }
.nav-links a:hover { color: #fff; text-decoration: none; }
.nav-actions { display: flex; align-items: center; gap: 10px; }
.btn {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 7px 14px; border-radius: 8px; font-size: 12.5px; font-weight: 600;
  border: 1px solid var(--line2); background: var(--panel); color: var(--ink);
  cursor: pointer; transition: all .18s; text-decoration: none !important;
}
.btn:hover { background: var(--panel2); border-color: var(--cyan); color: #fff; transform: translateY(-1px); }
.btn-primary {
  background: linear-gradient(90deg, #0e7490, #312e81); border-color: #38bdf8; color: #f0f9ff;
}
.btn-primary:hover { background: linear-gradient(90deg, #0891b2, #3730a3); border-color: #67e8f9; color: #fff; }

/* Container */
.container { max-width: 1440px; margin: 0 auto; padding: 32px 32px 80px; }

/* Hero Section */
.hero {
  position: relative; overflow: hidden; border: 1px solid var(--line2); border-radius: 20px;
  padding: 44px 44px 38px;
  background: radial-gradient(1100px 480px at 12% -10%, rgba(99,102,241,.32), transparent 55%),
              radial-gradient(900px 420px at 95% 0%, rgba(34,211,238,.18), transparent 50%),
              linear-gradient(180deg, #101736, #0b1020);
  box-shadow: var(--shadow); margin-bottom: 48px;
}
.hero::after {
  content: ""; position: absolute; inset: 0;
  background-image: radial-gradient(rgba(148,163,216,.12) 1px, transparent 1px);
  background-size: 22px 22px; pointer-events: none;
  mask-image: linear-gradient(180deg, #000 20%, transparent 90%);
}
.kicker {
  font-size: 11px; letter-spacing: .22em; text-transform: uppercase; color: var(--cyan);
  font-weight: 700; margin-bottom: 8px; display: flex; align-items: center; gap: 8px;
}
.hero h2 {
  font-size: 38px; letter-spacing: -.02em; line-height: 1.15; max-width: 960px; position: relative; color: #fff;
}
.hero h2 em {
  font-style: normal;
  background: linear-gradient(90deg, #67e8f9, #a5b4fc 55%, #c4b5fd);
  -webkit-background-clip: text; background-clip: text; color: transparent;
}
.hero p.lead {
  color: var(--ink2); margin-top: 14px; max-width: 860px; font-size: 15.5px; position: relative; line-height: 1.6;
}
.hero-cta { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 24px; position: relative; z-index: 2; }
.hero-cta .btn { padding: 10px 18px; font-size: 13.5px; border-radius: 10px; }

.kpis {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 14px; margin-top: 28px; position: relative;
}
.kpi {
  background: rgba(9,13,30,.65); border: 1px solid var(--line2); border-radius: 14px; padding: 14px 18px;
}
.kpi .v { font-size: 28px; font-weight: 750; letter-spacing: -.01em; font-family: var(--mono); color: #fff; }
.kpi .l { font-size: 11px; color: var(--ink2); text-transform: uppercase; letter-spacing: .08em; margin-top: 2px; font-weight: 600; }
.kpi .s { font-size: 11px; color: var(--ink3); margin-top: 3px; }

/* Section Styling */
section.block { margin-bottom: 56px; scroll-margin-top: 80px; }
h3.section-title { font-size: 24px; letter-spacing: -.01em; color: #fff; margin-bottom: 8px; }
.lede { color: var(--ink2); max-width: 900px; font-size: 14px; margin-bottom: 20px; }

/* Featured Reports Grid */
.reports-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }
.report-card {
  background: linear-gradient(180deg, var(--panel), var(--bg2));
  border: 1px solid var(--line); border-radius: var(--radius); padding: 24px 26px;
  position: relative; overflow: hidden; transition: all .2s; box-shadow: var(--shadow);
  display: flex; flex-direction: column; justify-content: space-between;
}
.report-card:hover { border-color: var(--line2); transform: translateY(-2px); }
.report-card::before {
  content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 4px;
}
.report-card.meas::before { background: linear-gradient(180deg, var(--cyan), var(--indigo)); }
.report-card.arch::before { background: linear-gradient(180deg, var(--violet), var(--teal)); }
.report-badge {
  display: inline-flex; align-items: center; gap: 6px;
  font-size: 10px; font-family: var(--mono); text-transform: uppercase; letter-spacing: .12em;
  padding: 3px 10px; border-radius: 999px; margin-bottom: 12px;
}
.report-card.meas .report-badge { background: rgba(34,211,238,.12); color: var(--cyan); border: 1px solid rgba(34,211,238,.3); }
.report-card.arch .report-badge { background: rgba(167,139,250,.12); color: var(--violet); border: 1px solid rgba(167,139,250,.3); }
.report-card h4 { font-size: 18px; color: #fff; margin-bottom: 8px; line-height: 1.3; }
.report-card p { font-size: 13.5px; color: var(--ink2); line-height: 1.55; margin-bottom: 18px; }
.report-features { list-style: none; display: flex; flex-direction: column; gap: 6px; margin-bottom: 22px; font-size: 12.5px; color: var(--ink2); }
.report-features li { display: flex; align-items: center; gap: 8px; }
.report-features li span.dot { width: 5px; height: 5px; border-radius: 50%; background: var(--cyan); flex: none; }
.report-actions { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; margin-top: auto; padding-top: 14px; border-top: 1px solid var(--line); }

/* Card Panels */
.card {
  background: linear-gradient(180deg, var(--panel), var(--bg2));
  border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow);
}
.card.pad { padding: 22px 24px; }

/* Filter Toolbars */
.toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-bottom: 16px; }
.search-input {
  background: var(--panel2); border: 1px solid var(--line2); color: var(--ink);
  border-radius: 8px; padding: 8px 14px; font-size: 13px; min-width: 240px; outline: none;
}
.search-input:focus { border-color: var(--cyan); }
.filter-pills { display: flex; gap: 6px; flex-wrap: wrap; }
.pill {
  cursor: pointer; border: 1px solid var(--line2); background: var(--panel);
  color: var(--ink2); font-size: 12px; font-weight: 500; padding: 6px 12px; border-radius: 8px; transition: all .15s;
}
.pill:hover { color: var(--ink); border-color: var(--line); }
.pill.active { background: linear-gradient(90deg, #164e63, #312e81); border-color: #38bdf8; color: #fff; font-weight: 600; }

/* Tables */
.tbl-wrap { overflow: auto; border: 1px solid var(--line); border-radius: var(--radius); max-height: 580px; }
table.tbl { width: 100%; border-collapse: collapse; font-size: 12.5px; text-align: left; }
table.tbl th {
  position: sticky; top: 0; background: #131b3f; color: var(--ink2); padding: 11px 12px;
  font-size: 11px; letter-spacing: .08em; text-transform: uppercase; border-bottom: 1px solid var(--line2);
  white-space: nowrap; cursor: pointer; user-select: none;
}
table.tbl td { padding: 9px 12px; border-bottom: 1px solid rgba(31,42,85,.5); vertical-align: middle; }
table.tbl tr:hover td { background: rgba(99,102,241,.06); }

.scorebar { position: relative; height: 16px; background: #1a2350; border-radius: 4px; min-width: 110px; overflow: hidden; }
.scorebar .fill { position: absolute; left: 0; top: 0; bottom: 0; border-radius: 4px; background: linear-gradient(90deg, #0e7490, #22d3ee); }
.scorebar span { position: absolute; left: 6px; top: 1px; font-size: 10.5px; font-weight: 700; color: #04121a; }

.verdict { font-size: 11px; font-weight: 700; white-space: nowrap; }
.v-strong { color: var(--green); }
.v-situational { color: var(--amber); }
.v-niche { color: var(--orange); }
.v-unranked { color: var(--ink3); }
.v-avoid { color: var(--red); }

.tag {
  display: inline-block; font-size: 10px; font-family: var(--mono);
  border: 1px solid var(--line2); padding: 2px 7px; border-radius: 4px; color: var(--ink2);
}
.tag-prov { color: var(--indigo); border-color: rgba(129,140,248,.3); background: rgba(129,140,248,.08); }
.tag-prompt { color: var(--cyan); border-color: rgba(34,211,238,.3); background: rgba(34,211,238,.08); }

/* Grid Cards for Paradigms & Findings */
.cards-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 16px; }
.f-card {
  background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius);
  padding: 18px 20px; position: relative; overflow: hidden;
}
.f-card::before { content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 3px; background: var(--cyan); }
.f-card.danger::before { background: var(--red); }
.f-card.warn::before { background: var(--amber); }
.f-card.info::before { background: var(--indigo); }
.f-card h4 { font-size: 14.5px; color: #fff; margin-bottom: 6px; line-height: 1.35; }
.f-card p { font-size: 12.5px; color: var(--ink2); line-height: 1.5; }

/* Footer */
footer.site-foot {
  border-top: 1px solid var(--line); padding: 40px 32px 30px; color: var(--ink3); font-size: 12px; line-height: 1.7;
  display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 24px;
}
footer.site-foot a { color: var(--ink2); }
footer.site-foot a:hover { color: var(--cyan); }

@media (max-width: 900px) {
  header.site-nav { flex-direction: column; align-items: flex-start; padding: 14px 20px; }
  .nav-links { display: none; }
  .container { padding: 20px 16px 60px; }
  .hero { padding: 28px 22px; }
  .hero h2 { font-size: 28px; }
  .reports-grid { grid-template-columns: 1fr; }
}
</style>
`
