#!/usr/bin/env python3
"""
Generate static SEO landing pages — one per state, one per top OEM, plus
a few topic pages — pulling real data from data/vahan_local.db.

Output: docs/seo/states/{slug}/index.html
        docs/seo/oems/{slug}/index.html
        docs/seo/topics/{slug}/index.html

Each page is fully self-contained HTML — no external CSS/JS bundles, no
SPA — so Googlebot indexes them on the first crawl. CTAs link back to the
main dashboard for interactive exploration.

Usage:
    python scripts/generate_seo_pages.py [--out PATH]

Default output dir: docs/seo/  (the GitHub Pages workflow copies it into _site/seo/).
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import date
from pathlib import Path
from textwrap import dedent

ROOT     = Path(__file__).resolve().parent.parent
DB_PATH  = ROOT / "data" / "vahan_local.db"
OUT_DIR  = ROOT / "docs" / "seo"
SITE_URL = "https://www.vahanintelligence.in"

TODAY    = date.today().isoformat()

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def slug(name: str) -> str:
    """Lowercase + hyphenated, suitable for URL paths."""
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s

def fmt_int(n: int | float | None) -> str:
    if n is None:
        return "—"
    n = int(round(float(n)))
    # Indian-style comma grouping
    s = str(abs(n))
    if len(s) <= 3:
        out = s
    else:
        last3 = s[-3:]
        rest = s[:-3]
        groups = []
        while len(rest) > 2:
            groups.insert(0, rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.insert(0, rest)
        out = ",".join(groups) + "," + last3
    return ("-" if n < 0 else "") + out

def fmt_pct(p: float | None, sign: bool = False) -> str:
    if p is None:
        return "—"
    return (f"{p:+.1f}%" if sign else f"{p:.1f}%")

def yoy(curr: float | None, prev: float | None) -> float | None:
    if curr is None or prev is None or prev == 0:
        return None
    return (curr / prev - 1.0) * 100.0

def safe(v: str) -> str:
    """Minimal HTML-escape for content interpolated into static pages."""
    return (v.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
             .replace('"', "&quot;").replace("'", "&#39;"))

# -------------------------------------------------------------------------
# Data access
# -------------------------------------------------------------------------

def conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c

def latest_period(c: sqlite3.Connection) -> tuple[int, int, str]:
    """Returns (year, month, fy) of the latest record we have."""
    row = c.execute("SELECT year, month, fy FROM vahan_registrations ORDER BY year DESC, month DESC LIMIT 1").fetchone()
    return int(row["year"]), int(row["month"]), str(row["fy"])

MONTH_NAMES = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_NAMES_FULL = ["", "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

# OEM display rename — strip noise so titles read clean.
OEM_DISPLAY = {
    "MARUTI SUZUKI INDIA LTD": "Maruti Suzuki",
    "MAHINDRA & MAHINDRA LIMITED": "Mahindra",
    "HYUNDAI MOTOR INDIA LTD": "Hyundai",
    "TATA MOTORS PASSENGER VEHICLES LTD": "Tata Motors PV",
    "TATA MOTORS LTD": "Tata Motors",
    "TATA PASSENGER ELECTRIC MOBILITY LTD": "Tata Passenger Electric Mobility",
    "TOYOTA KIRLOSKAR MOTOR PVT LTD": "Toyota Kirloskar",
    "KIA INDIA PRIVATE LIMITED": "Kia India",
    "SKODA AUTO VOLKSWAGEN INDIA PVT LTD": "Skoda Auto Volkswagen",
    "JSW MG MOTOR INDIA PVT LTD": "JSW MG Motor",
    "HONDA CARS INDIA LTD": "Honda Cars",
    "MAHINDRA ELECTRIC AUTOMOBILE LTD": "Mahindra Electric",
    "RENAULT INDIA PVT LTD": "Renault India",
    "NISSAN MOTOR INDIA PVT LTD": "Nissan India",
    "MERCEDES-BENZ INDIA PVT LTD": "Mercedes-Benz India",
    "BMW INDIA PVT LTD": "BMW India",
    "BYD INDIA PRIVATE LIMITED": "BYD India",
}

def display_oem(maker: str) -> str:
    if maker in OEM_DISPLAY:
        return OEM_DISPLAY[maker]
    # Generic shortener: drop common corporate suffixes
    s = maker.title()
    for suf in [" India Pvt Ltd", " India Ltd", " India Private Limited", " Private Limited", " Pvt Ltd", " Ltd", " Limited"]:
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s

# -------------------------------------------------------------------------
# HTML primitives
# -------------------------------------------------------------------------

PAGE_CSS = """
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#f6f8fb;--surface:#fff;--surface2:#f1f4f9;--border:#e2e8f0;
  --text:#0f172a;--text-mid:#475569;--text-dim:#64748b;
  --blue:#2563eb;--blue-bg:rgba(37,99,235,.08);
  --green:#059669;--red:#dc2626;
  --font:'IBM Plex Sans',-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;
}
html,body{font-family:var(--font);background:var(--bg);color:var(--text);line-height:1.55}
.container{max-width:980px;margin:0 auto;padding:24px 20px 60px}
header.brand{padding:14px 0;border-bottom:1px solid var(--border);margin-bottom:24px}
header.brand a{color:var(--text);font-weight:700;text-decoration:none;font-size:17px}
header.brand .nav{float:right;font-size:14px}
header.brand .nav a{color:var(--text-mid);margin-left:18px;font-weight:500}
h1{font-size:30px;font-weight:700;line-height:1.25;margin-bottom:10px;color:var(--text)}
.lede{font-size:17px;color:var(--text-mid);margin-bottom:24px;line-height:1.6}
h2{font-size:22px;font-weight:700;margin:36px 0 12px;color:var(--text)}
h3{font-size:17px;font-weight:600;margin:24px 0 8px;color:var(--text)}
p{margin:8px 0}
ul,ol{margin:8px 0 12px 22px}
li{margin:4px 0}
.kpi-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:20px 0 28px}
.kpi{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px}
.kpi__label{font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;font-weight:600}
.kpi__val{font-size:22px;font-weight:700;color:var(--text);line-height:1.1}
.kpi__delta{font-size:13px;margin-top:4px;font-weight:600}
.kpi__delta.up{color:var(--green)}
.kpi__delta.dn{color:var(--red)}
table{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin:14px 0;font-size:14px}
th,td{text-align:left;padding:9px 12px;border-bottom:1px solid var(--border)}
th{background:var(--surface2);font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:var(--text-mid)}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover{background:var(--surface2)}
.cta{background:var(--blue-bg);border:1px solid var(--blue);border-radius:10px;padding:18px 20px;margin:28px 0}
.cta a.btn{display:inline-block;background:var(--blue);color:#fff!important;padding:9px 16px;border-radius:8px;text-decoration:none;font-weight:600;font-size:14px;margin-top:6px}
.cta a.btn:hover{background:#1d4ed8}
a{color:var(--blue)}
.footer{margin-top:48px;padding-top:18px;border-top:1px solid var(--border);font-size:13px;color:var(--text-dim)}
.footer a{color:var(--text-mid)}
.footer p{margin:4px 0}
.related{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px;margin:8px 0 0}
.related a{display:block;padding:10px 12px;background:var(--surface);border:1px solid var(--border);border-radius:8px;text-decoration:none;color:var(--text);font-size:14px}
.related a:hover{background:var(--surface2);border-color:var(--blue)}
.faq dt{font-weight:600;color:var(--text);margin:14px 0 4px}
.faq dd{color:var(--text-mid);margin-left:0}
.callout{background:var(--surface);border-left:3px solid var(--blue);padding:12px 16px;margin:14px 0;border-radius:6px;font-size:14.5px;color:var(--text-mid)}
@media(max-width:560px){h1{font-size:24px}.lede{font-size:15px}h2{font-size:19px}.container{padding:16px 14px 40px}}
"""

GTM_HEAD = """<!-- GTM -->
<script>(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);})(window,document,'script','dataLayer','GTM-MZMTHW35');</script>
"""

def page_shell(*, title: str, description: str, canonical: str,
               h1: str, body_html: str, jsonld_blocks: list[str],
               breadcrumb_label: str | None = None) -> str:
    """Wrap body in the full HTML shell (head + branded chrome + footer)."""
    bc = ""
    if breadcrumb_label:
        bc = f"""<script type="application/ld+json">{{
  "@context":"https://schema.org",
  "@type":"BreadcrumbList",
  "itemListElement":[
    {{"@type":"ListItem","position":1,"name":"Home","item":"{SITE_URL}/"}},
    {{"@type":"ListItem","position":2,"name":"{safe(breadcrumb_label)}","item":"{canonical}"}}
  ]
}}</script>"""

    jsonld_html = "\n".join(jsonld_blocks)

    return dedent(f"""\
    <!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="UTF-8"/>
    <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
    {GTM_HEAD}
    <title>{safe(title)}</title>
    <meta name="description" content="{safe(description)}"/>
    <link rel="canonical" href="{canonical}"/>
    <meta name="robots" content="index,follow,max-image-preview:large"/>
    <meta property="og:type" content="article"/>
    <meta property="og:title" content="{safe(title)}"/>
    <meta property="og:description" content="{safe(description)}"/>
    <meta property="og:url" content="{canonical}"/>
    <meta property="og:image" content="{SITE_URL}/og-image.jpg"/>
    <meta property="og:site_name" content="Vahan Intelligence"/>
    <meta name="twitter:card" content="summary_large_image"/>
    <meta name="twitter:title" content="{safe(title)}"/>
    <meta name="twitter:description" content="{safe(description)}"/>
    <meta name="twitter:image" content="{SITE_URL}/og-image.jpg"/>
    <link rel="icon" href="/favicon.ico" sizes="any"/>
    <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Cdefs%3E%3ClinearGradient id='g' x1='0' y1='0' x2='1' y2='1'%3E%3Cstop offset='0' stop-color='%231d4ed8'/%3E%3Cstop offset='1' stop-color='%230ea5e9'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect width='64' height='64' rx='12' fill='url(%23g)'/%3E%3Ctext x='32' y='42' text-anchor='middle' font-family='system-ui,Arial,sans-serif' font-size='28' font-weight='800' fill='white'%3EVI%3C/text%3E%3C/svg%3E"/>
    <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet"/>
    <style>{PAGE_CSS}</style>
    {jsonld_html}
    {bc}
    </head>
    <body>
    <noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-MZMTHW35" height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
    <header class="brand">
      <div class="container" style="padding-top:0;padding-bottom:0">
        <a href="/">📊 Vahan Intelligence</a>
        <nav class="nav">
          <a href="/#oem">OEMs</a>
          <a href="/#geomap">States</a>
          <a href="/#ev">EV</a>
          <a href="/#blog">Blog</a>
          <a href="/#info">Info</a>
        </nav>
      </div>
    </header>
    <main class="container">
      <h1>{safe(h1)}</h1>
      {body_html}
      <div class="footer">
        <p><strong>Source.</strong> Government of India VAHAN/Parivahan portal, cleaned and aggregated by Vahan Intelligence. Last refreshed {TODAY}.</p>
        <p><strong>License.</strong> Data is published under <a href="https://creativecommons.org/licenses/by/4.0/" rel="license">CC BY 4.0</a>. Cite as: "Vahan Intelligence, vahanintelligence.in".</p>
        <p><strong>Caveat.</strong> Numbers reflect new vehicle registrations on the VAHAN portal. The current calendar month is excluded because the portal accepts late entries for ~2 weeks.</p>
        <p>Questions? <a href="mailto:info@vahanintelligence.in">info@vahanintelligence.in</a> · <a href="/#info">Methodology</a> · <a href="/">Live dashboard</a></p>
      </div>
    </main>
    </body>
    </html>
    """)

# -------------------------------------------------------------------------
# Per-state page
# -------------------------------------------------------------------------

def fetch_state_metrics(c, state_name: str) -> dict:
    """Pull all metrics needed for a state landing page."""
    cur = c.cursor()

    # latest period
    ly, lm, lfy = latest_period(c)
    prev_y = ly - 1
    prev_y2 = ly - 2

    # Total this state, latest month + same month prior year
    def state_filter(state):
        if state == "All India":
            return "state_code = 'ALL'"
        return "state_name = ? AND state_code != 'ALL'"

    flt = state_filter(state_name)

    if state_name == "All India":
        params_lm = (ly, lm)
        params_pm = (prev_y, lm)
    else:
        params_lm = (state_name, ly, lm)
        params_pm = (state_name, prev_y, lm)

    row = cur.execute(f"SELECT SUM(count) as t FROM vahan_registrations WHERE {flt} AND year=? AND month=?", params_lm).fetchone()
    latest_total = int(row["t"] or 0)
    row = cur.execute(f"SELECT SUM(count) as t FROM vahan_registrations WHERE {flt} AND year=? AND month=?", params_pm).fetchone()
    prior_total = int(row["t"] or 0)

    # FY 25-26 + 24-25 totals
    if state_name == "All India":
        rows = cur.execute(f"SELECT fy, SUM(count) as t FROM vahan_registrations WHERE state_code='ALL' AND fy IN ('FY2024-25','FY2025-26','FY2026-27') GROUP BY fy").fetchall()
    else:
        rows = cur.execute(f"SELECT fy, SUM(count) as t FROM vahan_registrations WHERE state_name=? AND state_code!='ALL' AND fy IN ('FY2024-25','FY2025-26','FY2026-27') GROUP BY fy", (state_name,)).fetchall()
    fy_totals = {r["fy"]: int(r["t"]) for r in rows}

    # Top 10 OEMs in state for latest month
    if state_name == "All India":
        rows = cur.execute("""
        SELECT maker, SUM(count) AS t FROM vahan_registrations
        WHERE state_code='ALL' AND year=? AND month=?
        GROUP BY maker ORDER BY t DESC LIMIT 10
        """, (ly, lm)).fetchall()
    else:
        rows = cur.execute("""
        SELECT maker, SUM(count) AS t FROM vahan_registrations
        WHERE state_name=? AND state_code!='ALL' AND year=? AND month=?
        GROUP BY maker ORDER BY t DESC LIMIT 10
        """, (state_name, ly, lm)).fetchall()
    top_oems = [(r["maker"], int(r["t"])) for r in rows]

    # YoY for each top OEM
    top_oems_yoy = []
    for maker, t in top_oems:
        if state_name == "All India":
            r = cur.execute("SELECT SUM(count) as t FROM vahan_registrations WHERE state_code='ALL' AND maker=? AND year=? AND month=?",
                            (maker, prev_y, lm)).fetchone()
        else:
            r = cur.execute("SELECT SUM(count) as t FROM vahan_registrations WHERE state_name=? AND state_code!='ALL' AND maker=? AND year=? AND month=?",
                            (state_name, maker, prev_y, lm)).fetchone()
        prev_t = int(r["t"] or 0)
        top_oems_yoy.append((maker, t, prev_t, yoy(t, prev_t)))

    # Fuel mix for state in latest month
    if state_name == "All India":
        rows = cur.execute("""
        SELECT fuel_type, SUM(count) AS t FROM vahan_registrations
        WHERE state_code='ALL' AND year=? AND month=?
        GROUP BY fuel_type ORDER BY t DESC
        """, (ly, lm)).fetchall()
    else:
        rows = cur.execute("""
        SELECT fuel_type, SUM(count) AS t FROM vahan_registrations
        WHERE state_name=? AND state_code!='ALL' AND year=? AND month=?
        GROUP BY fuel_type ORDER BY t DESC
        """, (state_name, ly, lm)).fetchall()
    fuel_mix = [(r["fuel_type"], int(r["t"])) for r in rows]
    fuel_total = sum(t for _, t in fuel_mix) or 1

    # 5-year FY trend
    fy_trend_rows = cur.execute(f"""
    SELECT fy, SUM(count) AS t FROM vahan_registrations
    WHERE {flt}
    AND fy IN ('FY2021-22','FY2022-23','FY2023-24','FY2024-25','FY2025-26')
    GROUP BY fy ORDER BY fy
    """, () if state_name == "All India" else (state_name,)).fetchall()
    fy_trend = [(r["fy"], int(r["t"])) for r in fy_trend_rows]

    # National benchmark for context (only for non-All-India states)
    if state_name != "All India":
        ai_lm = cur.execute("SELECT SUM(count) as t FROM vahan_registrations WHERE state_code='ALL' AND year=? AND month=?", (ly, lm)).fetchone()
        ai_total = int(ai_lm["t"] or 0)
        ai_ev = cur.execute("SELECT SUM(count) as t FROM vahan_registrations WHERE state_code='ALL' AND year=? AND month=? AND fuel_type='EV'", (ly, lm)).fetchone()
        ai_ev_pct = (int(ai_ev["t"] or 0) / ai_total * 100) if ai_total else 0
    else:
        ai_total = latest_total
        ai_ev_pct = 0

    return {
        "state": state_name,
        "latest_year": ly, "latest_month": lm, "latest_fy": lfy,
        "latest_total": latest_total,
        "prior_total": prior_total,
        "yoy_pct": yoy(latest_total, prior_total),
        "fy_totals": fy_totals,
        "top_oems_yoy": top_oems_yoy,
        "fuel_mix": fuel_mix, "fuel_total": fuel_total,
        "fy_trend": fy_trend,
        "ai_total_lm": ai_total,
        "ai_ev_pct": ai_ev_pct,
    }

def render_state_page(state_name: str, m: dict) -> str:
    s = state_name
    s_lower = s.lower()
    is_ai = (s == "All India")
    ly, lm, lfy = m["latest_year"], m["latest_month"], m["latest_fy"]
    month_full = f"{MONTH_NAMES_FULL[lm]} {ly}"
    month_short = f"{MONTH_NAMES[lm]} {ly}"

    canonical = f"{SITE_URL}/seo/states/{slug(s)}/" if not is_ai else f"{SITE_URL}/seo/states/india/"

    title_state = "India" if is_ai else s
    # Title references the most recent COMPLETE FY (2025-26), not the in-progress one
    # the latest month happens to fall in (e.g. Apr 2026 = FY 2026-27 but FY 25-26
    # just closed and is what people search for).
    title = f"{title_state} Passenger Vehicle Registrations — {month_short} + FY 2025-26 Numbers | Vahan Intelligence"
    desc_top_oem = display_oem(m["top_oems_yoy"][0][0]) if m["top_oems_yoy"] else "n/a"
    desc = (f"{title_state} car registrations: {fmt_int(m['latest_total'])} in {month_short} ({fmt_pct(m['yoy_pct'], sign=True)} YoY). "
            f"Top OEM {desc_top_oem}. Fuel mix, OEM ranking, FY 25-26 totals from VAHAN data.")

    h1 = f"{title_state} Passenger Vehicle Registrations — {month_short}"

    # KPI strip
    fy2526 = m["fy_totals"].get("FY2025-26", 0)
    fy2425 = m["fy_totals"].get("FY2024-25", 0)
    fy_yoy = yoy(fy2526, fy2425)
    ev_t = next((t for f, t in m["fuel_mix"] if f == "EV"), 0)
    ev_pct = (ev_t / m["fuel_total"] * 100) if m["fuel_total"] else 0
    ev_delta_label = ""
    if not is_ai and m["ai_ev_pct"] > 0:
        gap = ev_pct - m["ai_ev_pct"]
        ev_delta_label = f' <span class="kpi__delta {"up" if gap > 0 else "dn"}">{fmt_pct(gap, sign=True)} vs India</span>'

    kpi_html = f"""
    <div class="kpi-strip">
      <div class="kpi"><div class="kpi__label">{month_full} regs</div><div class="kpi__val">{fmt_int(m['latest_total'])}</div>
        <div class="kpi__delta {'up' if (m['yoy_pct'] or 0) > 0 else 'dn'}">{fmt_pct(m['yoy_pct'], sign=True)} YoY</div></div>
      <div class="kpi"><div class="kpi__label">FY 2025-26 total</div><div class="kpi__val">{fmt_int(fy2526)}</div>
        <div class="kpi__delta {'up' if (fy_yoy or 0) > 0 else 'dn'}">{fmt_pct(fy_yoy, sign=True)} vs FY 24-25</div></div>
      <div class="kpi"><div class="kpi__label">Top OEM ({month_short})</div><div class="kpi__val" style="font-size:18px">{safe(display_oem(m['top_oems_yoy'][0][0])) if m['top_oems_yoy'] else '—'}</div>
        <div class="kpi__delta">{fmt_int(m['top_oems_yoy'][0][1]) if m['top_oems_yoy'] else '—'} regs</div></div>
      <div class="kpi"><div class="kpi__label">EV share</div><div class="kpi__val">{ev_pct:.2f}%</div>{ev_delta_label}</div>
    </div>
    """

    # Lead paragraph
    if is_ai:
        lede = (f"India recorded <strong>{fmt_int(m['latest_total'])}</strong> new passenger vehicle registrations in {month_full} — "
                f"{'a' if (m['yoy_pct'] or 0) >= 0 else 'down'} <strong>{fmt_pct(abs(m['yoy_pct'] or 0))}</strong> "
                f"{'rise' if (m['yoy_pct'] or 0) >= 0 else 'decline'} versus {month_short.replace(str(ly), str(ly-1))}. "
                f"FY 2025-26 closed at <strong>{fmt_int(fy2526)}</strong> registrations "
                f"({fmt_pct(fy_yoy, sign=True)} vs FY 2024-25). "
                f"{display_oem(m['top_oems_yoy'][0][0])} held the top spot with {fmt_int(m['top_oems_yoy'][0][1])} registrations, while EV share reached {ev_pct:.2f}%.")
    else:
        ai_share = (m["latest_total"] / m["ai_total_lm"] * 100) if m["ai_total_lm"] else 0
        ai_ev_pct = m["ai_ev_pct"]
        ev_compare_clause = ""
        if ai_ev_pct:
            verb = "above" if ev_pct > ai_ev_pct else "below"
            ev_compare_clause = f" — {verb} the national {ai_ev_pct:.2f}%"
        lede = (f"{s} registered <strong>{fmt_int(m['latest_total'])}</strong> new passenger vehicles in {month_full} — "
                f"{fmt_pct(ai_share)} of India's total. "
                f"{'Up' if (m['yoy_pct'] or 0) >= 0 else 'Down'} <strong>{fmt_pct(abs(m['yoy_pct'] or 0))}</strong> versus the same month last year. "
                f"FY 2025-26 closed at <strong>{fmt_int(fy2526)}</strong> ({fmt_pct(fy_yoy, sign=True)} YoY). "
                f"Top OEM in {s}: <strong>{display_oem(m['top_oems_yoy'][0][0])}</strong> "
                f"({fmt_int(m['top_oems_yoy'][0][1])} regs). EV share at {ev_pct:.2f}%{ev_compare_clause}.")

    # Top OEMs table
    oem_rows = "\n".join(
        f"<tr><td>{i+1}</td><td>{safe(display_oem(maker))}</td><td class='num'>{fmt_int(t)}</td>"
        f"<td class='num' style='color:{'var(--green)' if (yoy_v or 0) > 0 else 'var(--red)'}'>{fmt_pct(yoy_v, sign=True)}</td></tr>"
        for i, (maker, t, _pt, yoy_v) in enumerate(m["top_oems_yoy"])
    )
    oem_table = f"""
    <table>
      <thead><tr><th>#</th><th>OEM</th><th class="num">{month_full} regs</th><th class="num">YoY %</th></tr></thead>
      <tbody>{oem_rows}</tbody>
    </table>
    """

    # Fuel mix table
    fuel_rows = "\n".join(
        f"<tr><td>{safe(f)}</td><td class='num'>{fmt_int(t)}</td><td class='num'>{(t / m['fuel_total'] * 100):.2f}%</td></tr>"
        for f, t in m["fuel_mix"]
    )
    fuel_table = f"""
    <table>
      <thead><tr><th>Fuel</th><th class="num">{month_full} regs</th><th class="num">Share</th></tr></thead>
      <tbody>{fuel_rows}</tbody>
    </table>
    """

    # 5-year FY trend
    trend_rows = "\n".join(
        f"<tr><td>FY {fy[2:]}</td><td class='num'>{fmt_int(t)}</td></tr>"
        for fy, t in m["fy_trend"]
    )
    trend_table = f"""
    <table>
      <thead><tr><th>Financial Year</th><th class="num">Registrations</th></tr></thead>
      <tbody>{trend_rows}</tbody>
    </table>
    """

    # FAQ specific to state
    faqs = [
        (f"What was the total number of car registrations in {title_state} in {month_full}?",
         f"{title_state} recorded {fmt_int(m['latest_total'])} new passenger vehicle registrations in {month_full}, sourced from the Government of India's VAHAN portal."),
        (f"Which OEM leads in {title_state}?",
         f"In {month_full}, {display_oem(m['top_oems_yoy'][0][0])} led {title_state} with {fmt_int(m['top_oems_yoy'][0][1])} registrations."),
        (f"How does {title_state} compare to All India?" if not is_ai else "How was the YoY growth?",
         (f"{title_state} accounted for {fmt_pct(ai_share)} of India's total PV registrations in {month_full}." if not is_ai
          else f"India's PV registrations changed by {fmt_pct(m['yoy_pct'], sign=True)} YoY in {month_full}, with FY 2025-26 totals at {fmt_int(fy2526)}.")),
        (f"What is the EV share in {title_state}?",
         f"EV passenger vehicles made up {ev_pct:.2f}% of {title_state}'s {month_full} registrations" + (
             f", versus {m['ai_ev_pct']:.2f}% nationally." if not is_ai else "."
         )),
    ]
    faq_dl = "\n".join(f"<dt>{safe(q)}</dt><dd>{safe(a)}</dd>" for q, a in faqs)
    faq_html = f'<dl class="faq">{faq_dl}</dl>'
    faq_jsonld = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {"@type": "Question", "name": q,
             "acceptedAnswer": {"@type": "Answer", "text": a}}
            for q, a in faqs
        ]
    }
    import json as _json
    faq_block = f'<script type="application/ld+json">{_json.dumps(faq_jsonld, separators=(",",":"))}</script>'

    # Article schema
    article_jsonld = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": title,
        "description": desc,
        "datePublished": TODAY,
        "dateModified": TODAY,
        "author": {"@type": "Organization", "name": "Vahan Intelligence"},
        "publisher": {"@type": "Organization", "name": "Vahan Intelligence",
                      "logo": {"@type": "ImageObject", "url": f"{SITE_URL}/og-image.jpg"}},
        "mainEntityOfPage": {"@type": "WebPage", "@id": canonical},
        "image": f"{SITE_URL}/og-image.jpg",
    }
    article_block = f'<script type="application/ld+json">{_json.dumps(article_jsonld, separators=(",",":"))}</script>'

    # Related links — 5 other states + a couple of OEM pages
    other_states = [r for r in [
        "Maharashtra","Uttar Pradesh","Tamil Nadu","Gujarat","Karnataka","Kerala",
        "Haryana","Rajasthan","Madhya Pradesh","Delhi","West Bengal","Andhra Pradesh"
    ] if r != s][:6]
    rel_html = "\n".join(f'<a href="/seo/states/{slug(r)}/">{safe(r)} car registrations</a>' for r in other_states)
    rel_html += '\n<a href="/seo/oems/maruti-suzuki/">Maruti Suzuki market share</a>'
    rel_html += '\n<a href="/seo/oems/tata-motors-pv/">Tata Motors registrations</a>'

    body_html = f"""
    <p class="lede">{lede}</p>
    {kpi_html}

    <h2>Top 10 OEMs in {title_state} — {month_full}</h2>
    {oem_table}

    <h2>Fuel mix — {month_full}</h2>
    {fuel_table}
    <p class="callout">EV share in {title_state} is <strong>{ev_pct:.2f}%</strong>{
        f" ({'above' if ev_pct > m['ai_ev_pct'] else 'below'} the All-India average of {m['ai_ev_pct']:.2f}%)" if not is_ai else ""
    }. The share has been climbing every quarter since FAME-II uptake intensified.</p>

    <h2>5-year financial-year trend</h2>
    {trend_table}

    <div class="cta">
      <strong>Explore the full live dashboard.</strong>
      <p>Drill into 13 years of monthly data, compare {title_state} against any state, run forecasts, and download the cleaned data.</p>
      <a class="btn" href="/{'#compare' if not is_ai else ''}">Open the {title_state} interactive view →</a>
    </div>

    <h2>FAQs</h2>
    {faq_html}

    <h2>Related pages</h2>
    <div class="related">{rel_html}</div>
    """

    return page_shell(
        title=title, description=desc, canonical=canonical,
        h1=h1, body_html=body_html,
        jsonld_blocks=[article_block, faq_block],
        breadcrumb_label=f"{title_state} car registrations",
    )

# -------------------------------------------------------------------------
# Per-OEM page
# -------------------------------------------------------------------------

def fetch_oem_metrics(c, maker: str) -> dict:
    cur = c.cursor()
    ly, lm, lfy = latest_period(c)
    prev_y = ly - 1

    # All India total for this OEM, latest month + prior year
    row = cur.execute("SELECT SUM(count) as t FROM vahan_registrations WHERE state_code='ALL' AND maker=? AND year=? AND month=?",
                      (maker, ly, lm)).fetchone()
    latest = int(row["t"] or 0)
    row = cur.execute("SELECT SUM(count) as t FROM vahan_registrations WHERE state_code='ALL' AND maker=? AND year=? AND month=?",
                      (maker, prev_y, lm)).fetchone()
    prior = int(row["t"] or 0)

    # Industry total latest month for share calc
    ai = cur.execute("SELECT SUM(count) as t FROM vahan_registrations WHERE state_code='ALL' AND year=? AND month=?",
                     (ly, lm)).fetchone()
    ai_total = int(ai["t"] or 0)
    market_share = (latest / ai_total * 100) if ai_total else 0

    # FY totals (5 years)
    rows = cur.execute("""
    SELECT fy, SUM(count) AS t FROM vahan_registrations
    WHERE state_code='ALL' AND maker=?
    AND fy IN ('FY2021-22','FY2022-23','FY2023-24','FY2024-25','FY2025-26')
    GROUP BY fy ORDER BY fy
    """, (maker,)).fetchall()
    fy_trend = [(r["fy"], int(r["t"])) for r in rows]

    # Top 10 states for this OEM in latest month
    rows = cur.execute("""
    SELECT state_name, SUM(count) AS t FROM vahan_registrations
    WHERE state_code != 'ALL' AND maker=? AND year=? AND month=?
    GROUP BY state_name ORDER BY t DESC LIMIT 10
    """, (maker, ly, lm)).fetchall()
    top_states = [(r["state_name"], int(r["t"])) for r in rows]

    # Fuel mix for OEM in FY 25-26
    rows = cur.execute("""
    SELECT fuel_type, SUM(count) AS t FROM vahan_registrations
    WHERE state_code='ALL' AND maker=? AND fy='FY2025-26'
    GROUP BY fuel_type ORDER BY t DESC
    """, (maker,)).fetchall()
    fuel_mix = [(r["fuel_type"], int(r["t"])) for r in rows]
    fuel_total = sum(t for _, t in fuel_mix) or 1

    # Top 3 competitors latest month
    rows = cur.execute("""
    SELECT maker, SUM(count) AS t FROM vahan_registrations
    WHERE state_code='ALL' AND year=? AND month=?
    GROUP BY maker ORDER BY t DESC LIMIT 6
    """, (ly, lm)).fetchall()
    competitors = [(r["maker"], int(r["t"])) for r in rows if r["maker"] != maker][:5]

    return {
        "maker": maker, "display": display_oem(maker),
        "latest_year": ly, "latest_month": lm, "latest_fy": lfy,
        "latest": latest, "prior": prior,
        "yoy_pct": yoy(latest, prior),
        "market_share": market_share,
        "ai_total": ai_total,
        "fy_trend": fy_trend,
        "top_states": top_states,
        "fuel_mix": fuel_mix, "fuel_total": fuel_total,
        "competitors": competitors,
    }

def render_oem_page(maker: str, m: dict) -> str:
    disp = m["display"]
    canonical = f"{SITE_URL}/seo/oems/{slug(disp)}/"

    ly, lm, lfy = m["latest_year"], m["latest_month"], m["latest_fy"]
    month_full = f"{MONTH_NAMES_FULL[lm]} {ly}"
    month_short = f"{MONTH_NAMES[lm]} {ly}"

    title = f"{disp} Market Share India — {month_short} Registrations + Trend | Vahan Intelligence"
    desc = (f"{disp} sold {fmt_int(m['latest'])} cars in India in {month_short} "
            f"({fmt_pct(m['market_share'])} market share, {fmt_pct(m['yoy_pct'], sign=True)} YoY). "
            f"State-wise sales, fuel mix, FY 25-26 totals.")

    h1 = f"{disp} Registrations & Market Share — India, {month_short}"

    fy2526 = next((t for fy, t in m["fy_trend"] if fy == "FY2025-26"), 0)
    fy2425 = next((t for fy, t in m["fy_trend"] if fy == "FY2024-25"), 0)
    fy_yoy = yoy(fy2526, fy2425)
    ev_t = next((t for f, t in m["fuel_mix"] if f == "EV"), 0)
    ev_pct = (ev_t / m["fuel_total"] * 100) if m["fuel_total"] else 0
    cng_t = next((t for f, t in m["fuel_mix"] if f == "CNG"), 0)
    cng_pct = (cng_t / m["fuel_total"] * 100) if m["fuel_total"] else 0

    kpi_html = f"""
    <div class="kpi-strip">
      <div class="kpi"><div class="kpi__label">{month_full} regs</div><div class="kpi__val">{fmt_int(m['latest'])}</div>
        <div class="kpi__delta {'up' if (m['yoy_pct'] or 0) > 0 else 'dn'}">{fmt_pct(m['yoy_pct'], sign=True)} YoY</div></div>
      <div class="kpi"><div class="kpi__label">Market share ({month_short})</div><div class="kpi__val">{m['market_share']:.1f}%</div>
        <div class="kpi__delta">of {fmt_int(m['ai_total'])}</div></div>
      <div class="kpi"><div class="kpi__label">FY 2025-26 total</div><div class="kpi__val">{fmt_int(fy2526)}</div>
        <div class="kpi__delta {'up' if (fy_yoy or 0) > 0 else 'dn'}">{fmt_pct(fy_yoy, sign=True)} vs FY 24-25</div></div>
      <div class="kpi"><div class="kpi__label">Strongest state ({month_short})</div><div class="kpi__val" style="font-size:18px">{safe(m['top_states'][0][0]) if m['top_states'] else '—'}</div>
        <div class="kpi__delta">{fmt_int(m['top_states'][0][1]) if m['top_states'] else '—'} regs</div></div>
    </div>
    """

    lede = (f"<strong>{disp}</strong> registered <strong>{fmt_int(m['latest'])}</strong> passenger vehicles in India in {month_full} — "
            f"a market share of <strong>{m['market_share']:.1f}%</strong>, "
            f"{'up' if (m['yoy_pct'] or 0) >= 0 else 'down'} <strong>{fmt_pct(abs(m['yoy_pct'] or 0))}</strong> versus the same month last year. "
            f"FY 2025-26 closed at <strong>{fmt_int(fy2526)}</strong> registrations "
            f"({fmt_pct(fy_yoy, sign=True)} vs FY 2024-25). "
            f"{disp}'s strongest state in {month_short} was {m['top_states'][0][0]} with {fmt_int(m['top_states'][0][1])} registrations.")

    # FY trend
    trend_rows = "\n".join(
        f"<tr><td>FY {fy[2:]}</td><td class='num'>{fmt_int(t)}</td></tr>"
        for fy, t in m["fy_trend"]
    )
    trend_table = f"""
    <table>
      <thead><tr><th>Financial Year</th><th class="num">Registrations</th></tr></thead>
      <tbody>{trend_rows}</tbody>
    </table>
    """

    # Top states table
    states_rows = "\n".join(
        f"<tr><td>{i+1}</td><td><a href='/seo/states/{slug(st)}/'>{safe(st)}</a></td><td class='num'>{fmt_int(t)}</td>"
        f"<td class='num'>{(t / m['latest'] * 100):.1f}%</td></tr>"
        for i, (st, t) in enumerate(m["top_states"])
    )
    states_table = f"""
    <table>
      <thead><tr><th>#</th><th>State</th><th class="num">{month_full} regs</th><th class="num">% of {disp}</th></tr></thead>
      <tbody>{states_rows}</tbody>
    </table>
    """

    # Fuel mix
    fuel_rows = "\n".join(
        f"<tr><td>{safe(f)}</td><td class='num'>{fmt_int(t)}</td><td class='num'>{(t / m['fuel_total'] * 100):.2f}%</td></tr>"
        for f, t in m["fuel_mix"]
    )
    fuel_table = f"""
    <table>
      <thead><tr><th>Fuel</th><th class="num">FY 25-26 regs</th><th class="num">Share of {disp}</th></tr></thead>
      <tbody>{fuel_rows}</tbody>
    </table>
    """

    # Competitors
    comp_rows = "\n".join(
        f"<tr><td>{i+1}</td><td><a href='/seo/oems/{slug(display_oem(comp))}/'>{safe(display_oem(comp))}</a></td><td class='num'>{fmt_int(t)}</td><td class='num'>{(t / m['ai_total'] * 100):.1f}%</td></tr>"
        for i, (comp, t) in enumerate(m["competitors"])
    )
    comp_table = f"""
    <table>
      <thead><tr><th>#</th><th>OEM</th><th class="num">{month_full} regs</th><th class="num">Market share</th></tr></thead>
      <tbody>{comp_rows}</tbody>
    </table>
    """

    # FAQ
    faqs = [
        (f"How many cars did {disp} sell in India in {month_full}?",
         f"{disp} registered {fmt_int(m['latest'])} passenger vehicles in India in {month_full}, capturing {m['market_share']:.1f}% market share."),
        (f"What is {disp}'s market share trend?",
         f"In {month_full}, {disp}'s market share was {m['market_share']:.1f}% — {'up' if (m['yoy_pct'] or 0) >= 0 else 'down'} {fmt_pct(abs(m['yoy_pct'] or 0))} YoY in volume. FY 2025-26 total was {fmt_int(fy2526)}."),
        (f"Which states are strongest for {disp}?",
         f"In {month_full}, {disp}'s top states were " + ", ".join(s for s, _ in m["top_states"][:5]) + "."),
        (f"What is {disp}'s fuel mix?",
         f"In FY 2025-26, {disp}'s fuel mix was: " + ", ".join(f"{f} {(t/m['fuel_total']*100):.1f}%" for f, t in m["fuel_mix"][:4]) + "."),
        (f"Who are {disp}'s top competitors?",
         "Top competitors in India by volume: " + ", ".join(display_oem(c) for c, _ in m["competitors"][:4]) + "."),
    ]
    faq_dl = "\n".join(f"<dt>{safe(q)}</dt><dd>{safe(a)}</dd>" for q, a in faqs)
    faq_html = f'<dl class="faq">{faq_dl}</dl>'
    faq_jsonld = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [{"@type": "Question", "name": q,
                        "acceptedAnswer": {"@type": "Answer", "text": a}}
                       for q, a in faqs]
    }
    import json as _json
    faq_block = f'<script type="application/ld+json">{_json.dumps(faq_jsonld, separators=(",",":"))}</script>'

    article_jsonld = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": title,
        "description": desc,
        "datePublished": TODAY,
        "dateModified": TODAY,
        "author": {"@type": "Organization", "name": "Vahan Intelligence"},
        "publisher": {"@type": "Organization", "name": "Vahan Intelligence",
                      "logo": {"@type": "ImageObject", "url": f"{SITE_URL}/og-image.jpg"}},
        "mainEntityOfPage": {"@type": "WebPage", "@id": canonical},
        "image": f"{SITE_URL}/og-image.jpg",
    }
    article_block = f'<script type="application/ld+json">{_json.dumps(article_jsonld, separators=(",",":"))}</script>'

    body_html = f"""
    <p class="lede">{lede}</p>
    {kpi_html}

    <h2>{disp} top states — {month_full}</h2>
    {states_table}

    <h2>{disp} fuel mix — FY 2025-26</h2>
    {fuel_table}
    {f'<p class="callout"><strong>EV share for {disp}: {ev_pct:.1f}%</strong>{". CNG mix: " + f"{cng_pct:.1f}%" if cng_pct > 1 else ""}.</p>' if (ev_pct + cng_pct) > 0 else ''}

    <h2>5-year FY trend</h2>
    {trend_table}

    <h2>Competitive context — top OEMs in India</h2>
    {comp_table}

    <div class="cta">
      <strong>Run a deeper analysis.</strong>
      <p>Compare {disp} against any other OEM, drill into specific states, see 13-year monthly trends, run forecasts.</p>
      <a class="btn" href="/#oem">Open the OEM analysis page →</a>
    </div>

    <h2>FAQs</h2>
    {faq_html}

    <h2>Related pages</h2>
    <div class="related">
      <a href="/seo/oems/maruti-suzuki/">Maruti Suzuki market share</a>
      <a href="/seo/oems/hyundai/">Hyundai India registrations</a>
      <a href="/seo/oems/tata-motors-pv/">Tata Motors PV</a>
      <a href="/seo/oems/mahindra/">Mahindra registrations</a>
      <a href="/seo/topics/ev-india/">India EV adoption</a>
      <a href="/seo/states/india/">India PV registrations summary</a>
    </div>
    """

    return page_shell(
        title=title, description=desc, canonical=canonical,
        h1=h1, body_html=body_html,
        jsonld_blocks=[article_block, faq_block],
        breadcrumb_label=f"{disp} market share",
    )

# -------------------------------------------------------------------------
# Topic page: India EV adoption
# -------------------------------------------------------------------------

def render_ev_topic_page(c) -> str:
    cur = c.cursor()
    ly, lm, lfy = latest_period(c)
    month_full = f"{MONTH_NAMES_FULL[lm]} {ly}"
    month_short = f"{MONTH_NAMES[lm]} {ly}"

    # State EV adoption rate (latest month)
    rows = cur.execute("""
    SELECT state_name,
           SUM(CASE WHEN fuel_type='EV' THEN count ELSE 0 END) AS ev,
           SUM(count) AS tot
    FROM vahan_registrations
    WHERE state_code != 'ALL' AND year=? AND month=?
    GROUP BY state_name
    HAVING tot > 1000
    """, (ly, lm)).fetchall()
    state_ev = [(r["state_name"], int(r["ev"]), int(r["tot"]),
                 (int(r["ev"]) / int(r["tot"]) * 100) if r["tot"] else 0)
                for r in rows]
    state_ev.sort(key=lambda x: -x[3])
    top_states_ev = state_ev[:15]

    # Top EV OEMs
    rows = cur.execute("""
    SELECT maker, SUM(count) AS t FROM vahan_registrations
    WHERE state_code='ALL' AND fuel_type='EV' AND fy='FY2025-26'
    GROUP BY maker ORDER BY t DESC LIMIT 10
    """).fetchall()
    top_ev_oems = [(r["maker"], int(r["t"])) for r in rows]
    ev_total_fy = sum(t for _, t in top_ev_oems)

    # India EV totals — last 5 FYs
    rows = cur.execute("""
    SELECT fy, SUM(CASE WHEN fuel_type='EV' THEN count ELSE 0 END) AS ev,
                SUM(count) AS tot
    FROM vahan_registrations
    WHERE state_code='ALL' AND fy IN ('FY2021-22','FY2022-23','FY2023-24','FY2024-25','FY2025-26')
    GROUP BY fy ORDER BY fy
    """).fetchall()
    ev_trend = [(r["fy"], int(r["ev"]), int(r["tot"]),
                 (int(r["ev"]) / int(r["tot"]) * 100) if r["tot"] else 0)
                for r in rows]

    canonical = f"{SITE_URL}/seo/topics/ev-india/"
    title = f"India EV Adoption — State-Wise + OEM Rankings, {month_short} | Vahan Intelligence"

    ai_ev_pct = next((p for fy, _e, _t, p in ev_trend if fy == "FY2025-26"), 0)
    desc = (f"India EV passenger-vehicle adoption: {ai_ev_pct:.2f}% share in FY 2025-26, "
            f"top adopting states ({top_states_ev[0][0]} at {top_states_ev[0][3]:.1f}%), top EV OEMs.")

    h1 = f"India EV Adoption — Passenger Vehicle Registrations, {month_short}"

    # KPI strip
    fy2526_ev = next((e for fy, e, _t, _p in ev_trend if fy == "FY2025-26"), 0)
    fy2425_ev = next((e for fy, e, _t, _p in ev_trend if fy == "FY2024-25"), 0)
    ev_yoy = yoy(fy2526_ev, fy2425_ev)
    fy2526_pct = next((p for fy, _e, _t, p in ev_trend if fy == "FY2025-26"), 0)
    fy2425_pct = next((p for fy, _e, _t, p in ev_trend if fy == "FY2024-25"), 0)

    kpi_html = f"""
    <div class="kpi-strip">
      <div class="kpi"><div class="kpi__label">FY 25-26 EV regs</div><div class="kpi__val">{fmt_int(fy2526_ev)}</div>
        <div class="kpi__delta up">{fmt_pct(ev_yoy, sign=True)} vs FY 24-25</div></div>
      <div class="kpi"><div class="kpi__label">EV share FY 25-26</div><div class="kpi__val">{fy2526_pct:.2f}%</div>
        <div class="kpi__delta">vs {fy2425_pct:.2f}% prior FY</div></div>
      <div class="kpi"><div class="kpi__label">Top EV state</div><div class="kpi__val" style="font-size:18px">{safe(top_states_ev[0][0])}</div>
        <div class="kpi__delta">{top_states_ev[0][3]:.2f}% adoption</div></div>
      <div class="kpi"><div class="kpi__label">Top EV OEM (FY 25-26)</div><div class="kpi__val" style="font-size:18px">{safe(display_oem(top_ev_oems[0][0]))}</div>
        <div class="kpi__delta">{fmt_int(top_ev_oems[0][1])} EV regs</div></div>
    </div>
    """

    lede = (f"India's electric passenger vehicle adoption reached <strong>{fy2526_pct:.2f}%</strong> in FY 2025-26 — "
            f"up from {fy2425_pct:.2f}% the year before. EV registrations grew "
            f"<strong>{fmt_pct(ev_yoy, sign=True)}</strong> YoY. {top_states_ev[0][0]} leads with "
            f"<strong>{top_states_ev[0][3]:.2f}%</strong> EV adoption rate, "
            f"and <strong>{display_oem(top_ev_oems[0][0])}</strong> dominates the EV OEM ranking with "
            f"<strong>{(top_ev_oems[0][1] / ev_total_fy * 100):.1f}%</strong> of EV volume.")

    # State EV table
    state_rows = "\n".join(
        f"<tr><td>{i+1}</td><td><a href='/seo/states/{slug(st)}/'>{safe(st)}</a></td><td class='num'>{fmt_int(ev)}</td><td class='num'>{p:.2f}%</td></tr>"
        for i, (st, ev, _, p) in enumerate(top_states_ev)
    )
    state_table = f"""
    <table>
      <thead><tr><th>#</th><th>State</th><th class="num">{month_full} EV regs</th><th class="num">EV share</th></tr></thead>
      <tbody>{state_rows}</tbody>
    </table>
    """

    # OEM table
    oem_rows = "\n".join(
        f"<tr><td>{i+1}</td><td><a href='/seo/oems/{slug(display_oem(maker))}/'>{safe(display_oem(maker))}</a></td><td class='num'>{fmt_int(t)}</td><td class='num'>{(t / ev_total_fy * 100):.1f}%</td></tr>"
        for i, (maker, t) in enumerate(top_ev_oems)
    )
    oem_table = f"""
    <table>
      <thead><tr><th>#</th><th>OEM</th><th class="num">FY 25-26 EV regs</th><th class="num">Share of EV</th></tr></thead>
      <tbody>{oem_rows}</tbody>
    </table>
    """

    # FY trend
    trend_rows = "\n".join(
        f"<tr><td>FY {fy[2:]}</td><td class='num'>{fmt_int(ev)}</td><td class='num'>{p:.2f}%</td></tr>"
        for fy, ev, _, p in ev_trend
    )
    trend_table = f"""
    <table>
      <thead><tr><th>Financial Year</th><th class="num">EV regs</th><th class="num">EV share</th></tr></thead>
      <tbody>{trend_rows}</tbody>
    </table>
    """

    faqs = [
        (f"What is India's EV adoption rate in {month_full}?",
         f"India's passenger-vehicle EV share was {fy2526_pct:.2f}% in FY 2025-26 (up from {fy2425_pct:.2f}% in FY 2024-25)."),
        ("Which Indian state has the highest EV adoption?",
         f"In {month_full}, {top_states_ev[0][0]} led with a {top_states_ev[0][3]:.2f}% EV share, followed by {top_states_ev[1][0]} ({top_states_ev[1][3]:.2f}%) and {top_states_ev[2][0]} ({top_states_ev[2][3]:.2f}%)."),
        ("Which OEM leads EV registrations in India?",
         f"In FY 2025-26, {display_oem(top_ev_oems[0][0])} held the top EV OEM spot with {fmt_int(top_ev_oems[0][1])} registrations ({(top_ev_oems[0][1]/ev_total_fy*100):.1f}% of EV market)."),
        ("How fast is EV adoption growing in India?",
         f"EV registrations grew {fmt_pct(ev_yoy, sign=True)} YoY in FY 2025-26, the steepest acceleration since FAME-II launched."),
    ]
    faq_dl = "\n".join(f"<dt>{safe(q)}</dt><dd>{safe(a)}</dd>" for q, a in faqs)
    faq_html = f'<dl class="faq">{faq_dl}</dl>'
    faq_jsonld = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [{"@type": "Question", "name": q,
                        "acceptedAnswer": {"@type": "Answer", "text": a}}
                       for q, a in faqs]
    }
    import json as _json
    faq_block = f'<script type="application/ld+json">{_json.dumps(faq_jsonld, separators=(",",":"))}</script>'

    article_jsonld = {
        "@context": "https://schema.org", "@type": "Article",
        "headline": title, "description": desc,
        "datePublished": TODAY, "dateModified": TODAY,
        "author": {"@type": "Organization", "name": "Vahan Intelligence"},
        "publisher": {"@type": "Organization", "name": "Vahan Intelligence",
                      "logo": {"@type": "ImageObject", "url": f"{SITE_URL}/og-image.jpg"}},
        "mainEntityOfPage": {"@type": "WebPage", "@id": canonical},
        "image": f"{SITE_URL}/og-image.jpg",
    }
    article_block = f'<script type="application/ld+json">{_json.dumps(article_jsonld, separators=(",",":"))}</script>'

    body_html = f"""
    <p class="lede">{lede}</p>
    {kpi_html}

    <h2>Top 15 states by EV adoption — {month_full}</h2>
    {state_table}

    <h2>Top 10 EV OEMs — FY 2025-26</h2>
    {oem_table}

    <h2>EV adoption trajectory — last 5 FYs</h2>
    {trend_table}

    <p class="callout">India's EV passenger-vehicle share crossed 1% in FY 2022-23, 2% in FY 2023-24, and 4%+ in FY 2025-26. Growth is concentrated in southern and western states with strong urban EV policy.</p>

    <div class="cta">
      <strong>Drill into the live EV view.</strong>
      <p>Explore EV adoption by state, top EV OEMs, hybrid + CNG context, and forecasts.</p>
      <a class="btn" href="/#ev">Open the EV & Green page →</a>
    </div>

    <h2>FAQs</h2>
    {faq_html}

    <h2>Related pages</h2>
    <div class="related">
      <a href="/seo/oems/tata-passenger-electric-mobility/">Tata Passenger Electric Mobility</a>
      <a href="/seo/oems/jsw-mg-motor/">JSW MG Motor</a>
      <a href="/seo/oems/mahindra-electric/">Mahindra Electric</a>
      <a href="/seo/oems/byd-india/">BYD India</a>
      <a href="/seo/states/maharashtra/">Maharashtra EV adoption</a>
      <a href="/seo/states/kerala/">Kerala EV adoption</a>
    </div>
    """

    return page_shell(
        title=title, description=desc, canonical=canonical,
        h1=h1, body_html=body_html,
        jsonld_blocks=[article_block, faq_block],
        breadcrumb_label="India EV adoption",
    )

# -------------------------------------------------------------------------
# Index pages (lists with crosslinks)
# -------------------------------------------------------------------------

def render_states_index(state_names: list[str]) -> str:
    canonical = f"{SITE_URL}/seo/states/"
    title = "India Car Registrations by State — All 36 States/UTs | Vahan Intelligence"
    desc = "Passenger vehicle registrations for every Indian state and UT — Maharashtra, UP, Tamil Nadu, Gujarat, Karnataka and more. Latest month data + FY totals from VAHAN."
    items = "\n".join(f'<a href="/seo/states/{slug(s)}/">{safe(s)} car registrations</a>' for s in state_names)
    body = f"""
    <p class="lede">A landing page for every Indian state and Union Territory with the latest passenger-vehicle registration numbers, top OEMs, fuel mix, and FY totals — sourced from the VAHAN portal and refreshed monthly.</p>
    <h2>All states (alphabetical)</h2>
    <div class="related">{items}</div>
    """
    return page_shell(title=title, description=desc, canonical=canonical, h1="India PV Registrations — by State",
                      body_html=body, jsonld_blocks=[], breadcrumb_label="States")

def render_oems_index(makers: list[tuple[str, int]]) -> str:
    canonical = f"{SITE_URL}/seo/oems/"
    title = "India Auto OEM Market Share — Maruti, Hyundai, Tata, Mahindra | Vahan Intelligence"
    desc = "Market share, registrations and trend pages for every major auto OEM in India — Maruti Suzuki, Hyundai, Tata, Mahindra, Toyota, Kia and more."
    items = "\n".join(
        f'<a href="/seo/oems/{slug(display_oem(m))}/">{safe(display_oem(m))} — {fmt_int(t)} regs (FY 25-26)</a>'
        for m, t in makers
    )
    body = f"""
    <p class="lede">Each major OEM in India has a dedicated page with market share trend, top states, fuel mix, FY totals and competitive context.</p>
    <h2>OEMs covered</h2>
    <div class="related">{items}</div>
    """
    return page_shell(title=title, description=desc, canonical=canonical, h1="India Auto OEMs",
                      body_html=body, jsonld_blocks=[], breadcrumb_label="OEMs")

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT_DIR)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    args = parser.parse_args()

    if not args.db.exists():
        print(f"ERROR: SQLite DB not found at {args.db}")
        return 1

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "states").mkdir(parents=True, exist_ok=True)
    (args.out / "oems").mkdir(parents=True, exist_ok=True)
    (args.out / "topics").mkdir(parents=True, exist_ok=True)

    c = sqlite3.connect(str(args.db))
    c.row_factory = sqlite3.Row

    # -------- State pages --------
    states = [r["state_name"] for r in c.execute(
        "SELECT DISTINCT state_name FROM vahan_registrations WHERE state_code != 'ALL' ORDER BY state_name"
    ).fetchall()]
    # Plus All India
    all_pages = ["All India"] + states

    written = []
    for s in all_pages:
        m = fetch_state_metrics(c, s)
        out_slug = "india" if s == "All India" else slug(s)
        out_dir = args.out / "states" / out_slug
        out_dir.mkdir(parents=True, exist_ok=True)
        html = render_state_page(s, m)
        (out_dir / "index.html").write_text(html, encoding="utf-8")
        written.append(f"states/{out_slug}/")
    print(f"Wrote {len(all_pages)} state pages")

    # State index page
    (args.out / "states" / "index.html").write_text(
        render_states_index(states), encoding="utf-8")
    written.append("states/")

    # -------- OEM pages (top 12) --------
    rows = c.execute("""
    SELECT maker, SUM(count) as t FROM vahan_registrations
    WHERE state_code='ALL' AND fy='FY2025-26'
    GROUP BY maker ORDER BY t DESC LIMIT 12
    """).fetchall()
    top_makers = [(r["maker"], int(r["t"])) for r in rows]

    for maker, t in top_makers:
        m = fetch_oem_metrics(c, maker)
        disp = m["display"]
        out_dir = args.out / "oems" / slug(disp)
        out_dir.mkdir(parents=True, exist_ok=True)
        html = render_oem_page(maker, m)
        (out_dir / "index.html").write_text(html, encoding="utf-8")
        written.append(f"oems/{slug(disp)}/")
    print(f"Wrote {len(top_makers)} OEM pages")

    # OEM index page
    (args.out / "oems" / "index.html").write_text(
        render_oems_index(top_makers), encoding="utf-8")
    written.append("oems/")

    # -------- Topic pages --------
    html = render_ev_topic_page(c)
    (args.out / "topics" / "ev-india").mkdir(parents=True, exist_ok=True)
    (args.out / "topics" / "ev-india" / "index.html").write_text(html, encoding="utf-8")
    written.append("topics/ev-india/")
    print(f"Wrote 1 topic page (EV India)")

    c.close()

    # -------- Sitemap fragment --------
    # Emit sitemap-seo.xml listing only the SEO landing pages (the main
    # sitemap.xml at site root references this as a sub-sitemap so they
    # stay decoupled).
    sitemap_urls = [f"{SITE_URL}/seo/{path}" for path in written]
    sitemap_xml = ['<?xml version="1.0" encoding="UTF-8"?>',
                   '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for url in sitemap_urls:
        sitemap_xml.append(f"  <url><loc>{url}</loc><lastmod>{TODAY}</lastmod><changefreq>monthly</changefreq><priority>0.7</priority></url>")
    sitemap_xml.append("</urlset>")
    (args.out / "sitemap-seo.xml").write_text("\n".join(sitemap_xml) + "\n", encoding="utf-8")

    print(f"\nTotal pages: {len(written)}")
    print(f"Sub-sitemap: {args.out / 'sitemap-seo.xml'} ({len(sitemap_urls)} URLs)")
    print(f"Output directory: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
