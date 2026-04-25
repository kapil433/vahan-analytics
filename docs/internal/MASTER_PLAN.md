# Vahan Analytics Dashboard — Master Rebuild Plan

End-to-end plan: **Data Scraper → Unclean Files → Clean Files → Backend → Integrated UI**

**Legacy dashboard:** [kapil433/ALL-India-Vahan-Analytics-Dashboard](https://github.com/kapil433/ALL-India-Vahan-Analytics-Dashboard) — KPIs, charts, dark theme. Use as UI base; wire to backend API instead of static JSON.

---

## 1. Data Flow (Pipeline)

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Vahan Scraper  │ →  │ Unclean CSVs     │ →  │ Clean Script    │ →  │ PostgreSQL      │ →  │ Dashboard UI    │
│  (Selenium)     │    │ *_merged.csv     │    │ Long format     │    │ vahan_registr.  │    │ (React/HTML)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
       │                        │                        │                        │                        │
       │ output/vahan_data/     │ output/vahan_data/     │ output/vahan_data_      │ migrations/            │ API + Frontend
       │ 37 states × years      │ (raw, repeated headers)│ cleaned/                │ 002_vahan_registr.     │ GET /data/*
       │ × 5 fuels              │                        │ vahan_registrations_   │ load_vahan_to_db.py    │
       └───────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┘
```

| Stage | Location | Format |
|-------|----------|--------|
| **Unclean** | `output/vahan_data/{state}_{year}_merged.csv` | Raw Excel export, repeated headers, comma numbers |
| **Clean** | `output/vahan_data_cleaned/vahan_registrations_cleaned.csv` | `state_code, state_name, year, fuel_type, maker, month, count` |
| **Backend** | PostgreSQL `vahan_registrations` | Same schema, indexed |
| **API** | `GET /data/registrations`, `GET /data/aggregates` | JSON |

---

## 2. KPIs (Dashboard Metrics)

### Primary KPIs (Vehicle Registrations)

| KPI | Formula | Use |
|-----|---------|-----|
| **Total Registrations** | `SUM(count)` by state/year/fuel | Hero metric, trend |
| **Registrations by Fuel** | Group by fuel_type | Pie/bar: CNG vs Petrol vs Diesel vs EV vs Strong Hybrid |
| **Registrations by Maker** | Group by maker | Top OEMs, market share |
| **Month-wise Trend** | Group by month | Seasonality, YoY |
| **EV Share %** | `EV / Total × 100` | Adoption rate |
| **YoY Growth %** | `(Y2 - Y1) / Y1 × 100` | Growth by state/fuel |

### Enriched KPIs (Join with research_data)

| KPI | Formula | Data Source |
|-----|---------|-------------|
| **Registrations per Capita** | `registrations / population` | vahan_registrations + population.csv |
| **EV per Charger** | `EV registrations / charger_count` | vahan_registrations + ev_chargers.csv |
| **CNG Stations vs CNG Vehicles** | Ratio | vahan_registrations + cng.csv |
| **Income-Adjusted Adoption** | `registrations / pci_rs` | vahan_registrations + pci.csv |

### Dashboard Views

1. **Overview** — All India, current year, fuel mix, top makers
2. **State Comparison** — Map or table, registrations by state
3. **Fuel Deep Dive** — EV share, CNG, diesel trends
4. **Maker Rankings** — Top 10 by state/year/fuel
5. **Time Series** — Month-wise, YoY

---

## 3. Technical Stack

| Layer | Technology | Notes |
|-------|------------|------|
| **Scraper** | Python, Selenium, Chrome | `vahan_scraper.py`, `vahan_scraper_master.py` |
| **Clean** | Python, pandas | `scripts/clean_vahan_data.py` |
| **DB** | PostgreSQL | `migrations/002_vahan_registrations.sql` |
| **Backend API** | FastAPI | `api/main.py` — already has `/data/registrations`, `/data/aggregates` |
| **Frontend** | Option A: Vanilla HTML/JS | Reuse legacy patterns |
| | Option B: React + Vite | Modern SPA |
| **Charts** | Chart.js or Apache ECharts | Same as TCO-Calculator legacy |
| **Hosting** | Local / Docker / Cloud | TBD |

---

## 4. Legacy HTML Reference (GitHub)

**Where to pull from:**

1. **Layout & styling** — `TCO-Calculator/legacy/index.html` (local)
   - Dark theme: `--bg`, `--s1`, `--acc`, `--txt`
   - Cards: `.vcard`, `.tco-card`, `.hero-kpis`
   - Tabs: `.mode-tabs`, `.result-tabs`
   - Charts: Chart.js CDN, `.chart-wrap`
   - Tables: `.rtbl`, comparison table

2. **GitHub repo** — If you have a separate vahan-dashboard or analytics-dashboard repo:
   - Clone and inspect existing HTML structure
   - Reuse: header, filter bar, KPI cards, chart containers, table markup
   - Adapt: Replace TCO inputs with state/year/fuel selectors

3. **Patterns to reuse**
   - Top bar with filters (state, year, fuel)
   - KPI grid (`.hero-kpis`, `.vgrid`)
   - Collapsible sections (`.sec-d`)
   - Chart.js line/bar/doughnut configs
   - Responsive grid (`.vgrid.n2`, `.vgrid.n3`)

---

## 5. Backend Buildup (Current + Extensions)

### Existing

| Endpoint | Purpose |
|----------|---------|
| `GET /data/registrations` | Raw rows, filter by state/year/fuel/maker |
| `GET /data/aggregates` | Sum by state, year, fuel |

### To Add

| Endpoint | Purpose |
|----------|---------|
| `GET /data/kpis?year=&state=` | Pre-computed KPIs (totals, EV share, YoY) |
| `GET /data/makers?year=&state=&fuel=` | Top makers with counts |
| `GET /data/monthly?year=&state=` | Month-wise series for charts |
| `GET /data/enriched` | Join with population/PCI/CNG/EV for per-capita metrics |

### Enriched Data

- Load `research_data/*.csv` into DB (already have migrations for population, pci, cng, ev_chargers)
- Add views or endpoints that JOIN vahan_registrations with these tables

---

## 6. Integrated UI — Build Order

### Phase 1: Core Dashboard (2–3 days)

1. **Shell** — HTML shell with header, filter bar (state, year, fuel)
2. **API wiring** — Fetch `/data/aggregates`, `/data/registrations`
3. **KPI cards** — Total registrations, EV share, top fuel
4. **Chart** — Fuel mix (doughnut/bar), month-wise (line)

### Phase 2: State & Maker Views (2 days)

5. **State selector** — Dropdown or map placeholder
6. **State comparison table** — Registrations by state
7. **Maker rankings** — Top 10 table, filter by fuel

### Phase 3: Enriched & Polish (2 days)

8. **Enriched KPIs** — Per capita, EV per charger (when joined data ready)
9. **Responsive** — Mobile-friendly grid
10. **Legacy styling** — Apply TCO dark theme, fonts, cards

### Phase 4: Monthly Update Integration (1 day)

11. **Status** — Show last update date, trigger monthly job link
12. **Scraper UI** — Link to existing `/` scraper UI or embed

---

## 7. File Map (What Exists vs To Build)

| Component | Exists | To Build |
|-----------|--------|----------|
| Scraper | ✅ `vahan_scraper.py`, `vahan_scraper_master.py` | — |
| Cleaner | ✅ `scripts/clean_vahan_data.py` | — |
| Load to DB | ✅ `scripts/load_vahan_to_db.py` | — |
| Migrations | ✅ `002_vahan_registrations.sql` | — |
| Data API | ✅ `/data/registrations`, `/data/aggregates` | `/data/kpis`, `/data/makers`, `/data/monthly`, `/data/enriched` |
| Scraper UI | ✅ `api/static/index.html` | — |
| **Dashboard UI** | ❌ | New: `dashboard/` or `api/static/dashboard.html` |
| Monthly job | ✅ `scripts/monthly_update.py` | Cron/scheduler config |

---

## 8. Quick Start (Rebuild Checklist)

```text
[ ] 1. Run full scrape: 37 states × 2012–2026 × all fuels
[ ] 2. Clean: python scripts/clean_vahan_data.py
[ ] 3. Migrate: psql -f migrations/002_vahan_registrations.sql
[ ] 4. Load: python scripts/load_vahan_to_db.py
[ ] 5. Add new API endpoints (kpis, makers, monthly)
[ ] 6. Create dashboard HTML/JS (or React app)
[ ] 7. Wire filters → API → charts
[ ] 8. Apply legacy styling from TCO-Calculator/legacy
[ ] 9. Set up monthly_update.py cron
```

---

## 9. Legacy HTML — Primary Reference

**[ALL-India-Vahan-Analytics-Dashboard](https://github.com/kapil433/ALL-India-Vahan-Analytics-Dashboard)** — Your existing dashboard repo.

### What to Reuse

| Asset | Location | Use |
|-------|----------|-----|
| **Full UI shell** | `index.html` (~6.4k lines) | Header, nav tabs, filter bar, KPI cards, chart containers |
| **CSS variables** | `:root` in index.html | `--bg`, `--surface`, `--blue`, `--petrol`, `--diesel`, `--cng`, `--ev`, `--hybrid` |
| **KPI cards** | `.kpi-card`, `.kpi-row` | Total Volume, YoY, EV Share, etc. |
| **Charts** | `.chart-card`, `.chart-wrap` | Chart.js line, bar, donut, stacked area |
| **Filter bar** | `.filter-bar`, `.filter-select` | Geography, Year Type, Year, Month, OEM, Fuel |
| **Upload portal** | `.upload-zone`, `.validation-panel` | State file upload + validation |
| **VAHAN_PLAN_v2.md** | Root | Data model (CY/FY), KPI definitions, 5 pages, build sequence |

### Data Model Difference

| Legacy (vahan_master.json) | Current Pipeline (vahan_registrations) |
|---------------------------|----------------------------------------|
| `cal_year`, `fy`, `fy_sort`, `maker`, `fuel`, `registrations` | `state_code`, `state_name`, `year`, `fuel_type`, `maker`, `month`, `count` |
| Pre-built JSON, client-side filter | PostgreSQL, API-driven |
| All India only (or single state upload) | 37 states in DB |

**Rebuild approach:** Keep legacy UI/UX. Replace `data/vahan_master.json` fetch with API calls to `GET /data/registrations`, `GET /data/aggregates`. Add FY mapping in backend or cleaner if needed.

### Clone & Inspect

```bash
git clone https://github.com/kapil433/ALL-India-Vahan-Analytics-Dashboard.git
# Inspect: index.html, data/, VAHAN_PLAN_v2.md
```

---

## 10. Other References

| Source | Path | Use |
|--------|------|-----|
| **TCO Calculator** | `C:\Users\Kapil\TCO-Calculator\legacy\index.html` | Additional chart/table patterns |
| **Vahan Scraper UI** | `vahan-analytics/api/static/index.html` | Filter bar for scraper |

---

*Last updated: March 2025*
