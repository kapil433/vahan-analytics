# Vahan Analytics Platform – Scraper

Scrapes vehicle registration data from the [Vahan Parivahan Dashboard](https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml) for monthly analytics.

## Workflow

See **[SCRAPING_WORKFLOW.md](./SCRAPING_WORKFLOW.md)** for the full spec.

**Data gaps / which layer to fix:** see **[CHANGE.md](./CHANGE.md)** (raw vs cleaner vs DB reload).

**Per state + year:**
1. Set base filters (Type: Actual Value, State, Y-Axis: Maker, X-Axis: Month Wise, Year) → Refresh — see `config/portal_filter_reference.md`
2. Expand sidebar (red "click here")
3. Set Vehicle Category (see `VEHICLE_CATEGORY_TARGET_LABELS` in `config/scraping_config.py`), Vehicle Class (Motor Car, Motor Cab)
4. For each fuel group (CNG, Petrol, Diesel, EV, Strong Hybrid): select fuels → Refresh → Download → annotate
5. Merge all fuel files → single state-year CSV → Python cleaner → analytics

## Setup

```bash
pip install -r requirements.txt
```

Requires **Chrome** (for Selenium/ChromeDriver).

## Usage

### 1. Discovery (first-time setup)

Capture page structure to get element IDs:

```bash
python scraper/discovery.py
```

This saves to `output/discovery/`:
- `reportview_page.html` – full page HTML
- `reportview_screenshot.png` – screenshot
- `element_ids.txt` – all element IDs

### 2. Update selectors

Edit `config/scraping_config.py` and replace placeholder `j_idtXX` values in `SELECTORS` with actual IDs from `element_ids.txt`.

### 3. Run scraper

```bash
python scraper/vahan_scraper.py
```

Edit `vahan_scraper.py` `if __name__ == "__main__"` to set `states` and `years`.

### 4. Output

- Per-session: `output/vahan_data/session_{state}_{year}_{ts}/`
- Merged file: `output/vahan_data/{state}_{year}_merged.csv` (with `fuel_type` column)
- Pass to your Python cleaner → analytics platform

### 5. API + Master Scraper (multiselect, parallel)

Optimized scraper (~2.5–3 min per state-year) with multi-window support:

```bash
python run_api.py
```

Open **http://localhost:8000/docs** (Swagger UI). Use:

- **GET /options** – states, fuels, calendar years (**2012–2026**), **`financial_years`** for FY mode (**FY2012-13** … **FY2025-26**; boundary partial FYs omitted), portal defaults
- **POST /scrape** – body: `{ "states": ["Maharashtra", "Gujarat"], "fuels": ["CNG", "Petrol"], "years": [2024], "parallel": true, "max_workers": 3 }`
- **GET /outputs** – list merged files in `output/vahan_data/`

Select all states/years/fuels = loop over all combinations. Output files go to `output/vahan_data/`.

## Architecture

See **[VAHAN_SITE_ARCHITECTURE.md](./VAHAN_SITE_ARCHITECTURE.md)** for site technology and design.

## Analytics dashboard

With PostgreSQL loaded and `python run_api.py` running, open **http://localhost:8000/dashboard** for the full analytics UI (data from `/data/vahan_master_compat`). Lighter API-only UI: **http://localhost:8000/platform**.

**Production:** deploy the API to [Render](https://dashboard.render.com/) and optionally keep a small GitHub Pages site that links to it or embeds the dashboard with `window.__VAHAN_API_BASE__` — see **[DEPLOY.md](./DEPLOY.md)** and **`deploy/github-pages/`**.

### PostgreSQL not running?

**Option A — SQLite (no server):** from project root run `python scripts/setup_local_sqlite.py` (reads `output/vahan_data_cleaned/vahan_registrations_cleaned.csv` into `data/vahan_local.db`). The API uses this file automatically when PostgreSQL is unavailable.

**Option B — Docker PostgreSQL:**

```bash
docker compose up -d
# CMD:  set DATABASE_URL=postgresql://vahan:vahan@localhost:5432/vahan_analytics
# PowerShell:  $env:DATABASE_URL="postgresql://vahan:vahan@localhost:5432/vahan_analytics"
# Apply migrations (psql or your client), then:
python scripts/load_vahan_to_db.py
```
