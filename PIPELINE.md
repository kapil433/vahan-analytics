# Vahan Data Pipeline

End-to-end flow: **Download → Clean → Load → API → Dashboard**

## Overview

| Step | What | Output |
|------|------|--------|
| 1. Download | Scrape 37 states × all fuels × years | `output/vahan_data/{state}_{year}_merged.csv` |
| 2. Clean | Parse, normalize numbers, long format | `output/vahan_data_cleaned/vahan_registrations_cleaned.csv` |
| 3. Load | Upsert into PostgreSQL | `vahan_registrations` table |
| 4. API | Dashboard fetches via REST | `GET /data/registrations`, `GET /data/aggregates` |
| 5. Monthly | Run for current year only | Updates DB, dashboard reflects new data |

## Full Initial Load (2012–2026)

```bash
# 1. Run migration (once)
psql $DATABASE_URL -f migrations/002_vahan_registrations.sql
psql $DATABASE_URL -f migrations/003_vahan_fy_column.sql

# 2. Scrape all: 37 states × 15 years × 5 fuels
#    Use UI at http://localhost:8000/ or:
python run_api.py   # then POST /scrape with all states, all years, all fuels

# 3. Clean
python scripts/clean_vahan_data.py

# 4. Load to DB
python scripts/load_vahan_to_db.py
```

## Dashboard & lab export

- **Dashboard (legacy UI):** `GET /dashboard` — needs data; uses PostgreSQL if `DATABASE_URL` works, else **`data/vahan_local.db`** after `python scripts/setup_local_sqlite.py`.
- **Lighter UI:** `GET /platform`.
- **Parquet snapshot for Complexity Lab:** `python scripts/export_snapshot.py` → `exports/` + `manifest.json` (PostgreSQL only; use SQLite → export from CSV/DuckDB separately if needed).

## Monthly Update (Current Year Only)

Run at the start of each month:

```bash
python scripts/monthly_update.py
# Or with options:
python scripts/monthly_update.py --year 2025 --headless
python scripts/monthly_update.py --skip-scrape  # if you already have fresh files
```

## Data API (for Dashboard)

| Endpoint | Purpose |
|----------|---------|
| `GET /data/registrations?state_code=MH&year=2024` | Raw rows for charts |
| `GET /data/aggregates?year=2024` | Sum by state, fuel for dashboards |

## Schema: vahan_registrations

| Column | Type |
|--------|------|
| state_code | VARCHAR (AP, MH, ALL, …) |
| state_name | VARCHAR |
| year | INT |
| fy | VARCHAR (e.g. FY2024-25, Apr–Mar; see `003_vahan_fy_column.sql`) |
| fuel_type | VARCHAR (CNG, Petrol, Diesel, EV, Strong Hybrid) |
| maker | VARCHAR |
| month | INT (1–12) |
| count | INT |

## Cleaned Format

Input: Raw merged CSV with repeated headers, comma-separated numbers.  
Output: `state_code, state_name, year, fy, fuel_type, maker, month, count`
