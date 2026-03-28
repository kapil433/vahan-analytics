# GitHub Pages (static dashboard)

## This repo (`vahan-analytics`)

1. Ingest raw CSVs: flat `*_merged.csv` and any `output/vahan_data/f1/**/*.csv` (see `scripts/clean_vahan_data.py`).
2. Clean: `python scripts/clean_vahan_data.py`
3. Load DB: `python scripts/setup_local_sqlite.py` or `scripts/load_vahan_to_db.py`
4. Export bundle: `python scripts/export_vahan_master_json.py -o docs/data/vahan_master.json`
5. Validate (includes **f1/** inventory): `python scripts/validate_vahan_pipeline.py --bundle docs/data/vahan_master.json`
6. Enable **GitHub Pages** on this repo: source **GitHub Actions** (workflow `pages.yml`).

The dashboard loads `data/vahan_master.json` next to `index.html`. If the JSON is missing, the UI shows its built-in data error panel.

## Public site: [ALL-India-Vahan-Analytics-Dashboard](https://github.com/kapil433/ALL-India-Vahan-Analytics-Dashboard)

That repository is **static only** (root `index.html`, `data/`, `.nojekyll`). There is no server-side API on GitHub Pages.

### Option A — GitHub Action (automated)

1. In `vahan-analytics`, create a secret **`PUBLIC_DASHBOARD_TOKEN`**: a PAT with `contents:write` on `kapil433/ALL-India-Vahan-Analytics-Dashboard`.
2. Commit `docs/data/vahan_master.json` when you refresh data, then push (or run workflow **Sync public dashboard** manually).

Workflow file: `.github/workflows/sync-public-dashboard.yml`.

### Option B — Local sync

```bash
python scripts/sync_public_dashboard.py --target /path/to/ALL-India-Vahan-Analytics-Dashboard --export-db
# or: --json-source docs/data/vahan_master.json
```

Then commit and push the public repo.

## Backend API (FastAPI + scraper)

GitHub Pages cannot host this. Deploy the **whole** `vahan-analytics` app (e.g. Railway, Render, Fly.io, VPS) using the root **`Dockerfile`**:

```bash
docker build -t vahan-api .
docker run -p 8000:8000 -e DATABASE_URL=... vahan-api
```

Scraping inside Docker needs extra browser/driver setup; for read-only dashboards, SQLite or PostgreSQL + static JSON export is enough.

### f1 folder naming

Place CSVs under `output/vahan_data/f1/<StateOrCode>/` with either:

- Year in the filename (e.g. `export_2024.csv`, `Punjab-2023.csv`), or  
- A 4-digit year folder: `f1/MH/2024/file.csv`

Folder labels are matched to states via `STATE_MAP` and state codes (e.g. `MH` → Maharashtra).
