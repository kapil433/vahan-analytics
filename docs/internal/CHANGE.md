# Data quality: where to fix issues

This project uses several **data layers**. A gap at one layer is often mistaken for a “database bug”; use this guide to fix the **right** layer.

## Layers (in order)

| Layer | Location | Role |
|--------|-----------|------|
| **Raw Vahan exports** | `output/vahan_data/*_merged.csv` | Portal/scraper output; layout varies by state/year. |
| **Clean registrations** | `output/vahan_data_cleaned/*_cleaned.csv`, `vahan_registrations_cleaned.csv` | Normalized long format from `scripts/clean_vahan_data.py`. |
| **PostgreSQL** | `vahan_registrations` (see `migrations/`) | Primary store when `DATABASE_URL` is set; load via `scripts/load_vahan_to_db.py`. |
| **SQLite (local fallback)** | `data/vahan_local.db` | API reads this if PostgreSQL is unavailable; build via `scripts/setup_local_sqlite.py`. |
| **Enriched research CSVs** | `scripts/data/*_validated.csv` | Population, PCI, CNG, EV; validate with `scripts/validate_data.py`. |

**Rule of thumb:** If a year/state is missing from **cleaned** CSVs, fix **raw** or **cleaner code** first. Only then **reload** PostgreSQL or SQLite; the DB will not invent rows that the cleaner did not produce.

---

## Automated checks

```text
# All raw merged files: layout, column count, whether cleaner produced rows
python scripts/audit_vahan_merged.py
python scripts/audit_vahan_merged.py --json output/vahan_merged_audit.json

# Enriched datasets (population, PCI, CNG, EV)
python scripts/validate_data.py
```

Re-run the audit after replacing raw files or changing `scripts/clean_vahan_data.py`.

---

## Latest audit snapshot (regenerate with commands above)

As of the last `audit_vahan_merged.py` run on this repo’s `output/vahan_data`:

- **207** merged files: **OK** (cleaner produced rows).
- **2** files: **wrong export type** (fix in **raw data** — re-export *Maker Month Wise*).

### Partial-year / 7-column exports (Layout C)

The cleaner supports **Layout C**: exactly **7** columns with the last header `fuel_type`, data rows `S No, Maker, JAN, FEB, MAR, TOTAL, fuel`. This is what the portal often returns for an **in-progress** calendar year (e.g. 2026 with only Q1 months). Parsed rows only include **months 1–3**; when the portal adds APR–DEC, use a full-width export and the standard Layout A/B path.

### Fix in **raw data** (re-scrape / re-export from Vahan)

| Issue | Files | Action |
|--------|--------|--------|
| **Vehicle Class Wise** (not Maker Month Wise) | `Gujarat_2013_merged.csv`, `Haryana_2014_merged.csv` | Download **Maker Month Wise** for that state and year; replace merged CSV; run `clean_vahan_data.py` then reload DB. |

### Fix in **pipeline** (code in this repo)

| Symptom | Action |
|---------|--------|
| Audit reports **Maker Month Wise** + enough columns but **zero** clean rows (`clean_empty_wide_ok`) | Extend `scripts/clean_vahan_data.py` for that layout; add a fixture file and a small test if possible. |
| New portal column layout (not A/B/C) | Add detection + row parser; re-run `audit_vahan_merged.py`. |

### Fix in **database** only (after data exists in CSV)

| Symptom | Action |
|---------|--------|
| Cleaned CSV has rows but API/DB looks stale | **PostgreSQL:** `python scripts/load_vahan_to_db.py --all` (with `DATABASE_URL`). **SQLite:** `python scripts/setup_local_sqlite.py` after regenerating `vahan_registrations_cleaned.csv`. |
| SQLite missing `fy` errors | `setup_local_sqlite.py` populates `fy`; ensure cleaned CSV includes or derives FY consistently with `config.mappings.month_to_fy`. |

### Enriched / research CSVs

`python scripts/validate_data.py` was **OK** for `population_validated.csv`, `pci_validated.csv`, `cng_validated.csv`, `ev_chargers_validated.csv` at last run. If any **FAIL**, fix the **source CSV** under `scripts/data/` (or the fetch scripts), not the Vahan cleaner.

---

## `setup_local_sqlite.py` (rectified)

`README.md` and `api/main.py` referenced `scripts/setup_local_sqlite.py` but the script was missing. It is now implemented: it loads `output/vahan_data_cleaned/vahan_registrations_cleaned.csv` into `data/vahan_local.db` for offline API use.
