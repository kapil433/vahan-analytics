"""
Vahan Incremental Pipeline  —  scripts/pipeline.py
====================================================
Handles BOTH raw file types, skips stable (already-cleaned) years,
loads to SQLite, then exports docs/data/vahan_master.json so the
API serves it instantly as a static file.

File types handled:
  TYPE A  output/vahan_data/{State}_{Year}_merged.csv       (Maker Month Wise)
  TYPE B  output/vahan_data/f1/{State}({n})_FUELWISE{YY}/   (Excel FUELWISE)

Usage:
  python scripts/pipeline.py                  # Incremental (only new/current year)
  python scripts/pipeline.py --year 2025      # Force-process one year only
  python scripts/pipeline.py --force          # Reprocess everything
  python scripts/pipeline.py --json-only      # Re-export master JSON only (no scrape/clean)
  python scripts/pipeline.py --no-json        # Run pipeline but skip JSON export
  python scripts/pipeline.py --dry-run        # Show what would run without doing it

Pipeline steps:
  1. Discover raw files (Type A + Type B)
  2. Decide what to process (incremental: skip stable years already cleaned)
  3. Clean each file → output/vahan_data_cleaned/{state}_{year}_cleaned.csv
  4. Merge all cleaned CSVs → output/vahan_data_cleaned/vahan_registrations_cleaned.csv
  5. Load master CSV into data/vahan_local.db (SQLite)
  6. Export docs/data/vahan_master.json  ← API serves this directly (instant)
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import NamedTuple

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "output" / "vahan_data"
F1_DIR = RAW_DIR / "f1"
CLEAN_DIR = ROOT / "output" / "vahan_data_cleaned"
MASTER_CLEANED_CSV = CLEAN_DIR / "vahan_registrations_cleaned.csv"
SQLITE_PATH = ROOT / "data" / "vahan_local.db"
STATIC_JSON = ROOT / "docs" / "data" / "vahan_master.json"

CURRENT_YEAR = datetime.now().year

# ---------------------------------------------------------------------------
# Re-use project mappings
# ---------------------------------------------------------------------------
sys.path.insert(0, str(ROOT))
from config.mappings import FUEL_CANONICAL, normalize_fuel, normalize_maker, month_to_fy, oem_short_display
from scripts.config import STATE_MAP, normalize_state

MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
FUEL_SET = set(FUEL_CANONICAL) | {"CNG", "Petrol", "Diesel", "EV", "Strong Hybrid"}

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class RawFile(NamedTuple):
    path: Path
    state_name: str
    year: int
    file_type: str   # "A" (merged CSV) or "B" (excel FUELWISE)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_type_a(raw_dir: Path) -> list[RawFile]:
    """Find *_merged.csv files directly in raw_dir."""
    files = []
    pattern = re.compile(r"^(.+?)_(\d{4})_merged\.csv$", re.IGNORECASE)
    for p in sorted(raw_dir.glob("*_merged.csv")):
        m = pattern.match(p.name)
        if m:
            state_raw, year_str = m.group(1), m.group(2)
            files.append(RawFile(p, state_raw.strip(), int(year_str), "A"))
    return files


def discover_type_b(f1_dir: Path) -> list[RawFile]:
    """Find Excel files inside f1/{State}(n)_FUELWISE{YY}/ subdirectories."""
    if not f1_dir.exists():
        return []
    files = []
    yr_pattern = re.compile(r"FUELWISE(\d{2})$", re.IGNORECASE)
    for state_dir in sorted(f1_dir.iterdir()):
        if not state_dir.is_dir():
            continue
        m = yr_pattern.search(state_dir.name)
        if not m:
            continue
        yr_2digit = int(m.group(1))
        year = 2000 + yr_2digit
        # State name: strip trailing (n)_FUELWISEYY
        state_raw = re.sub(r"\(\d+\)_FUELWISE\d+$", "", state_dir.name, flags=re.IGNORECASE).strip()
        for xlsx in sorted(state_dir.glob("*.xlsx")):
            files.append(RawFile(xlsx, state_raw, year, "B"))
    return files


def _clean_output_path(state_name: str, year: int) -> Path:
    safe = re.sub(r"[^A-Za-z0-9 ]+", "", state_name).strip().replace(" ", "_")
    return CLEAN_DIR / f"{safe}_{year}_cleaned.csv"


# ---------------------------------------------------------------------------
# Decide what to process
# ---------------------------------------------------------------------------

def needs_processing(rf: RawFile, force: bool, target_year: int | None) -> bool:
    """
    Incremental logic:
     - Current year (CURRENT_YEAR) and next year: ALWAYS reprocess (data updates monthly)
     - Past years: skip if cleaned CSV already exists
     - --force: always reprocess
     - --year N: only process that year
    """
    if target_year is not None and rf.year != target_year:
        return False
    if force:
        return True
    if rf.year >= CURRENT_YEAR:
        return True  # Always refresh current/future year
    out_path = _clean_output_path(rf.state_name, rf.year)
    return not out_path.exists()


# ---------------------------------------------------------------------------
# Cleaning helpers (reused from existing cleaner logic)
# ---------------------------------------------------------------------------

def _parse_count(val) -> int:
    if pd.isna(val):
        return 0
    s = str(val).replace(",", "").strip()
    try:
        return max(0, int(float(s)))
    except (ValueError, TypeError):
        return 0


def _get_state_code(state_name: str) -> str:
    canon = normalize_state(state_name) if callable(normalize_state) else state_name
    return STATE_MAP.get(canon, STATE_MAP.get(state_name, "XX"))


def _is_fuel_header(val) -> bool:
    if not val or pd.isna(val):
        return False
    s = str(val).strip()
    return s in FUEL_SET or normalize_fuel(s) in FUEL_CANONICAL


# ---------------------------------------------------------------------------
# Type A cleaner: Maker Month Wise merged CSV
# ---------------------------------------------------------------------------

def clean_type_a(rf: RawFile, verbose: bool = False) -> pd.DataFrame:
    """Parse a *_merged.csv file (Maker Month Wise format with repeated fuel sections)."""
    try:
        df_raw = pd.read_csv(rf.path, header=0, dtype=str)
    except Exception as e:
        print(f"  [WARN] Cannot read {rf.path.name}: {e}")
        return pd.DataFrame()

    if df_raw.empty or df_raw.shape[1] < 5:
        return pd.DataFrame()

    rows_out: list[dict] = []
    current_fuel: str | None = None

    for _, row in df_raw.iterrows():
        vals = [str(v).strip() if pd.notna(v) else "" for v in row.values]

        # Detect fuel section marker in last column
        last_val = vals[-1] if vals else ""
        if _is_fuel_header(last_val):
            current_fuel = normalize_fuel(last_val)
            continue

        # Skip header rows
        if any(x in " ".join(vals[:3]) for x in ["S No", "Maker", "Month Wise"]):
            continue
        if vals[0].strip() in ("", "S No") or not current_fuel:
            continue

        # Data row: cols ~ [S No, Maker, JAN, FEB, ..., DEC, TOTAL, fuel_type?]
        # Find maker (col index 1)
        maker_raw = vals[1] if len(vals) > 1 else ""
        if not maker_raw or maker_raw.lower() in ("maker", "s no", ""):
            continue

        maker = normalize_maker(maker_raw)
        if not maker:
            continue

        # Find month columns: look for 12 numeric columns after maker
        month_vals: list[str] = []
        for v in vals[2:]:
            if v in ("", "TOTAL") or (v and _is_fuel_header(v)):
                if month_vals and len(month_vals) < 12:
                    continue
                break
            month_vals.append(v)
            if len(month_vals) == 12:
                break

        if len(month_vals) < 12:
            # Try 7-col partial year (Jan-Mar only)
            if 3 <= len(month_vals) <= 7:
                for m_idx, mv in enumerate(month_vals[:3], start=1):
                    rows_out.append({
                        "state_code": _get_state_code(rf.state_name),
                        "state_name": rf.state_name,
                        "year": rf.year,
                        "fy": month_to_fy(rf.year, m_idx),
                        "fuel_type": current_fuel,
                        "maker": maker,
                        "month": m_idx,
                        "count": _parse_count(mv),
                    })
            continue

        for m_idx, mv in enumerate(month_vals, start=1):
            rows_out.append({
                "state_code": _get_state_code(rf.state_name),
                "state_name": rf.state_name,
                "year": rf.year,
                "fy": month_to_fy(rf.year, m_idx),
                "fuel_type": current_fuel,
                "maker": maker,
                "month": m_idx,
                "count": _parse_count(mv),
            })

    if not rows_out:
        # Fallback: delegate to existing clean_vahan_data module
        return _fallback_to_existing_cleaner(rf)

    df = pd.DataFrame(rows_out)
    if verbose:
        print(f"  → {len(df)} rows (Type A)")
    return df


# ---------------------------------------------------------------------------
# Type B cleaner: Excel FUELWISE
# ---------------------------------------------------------------------------

def clean_type_b(rf: RawFile, verbose: bool = False) -> pd.DataFrame:
    """Parse an Excel FUELWISE file: header row = Maker, FUEL, JAN..DEC."""
    try:
        df_raw = pd.read_excel(rf.path, header=0, dtype=str)
    except Exception as e:
        print(f"  [WARN] Cannot read {rf.path.name}: {e}")
        return pd.DataFrame()

    if df_raw.empty:
        return pd.DataFrame()

    # Detect columns: first col = S No, second = Maker, third = fuel, then months
    cols = [str(c).strip().upper() for c in df_raw.columns]

    # Find month columns
    month_col_indices: list[int] = []
    for i, c in enumerate(cols):
        if c in [m.upper() for m in MONTHS]:
            month_col_indices.append(i)

    if len(month_col_indices) < 3:
        if verbose:
            print(f"  [WARN] Cannot detect month columns in {rf.path.name}")
        return pd.DataFrame()

    # Find fuel column
    fuel_col_idx = None
    maker_col_idx = None
    for i, c in enumerate(cols):
        if c in ("FUEL", "FUEL TYPE", "FUELTYPE"):
            fuel_col_idx = i
        if c in ("MAKER", "MANUFACTURER", "MAKE"):
            maker_col_idx = i

    if maker_col_idx is None:
        maker_col_idx = 1  # second col is usually maker

    rows_out: list[dict] = []
    state_code = _get_state_code(rf.state_name)

    for _, row in df_raw.iterrows():
        vals = [str(v).strip() if pd.notna(v) else "" for v in row.values]
        if len(vals) <= maker_col_idx:
            continue

        maker_raw = vals[maker_col_idx]
        if not maker_raw or maker_raw.lower() in ("maker", "s no", "manufacturer", ""):
            continue

        maker = normalize_maker(maker_raw)
        if not maker:
            continue

        # Fuel type
        fuel_raw = vals[fuel_col_idx] if fuel_col_idx is not None and fuel_col_idx < len(vals) else ""
        if not fuel_raw:
            fuel_raw = "Petrol"  # default
        fuel = normalize_fuel(fuel_raw)

        for local_idx, col_idx in enumerate(month_col_indices):
            month_num = local_idx + 1
            count = _parse_count(vals[col_idx]) if col_idx < len(vals) else 0
            rows_out.append({
                "state_code": state_code,
                "state_name": rf.state_name,
                "year": rf.year,
                "fy": month_to_fy(rf.year, month_num),
                "fuel_type": fuel,
                "maker": maker,
                "month": month_num,
                "count": count,
            })

    df = pd.DataFrame(rows_out)
    if verbose:
        print(f"  → {len(df)} rows (Type B)")
    return df


def _fallback_to_existing_cleaner(rf: RawFile) -> pd.DataFrame:
    """Delegate to the existing clean_vahan_data module for complex layouts."""
    try:
        from scripts.clean_vahan_data import clean_file as _clean_file
        return _clean_file(rf.path)
    except (ImportError, AttributeError):
        pass
    try:
        from scripts import clean_vahan_data as _mod
        # The module exposes process_file or similar
        for fn_name in ("clean_file", "process_file", "parse_merged_csv"):
            fn = getattr(_mod, fn_name, None)
            if fn:
                return fn(rf.path)
    except Exception:
        pass
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Merge + Load to SQLite
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS vahan_registrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_code TEXT NOT NULL,
    state_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    fy TEXT,
    fuel_type TEXT NOT NULL,
    maker TEXT NOT NULL,
    month INTEGER NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(state_code, state_name, year, fuel_type, maker, month)
)
"""

UPSERT = """
INSERT INTO vahan_registrations
    (state_code, state_name, year, fy, fuel_type, maker, month, count, loaded_at)
VALUES (?,?,?,?,?,?,?,?,datetime('now'))
ON CONFLICT(state_code, state_name, year, fuel_type, maker, month)
DO UPDATE SET count=excluded.count, fy=excluded.fy, loaded_at=excluded.loaded_at
"""


def merge_all_cleaned(clean_dir: Path) -> pd.DataFrame:
    """Concatenate all *_cleaned.csv files (excluding the master file itself)."""
    frames: list[pd.DataFrame] = []
    for p in sorted(clean_dir.glob("*_cleaned.csv")):
        if p.name == MASTER_CLEANED_CSV.name:
            continue
        try:
            df = pd.read_csv(p, dtype={"count": "Int64"})
            frames.append(df)
        except Exception as e:
            print(f"  [WARN] Skipping {p.name}: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_to_sqlite(master_csv: Path, db_path: Path, verbose: bool = True) -> int:
    """Upsert master cleaned CSV into SQLite. Returns rows upserted."""
    df = pd.read_csv(master_csv, dtype={"count": "Int64"})
    df = df.dropna(subset=["state_code", "fuel_type", "maker", "month"])
    df["count"] = df["count"].fillna(0).astype(int)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(SCHEMA)

    rows = df[["state_code", "state_name", "year", "fy", "fuel_type", "maker", "month", "count"]].values.tolist()
    conn.executemany(UPSERT, rows)
    conn.commit()
    conn.close()
    if verbose:
        print(f"  Loaded {len(rows):,} rows into {db_path}")
    return len(rows)


# ---------------------------------------------------------------------------
# Export master JSON (static file for instant API serving)
# ---------------------------------------------------------------------------

def export_master_json(db_path: Path, out_path: Path, verbose: bool = True) -> bool:
    """Build vahan_master.json from SQLite and write to out_path."""
    if not db_path.exists():
        print("  [SKIP] No SQLite DB found, skipping JSON export")
        return False

    try:
        # Use existing master_bundle builder if available
        from api.master_bundle import build_vahan_master_bundle
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        bundle = build_vahan_master_bundle(conn, dialect="sqlite")
        conn.close()
    except Exception as e:
        print(f"  [WARN] master_bundle unavailable ({e}), using simple export")
        bundle = _simple_json_export(db_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".tmp.json")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(out_path)

    size_kb = out_path.stat().st_size / 1024
    if verbose:
        print(f"  Exported {out_path} ({size_kb:.0f} KB)")
    return True


def _simple_json_export(db_path: Path) -> dict:
    """Minimal JSON export if master_bundle is unavailable."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT state_code, state_name, year, month, fuel_type, maker, SUM(count) AS cnt
        FROM vahan_registrations
        GROUP BY state_code, state_name, year, month, fuel_type, maker
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    regions = sorted(set(r["state_name"] for r in rows))
    fuels = [f for f in FUEL_CANONICAL if any(r["fuel_type"] == f for r in rows)]
    makers = sorted(set(normalize_maker(r["maker"]) or r["maker"] for r in rows))
    region_idx = {n: i for i, n in enumerate(regions)}
    fuel_idx = {f: i for i, f in enumerate(fuels)}
    maker_idx = {m: i for i, m in enumerate(makers)}

    encoded = []
    maker_totals: dict[str, int] = {}
    for r in rows:
        reg = r["state_name"]
        fl = r["fuel_type"]
        mk = normalize_maker(r["maker"]) or r["maker"]
        fy = month_to_fy(r["year"], r["month"])
        fy_s = fy[2:] if fy.startswith("FY") else fy
        cnt = int(r["cnt"] or 0)
        if cnt <= 0:
            continue
        encoded.append([
            region_idx.get(reg, 0),
            r["year"], r["month"], fy_s,
            maker_idx.get(mk, 0), fuel_idx.get(fl, 0), cnt,
        ])
        maker_totals[mk] = maker_totals.get(mk, 0) + cnt

    top15 = [m for m, _ in sorted(maker_totals.items(), key=lambda x: -x[1]) if m != "Others"][:15]
    return {
        "meta": {
            "regions": regions,
            "cal_years": sorted(set(r["year"] for r in rows)),
            "fuels": fuels,
            "makers": makers,
            "total_records": len(encoded),
            "last_updated": date.today().isoformat(),
        },
        "regions": regions,
        "makers": makers,
        "fuels": fuels,
        "data": encoded,
        "top15_oems": top15,
        "top10_oems": top15[:10],
    }


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(
    force: bool = False,
    target_year: int | None = None,
    json_only: bool = False,
    no_json: bool = False,
    dry_run: bool = False,
    verbose: bool = True,
) -> dict:
    """
    Main entry point. Returns summary dict with stats.
    """
    t0 = time.time()
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    summary = {
        "discovered": 0,
        "to_process": 0,
        "processed": 0,
        "skipped": 0,
        "errors": [],
        "json_exported": False,
        "elapsed_s": 0,
    }

    if json_only:
        print("─── JSON-only export ───")
        ok = export_master_json(SQLITE_PATH, STATIC_JSON, verbose)
        summary["json_exported"] = ok
        summary["elapsed_s"] = round(time.time() - t0, 1)
        return summary

    # ── Step 1: Discover ─────────────────────────────────────────────────────
    all_raw: list[RawFile] = []
    all_raw.extend(discover_type_a(RAW_DIR))
    all_raw.extend(discover_type_b(F1_DIR))
    summary["discovered"] = len(all_raw)
    print(f"Discovered {len(all_raw)} raw files (Type A: {sum(1 for r in all_raw if r.file_type=='A')}, "
          f"Type B: {sum(1 for r in all_raw if r.file_type=='B')})")

    # ── Step 2: Filter ───────────────────────────────────────────────────────
    to_process = [rf for rf in all_raw if needs_processing(rf, force, target_year)]
    to_skip = len(all_raw) - len(to_process)
    summary["to_process"] = len(to_process)
    summary["skipped"] = to_skip
    print(f"To process: {len(to_process)}  |  Skipping (already clean): {to_skip}")

    if dry_run:
        print("\n[DRY RUN] Would process:")
        for rf in to_process[:20]:
            print(f"  {rf.file_type}  {rf.state_name}  {rf.year}  →  {_clean_output_path(rf.state_name, rf.year).name}")
        if len(to_process) > 20:
            print(f"  ... and {len(to_process)-20} more")
        return summary

    # ── Step 3: Clean each file ───────────────────────────────────────────────
    processed = 0
    for i, rf in enumerate(to_process, 1):
        label = f"[{i}/{len(to_process)}] {rf.state_name} {rf.year}"
        print(label, end=" ", flush=True)
        try:
            if rf.file_type == "A":
                df = clean_type_a(rf, verbose=False)
            else:
                df = clean_type_b(rf, verbose=False)

            if df.empty:
                print("→ EMPTY (skipped)")
                summary["errors"].append(f"{rf.state_name} {rf.year}: empty output")
                continue

            out_path = _clean_output_path(rf.state_name, rf.year)
            df.to_csv(out_path, index=False)
            print(f"→ {len(df):,} rows")
            processed += 1
        except Exception as e:
            print(f"→ ERROR: {e}")
            summary["errors"].append(f"{rf.state_name} {rf.year}: {e}")

    summary["processed"] = processed
    print(f"\nProcessed {processed}/{len(to_process)} files")

    # ── Step 4: Merge all cleaned CSVs ───────────────────────────────────────
    if processed > 0 or not MASTER_CLEANED_CSV.exists():
        print("Merging all cleaned files into master CSV…")
        master_df = merge_all_cleaned(CLEAN_DIR)
        if not master_df.empty:
            master_df.to_csv(MASTER_CLEANED_CSV, index=False)
            print(f"  Master CSV: {len(master_df):,} rows → {MASTER_CLEANED_CSV}")
        else:
            print("  [WARN] No data to merge")

    # ── Step 5: Load to SQLite ────────────────────────────────────────────────
    if MASTER_CLEANED_CSV.exists():
        print("Loading to SQLite…")
        load_to_sqlite(MASTER_CLEANED_CSV, SQLITE_PATH, verbose=True)
    else:
        print("[WARN] No master CSV, skipping SQLite load")

    # ── Step 6: Export master JSON ────────────────────────────────────────────
    if not no_json:
        print("Exporting master JSON for fast API serving…")
        ok = export_master_json(SQLITE_PATH, STATIC_JSON, verbose=True)
        summary["json_exported"] = ok

    summary["elapsed_s"] = round(time.time() - t0, 1)
    print(f"\n✓ Pipeline complete in {summary['elapsed_s']}s")
    if summary["errors"]:
        print(f"  Warnings: {len(summary['errors'])} files had issues")
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Vahan incremental data pipeline")
    parser.add_argument("--year", type=int, default=None, help="Only process this calendar year")
    parser.add_argument("--force", action="store_true", help="Reprocess all files (ignore cache)")
    parser.add_argument("--json-only", action="store_true", help="Re-export JSON from existing DB only")
    parser.add_argument("--no-json", action="store_true", help="Skip JSON export step")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run without doing it")
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    args = parser.parse_args()

    summary = run_pipeline(
        force=args.force,
        target_year=args.year,
        json_only=args.json_only,
        no_json=args.no_json,
        dry_run=args.dry_run,
        verbose=not args.quiet,
    )
    if summary.get("errors"):
        print("\nFiles with issues:")
        for e in summary["errors"]:
            print(f"  - {e}")


if __name__ == "__main__":
    main()
