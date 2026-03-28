#!/usr/bin/env python3
"""
Load cleaned Vahan registration data into PostgreSQL.

Usage:
  python load_vahan_to_db.py [--cleaned-dir PATH] [--all]
  python load_vahan_to_db.py --year 2024          # Load only 2024
  python load_vahan_to_db.py --year 2024 --upsert # Upsert (update existing)

Requires: DATABASE_URL environment variable.
"""
import argparse
import os
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.mappings import month_to_fy

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("Install: pip install psycopg2-binary")
    sys.exit(1)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/vahan_analytics")
DEFAULT_CLEANED_DIR = PROJECT_ROOT / "output" / "vahan_data_cleaned"


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def load_cleaned_csv(conn, csv_path: Path, upsert: bool = True) -> int:
    """Load a single cleaned CSV into vahan_registrations."""
    df = pd.read_csv(csv_path, encoding="utf-8")
    required = ["state_code", "state_name", "year", "fuel_type", "maker", "month", "count"]
    if not all(c in df.columns for c in required):
        print(f"  Skip {csv_path.name}: missing columns")
        return 0

    df = df[required].copy()
    if "fy" in df.columns:
        df["fy"] = df["fy"].astype(str).str.strip()
    else:
        df["fy"] = df.apply(
            lambda r: month_to_fy(int(r["year"]), int(r["month"])),
            axis=1,
        )
    df["source"] = "vahan_parivahan"

    cols = ["state_code", "state_name", "year", "fuel_type", "maker", "month", "count", "fy", "source"]
    rows = [tuple(r) for r in df[cols].to_numpy()]
    if not rows:
        return 0

    with conn.cursor() as cur:
        if upsert:
            execute_values(
                cur,
                """
                INSERT INTO vahan_registrations (state_code, state_name, year, fuel_type, maker, month, count, fy, source)
                VALUES %s
                ON CONFLICT (state_code, state_name, year, fuel_type, maker, month) DO UPDATE SET
                  count = EXCLUDED.count,
                  fy = EXCLUDED.fy,
                  loaded_at = NOW()
                """,
                rows,
            )
        else:
            execute_values(
                cur,
                """
                INSERT INTO vahan_registrations (state_code, state_name, year, fuel_type, maker, month, count, fy, source)
                VALUES %s
                ON CONFLICT (state_code, state_name, year, fuel_type, maker, month) DO NOTHING
                """,
                rows,
            )
    conn.commit()
    return len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cleaned-dir", type=Path, default=DEFAULT_CLEANED_DIR)
    parser.add_argument("--year", type=int, help="Load only this year")
    parser.add_argument("--upsert", action="store_true", default=True, help="Update existing rows")
    parser.add_argument("--no-upsert", action="store_true", help="Skip existing rows")
    args = parser.parse_args()

    cleaned_dir = args.cleaned_dir
    if not cleaned_dir.exists():
        print(f"Cleaned dir not found: {cleaned_dir}")
        print("Run: python scripts/clean_vahan_data.py first")
        sys.exit(1)

    # Prefer combined file, else individual cleaned files
    combined = cleaned_dir / "vahan_registrations_cleaned.csv"
    if combined.exists():
        files = [combined]
    else:
        files = list(cleaned_dir.glob("*_cleaned.csv"))

    if not files:
        print("No cleaned CSV files found.")
        sys.exit(1)

    conn = get_conn()
    total = 0
    try:
        for f in files:
            df = pd.read_csv(f, encoding="utf-8")
            if args.year and "year" in df.columns:
                df = df[df["year"] == args.year]
                if df.empty:
                    print(f"  Skip {f.name}: no {args.year} data")
                    continue
                tmp = cleaned_dir / f"_tmp_{args.year}.csv"
                df.to_csv(tmp, index=False)
                try:
                    n = load_cleaned_csv(conn, tmp, upsert=not args.no_upsert)
                finally:
                    tmp.unlink(missing_ok=True)
            else:
                n = load_cleaned_csv(conn, f, upsert=not args.no_upsert)
            total += n
            print(f"  Loaded {f.name}: {n} rows")
    finally:
        conn.close()

    print(f"Total: {total} rows loaded into vahan_registrations")


if __name__ == "__main__":
    main()
