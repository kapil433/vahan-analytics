#!/usr/bin/env python3
"""
Create data/vahan_local.db from cleaned registrations CSV (SQLite fallback for the API).

Reads: output/vahan_data_cleaned/vahan_registrations_cleaned.csv
Writes: data/vahan_local.db with table vahan_registrations (schema compatible with api/main.py).

Usage (from repo root):
  python scripts/setup_local_sqlite.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.mappings import month_to_fy  # noqa: E402

DEFAULT_CSV = PROJECT_ROOT / "output" / "vahan_data_cleaned" / "vahan_registrations_cleaned.csv"
DB_PATH = PROJECT_ROOT / "data" / "vahan_local.db"


def main() -> int:
    csv_path = DEFAULT_CSV
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])

    if not csv_path.is_file():
        print(f"Missing cleaned CSV: {csv_path}")
        print("Run: python scripts/clean_vahan_data.py (or clean_all) first.")
        return 1

    df = pd.read_csv(csv_path, encoding="utf-8")
    required = ["state_code", "state_name", "year", "fuel_type", "maker", "month", "count"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"CSV missing columns: {missing}")
        return 1

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE vahan_registrations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              state_code TEXT NOT NULL,
              state_name TEXT NOT NULL,
              year INTEGER NOT NULL,
              fuel_type TEXT NOT NULL,
              maker TEXT NOT NULL,
              month INTEGER NOT NULL,
              count INTEGER NOT NULL DEFAULT 0,
              fy TEXT,
              source TEXT DEFAULT 'vahan_parivahan',
              loaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(state_code, state_name, year, fuel_type, maker, month)
            )
            """
        )
        cur.execute("CREATE INDEX idx_vahan_reg_state_year ON vahan_registrations(state_code, year)")
        cur.execute("CREATE INDEX idx_vahan_reg_year_fuel ON vahan_registrations(year, fuel_type)")
        cur.execute("CREATE INDEX idx_vahan_reg_fy ON vahan_registrations(fy)")

        work = df[required].copy()
        # Combined CSV can repeat the same (state, year, fuel, maker, month); sum counts
        # so DB totals match SUM(count) in the file (INSERT OR REPLACE alone = last row wins).
        key_cols = ["state_code", "state_name", "year", "fuel_type", "maker", "month"]
        work = work.groupby(key_cols, as_index=False)["count"].sum()
        work["fy"] = work.apply(
            lambda r: month_to_fy(int(r["year"]), int(r["month"])),
            axis=1,
        )

        rows = [
            (
                str(r["state_code"]),
                str(r["state_name"]),
                int(r["year"]),
                str(r["fuel_type"]),
                str(r["maker"]),
                int(r["month"]),
                int(r["count"]),
                str(r["fy"]),
                "vahan_parivahan",
            )
            for _, r in work.iterrows()
        ]

        cur.executemany(
            """
            INSERT OR REPLACE INTO vahan_registrations
              (state_code, state_name, year, fuel_type, maker, month, count, fy, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM vahan_registrations")
        n = cur.fetchone()[0]
        print(f"Loaded {n:,} rows into {DB_PATH}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
