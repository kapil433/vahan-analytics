#!/usr/bin/env python3
"""
Load validated CSV files into PostgreSQL.
Requires: DATABASE_URL environment variable.
Usage: python load_to_db.py [--population] [--pci] [--cng] [--ev] [--all]
"""
import argparse
import os
import sys
from pathlib import Path

import pandas as pd

from config import DATA_DIR

try:
    import psycopg2
    from psycopg2.extras import execute_values
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    import sqlite3
    HAS_SQLITE = True
except ImportError:
    HAS_SQLITE = False


def get_conn():
    url = os.getenv("DATABASE_URL", "sqlite:///")
    if url.startswith("sqlite") or not url:
        path = url.replace("sqlite:///", "").replace("sqlite:", "").strip()
        if not path:
            path = str(Path(__file__).resolve().parent.parent / "vahan_research.db")
        return sqlite3.connect(path)
    if HAS_PSYCOPG2:
        return psycopg2.connect(url or "postgresql://localhost/vahan_analytics")
    raise RuntimeError("Need psycopg2 for PostgreSQL or use DATABASE_URL=sqlite:///path/to/db.db")


def _is_sqlite(conn):
    return conn.__class__.__module__ == "sqlite3"


def load_population(conn):
    path = DATA_DIR / "population_validated.csv"
    if not path.exists():
        print("population_validated.csv not found. Run fetch_population.py first.")
        return 0
    df = pd.read_csv(path)
    cols = ["state_code", "state_name", "year", "population", "reference_date", "source"]
    df = df[[c for c in cols if c in df.columns]]
    if _is_sqlite(conn):
        df.to_sql("state_population", conn, if_exists="replace", index=False)
    else:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO state_population (state_code, state_name, year, population, reference_date, source)
                VALUES %s
                ON CONFLICT (state_code, year) DO UPDATE SET
                  population = EXCLUDED.population,
                  fetched_at = NOW()
                """,
                [tuple(r) for r in df.to_numpy()],
            )
        conn.commit()
    print(f"Loaded {len(df)} population rows")
    return len(df)


def load_pci(conn):
    path = DATA_DIR / "pci_validated.csv"
    if not path.exists():
        print("pci_validated.csv not found. Run fetch_pci.py first.")
        return 0
    df = pd.read_csv(path)
    cols = ["state_code", "state_name", "fy", "pci_rs", "source"]
    df = df[[c for c in cols if c in df.columns]]
    if _is_sqlite(conn):
        df.to_sql("state_per_capita_income", conn, if_exists="replace", index=False)
    else:
        with conn.cursor() as cur:
            execute_values(
            cur,
            """
            INSERT INTO state_per_capita_income (state_code, state_name, fy, pci_rs, source)
            VALUES %s
            ON CONFLICT (state_code, fy) DO UPDATE SET
              pci_rs = EXCLUDED.pci_rs,
              fetched_at = NOW()
            """,
                [tuple(r) for r in df.to_numpy()],
            )
        conn.commit()
    print(f"Loaded {len(df)} PCI rows")
    return len(df)


def load_cng(conn):
    path = DATA_DIR / "cng_validated.csv"
    if not path.exists():
        print("cng_validated.csv not found. Run fetch_cng.py first.")
        return 0
    df = pd.read_csv(path)
    cols = ["state_code", "state_name", "year", "month", "station_count", "source"]
    df = df[[c for c in cols if c in df.columns]]
    if "source" not in df.columns:
        df["source"] = "PNGRB_CGD_MIS"
    if _is_sqlite(conn):
        df.to_sql("cng_stations", conn, if_exists="replace", index=False)
    else:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO cng_stations (state_code, state_name, year, month, station_count, source)
                VALUES %s
                ON CONFLICT (state_code, year, month) DO UPDATE SET
                  station_count = EXCLUDED.station_count,
                  fetched_at = NOW()
                """,
                [tuple(r) for r in df.to_numpy()],
            )
        conn.commit()
    print(f"Loaded {len(df)} CNG rows")
    return len(df)


def load_ev(conn):
    path = DATA_DIR / "ev_chargers_validated.csv"
    if not path.exists():
        print("ev_chargers_validated.csv not found. Run fetch_ev_chargers.py first.")
        return 0
    df = pd.read_csv(path)
    if "month" not in df.columns:
        df["month"] = 12
    if "charger_type" not in df.columns:
        df["charger_type"] = "total"
    if "source" not in df.columns:
        df["source"] = "data.gov.in"
    cols = ["state_code", "state_name", "year", "month", "charger_count", "charger_type", "source"]
    df = df[[c for c in cols if c in df.columns]]
    if _is_sqlite(conn):
        df.to_sql("ev_chargers", conn, if_exists="replace", index=False)
    else:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO ev_chargers (state_code, state_name, year, month, charger_count, charger_type, source)
                VALUES %s
                ON CONFLICT (state_code, year, month) DO UPDATE SET
                  charger_count = EXCLUDED.charger_count,
                  fetched_at = NOW()
                """,
                [tuple(r) for r in df.to_numpy()],
            )
        conn.commit()
    print(f"Loaded {len(df)} EV charger rows")
    return len(df)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--population", action="store_true")
    parser.add_argument("--pci", action="store_true")
    parser.add_argument("--cng", action="store_true")
    parser.add_argument("--ev", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.all:
        args.population = args.pci = args.cng = args.ev = True

    if not any([args.population, args.pci, args.cng, args.ev]):
        parser.print_help()
        return

    conn = get_conn()
    try:
        if args.population:
            load_population(conn)
        if args.pci:
            load_pci(conn)
        if args.cng:
            load_cng(conn)
        if args.ev:
            load_ev(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
