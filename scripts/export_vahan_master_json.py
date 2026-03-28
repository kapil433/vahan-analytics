#!/usr/bin/env python3
"""
Write the legacy dashboard bundle (same shape as GET /data/vahan_master_compat) to disk.

Uses PostgreSQL if DATABASE_URL connects; otherwise data/vahan_local.db if present.

Usage (from repo root):
  python scripts/export_vahan_master_json.py
  python scripts/export_vahan_master_json.py -o docs/data/vahan_master.json
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from api.master_bundle import build_vahan_master_bundle  # noqa: E402

SQLITE_LOCAL = PROJECT_ROOT / "data" / "vahan_local.db"


def _try_postgres():
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        return None
    url = os.getenv("DATABASE_URL", "postgresql://localhost/vahan_analytics")
    try:
        return psycopg2.connect(url, cursor_factory=RealDictCursor)
    except Exception:
        return None


def _try_sqlite():
    if not SQLITE_LOCAL.is_file():
        return None
    try:
        c = sqlite3.connect(str(SQLITE_LOCAL), check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c
    except sqlite3.Error:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Export vahan_master.json for static hosting.")
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=PROJECT_ROOT / "docs" / "data" / "vahan_master.json",
        help="Output JSON path",
    )
    args = ap.parse_args()

    conn = _try_postgres()
    dialect = "postgres"
    if not conn:
        conn = _try_sqlite()
        dialect = "sqlite"
    if not conn:
        print(
            "No database: set DATABASE_URL for PostgreSQL or run scripts/setup_local_sqlite.py",
            file=sys.stderr,
        )
        return 1

    try:
        bundle = build_vahan_master_bundle(conn, dialect=dialect)
    finally:
        conn.close()

    out: Path = args.output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(bundle, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {out} ({len(bundle.get('data', []))} encoded rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
