#!/usr/bin/env python3
"""
Rank OEM/maker strings by total registration count from cleaned CSV (for Top-15 mapping work).

Usage:
  python scripts/report_oem_volumes.py
  python scripts/report_oem_volumes.py --cleaned output/vahan_data_cleaned/vahan_registrations_cleaned.csv --top 30
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.mappings import normalize_maker, oem_short_display  # noqa: E402

DEFAULT_CSV = PROJECT_ROOT / "output" / "vahan_data_cleaned" / "vahan_registrations_cleaned.csv"


def main() -> int:
    ap = argparse.ArgumentParser(description="Report OEM volumes from cleaned registrations CSV.")
    ap.add_argument("--cleaned", type=Path, default=DEFAULT_CSV, help="Path to vahan_registrations_cleaned.csv")
    ap.add_argument("--top", type=int, default=25, help="How many rows to print")
    args = ap.parse_args()
    path = args.cleaned
    if not path.is_file():
        print(f"Missing: {path}", file=sys.stderr)
        return 1

    df = pd.read_csv(path, encoding="utf-8")
    if "maker" not in df.columns or "count" not in df.columns:
        print("CSV needs maker and count columns", file=sys.stderr)
        return 1

    # Raw maker strings (portal spelling) totals
    raw = df.groupby(df["maker"].astype(str).str.strip(), as_index=False)["count"].sum()
    raw = raw.sort_values("count", ascending=False)

    print("By stored maker (normalized uppercase in DB after clean):")
    for _, row in raw.head(args.top).iterrows():
        m = str(row["maker"])
        short = oem_short_display(normalize_maker(m))
        print(f"  {int(row['count']):>12,}  {m!r}  -> UI: {short}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
