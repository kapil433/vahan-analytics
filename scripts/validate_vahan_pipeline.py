#!/usr/bin/env python3
"""
Sanity-check cleaned registrations and optional vahan_master.json bundle.

- fuel_type must be in FUEL_CANONICAL
- state_name should resolve via STATE_MAP (warn on likely unknowns)
- if --bundle: decoded totals match CSV aggregates (sample dimensions)

Usage:
  python scripts/validate_vahan_pipeline.py
  python scripts/validate_vahan_pipeline.py --bundle docs/data/vahan_master.json
  python scripts/validate_vahan_pipeline.py --raw-dir output/vahan_data
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.mappings import FUEL_CANONICAL_SET  # noqa: E402
from scripts.clean_vahan_data import (  # noqa: E402
    iter_raw_vahan_csv_files,
    parse_state_year_for_raw_file,
    parse_state_year_from_f1_xlsx,
)
from scripts.config import STATE_MAP, normalize_state  # noqa: E402

DEFAULT_CLEANED = PROJECT_ROOT / "output" / "vahan_data_cleaned" / "vahan_registrations_cleaned.csv"
DEFAULT_RAW = PROJECT_ROOT / "output" / "vahan_data"


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate cleaned Vahan CSV and optional master JSON.")
    ap.add_argument("--cleaned", type=Path, default=DEFAULT_CLEANED)
    ap.add_argument("--bundle", type=Path, help="Optional vahan_master.json from export")
    ap.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW,
        help="Raw root for merged + f1/ inventory (default: output/vahan_data)",
    )
    ap.add_argument(
        "--no-raw-inventory",
        action="store_true",
        help="Skip merged + f1/ file inventory",
    )
    ap.add_argument(
        "--fail-on-f1-unparsed",
        action="store_true",
        help="Exit 1 if any file under f1/ lacks parsable state+year",
    )
    args = ap.parse_args()

    errs: list[str] = []
    warns: list[str] = []

    raw_root = args.raw_dir.resolve() if args.raw_dir else None
    if not args.no_raw_inventory and raw_root and raw_root.is_dir():
        inv = iter_raw_vahan_csv_files(raw_root, recursive=True)
        f1_files = [p for p in inv if p.relative_to(raw_root).as_posix().startswith("f1/")]

        def _parse_f1_or_merged(p: Path) -> tuple[str, int] | None:
            if p.suffix.lower() == ".xlsx":
                return parse_state_year_from_f1_xlsx(p, raw_root)
            return parse_state_year_for_raw_file(p, raw_root)

        f1_unparsed: list[str] = []
        for p in f1_files:
            if _parse_f1_or_merged(p) is None:
                f1_unparsed.append(p.relative_to(raw_root).as_posix())
        merged_n = len(inv) - len(f1_files)
        print(
            f"RAW inventory: merged_pattern={merged_n}  f1_files={len(f1_files)}  "
            f"f1_unparsed_state_year={len(f1_unparsed)}"
        )
        if f1_unparsed:
            sample = f1_unparsed[:15]
            print("  f1 unparsed (sample):", "; ".join(sample))
            if len(f1_unparsed) > 15:
                print(f"  ... +{len(f1_unparsed) - 15} more")
            warns.append(f"f1/ has {len(f1_unparsed)} file(s) without parsable state+year")
            if args.fail_on_f1_unparsed:
                errs.append("Unparsed f1/ files (fix names or folder layout)")
    elif not args.no_raw_inventory and args.raw_dir:
        warns.append(f"--raw-dir not a directory: {args.raw_dir}")

    if not args.cleaned.is_file():
        print(f"Missing cleaned CSV: {args.cleaned}", file=sys.stderr)
        return 1

    df = pd.read_csv(args.cleaned, encoding="utf-8")
    req = ["state_code", "state_name", "year", "fuel_type", "maker", "month", "count"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        print(f"Missing columns: {missing}", file=sys.stderr)
        return 1

    bad_fuels = sorted(set(df["fuel_type"].astype(str)) - FUEL_CANONICAL_SET)
    if bad_fuels:
        errs.append(f"fuel_type not in FUEL_CANONICAL: {bad_fuels}")

    for sn in sorted(df["state_name"].astype(str).unique()):
        if sn == "All India":
            continue
        if normalize_state(sn) is None:
            warns.append(f"state_name may be missing from STATE_MAP: {sn!r}")

    if args.bundle and args.bundle.is_file():
        bundle = json.loads(args.bundle.read_text(encoding="utf-8"))
        regions = bundle.get("regions") or []
        makers = bundle.get("makers") or []
        fuels = bundle.get("fuels") or []
        data = bundle.get("data") or []

        bundle_total = 0
        for row in data:
            if len(row) < 7:
                continue
            bundle_total += int(row[6])

        csv_total = int(df["count"].sum())

        if bundle_total != csv_total:
            errs.append(f"Grand total mismatch: bundle={bundle_total:,} csv={csv_total:,}")

        top15 = bundle.get("top15_oems") or bundle.get("top10_oems") or []
        if not top15:
            warns.append("bundle has no top15_oems/top10_oems")
        meta = bundle.get("meta") or {}
        if not meta.get("regions"):
            warns.append("bundle.meta.regions empty")

    for w in warns:
        print(f"WARN {w}")
    for e in errs:
        print(f"FAIL {e}", file=sys.stderr)

    if errs:
        return 1
    print("OK cleaned CSV checks" + (" + bundle totals" if args.bundle and args.bundle.is_file() else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
