#!/usr/bin/env python3
"""
Inventory raw registration CSVs: *_merged.csv (any depth) plus output/vahan_data/f1/**/*.csv.

Classifies where the gap lives:
  - raw_data: wrong export type (vehicle class) or incomplete columns
  - pipeline: Maker Month Wise + enough columns but cleaner produced no rows
  - ok: cleaner produced rows

Run from repo root:
  python scripts/audit_vahan_merged.py
  python scripts/audit_vahan_merged.py --json output/vahan_merged_audit.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.clean_vahan_data import (  # noqa: E402
    _merged_csv_kind,
    clean_merged_csv,
    iter_raw_vahan_csv_files,
    parse_state_year_for_raw_file,
    read_vahan_csv_flexible,
)


def min_cols_for(df: pd.DataFrame) -> int:
    k = _merged_csv_kind(df)
    if k == "maker_monthwise" and len(df.columns) >= 17:
        return 17
    return 16


def classify(
    kind: str,
    ncols: int,
    min_c: int,
    clean_rows: int,
) -> tuple[str, str]:
    """
    Returns (bucket, owner) where owner is raw_data | pipeline | ok.
    """
    if clean_rows > 0:
        return "ok", "ok"
    if kind == "vehicle_class_only":
        return "vehicle_class_only", "raw_data"
    if kind == "maker_monthwise" and ncols < min_c:
        return "incomplete_columns", "raw_data"
    if kind == "maker_monthwise" and ncols >= min_c:
        return "clean_empty_wide_ok", "pipeline"
    if kind == "unknown" and ncols < min_c:
        return "unknown_short_columns", "raw_data"
    if kind == "unknown":
        return "unknown_layout", "pipeline"
    return "unknown_empty", "pipeline"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", type=Path, help="Write full report JSON to this path")
    ap.add_argument(
        "--raw-dir",
        type=Path,
        default=ROOT / "output" / "vahan_data",
        help="Root directory containing *_merged.csv (flat and/or subfolders like f1/)",
    )
    ap.add_argument(
        "--no-recurse",
        action="store_true",
        help="Only scan top-level raw-dir (no subfolders)",
    )
    args = ap.parse_args()
    raw_dir: Path = args.raw_dir.resolve()

    if not raw_dir.is_dir():
        print(f"Missing directory: {raw_dir}", file=sys.stderr)
        return 1

    files = iter_raw_vahan_csv_files(raw_dir, recursive=not args.no_recurse)

    records: list[dict] = []
    counts = defaultdict(int)
    for path in files:
        try:
            rel = str(path.relative_to(raw_dir).as_posix())
        except ValueError:
            rel = path.name
        src = "f1" if rel.startswith("f1/") else "flat_merged"
        parsed = parse_state_year_for_raw_file(path, raw_dir)
        if not parsed:
            rec = {
                "file": path.name,
                "rel_path": rel,
                "source": src,
                "state": None,
                "year": None,
                "kind": None,
                "ncols": None,
                "min_cols": None,
                "clean_rows": 0,
                "bucket": "bad_filename",
                "fix_in": "raw_data",
                "note": "Could not parse state+year from filename or f1/ folder layout",
            }
            records.append(rec)
            counts["bad_filename"] += 1
            continue

        state_name, year = parsed
        df_head = read_vahan_csv_flexible(path, nrows=2)
        kind = _merged_csv_kind(df_head)
        ncols = len(df_head.columns)
        min_c = min_cols_for(df_head)
        df_clean = clean_merged_csv(path, state_name, year)
        clean_rows = len(df_clean)
        bucket, fix_in = classify(kind, ncols, min_c, clean_rows)

        notes = []
        if bucket == "vehicle_class_only":
            notes.append("Portal export is Vehicle Class Wise (or similar), not Maker Month Wise.")
        elif bucket == "incomplete_columns":
            notes.append(
                f"Only {ncols} columns; cleaner needs at least {min_c} for full maker-month grid."
            )
        elif bucket == "clean_empty_wide_ok":
            notes.append("Maker Month Wise + enough columns but zero parsed rows; inspect raw layout vs cleaner.")
        elif bucket == "unknown_layout":
            notes.append("Layout not recognized; may need parser update or corrected raw export.")
        elif bucket == "unknown_short_columns":
            notes.append("Too few columns for standard grid; re-export or partial-year parser.")

        rec = {
            "file": path.name,
            "rel_path": rel,
            "source": src,
            "state": state_name,
            "year": year,
            "kind": kind,
            "ncols": ncols,
            "min_cols": min_c,
            "clean_rows": clean_rows,
            "clean_total_count": int(df_clean["count"].sum()) if clean_rows else 0,
            "bucket": bucket,
            "fix_in": fix_in,
            "note": " ".join(notes) if notes else "",
        }
        records.append(rec)
        counts[bucket] += 1

    f1_total = sum(1 for r in records if r.get("source") == "f1")
    f1_unparsed = sum(1 for r in records if r.get("source") == "f1" and r.get("bucket") == "bad_filename")
    f1_parsed = f1_total - f1_unparsed

    # Summary
    print("Vahan raw CSV audit (merged + f1/)")
    print(f"  Directory: {raw_dir}")
    print(f"  Files scanned: {len(files)}")
    print(f"  Under f1/: csv_files={f1_total}  parsed_state_year={f1_parsed}  unparsed_path_or_name={f1_unparsed}")
    print("  By bucket:")
    for k in sorted(counts.keys()):
        print(f"    {k}: {counts[k]}")
    print("  Fix ownership:")
    raw_n = sum(1 for r in records if r.get("fix_in") == "raw_data")
    pipe_n = sum(1 for r in records if r.get("fix_in") == "pipeline")
    ok_n = sum(1 for r in records if r.get("fix_in") == "ok")
    print(f"    raw_data (re-scrape / re-export): {raw_n}")
    print(f"    pipeline (code / merge script): {pipe_n}")
    print(f"    ok: {ok_n}")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        out = {
            "raw_dir": str(raw_dir),
            "summary": dict(counts),
            "f1_inventory": {
                "csv_files": f1_total,
                "parsed_state_year": f1_parsed,
                "unparsed_state_year": f1_unparsed,
            },
            "records": records,
        }
        args.json.write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(f"  Wrote {args.json}")

    # Non-zero exit if pipeline gaps exist (actionable in-repo)
    pipe_gaps = [r["file"] for r in records if r.get("bucket") == "clean_empty_wide_ok"]
    if pipe_gaps:
        print("\nFiles with pipeline gaps (Maker Month Wise + enough columns, but zero clean rows):")
        for f in pipe_gaps[:40]:
            print(f"  - {f}")
        if len(pipe_gaps) > 40:
            print(f"  ... +{len(pipe_gaps) - 40} more")
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
