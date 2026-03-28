#!/usr/bin/env python3
"""
Copy the static dashboard into a clone of the public GitHub Pages repo layout.

Target layout (e.g. kapil433/ALL-India-Vahan-Analytics-Dashboard):
  index.html
  .nojekyll
  data/vahan_master.json   (optional if --export-db)

Usage:
  python scripts/sync_public_dashboard.py --target D:\\repos\\ALL-India-Vahan-Analytics-Dashboard
  python scripts/sync_public_dashboard.py --target ../ALL-India-Vahan-Analytics-Dashboard --export-db
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_HTML = PROJECT_ROOT / "api" / "static" / "dashboard" / "index.html"
EXPORT_SCRIPT = PROJECT_ROOT / "scripts" / "export_vahan_master_json.py"


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync dashboard + JSON into public Pages repo folder.")
    ap.add_argument("--target", type=Path, required=True, help="Path to ALL-India-Vahan-Analytics-Dashboard (local clone)")
    ap.add_argument(
        "--export-db",
        action="store_true",
        help="Run export_vahan_master_json.py into target data/ before copy (needs SQLite or DATABASE_URL)",
    )
    ap.add_argument(
        "--json-source",
        type=Path,
        help="Copy this JSON to target data/vahan_master.json instead of --export-db",
    )
    args = ap.parse_args()

    target: Path = args.target.resolve()
    if not DASHBOARD_HTML.is_file():
        print(f"Missing dashboard: {DASHBOARD_HTML}", file=sys.stderr)
        return 1

    target.mkdir(parents=True, exist_ok=True)
    data_dir = target / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if args.export_db:
        out_json = data_dir / "vahan_master.json"
        r = subprocess.run(
            [sys.executable, str(EXPORT_SCRIPT), "-o", str(out_json)],
            cwd=PROJECT_ROOT,
        )
        if r.returncode != 0:
            return r.returncode
    elif args.json_source and args.json_source.is_file():
        shutil.copy2(args.json_source, data_dir / "vahan_master.json")

    shutil.copy2(DASHBOARD_HTML, target / "index.html")
    (target / ".nojekyll").write_text("", encoding="utf-8")

    print(f"Synced dashboard -> {target}")
    print("  Next: cd target && git add -A && git commit -m \"Update dashboard\" && git push")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
