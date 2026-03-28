#!/usr/bin/env python3
"""
Monthly update: Scrape current year only, clean, load to DB.
Run at start of each month to refresh that year's data.

Usage:
  python monthly_update.py [--year YYYY] [--headless] [--skip-scrape] [--skip-load]

Steps:
  1. Scrape: 37 states × current year × all fuels (via master scraper)
  2. Clean: Parse merged CSVs -> normalized format
  3. Load: Upsert into vahan_registrations table
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUTPUT_BASE = PROJECT_ROOT / "output" / "vahan_data"
CLEANED_DIR = PROJECT_ROOT / "output" / "vahan_data_cleaned"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=datetime.now().year)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping, use existing files")
    parser.add_argument("--skip-load", action="store_true", help="Skip DB load")
    args = parser.parse_args()

    year = args.year
    print(f"Monthly update for year {year}")

    if not args.skip_scrape:
        print("\n1. Scraping...")
        from scraper.vahan_scraper_master import run_batch_sequential
        from api.main import AVAILABLE_STATES

        run_batch_sequential(
            states=AVAILABLE_STATES,
            years=[year],
            fuels=None,  # all fuels
            output_base=OUTPUT_BASE,
            headless=args.headless,
        )
    else:
        print("\n1. Skipping scrape (--skip-scrape)")

    print("\n2. Cleaning...")
    from scripts.clean_vahan_data import clean_all

    clean_all(OUTPUT_BASE, CLEANED_DIR)

    if not args.skip_load:
        print("\n3. Loading to DB...")
        import subprocess
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "load_vahan_to_db.py"), "--year", str(year), "--upsert"],
            cwd=str(PROJECT_ROOT),
        )
        if result.returncode != 0:
            sys.exit(result.returncode)
    else:
        print("\n3. Skipping load (--skip-load)")

    print("\nDone. Dashboard can now fetch updated data via API.")


if __name__ == "__main__":
    main()
