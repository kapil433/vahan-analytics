#!/usr/bin/env python3
"""
Run Vahan scraper for All India, 2012-2026.
Uses vahan_scraper_master → vahan_scraper.

Output: output/vahan_data/All Vahan4 Running States (36_36)_YYYY_merged.csv

After completion:
  python scripts/clean_vahan_data.py
  python scripts/load_vahan_to_db.py
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scraper.vahan_scraper_master import run_batch_sequential

ALL_INDIA = "All Vahan4 Running States (36/36)"
YEARS = list(range(2012, 2027))
OUTPUT_BASE = PROJECT_ROOT / "output" / "vahan_data"

if __name__ == "__main__":
    print(f"Scraping All India for years {YEARS[0]}-{YEARS[-1]} ({len(YEARS)} years)")
    print(f"Output: {OUTPUT_BASE}")
    print("~2-3 min per year. Total ~45 min. Press Ctrl+C to stop.\n")

    results = run_batch_sequential(
        states=[ALL_INDIA],
        years=YEARS,
        fuels=None,
        output_base=OUTPUT_BASE,
        headless=False,
    )

    print(f"\nDone. {len(results)} files written.")
    if results:
        print("Next: python scripts/clean_vahan_data.py")
