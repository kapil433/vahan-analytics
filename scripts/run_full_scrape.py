#!/usr/bin/env python3
"""
Run full Vahan scrape: All India, all years (2012-2026), all fuels.
Output: output/vahan_data/All Vahan4 Running States (36_36)_YYYY_merged.csv
~2-3 min per year. Total ~45 min.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.scraping_config import YEAR_MIN, YEAR_MAX
from scraper.vahan_scraper_master import run_batch_sequential

ALL_INDIA = "All Vahan4 Running States (36/36)"
YEARS = list(range(YEAR_MIN, YEAR_MAX + 1))
OUTPUT_BASE = PROJECT_ROOT / "output" / "vahan_data"

if __name__ == "__main__":
    print(f"Full scrape: All India, years {YEARS[0]}-{YEARS[-1]} ({len(YEARS)} years), all fuels")
    print(f"Output: {OUTPUT_BASE}")
    print("~2-3 min per year. Press Ctrl+C to stop.\n")

    results = run_batch_sequential(
        states=[ALL_INDIA],
        years=YEARS,
        fuels=None,
        output_base=OUTPUT_BASE,
        headless=False,
    )

    print(f"\nDone. {len(results)} file(s) saved to {OUTPUT_BASE}")
    for p in results:
        print(f"  {p.name}")
