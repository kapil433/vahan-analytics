#!/usr/bin/env python3
"""Run All India scrape for a single year. Usage: python scripts/run_all_india_year.py 2025"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scraper.vahan_scraper_master import run_batch_sequential

ALL_INDIA = "All Vahan4 Running States (36/36)"

if __name__ == "__main__":
    year = int(sys.argv[1]) if len(sys.argv) >= 2 else 2025
    print(f"Scraping All India for {year}. Output: output/vahan_data/")
    results = run_batch_sequential(
        states=[ALL_INDIA],
        years=[year],
        fuels=None,
        output_base=PROJECT_ROOT / "output" / "vahan_data",
        headless=False,
    )
    print(f"Done. {len(results)} file(s): {results}")
