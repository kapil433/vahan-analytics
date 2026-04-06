"""
Smart Monthly Update  —  scripts/monthly_update_smart.py
=========================================================
Run this at the start of each month to pull fresh data for the CURRENT year.
Past years (≤ last year) are NEVER re-scraped — only the current calendar year.

What it does:
  1. Scrape current year for all states (via the /scrape API or direct call)
  2. Clean only the current-year files (incremental)
  3. Merge + load into SQLite
  4. Export docs/data/vahan_master.json (so API serves it instantly)
  5. Optionally push to git / trigger CI

Usage:
  python scripts/monthly_update_smart.py               # Current year, all states
  python scripts/monthly_update_smart.py --states MH DL  # Selected states only
  python scripts/monthly_update_smart.py --skip-scrape    # Clean/load/export only
  python scripts/monthly_update_smart.py --json-only      # Just refresh JSON from DB
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

CURRENT_YEAR = datetime.now().year
ALL_STATES = [
    "All Vahan4 Running States (36/36)",
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu and Kashmir",
    "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra",
    "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab",
    "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal", "Delhi", "Puducherry",
    "Lakshadweep", "Andaman and Nicobar Islands",
    "Dadra and Nagar Haveli and Daman and Diu", "Chandigarh", "Ladakh",
]
ALL_FUELS = ["CNG", "Petrol", "Diesel", "EV", "Strong Hybrid"]


def scrape_current_year(states: list[str], fuels: list[str], api_url: str) -> bool:
    """POST /scrape to the running API (parallel). Returns True on success."""
    import urllib.request, json
    payload = json.dumps({
        "states": states,
        "fuels": fuels,
        "years": [CURRENT_YEAR],
        "parallel": True,
        "max_workers": 3,
    }).encode()
    req = urllib.request.Request(
        f"{api_url}/scrape",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            print(f"  Scrape triggered: {result.get('message', result)}")
            return True
    except Exception as e:
        print(f"  [WARN] Scrape API call failed: {e}")
        print("  → Run scraper manually: python run_api.py then POST /scrape")
        return False


def main():
    parser = argparse.ArgumentParser(description="Smart monthly update for current year")
    parser.add_argument("--states", nargs="*", default=None, help="State names to scrape (default: all)")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping, just clean/load/export")
    parser.add_argument("--json-only", action="store_true", help="Re-export JSON only")
    parser.add_argument("--api-url", default="http://localhost:8000", help="Local API URL for /scrape call")
    args = parser.parse_args()

    states = args.states or ALL_STATES
    print(f"Monthly Update  |  Year: {CURRENT_YEAR}  |  States: {len(states)}")
    print(f"{'─'*60}")

    from scripts.pipeline import run_pipeline

    if args.json_only:
        run_pipeline(json_only=True)
        return

    if not args.skip_scrape:
        print(f"Step 1/4  Triggering scrape for {CURRENT_YEAR}…")
        ok = scrape_current_year(states, ALL_FUELS, args.api_url)
        if not ok:
            print("  Continuing with clean/load/export using existing raw files…")
    else:
        print("Step 1/4  [SKIPPED] Scraping")

    print(f"\nStep 2-4  Clean → Load → Export JSON…")
    summary = run_pipeline(
        force=False,
        target_year=CURRENT_YEAR,
        no_json=False,
        verbose=True,
    )

    print(f"\n{'─'*60}")
    print(f"Monthly update complete.")
    print(f"  Processed: {summary['processed']} files")
    print(f"  JSON exported: {summary['json_exported']}")
    print(f"  Elapsed: {summary['elapsed_s']}s")
    if summary["errors"]:
        print(f"  Issues: {len(summary['errors'])} files")


if __name__ == "__main__":
    main()
