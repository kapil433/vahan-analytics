#!/usr/bin/env python3
"""Run scrape from API - subprocess entry point. Ensures Chrome opens visibly on Windows."""
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

from scraper.vahan_scraper_master import run_batch_parallel, run_batch_sequential

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        cfg = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    else:
        cfg = json.load(sys.stdin)

    states = cfg.get("states", [])
    years = cfg.get("years", [])
    fuels = cfg.get("fuels") or None
    parallel = cfg.get("parallel", True)
    max_workers = cfg.get("max_workers", 8)
    output_base = Path(cfg.get("output_base", str(PROJECT_ROOT / "output" / "vahan_data")))

    if parallel:
        run_batch_parallel(states, years, fuels, output_base, headless=False, max_workers=max_workers)
    else:
        run_batch_sequential(states, years, fuels, output_base, headless=False)
