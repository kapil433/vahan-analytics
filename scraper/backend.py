"""Choose Selenium (default) or Playwright via VAHAN_SCRAPER_BACKEND=playwright."""

from __future__ import annotations

import os


def get_vahan_scraper_class():
    raw = os.environ.get("VAHAN_SCRAPER_BACKEND", "selenium").strip().lower()
    # Only these enable Playwright; any other value (including empty) → Selenium.
    backend = raw if raw in ("playwright", "pw") else "selenium"
    if backend in ("playwright", "pw"):
        try:
            from scraper.vahan_scraper_playwright import VahanScraperPlaywright
        except ImportError as e:
            raise ImportError(
                "VAHAN_SCRAPER_BACKEND=playwright requires: pip install playwright && playwright install chromium"
            ) from e
        return VahanScraperPlaywright
    from scraper.vahan_scraper import VahanScraper

    return VahanScraper
