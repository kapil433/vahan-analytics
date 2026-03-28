"""
Vahan Master API - Scraper UI + Swagger. HTML linked here.

  cd vahan-analytics
  python run_api.py

Then open:
  - UI:  http://localhost:8000/
  - Swagger: http://localhost:8000/docs

HTML UI: api/static/index.html (multiselect for states/fuels/years -> vahan_scraper_master)
"""

import os
import sys
from pathlib import Path

# Ensure project root is cwd and on path
PROJECT_ROOT = Path(__file__).resolve().parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

from scraper.console_win import configure_stdio_utf8

configure_stdio_utf8()

# This entry point uses Selenium by default (set env before launch to use Playwright).
os.environ.setdefault("VAHAN_SCRAPER_BACKEND", "selenium")

# HTML linked to this master entry point
STATIC_HTML = PROJECT_ROOT / "api" / "static" / "index.html"

import uvicorn

if __name__ == "__main__":
    # reload=True spawns a child process on Windows and often breaks Ctrl+C / clean shutdown.
    # Enable dev reload with: set VAHAN_UVICORN_RELOAD=1  (or run: uvicorn api.main:app --reload)
    _reload = os.environ.get("VAHAN_UVICORN_RELOAD", "").strip().lower() in ("1", "true", "yes")
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=_reload)
