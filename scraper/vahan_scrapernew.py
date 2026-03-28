"""
vahan_scraper_robust.py
=======================
Robust VAHAN Parivahan Dashboard scraper.
URL: https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml

Workflow (matches confirmed intercepted POST payload):
  1. Open page, set base filters (Type=A, State, RTO=-1, Y-Axis, X-Axis, Year, YearType=C)
  2. Click Refresh → wait for AJAX to settle
  3. Expand sidebar ("Click Here" red GIF)
  4. Set Vehicle Category + Vehicle Class checkboxes in sidebar
  5. For each fuel group:
       a. (2nd+ fuel) main Refresh → re-expand sidebar → re-set category/class
       b. Deselect all fuels → select this fuel group
       c. Sidebar Refresh → wait → click Excel icon → wait for download
       d. Annotate CSV with fuel label
  6. Merge all fuel CSVs → one file per state/year

Install:
    pip install selenium webdriver-manager pandas openpyxl
"""

import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

try:
    from selenium import webdriver
    from selenium.common.exceptions import (
        ElementClickInterceptedException,
        NoSuchElementException,
        StaleElementReferenceException,
        TimeoutException,
        WebDriverException,
    )
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import Select, WebDriverWait
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install selenium webdriver-manager pandas openpyxl")
    sys.exit(1)


# ══════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════

def setup_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("vahan")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    fh = logging.FileHandler(log_dir / f"vahan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    fh.setFormatter(fmt)
    logger.handlers.clear()
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


log = logging.getLogger("vahan")


# ══════════════════════════════════════════════════════
# CONFIGURATION  — edit these, not the scraper logic
# ══════════════════════════════════════════════════════

REPORT_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"

BASE_FILTERS = {
    "type":      "Actual Value",   # internal value = "A"
    "yaxis":     "Maker",
    "xaxis":     "Month Wise",
    "year_type": "Calendar Year",  # internal = "C"
}

# These states map label-text → internal value from intercepted POST
# -1 = All States.  Add more from the dropdown as needed.
STATE_CODES = {
    "All Vahan4 Running States (36/36)": "-1",
    "Andhra Pradesh": "2",
    "Maharashtra": "27",
    "Karnataka": "15",
    "Tamil Nadu": "33",
    "Gujarat": "9",
    "Rajasthan": "29",
    "Uttar Pradesh": "36",
    "West Bengal": "38",
    "Telangana": "34",
}

YEAR_RANGE = (2019, 2026)   # inclusive

# Fuel groups: label → list of checkbox IDs to select
# IDs from confirmed DOM inspection — fallback to label text if IDs change
FUEL_GROUPS = {
    "Petrol":   ["fuel1", "fuel_petrol"],
    "Diesel":   ["fuel2", "fuel_diesel"],
    "CNG":      ["fuel3", "fuel_cng"],
    "Electric": ["fuel4", "fuel_electric"],
    "Others":   ["fuel5", "fuel6", "fuel7"],
}

# All fuel checkbox IDs (union of all groups) — for "deselect all" step
ALL_FUEL_IDS = list({cid for ids in FUEL_GROUPS.values() for cid in ids})

# Vehicle category checkbox IDs
VEHICLE_CATEGORY_IDS = ["VhCatg_1", "VhCatg_2", "VhCatg_3"]

# Vehicle class checkbox IDs — fallback to label text matching
VEHICLE_CLASS_IDS = ["VhClass_1", "VhClass_2"]
VEHICLE_CLASS_LABELS = ["Motor Car", "Motor Cab"]

# Timeouts (seconds)
DEFAULT_WAIT        = 20
PAGE_LOAD_TIMEOUT   = 120
AJAX_QUIET_TIMEOUT  = 60
DOWNLOAD_TIMEOUT    = 45
REFRESH_TO_DL_WAIT  = 2.5
REQUEST_DELAY       = 0.4
MAX_RETRIES         = 4

FUEL_COLUMN = "fuel_type"
ALL_INDIA_MARKERS = ("All Vahan4", "All India", "36/36")


# ══════════════════════════════════════════════════════
# UTILITIES / DECORATORS
# ══════════════════════════════════════════════════════

_cdm_lock = threading.Lock()


def retry_stale(max_attempts: int = MAX_RETRIES, delay: float = 0.4):
    """Decorator: retry on StaleElementReferenceException."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except StaleElementReferenceException:
                    if attempt < max_attempts - 1:
                        time.sleep(delay * (attempt + 1))
                    else:
                        raise
        return wrapper
    return decorator


def save_debug(driver, session_dir: Path, tag: str):
    """Save screenshot + page source for post-mortem analysis."""
    try:
        session_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%H%M%S")
        driver.save_screenshot(str(session_dir / f"debug_{tag}_{ts}.png"))
        (session_dir / f"debug_{tag}_{ts}.html").write_text(
            driver.page_source, encoding="utf-8", errors="replace"
        )
    except Exception as e:
        log.warning(f"save_debug failed: {e}")


# ══════════════════════════════════════════════════════
# DRIVER FACTORY
# ══════════════════════════════════════════════════════

def create_driver(download_dir: Path, headless: bool = False) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--page-load-strategy=eager")  # Don't wait for all resources
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("prefs", {
        "download.default_directory":        str(download_dir.resolve()),
        "download.prompt_for_download":      False,
        "download.directory_upgrade":        True,
        "safebrowsing.enabled":              True,
        "profile.default_content_settings.popups": 0,
        "plugins.always_open_pdf_externally": True,
    })

    log.info("Resolving ChromeDriver (may download on first run)...")
    with _cdm_lock:
        driver_path = ChromeDriverManager().install()

    driver = webdriver.Chrome(service=Service(driver_path), options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.set_script_timeout(90)
    log.info("Browser started.")
    return driver


# ══════════════════════════════════════════════════════
# WAIT HELPERS
# ══════════════════════════════════════════════════════

def wait_ajax_quiet(driver, timeout: float = AJAX_QUIET_TIMEOUT):
    """Wait until PrimeFaces block overlays are gone."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            overlays = (
                driver.find_elements(By.CSS_SELECTOR, "div.ui-widget-overlay.ui-blockui") +
                driver.find_elements(By.CSS_SELECTOR, "div.blockUI.blockOverlay")
            )
            visible = [e for e in overlays if e.is_displayed()]
            if not visible:
                return
        except StaleElementReferenceException:
            pass
        except Exception:
            return
        time.sleep(0.25)


def wait_for_excel_icon(driver, timeout: int = 15):
    """Wait until Excel/CSV download icon is clickable."""
    selectors = [
        (By.ID, "vchgroupTable:xls"),
        (By.CSS_SELECTOR, "img[title='Download EXCEL file']"),
        (By.CSS_SELECTOR, "a[id*='xls']"),
        (By.XPATH, "//img[contains(@title,'EXCEL')]"),
    ]
    for t in [timeout, min(timeout, 8), 25]:
        for by, val in selectors:
            try:
                WebDriverWait(driver, t).until(EC.element_to_be_clickable((by, val)))
                return
            except Exception:
                continue
        time.sleep(0.6)
    time.sleep(0.5)


# ══════════════════════════════════════════════════════
# SAFE CLICK
# ══════════════════════════════════════════════════════

@retry_stale(max_attempts=MAX_RETRIES)
def safe_click(driver, element) -> bool:
    """Scroll → normal click → JS click fallback."""
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        time.sleep(0.08)
        element.click()
        return True
    except ElementClickInterceptedException:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False
    except StaleElementReferenceException:
        raise
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False


def click_by_text(driver, text: str, partial: bool = True, timeout: int = DEFAULT_WAIT) -> bool:
    xpath = f"//*[contains(text(),'{text}')]" if partial else f"//*[text()='{text}']"
    for attempt in range(MAX_RETRIES):
        try:
            elem = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            return safe_click(driver, elem)
        except StaleElementReferenceException:
            time.sleep(0.35 * (attempt + 1))
        except Exception:
            return False
    return False


# ══════════════════════════════════════════════════════
# DROPDOWN HELPERS  (robust against j_idt* changes)
# ══════════════════════════════════════════════════════

# Stable IDs that don't change on redeploy
STABLE_IDS = {
    "yaxis":     "yaxisVar",
    "xaxis":     "xaxisVar",
    "year_type": "selectedYearType",
    "year":      "selectedYear",
    "rto":       "selectedRto",
}

# Position-based fallback: Type=0, State=1, RTO=2, Y-Axis=3, X-Axis=4, YearType=5, Year=6
DROPDOWN_ORDER = {
    "type": 0, "state": 1, "rto": 2,
    "yaxis": 3, "xaxis": 4, "year_type": 5, "year": 6,
}

# Label text → XPath fallbacks live in vahan_scraper._find_dropdown_trigger (Type:/State: etc.).
LABEL_MAP = {
    "type": "Type",
    "state": "State",
    "rto": "RTO",
    "yaxis": "Y-Axis",
    "xaxis": "X-Axis",
    "year_type": "Year Type",
}

# Deprecated: use scraper.vahan_scraper.VahanScraper — robustness merged there + scraper_robust.py.