"""
Vahan Parivahan Dashboard Scraper
https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml

Workflow:
  1. Top bar: **Type** and **Year type** left at portal defaults (not changed). **State** only if not all-India.
     Then **RTO** (optional) → **Y-Axis** (Maker) → **X-Axis** (Month Wise) → **Year**.
  2. Main **Refresh** (required: repaints west-pane markup incl. `#VhCatg` / `#fuel` — do not use Vehicle Category before this)
  3. Expand west sidebar (`#filterLayout` is hidden until opened)
  4. Sidebar filters: Vehicle Category, Vehicle Class; then **sidebar Refresh** so the report reflects them
  5. For **each** fuel group: clear others → select group → **sidebar Refresh** → wait → **Download** (Excel)
  6. For fuel 2+: repeat step 2–4 (main Refresh → expand → sidebar filters → sidebar Refresh), then step 5
  7. Merge all fuel files → single state-year file → Python cleaner

Top-bar dropdown resolution chain (see _resolve_hidden_select_for_key):
  config {j_idt*}_input (fingerprint-verified) → stable ids (yaxisVar, selectedYear, …) →
  content fingerprint scan → DOM-order position → label→menu hidden <select> XPath.
  PrimeFaces menu clicks still use _find_dropdown_trigger (id → stable → position → label div).
"""

import contextvars
import logging
import os
import re
import shutil
import threading
import time
from pathlib import Path
from datetime import datetime

try:
    from selenium.common.exceptions import (
        ElementClickInterceptedException,
        StaleElementReferenceException,
        TimeoutException,
        WebDriverException,
    )
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys
    from webdriver_manager.chrome import ChromeDriverManager
    import pandas as pd
except ImportError as e:
    print("Install: pip install selenium webdriver-manager pandas openpyxl")
    raise

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from scraper.console_win import configure_stdio_utf8

    configure_stdio_utf8()
except Exception:
    pass
from scraper.scraper_robust import (
    retry as _retry_op,
    save_selenium_debug,
    wait_for_chrome_download,
)
from scraper.batch_control import batch_stop_requested

from config.scraping_config import (
    BASE_FILTERS,
    VEHICLE_CATEGORY_IDS,
    VEHICLE_CATEGORY_TARGET_LABELS,
    VEHICLE_CLASS_IDS,
    VEHICLE_CLASS_LABELS,
    FUEL_GROUP_CHECKBOX_IDS,
    ALL_FUEL_IDS,
    YEAR_MIN,
    YEAR_MAX,
    SELECTORS,
    MAIN_REPORT_FORM_ID,
    PRIMEFACES_MENU_UPDATES,
    STATE_PORTAL_OPTION_VALUE,
    state_portal_option_value,
)

# 2-letter (etc.) option values in the State hidden <select> — robust fingerprint when portal text drifts.
_KNOWN_STATE_OPTION_VALUES = frozenset(STATE_PORTAL_OPTION_VALUE.values())

REPORT_URL = os.environ.get(
    "VAHAN_REPORT_URL",
    "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml",
).strip()
# Ceiling for most in-page waits (WebDriverWait, AJAX quiet, sidebar, state poll, etc.).
# Does not apply to driver.get() page load, script timeout, file download polling, or
# _long_wait_sec() used for first paint / toolbar readiness (see below).
# Default was 7s; gov portal + heavier JSF builds often need 25–45s+ after updates.
WAIT_CAP_SEC = float(os.environ.get("VAHAN_WAIT_CAP_SEC", "30"))

# Upper bound for explicit long waits (initial form / toolbar); independent of WAIT_CAP_SEC.
_LONG_WAIT_CAP_SEC = float(os.environ.get("VAHAN_LONG_WAIT_CAP_SEC", "120"))


def _long_wait_sec(sec: float | int) -> float:
    """WebDriverWait timeouts that must exceed WAIT_CAP_SEC (e.g. first load after navigation)."""
    return max(0.5, min(float(sec), _LONG_WAIT_CAP_SEC))
# Gov dashboard first paint often exceeds short caps; keep separate from WAIT_CAP_SEC.
PAGE_LOAD_TIMEOUT = max(
    15.0,
    float(os.environ.get("VAHAN_PAGE_LOAD_TIMEOUT", "120")),
)
SCRIPT_TIMEOUT = max(
    10.0,
    float(os.environ.get("VAHAN_SCRIPT_TIMEOUT_SEC", "90")),
)
_DOWNLOAD_MAX_SEC = float(os.environ.get("VAHAN_DOWNLOAD_MAX_SEC", "300"))


def _wait_s(sec: float | int | None = None) -> float:
    """Clamp wait seconds to [0.25, WAIT_CAP_SEC] for short UI/AJAX polling."""
    if sec is None:
        return max(0.25, WAIT_CAP_SEC)
    return max(0.25, min(float(sec), WAIT_CAP_SEC))


def _download_wait_sec(timeout: float | int | None = None) -> float:
    """Excel/csv export polling — not limited by VAHAN_WAIT_CAP_SEC."""
    if timeout is None:
        v = float(os.environ.get("VAHAN_DOWNLOAD_WAIT_SEC", "60"))
    else:
        v = float(timeout)
    return max(0.5, min(v, _DOWNLOAD_MAX_SEC))


DEFAULT_WAIT = _wait_s(float(os.environ.get("VAHAN_DEFAULT_WAIT_SEC", str(int(WAIT_CAP_SEC)))))
AJAX_QUIET_TIMEOUT = _wait_s(float(os.environ.get("VAHAN_AJAX_QUIET_SEC", str(int(WAIT_CAP_SEC)))))
SIDEBAR_WAIT = _wait_s(float(os.environ.get("VAHAN_SIDEBAR_WAIT_SEC", str(int(WAIT_CAP_SEC)))))
# Retained for compatibility with config / external use.
MAX_WAIT_S = _wait_s(float(os.environ.get("VAHAN_MAX_WAIT_SEC", str(int(WAIT_CAP_SEC)))))


def _cap_wait(sec: float | int) -> float:
    return _wait_s(sec)


SELENIUM_ACTION_RETRIES = 3
REQUEST_DELAY = 0.12
REFRESH_TO_DOWNLOAD_WAIT = float(os.environ.get("VAHAN_REFRESH_TO_DOWNLOAD_SEC", "0.35"))
DOWNLOAD_WAIT = int(_download_wait_sec())
FUEL_COLUMN = "fuel_type"

_LOG = logging.getLogger("vahan.scraper")
_active_session_log: contextvars.ContextVar[logging.Logger | None] = contextvars.ContextVar(
    "vahan_active_session_log", default=None
)
_logging_init_lock = threading.Lock()
_vahan_parent_stream_handler_added = False


def _active_log() -> logging.Logger:
    """During a scrape job, log to the per-session child logger (parallel-safe)."""
    lg = _active_session_log.get()
    return lg if lg is not None else _LOG


def _configure_scrape_logger(session_dir: Path) -> logging.Logger:
    """Per-session file log + one shared console on parent (parallel workers must not clear each other's handlers)."""
    session_dir.mkdir(parents=True, exist_ok=True)
    safe = "".join(c if (c.isalnum() or c in "-_") else "_" for c in session_dir.name)
    child = logging.getLogger(f"vahan.scraper.{safe}")
    child.setLevel(logging.DEBUG)
    child.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
    fh = logging.FileHandler(session_dir / "scrape.log", encoding="utf-8")
    fh.setFormatter(fmt)
    child.addHandler(fh)
    child.propagate = True

    parent = logging.getLogger("vahan.scraper")
    parent.setLevel(logging.DEBUG)
    global _vahan_parent_stream_handler_added
    with _logging_init_lock:
        if not _vahan_parent_stream_handler_added:
            sh = logging.StreamHandler()
            sh.setFormatter(fmt)
            parent.addHandler(sh)
            _vahan_parent_stream_handler_added = True
    parent.propagate = False
    return child


def _is_transient_navigation_error(exc: BaseException) -> bool:
    s = str(exc).lower()
    return any(
        token in s
        for token in (
            "err_connection",
            "timed_out",
            "timeout",
            "net::",
            "disconnected",
            "cannot navigate",
            "target frame detached",
            "invalid session",
        )
    )


def _load_report_page(driver, *, max_attempts: int | None = None) -> None:
    """
    Navigate to REPORT_URL. Retries on flaky network (e.g. net::ERR_CONNECTION_TIMED_OUT).
    TimeoutException: stop document load and return so caller can use a partial DOM.
    """
    attempts = max_attempts if max_attempts is not None else int(
        os.environ.get("VAHAN_PAGE_LOAD_RETRIES", "6")
    )
    backoff = _wait_s(os.environ.get("VAHAN_NAV_RETRY_BACKOFF_SEC", "1"))
    for i in range(attempts):
        try:
            driver.get(REPORT_URL)
            return
        except TimeoutException:
            print(
                f"  Page load hit {PAGE_LOAD_TIMEOUT:.0f}s timeout - stopping load and using partial page if any.",
                flush=True,
            )
            _active_log().debug(
                "Page load timeout on navigation attempt %s — stopping document", i + 1
            )
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            return
        except WebDriverException as e:
            if not _is_transient_navigation_error(e):
                raise
            if i >= attempts - 1:
                raise
            delay = min(
                backoff * (i + 1),
                float(os.environ.get("VAHAN_NAV_RETRY_DELAY_CAP_SEC", "60")),
            )
            _active_log().warning(
                "Navigation attempt %s/%s failed (%s: %s); retrying in %.1fs",
                i + 1,
                attempts,
                type(e).__name__,
                e,
                delay,
            )
            time.sleep(delay)


def _recover_view_expired(driver) -> None:
    """Fresh navigation already loads new ViewState; recover if session shows expiry."""
    try:
        src = driver.page_source or ""
        markers = (
            "ViewExpiredException",
            "view could not be restored",
            "javax.faces.application.ViewExpiredException",
            "State error",
        )
        if any(m in src for m in markers):
            _active_log().warning("ViewState/session expiry text detected — refreshing once")
            driver.refresh()
            time.sleep(1.8)
            WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.presence_of_element_located((By.TAG_NAME, "form"))
            )
    except Exception as e:
        _active_log().debug("view recovery skipped: %s", e)

from scraper.state_aggregate import ALL_INDIA_STATES, _is_aggregate_state_name


def _state_option_matches(requested: str, option_label: str) -> bool:
    """
    Match user/API state string to a portal option without substring traps
    (e.g. 'Delhi' must not match 'New Delhi …').
    """
    v = (requested or "").replace("\xa0", " ").strip().casefold()
    l = (option_label or "").replace("\xa0", " ").strip().casefold()
    if not v or not l:
        return False
    if l == v:
        return True
    if l.startswith(v + "(") or l.startswith(v + " "):
        return True
    if len(v) >= 3 and l.startswith(v):
        rest = l[len(v) : len(v) + 1]
        return rest in ("", " ", "(")
    return False


def _state_aggregate_label_text(label: str) -> bool:
    t = (label or "").replace("\xa0", " ").strip().casefold()
    if not t:
        return False
    return "all vahan4" in t or t == "all india" or t.startswith("all india ")

# Parallel workers calling ChromeDriverManager().install() at once can deadlock on Windows.
_chromedriver_install_lock = threading.Lock()
_chromedriver_cached_path: str | None = None


def resolve_chromedriver_path() -> str:
    """
    Resolve the chromedriver binary once and reuse for all workers.
    Call from the batch orchestrator before ThreadPoolExecutor work to avoid a long
    serial queue where every worker blocks on install() inside the lock.
    """
    global _chromedriver_cached_path
    with _chromedriver_install_lock:
        if _chromedriver_cached_path is None:
            print(
                "  [Chrome] Resolving ChromeDriver (first launch may download; can take 1-2 min)...",
                flush=True,
            )
            _chromedriver_cached_path = ChromeDriverManager().install()
        return _chromedriver_cached_path


def create_driver(
    download_dir: Path,
    headless: bool = False,
    *,
    window_layout_slot: int | None = None,
) -> webdriver.Chrome:
    """Create Chrome WebDriver with download directory set.

    When ``window_layout_slot`` is set (parallel batch jobs), tile windows so they are not
    all stacked at (0,0) behind each other. Otherwise use a single maximized window.
    """
    options = Options()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
    elif window_layout_slot is not None:
        # Tile so parallel jobs are not one maximized stack (last --window-size wins — do not add 1920 below).
        col = int(window_layout_slot) % 4
        row = int(window_layout_slot) // 4
        x = col * 480
        y = row * 80
        options.add_argument(f"--window-position={x},{y}")
        options.add_argument("--window-size=960,900")
    else:
        options.add_argument("--start-maximized")  # Visible window when running from API
        options.add_argument("--window-position=0,0")
        options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")  # Reduce bot detection
    options.add_argument("--disable-extensions")
    # "eager" can leave an incomplete DOM on flaky links; default "normal" finishes first paint.
    _pls = (os.environ.get("VAHAN_PAGE_LOAD_STRATEGY") or "normal").strip().lower()
    if _pls in ("eager", "none", "normal"):
        try:
            options.page_load_strategy = _pls
        except Exception:
            pass

    # Set download directory (use absolute path; Windows-friendly)
    dl_path = str(download_dir.resolve()).replace("/", "\\")
    prefs = {
        "download.default_directory": dl_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0,
        "plugins.always_open_pdf_externally": True,  # Don't open xlsx in browser
    }
    options.add_experimental_option("prefs", prefs)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])

    # Each parallel job must use its own profile and debug port or later Chromes can fail to start
    # or attach to the first instance (only one window appears, others look "stuck").
    profile_dir = download_dir.resolve().parent / "chrome_user_data"
    profile_dir.mkdir(parents=True, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")
    # Do not set --remote-debugging-port here: ChromeDriver negotiates CDP; a fixed port can
    # prevent extra instances from starting on some Windows/Chrome builds (user sees only 1–2 windows).

    driver_path = resolve_chromedriver_path()
    service = Service(driver_path)
    print("  [Chrome] Starting browser...", flush=True)
    driver = webdriver.Chrome(service=service, options=options)
    # Without this, driver.get() can hang indefinitely on slow/blocked loads.
    driver.set_page_load_timeout(max(3.0, PAGE_LOAD_TIMEOUT))
    driver.set_script_timeout(max(3.0, SCRIPT_TIMEOUT))
    return driver


def _wait_primefaces_quiet(driver, timeout: float | None = None) -> bool:
    """
    Wait until PrimeFaces block overlays from Refresh/AJAX are gone.
    Returns False if timeout (caller can log); does not fail silently.
    """
    if timeout is None:
        timeout = AJAX_QUIET_TIMEOUT
    timeout = _wait_s(timeout)
    deadline = time.time() + timeout
    loop_start = time.time()
    while time.time() < deadline:
        try:
            overlays = driver.find_elements(By.CSS_SELECTOR, "div.ui-widget-overlay")
            combo = driver.find_elements(By.CSS_SELECTOR, "div.ui-widget-overlay.ui-blockui")
            blockui = driver.find_elements(By.CSS_SELECTOR, "div.ui-blockui, div.blockUI.blockOverlay")
            all_els = overlays + combo + blockui
            visible = []
            for e in all_els:
                try:
                    if e.is_displayed():
                        visible.append(e)
                except StaleElementReferenceException:
                    continue
            if not visible:
                elapsed = time.time() - loop_start
                if elapsed > max(1.0, WAIT_CAP_SEC * 0.4):
                    _active_log().debug("PrimeFaces overlays cleared after %.1fs", elapsed)
                return True
        except StaleElementReferenceException:
            pass
        except Exception as e:
            _active_log().debug("overlay poll: %s", e)
        time.sleep(0.12)
    _active_log().warning(
        "PrimeFaces overlays still visible after %.0fs — continuing anyway (may be flaky)",
        timeout,
    )
    return False


def _safe_click(driver, element) -> bool:
    """Scroll, normal click, then JS click; handles intercepts. Returns False if element went stale."""
    for _ in range(SELENIUM_ACTION_RETRIES):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
            time.sleep(0.07)
            element.click()
            return True
        except StaleElementReferenceException:
            return False
        except ElementClickInterceptedException:
            try:
                driver.execute_script("arguments[0].click();", element)
                return True
            except StaleElementReferenceException:
                return False
            except Exception:
                time.sleep(0.28)
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", element)
                return True
            except StaleElementReferenceException:
                return False
            except Exception:
                time.sleep(0.28)
    return False


def _find_clickable(driver, by, value, timeout=DEFAULT_WAIT):
    """Wait for element and return when clickable."""
    return WebDriverWait(driver, _wait_s(timeout)).until(
        EC.element_to_be_clickable((by, value))
    )


_YEAR_OPTION_RE = re.compile(r"^\s*\d{4}\s*$")


def _master_form(driver):
    for css in (
        "form#masterLayout_formlogin",
        "form[name='masterLayout_formlogin']",
        "form[action*='reportview']",
    ):
        try:
            return driver.find_element(By.CSS_SELECTOR, css)
        except Exception:
            continue
    try:
        return driver.find_element(By.TAG_NAME, "form")
    except Exception:
        return None


def _hidden_select_meta(sel_el):
    opts = sel_el.find_elements(By.TAG_NAME, "option")
    vals = frozenset((o.get_attribute("value") or "").strip() for o in opts)
    texts = [(o.text or "").replace("\xa0", " ").strip() for o in opts]
    blob = " ".join(texts).lower()
    return vals, texts, blob, len(opts)


def _match_type_hidden(sel_el) -> bool:
    vals, _, blob, _ = _hidden_select_meta(sel_el)
    return vals >= {"T", "L", "C", "A"} or (
        "actual value" in blob and "in crore" in blob and "in lakh" in blob
    )


def _match_state_hidden(sel_el) -> bool:
    """
    State list: ~37 entries (aggregate + states/UT). Do not require specific state names in
    the blob — portal wording/ordering changes broke matching and mapped the wrong <select>.
    """
    vals, texts, blob, n = _hidden_select_meta(sel_el)
    if n < 12 or n > 80:
        return False
    blob_l = blob.lower()
    # Not the year <select> (many 4-digit options)
    if sum(1 for t in texts if _YEAR_OPTION_RE.match((t or "").replace("\xa0", " ").strip())) >= 5:
        return False
    # Strong signal: many known state codes (MH, AP, …) as option values.
    if len(vals & _KNOWN_STATE_OPTION_VALUES) >= 5:
        return True
    # Aggregate row wording varies by portal build (36/36 vs singular "State").
    agg = (
        "all vahan4" in blob_l
        or "all india" in blob_l
        or "running states" in blob_l
        or "running state" in blob_l
    )
    if agg:
        return True
    # RTO/office lists also use parentheses; do not steal them for State.
    if "rto" in blob_l and ("office" in blob_l or "running office" in blob_l):
        return False
    # New builds: aggregate wording alone may be missing; state rows often contain "(…)" codes.
    paren_rows = sum(1 for t in texts if "(" in (t or ""))
    if n >= 20 and paren_rows >= min(8, max(4, n // 5)):
        return True
    return False


def _discovery_has_core(d: dict) -> bool:
    return bool(
        d.get("type_dropdown") and d.get("state_dropdown") and d.get("year_dropdown")
    )


def _merge_discovery_with_config(driver, discovered: dict) -> dict:
    """Fill missing dropdown keys using configured widget ids only when fingerprint matches."""
    out = dict(discovered)
    for key in (
        "type_dropdown",
        "state_dropdown",
        "rta_dropdown",
        "y_axis_dropdown",
        "x_axis_dropdown",
        "year_type_dropdown",
        "year_dropdown",
    ):
        if key in out:
            continue
        wid = SELECTORS.get(key)
        if not wid:
            continue
        try:
            inp = driver.find_element(By.ID, f"{wid}_input")
            if inp.tag_name.lower() == "select" and _verify_hidden_select_for_key(
                inp, key
            ):
                out[key] = inp
        except Exception:
            pass
    return out


def _match_rta_hidden(sel_el) -> bool:
    _, _, blob, n = _hidden_select_meta(sel_el)
    if n > 120:
        return False
    return "rto" in blob or "running office" in blob or ("office" in blob and "all " in blob)


def _match_y_axis_hidden(sel_el) -> bool:
    _, _, blob, n = _hidden_select_meta(sel_el)
    if n < 2:
        return False
    bl = blob.lower()
    # Portal option text varies; some builds say Manufacturer/OEM instead of Maker.
    axis_hint = (
        "maker" in bl
        or "manufacturer" in bl
        or "oem" in bl
    )
    if not axis_hint:
        return False
    # Do not require "vehicle class" substring (breaks newer builds).
    return (
        "vehicle class" in bl
        or "vehicle category" in bl
        or "regional" in bl
        or "fuel" in bl
        or n >= 3
    )


def _match_x_axis_hidden(sel_el) -> bool:
    """Do not use 'financial year' here — that collides with year_type hidden select."""
    _, _, blob, _ = _hidden_select_meta(sel_el)
    if "calendar year" in blob and "month wise" not in blob:
        return False
    return "month wise" in blob or "district wise" in blob


def _match_year_type_hidden(sel_el) -> bool:
    _, _, blob, n = _hidden_select_meta(sel_el)
    if n > 12:
        return False
    return "calendar year" in blob or "financial year" in blob or "fiscal" in blob


def _match_year_hidden(sel_el) -> bool:
    _, texts, _, n = _hidden_select_meta(sel_el)
    if n < 5:
        return False
    return sum(1 for t in texts if _YEAR_OPTION_RE.match(t)) >= 5


# --- Robust top-bar <select> resolution (config → fingerprint → position → label XPath) ---
_DROPDOWN_FINGERPRINT_MATCHERS: dict[str, object] = {
    "type_dropdown": _match_type_hidden,
    "state_dropdown": _match_state_hidden,
    "rta_dropdown": _match_rta_hidden,
    "y_axis_dropdown": _match_y_axis_hidden,
    "x_axis_dropdown": _match_x_axis_hidden,
    "year_type_dropdown": _match_year_type_hidden,
    "year_dropdown": _match_year_hidden,
}

# PrimeFaces stable widget roots (often survive j_idt* churn).
_STABLE_MENU_WIDGET_IDS: dict[str, str] = {
    "y_axis_dropdown": "yaxisVar",
    "x_axis_dropdown": "xaxisVar",
    "year_type_dropdown": "selectedYearType",
    "year_dropdown": "selectedYear",
    "rta_dropdown": "selectedRto",
}

# Typical main-toolbar order: Type, State, RTO, Y, X, Year type, Year (hidden selects in DOM).
_DROPDOWN_ORDER_INDEX: dict[str, int] = {
    "type_dropdown": 0,
    "state_dropdown": 1,
    "rta_dropdown": 2,
    "y_axis_dropdown": 3,
    "x_axis_dropdown": 4,
    "year_type_dropdown": 5,
    "year_dropdown": 6,
}

# Toolbar: do not use discovery cache alone for these — _discovery_has_core only needs
# type/state/year, so cached Y/X/Year/RTO can point at the wrong <select>.
_TOOLBAR_HIDDEN_ALWAYS_RESOLVE: frozenset[str] = frozenset(
    {
        "y_axis_dropdown",
        "x_axis_dropdown",
        "year_dropdown",
        "year_type_dropdown",
        "rta_dropdown",
    }
)


def _verify_hidden_select_for_key(sel_el, selector_key: str) -> bool:
    fn = _DROPDOWN_FINGERPRINT_MATCHERS.get(selector_key)
    if not fn:
        return True
    try:
        return bool(fn(sel_el))
    except Exception:
        return False


def _list_ordered_hidden_selects(driver) -> list:
    form = _master_form(driver)
    if not form:
        return []
    try:
        return list(
            form.find_elements(
                By.CSS_SELECTOR, "div.ui-helper-hidden-accessible select"
            )
        )
    except Exception:
        return []


def _find_hidden_select_via_label_xpath(driver, selector_key: str):
    """
    Last-resort: locate native <select> under the PrimeFaces menu next to a field label.
    Mirrors _find_dropdown_trigger label XPaths but returns the hidden <select>.
    """
    rows: tuple[tuple[str, ...], ...] = ()
    if selector_key == "type_dropdown":
        rows = (
            (
                "//form[@id='masterLayout_formlogin']//label[contains(., 'Type:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
                "//form[contains(@action,'reportview')]//label[contains(., 'Type:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
            ),
        )
    elif selector_key == "state_dropdown":
        rows = (
            (
                "//form[@id='masterLayout_formlogin']//label[contains(., 'State:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
                "//form[contains(@action,'reportview')]//label[contains(., 'State:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
            ),
        )
    elif selector_key == "rta_dropdown":
        rows = (
            (
                "//form[@id='masterLayout_formlogin']//label[contains(., 'RTO:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
                "//form[@id='masterLayout_formlogin']//label[contains(., 'Office')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
            ),
        )
    elif selector_key == "y_axis_dropdown":
        rows = (
            (
                "//form[@id='masterLayout_formlogin']//label[contains(., 'Y-Axis:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
                "//form[@id='masterLayout_formlogin']//label[contains(., 'Y Axis')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
            ),
        )
    elif selector_key == "x_axis_dropdown":
        rows = (
            (
                "//form[@id='masterLayout_formlogin']//label[contains(., 'X-Axis:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
                "//form[@id='masterLayout_formlogin']//label[contains(., 'X Axis')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
            ),
        )
    elif selector_key == "year_type_dropdown":
        rows = (
            (
                "//form[@id='masterLayout_formlogin']//label[contains(., 'Year Type')]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
            ),
        )
    elif selector_key == "year_dropdown":
        rows = (
            (
                "//form[@id='masterLayout_formlogin']//label[contains(normalize-space(.),'Year:') and not(contains(normalize-space(.),'Type'))]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
                "//form[contains(@action,'reportview')]//label[contains(normalize-space(.),'Year:') and not(contains(normalize-space(.),'Type'))]/following-sibling::div[contains(@class,'ui-selectonemenu')]//div[contains(@class,'ui-helper-hidden-accessible')]//select",
            ),
        )
    for group in rows:
        for xp in group:
            try:
                el = driver.find_element(By.XPATH, xp)
                if el.tag_name.lower() == "select":
                    return el
            except Exception:
                continue
    return None


def _stable_hidden_select_id(selector_key: str) -> str | None:
    st = _STABLE_MENU_WIDGET_IDS.get(selector_key)
    return f"{st}_input" if st else None


def _is_stable_semantic_hidden_select(selector_key: str, sel_el) -> bool:
    sid = _stable_hidden_select_id(selector_key)
    if not sid or sel_el is None:
        return False
    try:
        return (sel_el.get_attribute("id") or "").strip() == sid
    except Exception:
        return False


def _resolve_hidden_select_for_key(driver, selector_key: str):
    """
    Resolve the native hidden <select> for a main-form dropdown.
    1) Stable widget id (yaxisVar_input, …) — trusted first (survives j_idt* drift / bad discovered_selectors.json)
    2) Config {wid}_input if fingerprint matches
    3) Fingerprint scan → position → label XPath (each verified unless stable)
    """
    stable = _STABLE_MENU_WIDGET_IDS.get(selector_key)
    if stable:
        try:
            el = driver.find_element(By.ID, f"{stable}_input")
            if el.tag_name.lower() == "select":
                return el
        except Exception:
            pass

    wid_cfg = SELECTORS.get(selector_key)
    if wid_cfg and (not stable or wid_cfg != stable):
        try:
            el = driver.find_element(By.ID, f"{wid_cfg}_input")
            if el.tag_name.lower() == "select" and _verify_hidden_select_for_key(
                el, selector_key
            ):
                return el
        except Exception:
            pass

    ordered = _list_ordered_hidden_selects(driver)
    matches = []
    for s in ordered:
        try:
            if _verify_hidden_select_for_key(s, selector_key):
                matches.append(s)
        except Exception:
            continue
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        idx = _DROPDOWN_ORDER_INDEX.get(selector_key)
        if idx is not None and idx < len(ordered):
            cand = ordered[idx]
            if cand in matches:
                return cand
        return matches[0]

    idx = _DROPDOWN_ORDER_INDEX.get(selector_key)
    if idx is not None and idx < len(ordered):
        cand = ordered[idx]
        if _verify_hidden_select_for_key(cand, selector_key):
            return cand

    el = _find_hidden_select_via_label_xpath(driver, selector_key)
    if el is not None and _verify_hidden_select_for_key(el, selector_key):
        return el
    return None


def discover_main_hidden_selects(driver) -> dict:
    """
    Map selector_key → hidden <select> (PrimeFaces ui-helper-hidden-accessible).
    Extends the old Type-only fingerprint approach to all top form dropdowns.
    """
    out: dict = {}
    form = _master_form(driver)
    if not form:
        return _merge_discovery_with_config(driver, out)
    try:
        pool = list(
            form.find_elements(By.CSS_SELECTOR, "div.ui-helper-hidden-accessible select")
        )
    except Exception:
        return _merge_discovery_with_config(driver, out)

    def _take(matcher):
        for i, s in enumerate(pool):
            try:
                if matcher(s):
                    pool.pop(i)
                    return s
            except Exception:
                continue
        return None

    order = (
        ("type_dropdown", _match_type_hidden),
        ("state_dropdown", _match_state_hidden),
        ("rta_dropdown", _match_rta_hidden),
        ("y_axis_dropdown", _match_y_axis_hidden),
        ("x_axis_dropdown", _match_x_axis_hidden),
        ("year_type_dropdown", _match_year_type_hidden),
        ("year_dropdown", _match_year_hidden),
    )
    for key, matcher in order:
        got = _take(matcher)
        if got is not None:
            out[key] = got
    # Second pass: ordered _take can miss State if an earlier matcher stole a select or pool order shifted.
    if "state_dropdown" not in out:
        try:
            used_select_ids = set()
            for v in out.values():
                try:
                    i = (v.get_attribute("id") or "").strip()
                    if i:
                        used_select_ids.add(i)
                except Exception:
                    pass
            for s in form.find_elements(
                By.CSS_SELECTOR, "div.ui-helper-hidden-accessible select"
            ):
                sid = (s.get_attribute("id") or "").strip()
                if sid and sid in used_select_ids:
                    continue
                try:
                    if _match_state_hidden(s):
                        out["state_dropdown"] = s
                        break
                except Exception:
                    continue
        except Exception:
            pass
    return _merge_discovery_with_config(driver, out)


def _discover_hidden_selects_with_retry(driver) -> dict:
    best: dict = {}
    for attempt in range(3):
        invalidate_main_hidden_select_cache(driver)
        merged = discover_main_hidden_selects(driver)
        if _discovery_has_core(merged):
            return merged
        if len(merged) > len(best):
            best = merged
        if attempt < 2:
            time.sleep(0.22)
            try:
                _wait_primefaces_quiet(driver, timeout=min(2.0, WAIT_CAP_SEC))
            except Exception:
                pass
    return _merge_discovery_with_config(driver, best) if best else discover_main_hidden_selects(driver)


def invalidate_main_hidden_select_cache(driver) -> None:
    if getattr(driver, "_vahan_hidden_selects", None) is not None:
        try:
            delattr(driver, "_vahan_hidden_selects")
        except Exception:
            driver._vahan_hidden_selects = None  # type: ignore[attr-defined]


def invalidate_sidebar_discovery_cache(driver) -> None:
    """Sidebar checkbox ids go stale after full navigation or AJAX rebuild."""
    if getattr(driver, "_vahan_sidebar_checkbox_ids", None) is None:
        return
    try:
        delattr(driver, "_vahan_sidebar_checkbox_ids")
    except Exception:
        try:
            driver._vahan_sidebar_checkbox_ids = None  # type: ignore[attr-defined]
        except Exception:
            pass


def _ensure_main_hidden_selects(driver) -> dict:
    cached = getattr(driver, "_vahan_hidden_selects", None)
    if isinstance(cached, dict) and cached:
        st = cached.get("state_dropdown")
        if st is not None:
            try:
                if not _match_state_hidden(st):
                    invalidate_main_hidden_select_cache(driver)
                    cached = None
            except Exception:
                invalidate_main_hidden_select_cache(driver)
                cached = None
    if isinstance(cached, dict) and cached and _discovery_has_core(cached):
        return cached
    if isinstance(cached, dict) and cached:
        invalidate_main_hidden_select_cache(driver)
    discovered = _discover_hidden_selects_with_retry(driver)
    setattr(driver, "_vahan_hidden_selects", discovered)
    return discovered


def _hidden_select_visible_matches(current: str, value: str) -> bool:
    """
    Match selected/option visible text to desired value without substring traps
    (e.g. value '1' must not match current '2021'; 4-digit years must align to full year).
    """
    cur = (current or "").replace("\xa0", " ").strip()
    v = (value or "").strip()
    if not v or not cur:
        return False
    if v.isdigit() and len(v) == 4:
        if cur == v:
            return True
        if cur.startswith(v):
            rest = cur[len(v) : len(v) + 1]
            return not (rest and rest.isdigit())
        return False
    if v.isdigit() and len(v) <= 2:
        return cur == v
    cur_cf = cur.casefold()
    v_cf = v.casefold()
    if v_cf == cur_cf or v_cf in cur_cf or cur_cf.startswith(v_cf):
        return True
    return v in cur or cur.startswith(v)


def _primefaces_u_from_onchange(driver, sel_el) -> str | None:
    """Read PrimeFaces partial-update target `u` from the native select's onchange attribute."""
    try:
        oc = driver.execute_script(
            "var s=arguments[0]; return (s.getAttribute('onchange')||'').trim();",
            sel_el,
        )
        if not isinstance(oc, str) or len(oc) < 8:
            return None
        m = re.search(r'\bu\s*:\s*"([^"]+)"', oc)
        if m:
            return m.group(1).strip()
        m = re.search(r"\bu\s*:\s*'([^']+)'", oc)
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return None


def _hidden_select_selected_text(driver, sel_el) -> str:
    """Read selected option label without Selenium Select (hidden selects are not interactable in Chrome)."""
    try:
        t = driver.execute_script(
            "var s=arguments[0];"
            "if(!s||s.selectedIndex<0)return '';"
            "var o=s.options[s.selectedIndex];"
            "return o?(o.textContent||'').replace(/\\u00a0/g,' ').trim():'';",
            sel_el,
        )
        return (t or "").strip() if isinstance(t, str) else ""
    except Exception:
        return ""


def _hidden_select_list_options(driver, sel_el) -> list[tuple[str, str, int]]:
    """Return (option value, visible text, index) for each option via JS (no interaction)."""
    try:
        raw = driver.execute_script(
            """
            var s=arguments[0], out=[];
            for (var i=0;i<s.options.length;i++) {
              out.push([
                String(s.options[i].value||''),
                (s.options[i].textContent||'').replace(/\\u00a0/g,' ').trim(),
                i
              ]);
            }
            return out;
            """,
            sel_el,
        )
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    out: list[tuple[str, str, int]] = []
    for row in raw:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        try:
            out.append((str(row[0]), str(row[1]), int(row[2])))
        except (TypeError, ValueError):
            continue
    return out


def _hidden_select_set_index(driver, sel_el, index: int) -> bool:
    try:
        ok = driver.execute_script(
            "var s=arguments[0],i=arguments[1];"
            "if(i<0||i>=s.options.length)return false;"
            "s.selectedIndex=i;return true;",
            sel_el,
            int(index),
        )
        return bool(ok)
    except Exception:
        return False


def _hidden_select_select_by_value(driver, sel_el, value: str) -> bool:
    """Set option by value attribute using JS (avoids element not interactable on hidden <select>)."""
    try:
        ok = driver.execute_script(
            "var s=arguments[0],v=arguments[1];"
            "for(var i=0;i<s.options.length;i++){"
            "if(String(s.options[i].value)===v){s.selectedIndex=i;return true;}}"
            "return false;",
            sel_el,
            str(value),
        )
        return bool(ok)
    except Exception:
        return False


def _select_hidden_select_by_text(
    driver, sel_el, value: str, selector_key: str | None = None
) -> bool:
    """Set a native hidden <select> by visible text (or value code); dispatch change for PrimeFaces."""
    sk = selector_key or ""

    def _opt_matches(ot: str, val: str) -> bool:
        if sk == "state_dropdown":
            return _state_option_matches(val, ot)
        return _hidden_select_visible_matches(ot, val)

    try:
        cur_t = _hidden_select_selected_text(driver, sel_el)
        if _opt_matches(cur_t, value):
            return True
        for _ov, ot, idx in _hidden_select_list_options(driver, sel_el):
            if _opt_matches(ot, value):
                if not _hidden_select_set_index(driver, sel_el, idx):
                    return False
                _dispatch_primefaces_select_change(driver, sel_el)
                time.sleep(0.2)
                return True
        if value == "Actual Value":
            if _hidden_select_select_by_value(driver, sel_el, "A"):
                _dispatch_primefaces_select_change(driver, sel_el)
                time.sleep(0.2)
                return True
        if value.isdigit() and len(value) == 4:
            if _hidden_select_select_by_value(driver, sel_el, value):
                _dispatch_primefaces_select_change(driver, sel_el)
                time.sleep(0.2)
                return True
        if sk == "state_dropdown":
            pv = state_portal_option_value(value)
            if pv and _hidden_select_select_by_value(driver, sel_el, pv):
                _dispatch_primefaces_select_change(driver, sel_el)
                time.sleep(0.28)
                return True
    except Exception:
        pass
    return False


def _dispatch_primefaces_select_change(driver, sel_el) -> None:
    """
    PrimeFaces selectOneMenu wires each native <select id='{widget}_input'> with an inline
    onchange=\"PrimeFaces.ab({s,p,u,f})\" that posts partial submit and updates dependent fields.
    Synthetic DOM events do NOT run that handler — the top bar stays on All India unless we
    execute the same ab (or eval the attribute).
    """
    try:
        ran = driver.execute_script(
            """
            var s = arguments[0];
            var oc = s.getAttribute('onchange');
            if (oc && oc.length) {
              try { (0, eval)(oc); return true; } catch (e) {}
            }
            return false;
            """,
            sel_el,
        )
        if ran:
            return
    except Exception:
        pass
    try:
        sel_id = (sel_el.get_attribute("id") or "").strip()
        if sel_id.endswith("_input"):
            wid = sel_id[: -len("_input")]
            u = PRIMEFACES_MENU_UPDATES.get(wid)
            if u is None and wid == SELECTORS.get("type_dropdown"):
                u = wid
            elif u is None and wid == SELECTORS.get("state_dropdown"):
                u = "selectedRto yaxisVar"
            elif u is None and wid == SELECTORS.get("rta_dropdown"):
                u = "yaxisVar"
            elif u is None and wid == SELECTORS.get("y_axis_dropdown"):
                u = "xaxisVar"
            elif u is None and wid == SELECTORS.get("x_axis_dropdown"):
                u = "multipleYear"
            elif u is None and wid == SELECTORS.get("year_type_dropdown"):
                u = "selectedYear"
            elif u is None and wid == SELECTORS.get("year_dropdown"):
                u = "selectedYear"
            if (not u) and wid:
                u = _primefaces_u_from_onchange(driver, sel_el) or u
            if wid and u and isinstance(u, str):
                try:
                    live_form_id = driver.execute_script(
                        "var f = document.querySelector('form[id]'); "
                        "return f ? f.id : arguments[0];",
                        MAIN_REPORT_FORM_ID,
                    ) or MAIN_REPORT_FORM_ID
                except Exception:
                    live_form_id = MAIN_REPORT_FORM_ID
                driver.execute_script(
                    """
                    var w=arguments[0], u=arguments[1], f=arguments[2];
                    if (typeof PrimeFaces !== 'undefined' && PrimeFaces.ab) {
                      PrimeFaces.ab({s:w, e:'change', f:f, p:w, u:u});
                    }
                    """,
                    wid,
                    u,
                    live_form_id,
                )
                return
    except Exception:
        pass
    try:
        driver.execute_script(
            "var e=arguments[0];"
            "e.dispatchEvent(new Event('input',{bubbles:true}));"
            "e.dispatchEvent(new Event('change',{bubbles:true}));"
            "if(typeof jQuery!=='undefined'&&jQuery(e).trigger){jQuery(e).trigger('change');}",
            sel_el,
        )
    except Exception:
        try:
            driver.execute_script(
                "arguments[0].dispatchEvent(new Event('change', { bubbles: true }));",
                sel_el,
            )
        except Exception:
            pass


def _try_select_via_primefaces_hidden_input(driver, selector_key: str, value: str) -> bool:
    """Try stable *_input first, then config j_idt* only when fingerprint matches (avoids wrong widget)."""
    tried: list[str] = []
    st = _STABLE_MENU_WIDGET_IDS.get(selector_key)
    if st:
        tried.append(st)
    wcfg = SELECTORS.get(selector_key)
    if wcfg and wcfg not in tried:
        tried.append(wcfg)
    for wid in tried:
        try:
            el = driver.find_element(By.ID, f"{wid}_input")
            if el.tag_name.lower() != "select":
                continue
            if wid != st and not _verify_hidden_select_for_key(el, selector_key):
                continue
            if _select_hidden_select_by_text(driver, el, value, selector_key):
                return True
        except Exception:
            continue
    return False


def _try_select_via_hidden_select(driver, selector_key: str, value: str) -> bool:
    """
    Use discovered/cached hidden <select> for type/state; always re-resolve Y/X/Year/RTO/year-type
    so stable ids and fingerprints win over a partial discovery map.
    """
    cache: dict | None = None
    sel_el = None
    if selector_key in _TOOLBAR_HIDDEN_ALWAYS_RESOLVE:
        sel_el = _resolve_hidden_select_for_key(driver, selector_key)
        if sel_el is not None:
            cache = getattr(driver, "_vahan_hidden_selects", None)
            if isinstance(cache, dict):
                cache[selector_key] = sel_el
                setattr(driver, "_vahan_hidden_selects", cache)
    else:
        cache = _ensure_main_hidden_selects(driver)
        sel_el = cache.get(selector_key)
        if sel_el is not None:
            try:
                if not _is_stable_semantic_hidden_select(
                    selector_key, sel_el
                ) and not _verify_hidden_select_for_key(sel_el, selector_key):
                    sel_el = None
            except Exception:
                sel_el = None
        if sel_el is None:
            sel_el = _resolve_hidden_select_for_key(driver, selector_key)
            if sel_el is not None and isinstance(cache, dict):
                cache[selector_key] = sel_el
                setattr(driver, "_vahan_hidden_selects", cache)
    if sel_el is None:
        _active_log().warning(
            "hidden <select> not resolved for %s — will try menu / PF fallback",
            selector_key,
        )
        return False
    try:
        sel_el.is_displayed()
    except Exception:
        invalidate_main_hidden_select_cache(driver)
        invalidate_sidebar_discovery_cache(driver)
        sel_el = _resolve_hidden_select_for_key(driver, selector_key)
        if sel_el is None:
            return False
        cache = _ensure_main_hidden_selects(driver)
        if isinstance(cache, dict):
            cache[selector_key] = sel_el
    return _select_hidden_select_by_text(driver, sel_el, value, selector_key)


def discover_sidebar_checkboxes(driver) -> dict[str, list[str]]:
    """
    Once sidebar is open: collect checkbox input ids under VhCatg / VhClass / fuel tables.
    Used to scope label[for=ID] clicks to real DOM ids only.
    """
    out: dict[str, list[str]] = {}
    for container_id in ("VhCatg", "VhClass", "fuel"):
        try:
            root = driver.find_element(By.ID, container_id)
            inputs = root.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
            ids = []
            for inp in inputs:
                iid = inp.get_attribute("id")
                if iid:
                    ids.append(iid)
            out[container_id] = ids
        except Exception:
            out[container_id] = []
    return out


def _click_main_refresh(driver) -> bool:
    """Main form Refresh: config → known portal ids (drift) → XPath."""
    for iid in (
        SELECTORS.get("refresh_btn_main"),
        "j_idt67",
        "j_idt70",
        "j_idt68",
    ):
        if not iid:
            continue
        try:
            btn = driver.find_element(By.ID, iid)
            if _safe_click(driver, btn):
                return True
            try:
                driver.execute_script("arguments[0].click();", btn)
                return True
            except Exception:
                pass
        except Exception:
            continue
    for xp in (
        "//form[@id='masterLayout_formlogin']//*[contains(normalize-space(.),'Refresh') and (self::button or self::a or self::span)]",
        "//form[contains(@action,'reportview')]//*[contains(normalize-space(.),'Refresh') and (self::button or self::a or self::span)]",
    ):
        try:
            for el in driver.find_elements(By.XPATH, xp):
                try:
                    if not el.is_displayed():
                        continue
                except Exception:
                    continue
                if _safe_click(driver, el):
                    return True
                try:
                    driver.execute_script("arguments[0].click();", el)
                    return True
                except Exception:
                    pass
        except Exception:
            continue
    return False


def _click_sidebar_refresh(driver) -> bool:
    """Sidebar Refresh: config id → recent portal ids → Refresh inside #filterLayout."""
    for iid in (SELECTORS.get("refresh_btn_sidebar"), "j_idt79", "j_idt84"):
        if not iid:
            continue
        try:
            btn = driver.find_element(By.ID, iid)
            if _safe_click(driver, btn):
                return True
            driver.execute_script("arguments[0].click();", btn)
            return True
        except Exception:
            continue
    try:
        fl = driver.find_element(By.ID, "filterLayout")
        for el in fl.find_elements(
            By.XPATH,
            ".//*[contains(normalize-space(.),'Refresh') and (self::button or self::a or self::span)]",
        ):
            try:
                if not el.is_displayed():
                    continue
            except Exception:
                continue
            if _safe_click(driver, el):
                return True
            try:
                driver.execute_script("arguments[0].click();", el)
                return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def _find_dropdown_trigger(driver, selector_key: str):
    """Find dropdown trigger: stable widget ids FIRST (override-proof), then SELECTORS, then position/label XPaths."""
    if selector_key in _STABLE_MENU_WIDGET_IDS:
        try:
            return driver.find_element(
                By.ID, _STABLE_MENU_WIDGET_IDS[selector_key]
            )
        except Exception:
            pass
    sel = SELECTORS.get(selector_key)
    if sel and sel != _STABLE_MENU_WIDGET_IDS.get(selector_key):
        try:
            return driver.find_element(By.ID, sel)
        except Exception:
            pass
    # Fallback: main form dropdowns by order Type(0), State(1), RTO(2), Y-Axis(3), X-Axis(4), YearType(5), Year(6)
    idx_map = {
        "type_dropdown": 0,
        "state_dropdown": 1,
        "rta_dropdown": 2,
        "y_axis_dropdown": 3,
        "x_axis_dropdown": 4,
        "year_type_dropdown": 5,
        "year_dropdown": 6,
    }
    idx = idx_map.get(selector_key)
    if idx is not None:
        for form_sel in (
            "form#masterLayout_formlogin div.ui-selectonemenu",
            "form[action*='reportview'] div.ui-selectonemenu",
            "form div.ui-selectonemenu",
        ):
            try:
                menus = driver.find_elements(By.CSS_SELECTOR, form_sel)
                if idx < len(menus):
                    return menus[idx]
            except Exception:
                pass
    # Type dropdown: JSF ids (j_idt27) change on deploy — find by label or option signature
    if selector_key == "type_dropdown":
        for xp in (
            "//form[@id='masterLayout_formlogin']//label[contains(., 'Type:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[contains(@action,'reportview')]//label[contains(., 'Type:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//label[contains(., 'Type:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
        ):
            try:
                el = driver.find_element(By.XPATH, xp)
                if el:
                    return el
            except Exception:
                pass
    if selector_key == "state_dropdown":
        for xp in (
            "//form[@id='masterLayout_formlogin']//label[contains(., 'State:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[contains(@action,'reportview')]//label[contains(., 'State:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//label[contains(., 'State:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
        ):
            try:
                el = driver.find_element(By.XPATH, xp)
                if el:
                    return el
            except Exception:
                pass
    if selector_key == "rta_dropdown":
        for xp in (
            "//form[@id='masterLayout_formlogin']//label[contains(., 'RTO:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[contains(@action,'reportview')]//label[contains(., 'RTO:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//label[contains(., 'RTO:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
        ):
            try:
                el = driver.find_element(By.XPATH, xp)
                if el:
                    return el
            except Exception:
                pass
    if selector_key == "y_axis_dropdown":
        for xp in (
            "//form[@id='masterLayout_formlogin']//label[contains(., 'Y-Axis:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[@id='masterLayout_formlogin']//label[contains(., 'Y Axis')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[contains(@action,'reportview')]//label[contains(., 'Y-Axis:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//label[contains(., 'Y-Axis:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
        ):
            try:
                el = driver.find_element(By.XPATH, xp)
                if el:
                    return el
            except Exception:
                pass
    if selector_key == "x_axis_dropdown":
        for xp in (
            "//form[@id='masterLayout_formlogin']//label[contains(., 'X-Axis:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[@id='masterLayout_formlogin']//label[contains(., 'X Axis')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[contains(@action,'reportview')]//label[contains(., 'X-Axis:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//label[contains(., 'X-Axis:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
        ):
            try:
                el = driver.find_element(By.XPATH, xp)
                if el:
                    return el
            except Exception:
                pass
    if selector_key == "year_type_dropdown":
        for xp in (
            "//form[@id='masterLayout_formlogin']//label[contains(., 'Year Type:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[@id='masterLayout_formlogin']//label[contains(., 'Year Type')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[contains(@action,'reportview')]//label[contains(., 'Year Type')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//label[contains(., 'Year Type')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
        ):
            try:
                el = driver.find_element(By.XPATH, xp)
                if el:
                    return el
            except Exception:
                pass
    if selector_key == "year_dropdown":
        for xp in (
            "//form[@id='masterLayout_formlogin']//label[contains(., 'Year:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[@id='masterLayout_formlogin']//label[contains(., 'Year :')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//form[contains(@action,'reportview')]//label[contains(., 'Year:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
            "//label[contains(., 'Year:')]/following-sibling::div[contains(@class,'ui-selectonemenu')]",
        ):
            try:
                el = driver.find_element(By.XPATH, xp)
                if el:
                    return el
            except Exception:
                pass
    return None


def _set_type_filter(driver, value: str) -> bool:
    """Set Type dropdown via hidden <select> fingerprint (shared with all top dropdowns) or UI trigger."""
    ok = False
    if _try_select_via_primefaces_hidden_input(driver, "type_dropdown", value):
        ok = True
    elif _try_select_via_hidden_select(driver, "type_dropdown", value):
        ok = True
    else:
        ok = _select_by_text(driver, "type_dropdown", value)
    if ok:
        time.sleep(0.3)
        invalidate_main_hidden_select_cache(driver)
    return ok


def _read_state_combobox_label(driver) -> str:
    """Prefer widget id from discovered state <select> so verification works when config j_idt* drifts."""
    for _ in range(2):
        try:
            cache = _ensure_main_hidden_selects(driver)
            sel_el = cache.get("state_dropdown")
            if sel_el is not None:
                iid = (sel_el.get_attribute("id") or "").strip()
                if iid.endswith("_input"):
                    lid = f"{iid[: -len('_input')]}_label"
                    el = driver.find_element(By.ID, lid)
                    return (el.text or "").replace("\xa0", " ").strip()
        except StaleElementReferenceException:
            invalidate_main_hidden_select_cache(driver)
            continue
        except Exception:
            pass
        break
    wid = SELECTORS.get("state_dropdown")
    if not wid:
        return ""
    try:
        el = driver.find_element(By.ID, f"{wid}_label")
        return (el.text or "").replace("\xa0", " ").strip()
    except Exception:
        return ""


def _read_state_hidden_selected_text(driver) -> str:
    for _ in range(2):
        try:
            cache = _ensure_main_hidden_selects(driver)
            sel_el = cache.get("state_dropdown")
            if sel_el is not None:
                return _hidden_select_selected_text(driver, sel_el)
        except StaleElementReferenceException:
            invalidate_main_hidden_select_cache(driver)
            continue
        except Exception:
            pass
        break
    wid = SELECTORS.get("state_dropdown")
    if not wid:
        return ""
    try:
        sel_el = driver.find_element(By.ID, f"{wid}_input")
        return _hidden_select_selected_text(driver, sel_el)
    except Exception:
        return ""


def _state_selection_verified(driver, state: str) -> bool:
    """True if hidden <select> and/or visible label reflect ``state`` (not still all-India)."""
    ht = _read_state_hidden_selected_text(driver)
    lt = _read_state_combobox_label(driver)
    h_ok = _state_option_matches(state, ht)
    l_ok = _state_option_matches(state, lt)
    if not h_ok and not l_ok:
        return False
    if _state_aggregate_label_text(ht) and _state_aggregate_label_text(lt):
        return False
    return True


def _poll_until_state_verified(driver, state: str, timeout: float | None = None) -> bool:
    """PrimeFaces state change can lag; poll until hidden/label match or timeout."""
    hi = float(os.environ.get("VAHAN_STATE_VERIFY_MAX_SEC", "45"))
    if timeout is not None:
        t = max(0.35, min(float(timeout), hi))
    else:
        t = max(
            0.35,
            min(float(os.environ.get("VAHAN_STATE_VERIFY_SEC", "15")), hi),
        )
    deadline = time.time() + t
    step = min(0.28, max(0.08, min(WAIT_CAP_SEC / 22.0, 0.25)))
    while time.time() < deadline:
        if _state_selection_verified(driver, state):
            return True
        time.sleep(step)
    return _state_selection_verified(driver, state)


def _state_dropdown_widget_id_verified(driver, wid: str) -> bool:
    """True only if the hidden State <select> lives at {wid}_input and fingerprint matches."""
    if not wid:
        return False
    try:
        inp = driver.find_element(By.ID, f"{wid}_input")
        if inp.tag_name.lower() != "select":
            return False
        return _match_state_hidden(inp)
    except Exception:
        return False


def _scan_first_state_hidden_select(driver):
    """Find State <select> by content fingerprint (JSF widget ids drift; div#j_idt36 may be reused elsewhere)."""
    form = _master_form(driver)
    if not form:
        return None
    try:
        for s in form.find_elements(
            By.CSS_SELECTOR, "div.ui-helper-hidden-accessible select"
        ):
            try:
                if _match_state_hidden(s):
                    return s
            except Exception:
                continue
    except Exception:
        pass
    return None


def _state_dropdown_trigger_id(driver) -> str | None:
    """Widget id for State selectOneMenu — always derived from the real hidden <select>, not a stale div id."""
    try:
        cache = _ensure_main_hidden_selects(driver)
        sel = cache.get("state_dropdown")
        if sel is not None:
            try:
                if _match_state_hidden(sel):
                    iid = (sel.get_attribute("id") or "").strip()
                    if iid.endswith("_input"):
                        return iid[: -len("_input")]
            except Exception:
                pass
    except Exception:
        pass

    scanned = _scan_first_state_hidden_select(driver)
    if scanned is not None:
        iid = (scanned.get_attribute("id") or "").strip()
        if iid.endswith("_input"):
            wid = iid[: -len("_input")]
            try:
                c = getattr(driver, "_vahan_hidden_selects", None)
                if isinstance(c, dict):
                    c["state_dropdown"] = scanned
            except Exception:
                pass
            return wid

    w = SELECTORS.get("state_dropdown")
    if w and _state_dropdown_widget_id_verified(driver, w):
        return w
    return None


def _select_state_via_primefaces_menu(driver, state: str) -> bool:
    """Open State selectOneMenu and click the matching row (user path)."""
    wid = _state_dropdown_trigger_id(driver)
    if not wid:
        return False
    try:
        trig = driver.find_element(By.ID, wid)
    except Exception:
        return False
    if not _safe_click(driver, trig):
        try:
            driver.execute_script("arguments[0].click();", trig)
        except Exception:
            return False
    time.sleep(0.28)
    items_id = f"{wid}_items"
    try:
        items = WebDriverWait(driver, DEFAULT_WAIT).until(
            EC.visibility_of_element_located((By.ID, items_id))
        )
    except Exception:
        return False
    opts = items.find_elements(By.CSS_SELECTOR, "li[data-label], li.ui-selectonemenu-item")
    for opt in opts:
        label = (opt.get_attribute("data-label") or opt.text or "").replace("\xa0", " ").strip()
        if not label:
            continue
        if not _state_option_matches(state, label):
            continue
        if not _safe_click(driver, opt):
            try:
                opt.click()
            except Exception:
                return False
        time.sleep(0.3)
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        except Exception:
            pass
        _wait_primefaces_quiet(driver)
        return True
    return False


def _apply_state_for_scrape(driver, state: str) -> bool:
    """
    Select a specific state/UT: try native/hidden path, verify; if still aggregate or
    mismatch, use visible PrimeFaces menu, then a final native attempt.
    """
    pv = state_portal_option_value(state)

    def _verify_after_select() -> bool:
        # Do not invalidate the hidden-select cache *before* polling: rediscovery can
        # re-assign the wrong <select> to state_dropdown and make readback fail.
        ok = _poll_until_state_verified(driver, state)
        invalidate_main_hidden_select_cache(driver)
        return ok

    # Portal hidden <option value="MH"> is stable; try before visible-text/menu paths.
    # Use JS + selectedIndex — Selenium Select() fails with "element not interactable" on hidden PF selects.
    if pv:
        wid = _state_dropdown_trigger_id(driver)
        if wid:
            try:
                sel_el = driver.find_element(By.ID, f"{wid}_input")
                if sel_el.tag_name.lower() == "select":
                    if _hidden_select_select_by_value(driver, sel_el, pv):
                        _dispatch_primefaces_select_change(driver, sel_el)
                        time.sleep(0.28)
                        _wait_primefaces_quiet(driver)
                        if _verify_after_select():
                            return True
            except Exception as e:
                _active_log().debug("state set value %s wid=%s: %s", pv, wid, e)

    if _select_by_text(driver, "state_dropdown", state):
        time.sleep(0.28)
        _wait_primefaces_quiet(driver)
        if _verify_after_select():
            return True

    if _select_state_via_primefaces_menu(driver, state):
        time.sleep(0.35)
        _wait_primefaces_quiet(driver)
        if _verify_after_select():
            return True

    if _try_select_via_primefaces_hidden_input(driver, "state_dropdown", state):
        time.sleep(0.28)
        _wait_primefaces_quiet(driver)
        if _verify_after_select():
            return True

    _active_log().warning(
        "Could not apply state %r; readback hidden=%r label=%r",
        state,
        _read_state_hidden_selected_text(driver),
        _read_state_combobox_label(driver),
    )
    return False


def _read_portal_main_filter_labels(driver) -> dict[str, str]:
    """Read selected option text from discovered hidden <select>s, else stable *_input ids."""
    mapping = (
        ("type", "type_dropdown"),
        ("state", "state_dropdown"),
        ("rta", "rta_dropdown"),
        ("y_axis", "y_axis_dropdown"),
        ("x_axis", "x_axis_dropdown"),
        ("year_type", "year_type_dropdown"),
        ("year", "year_dropdown"),
    )
    out: dict[str, str] = {}
    cache = _ensure_main_hidden_selects(driver)
    for name, sk in mapping:
        sel_el = cache.get(sk)
        if sel_el is not None:
            try:
                t = _hidden_select_selected_text(driver, sel_el)
                if t:
                    out[name] = t
                    continue
            except Exception:
                pass
        wid = SELECTORS.get(sk)
        if not wid:
            continue
        sel_id = f"{wid}_input"
        try:
            txt = driver.execute_script(
                "var s=document.getElementById(arguments[0]);"
                "if(!s||s.selectedIndex<0)return'';"
                "var o=s.options[s.selectedIndex];"
                "return o ? (o.textContent||'').trim() : '';",
                sel_id,
            )
            if isinstance(txt, str) and txt.strip():
                out[name] = txt.strip()
        except Exception:
            pass
    return out


def _select_by_text(driver, selector_key: str, value: str, by=By.ID) -> bool:
    """Select PrimeFaces dropdown; retries on stale DOM after AJAX."""
    for attempt in range(SELENIUM_ACTION_RETRIES):
        try:
            if _select_by_text_once(driver, selector_key, value, by):
                return True
        except StaleElementReferenceException:
            pass
        time.sleep(0.35 + 0.12 * attempt)
    return False


def _menu_option_matches_selector(selector_key: str, value: str, label: str) -> bool:
    """Whether a PrimeFaces panel row matches the desired value for this dropdown."""
    label = (label or "").replace("\xa0", " ").strip()
    value = (value or "").strip()
    if not label or not value:
        return False
    if selector_key == "state_dropdown":
        return _state_option_matches(value, label)
    if selector_key == "year_dropdown" and value.isdigit() and len(value) == 4:
        return label.startswith(value) or label == value or value in label.split()
    lc = label.casefold()
    vc = value.casefold()
    return bool(
        value == label or vc == lc or vc in lc or lc.startswith(vc) or value in label
    )


def _select_via_primefaces_menu_panel(driver, selector_key: str, value: str) -> bool:
    """Open the visible selectOneMenu and pick an option (works when hidden <select> path is flaky)."""
    trigger = _find_dropdown_trigger(driver, selector_key)
    if not trigger:
        _active_log().debug("menu panel: no trigger for %s", selector_key)
        return False
    tid = (trigger.get_attribute("id") or "").strip()
    if tid:
        try:
            WebDriverWait(
                driver, _wait_s(min(14.0, float(DEFAULT_WAIT) * 2))
            ).until(EC.element_to_be_clickable((By.ID, tid)))
            trigger = driver.find_element(By.ID, tid)
        except Exception:
            pass
    try:
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center',inline:'nearest'});", trigger
        )
        time.sleep(0.06)
    except Exception:
        pass
    if not _safe_click(driver, trigger):
        try:
            driver.execute_script("arguments[0].click();", trigger)
        except Exception:
            return False
    time.sleep(0.28)
    trigger_id = (trigger.get_attribute("id") or "").strip()
    items_id = f"{trigger_id}_items" if trigger_id else None
    items = None
    if items_id:
        try:
            items = WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.visibility_of_element_located((By.ID, items_id))
            )
        except Exception:
            pass
    if not items:
        try:
            items = driver.find_element(
                By.CSS_SELECTOR, "div.ui-selectonemenu-panel:not(.ui-helper-hidden) ul.ui-selectonemenu-items"
            )
        except Exception:
            try:
                items = driver.find_element(
                    By.CSS_SELECTOR,
                    "ul.ui-selectonemenu-items:not([style*='display: none'])",
                )
            except Exception:
                _active_log().debug("menu panel: items list not found for %s", selector_key)
                return False
    opts = items.find_elements(By.CSS_SELECTOR, "li[data-label], li.ui-selectonemenu-item")
    for opt in opts:
        lab = (opt.get_attribute("data-label") or opt.text or "").replace(
            "\xa0", " "
        ).strip()
        if not _menu_option_matches_selector(selector_key, value, lab):
            continue
        if not _safe_click(driver, opt):
            try:
                opt.click()
            except Exception:
                continue
        time.sleep(0.18)
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        except Exception:
            pass
        _wait_primefaces_quiet(driver)
        return True
    if trigger_id:
        try:
            select_elem = driver.find_element(By.ID, f"{trigger_id}_input")
            for _ov, ot, idx in _hidden_select_list_options(driver, select_elem):
                if not _menu_option_matches_selector(selector_key, value, ot):
                    continue
                if not _hidden_select_set_index(driver, select_elem, idx):
                    continue
                _dispatch_primefaces_select_change(driver, select_elem)
                time.sleep(0.18)
                _wait_primefaces_quiet(driver)
                return True
        except StaleElementReferenceException:
            raise
        except Exception:
            pass
    return False


def _select_by_text_once(driver, selector_key: str, value: str, by=By.ID) -> bool:
    """Hidden native select (stable id first) → visible menu → config id fallback."""
    _active_log().info("Top bar select: %s -> %r", selector_key, value)
    if _try_select_via_hidden_select(driver, selector_key, value):
        return True
    if _select_via_primefaces_menu_panel(driver, selector_key, value):
        return True
    if _try_select_via_primefaces_hidden_input(driver, selector_key, value):
        return True
    return False


def _select_by_xpath(driver, xpath: str, value: str) -> bool:
    """Select option via XPath (for JSF custom components)."""
    try:
        # Try clicking dropdown to open, then click option
        opts = driver.find_elements(By.XPATH, f"//*[contains(text(), '{value}')]")
        for o in opts:
            if o.is_displayed():
                o.click()
                time.sleep(0.3)
                return True
        return False
    except Exception:
        return False


def _set_checkboxes(driver, checkbox_ids: list, wait_timeout: float | None = None, pause_s: float = 0.07) -> bool:
    """Select checkboxes by ID (page-wide). Prefer _set_checkboxes_scoped for sidebar tables after layout moves."""
    if not checkbox_ids:
        return True
    wt = int(_wait_s(float(wait_timeout if wait_timeout is not None else SIDEBAR_WAIT)))
    for cid in checkbox_ids:
        toggled = False
        for attempt in range(SELENIUM_ACTION_RETRIES):
            try:
                cb = WebDriverWait(driver, wt).until(
                    EC.presence_of_element_located((By.ID, cid))
                )
                time.sleep(0.04)
                if cb.is_selected():
                    toggled = True
                    break
                if _safe_click(driver, cb):
                    toggled = True
                    break
                try:
                    label = driver.find_element(By.CSS_SELECTOR, f"label[for='{cid}']")
                    if _safe_click(driver, label):
                        toggled = True
                        break
                except StaleElementReferenceException:
                    pass
            except StaleElementReferenceException:
                time.sleep(0.12 + 0.06 * attempt)
            except Exception:
                try:
                    label = driver.find_element(By.CSS_SELECTOR, f"label[for='{cid}']")
                    if _safe_click(driver, label):
                        toggled = True
                        break
                except Exception:
                    time.sleep(0.08)
        if not toggled:
            try:
                label = driver.find_element(By.CSS_SELECTOR, f"label[for='{cid}']")
                _safe_click(driver, label)
            except Exception:
                pass
        time.sleep(pause_s)
    return True


def _set_checkboxes_scoped(
    driver,
    container_id: str,
    checkbox_ids: list[str],
    *,
    wait_timeout: float | None = None,
    pause_s: float = 0.05,
) -> bool:
    """
    Toggle checkboxes that live under a single table/panel (e.g. #VhCatg).
    Uses label[for=…] inside the container so clicks stay correct after sidebar expand/scroll.
    If ``discover_sidebar_checkboxes`` ran, only ids present in the DOM are used.
    """
    if not checkbox_ids:
        return True
    discovered = getattr(driver, "_vahan_sidebar_checkbox_ids", None) or {}
    allowed = discovered.get(container_id) if isinstance(discovered, dict) else None
    if allowed:
        allow_set = set(allowed)
        filt = [c for c in checkbox_ids if c in allow_set]
        if filt:
            checkbox_ids = filt
        elif allow_set:
            _active_log().debug(
                "scoped checkboxes: no overlap with discovery for #%s; using configured ids",
                container_id,
            )
    wt = int(_wait_s(float(wait_timeout if wait_timeout is not None else SIDEBAR_WAIT)))
    try:
        WebDriverWait(driver, wt).until(
            EC.visibility_of_element_located((By.ID, container_id))
        )
    except Exception:
        _active_log().warning("Checkbox container #%s not visible", container_id)
        return False
    try:
        driver.execute_script(
            "var p=document.getElementById(arguments[0]); if(p){p.scrollTop=0; p.scrollIntoView({block:'start'});}",
            container_id,
        )
    except Exception:
        pass
    time.sleep(0.06)

    for cid in checkbox_ids:
        for attempt in range(3):
            try:
                container = driver.find_element(By.ID, container_id)
                inp = container.find_element(By.ID, cid)
                if inp.is_selected():
                    break
                try:
                    lab = container.find_element(By.CSS_SELECTOR, f"label[for='{cid}']")
                except Exception:
                    lab = None
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'nearest',inline:'nearest'});",
                    lab or inp,
                )
                time.sleep(0.03)
                if lab and _safe_click(driver, lab):
                    pass
                elif not _safe_click(driver, inp):
                    try:
                        driver.execute_script("arguments[0].click();", lab or inp)
                    except Exception:
                        pass
                time.sleep(pause_s)
                inp = driver.find_element(By.ID, cid)
                if inp.is_selected():
                    break
            except StaleElementReferenceException:
                time.sleep(0.08)
            except Exception as e:
                _active_log().debug("scoped checkbox %s: %s", cid, e)
                time.sleep(0.06)
        time.sleep(pause_s)
    return True


def _norm_vehicle_category_label(s: str) -> str:
    return " ".join((s or "").split())


def _set_vehicle_categories_by_target_labels(
    driver, targets: list[str] | list[tuple[str, ...]] | list
) -> None:
    """
    Select Vehicle Category rows by normalized label text inside #VhCatg only.
    Robust when the portal inserts rows (VhCatg:N indices shift).
    Each target may be a str or a tuple of alternate labels (see VEHICLE_CATEGORY_TARGET_LABELS).
    """
    if not targets:
        return
    WebDriverWait(driver, DEFAULT_WAIT).until(EC.visibility_of_element_located((By.ID, "VhCatg")))
    try:
        fl = driver.find_element(By.ID, "filterLayout")
        driver.execute_script("arguments[0].scrollTop = 0;", fl)
    except Exception:
        pass
    time.sleep(0.08)

    for want in targets:
        candidates: tuple[str, ...] = (want,) if isinstance(want, str) else tuple(want)
        ok = False
        for want_label in candidates:
            want_n = _norm_vehicle_category_label(want_label)
            for attempt in range(4):
                try:
                    table = driver.find_element(By.ID, "VhCatg")
                    for lab in table.find_elements(By.CSS_SELECTOR, "label[for^='VhCatg:']"):
                        try:
                            if _norm_vehicle_category_label(lab.text) != want_n:
                                continue
                            fid = lab.get_attribute("for")
                            if not fid:
                                continue
                            inp = driver.find_element(By.ID, fid)
                            driver.execute_script(
                                "arguments[0].scrollIntoView({block:'nearest',inline:'nearest'});",
                                lab,
                            )
                            time.sleep(0.04)
                            if inp.is_selected():
                                ok = True
                                break
                            if not _safe_click(driver, lab):
                                driver.execute_script("arguments[0].click();", lab)
                            time.sleep(0.07)
                            if driver.find_element(By.ID, fid).is_selected():
                                ok = True
                                break
                        except StaleElementReferenceException:
                            break
                        except Exception:
                            continue
                    if ok:
                        break
                except StaleElementReferenceException:
                    time.sleep(0.1)
                except Exception as e:
                    _active_log().debug("vehicle category %r: %s", want_label, e)
                    time.sleep(0.08)
            if ok:
                break
        if not ok:
            _active_log().warning(
                "Could not select Vehicle Category row (tried %r)", candidates
            )
        time.sleep(0.05)


def _safe_xpath_text(t: str) -> str:
    if "'" not in t:
        return f"'{t}'"
    if '"' not in t:
        return f'"{t}"'
    parts = t.split("'")
    return "concat('" + "', \"'\", '".join(parts) + "')"


def _set_vehicle_class_by_label(driver, labels: list[str]) -> bool:
    """Select Vehicle Class checkboxes by label text — only under #VhClass (avoids wrong section)."""
    for text in labels:
        try:
            table = WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.presence_of_element_located((By.ID, "VhClass"))
            )
        except Exception:
            continue
        try:
            for el in table.find_elements(
                By.XPATH,
                f".//label[contains(normalize-space(.), {_safe_xpath_text(text)})]",
            ):
                try:
                    if el.is_displayed():
                        driver.execute_script(
                            "arguments[0].scrollIntoView({block:'nearest'});", el
                        )
                        time.sleep(0.04)
                        if not _safe_click(driver, el):
                            el.click()
                        time.sleep(0.06)
                        break
                except Exception:
                    continue
        except Exception:
            continue
    return True


def _click_by_text(driver, text: str, partial: bool = True) -> bool:
    """Click element containing text; retries stale / intercept."""
    xpath = f"//*[contains(text(), '{text}')]" if partial else f"//*[text()='{text}']"
    for attempt in range(SELENIUM_ACTION_RETRIES):
        try:
            elem = WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            if _safe_click(driver, elem):
                time.sleep(0.2)
                return True
        except StaleElementReferenceException:
            pass
        except Exception:
            pass
        time.sleep(0.35 + 0.1 * attempt)
    return False


def _wait_for_download(
    download_dir: Path,
    timeout: int = DOWNLOAD_WAIT,
    *,
    initial_paths: set | None = None,
) -> Path | None:
    """Wait for Chrome download to finish (.crdownload-aware)."""
    tw = _download_wait_sec(timeout)
    got = wait_for_chrome_download(
        download_dir,
        timeout=tw,
        poll_s=0.14,
        initial_paths=initial_paths,
    )
    if got:
        return got
    _active_log().debug("wait_for_chrome_download returned None, trying legacy snapshot diff")
    if initial_paths is not None:
        initial = {Path(p).resolve() for p in initial_paths}
    else:
        initial = {p.resolve() for p in download_dir.iterdir()} if download_dir.exists() else set()
    deadline = time.time() + tw
    while time.time() < deadline:
        time.sleep(0.14)
        current = set(download_dir.iterdir()) if download_dir.exists() else set()
        for f in current:
            if f.resolve() in initial:
                continue
            if ".crdownload" in f.name or ".tmp" in f.name:
                continue
            suf = f.suffix.lower()
            if suf in (".csv", ".xlsx", ".xls") or (suf == "" and f.name != "downloads"):
                return f
    return None


def _wait_for_loading_finish(driver, timeout: int | None = None) -> None:
    """Wait for PrimeFaces to finish loading so Excel icon is clickable."""
    w = _wait_s(float(timeout) if timeout is not None else DEFAULT_WAIT)
    excel_ids = []
    for x in (SELECTORS.get("download_btn"), "vchgroupTable:xls", "groupingTable:xls"):
        if x and x not in excel_ids:
            excel_ids.append(x)

    excel_conds = tuple(
        EC.element_to_be_clickable((By.ID, eid)) for eid in excel_ids
    )
    for t in (w, w):
        try:
            if excel_conds:
                WebDriverWait(driver, t).until(EC.any_of(*excel_conds))
            return
        except Exception:
            time.sleep(min(0.2, max(0.08, w * 0.04)))
    time.sleep(0.1)


def _click_excel_icon(driver) -> bool:
    """Click the Excel download icon. Tries multiple selectors; second pass after short wait."""
    dl_cfg = SELECTORS.get("download_btn")
    id_order = []
    for x in (dl_cfg, "vchgroupTable:xls", "groupingTable:xls"):
        if x and x not in id_order:
            id_order.append(x)
    selectors = [(By.ID, eid) for eid in id_order] + [
        (By.CSS_SELECTOR, "img[title='Download EXCEL file']"),
        (By.CSS_SELECTOR, "a[id*='xls']"),
        (By.XPATH, "//img[contains(@title, 'EXCEL')]"),
        (By.XPATH, "//a[contains(@id, 'xls')]"),
    ]
    for round_i in range(2):
        for by, value in selectors:
            try:
                dl = driver.find_element(by, value)
                if _safe_click(driver, dl):
                    return True
                time.sleep(0.15)
            except Exception:
                continue
        for eid in id_order:
            try:
                dl = driver.find_element(By.ID, eid)
                driver.execute_script("arguments[0].click();", dl)
                return True
            except Exception:
                continue
        if round_i == 0:
            time.sleep(0.35)
    return False


def _annotate_and_save(file_path: Path, fuel_label: str, output_path: Path) -> bool:
    """Add fuel column to every row and save."""
    try:
        suf = file_path.suffix.lower()
        if suf == ".csv":
            df = pd.read_csv(file_path, encoding="utf-8", on_bad_lines="skip")
        elif suf in (".xlsx", ".xls"):
            df = pd.read_excel(file_path, engine="openpyxl")
        else:
            return False
        df[FUEL_COLUMN] = fuel_label
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return True
    except Exception as e:
        print(f"  Error annotating {file_path}: {e}")
        return False


def _annotate_and_get_df(file_path: Path, fuel_label: str, output_path: Path) -> pd.DataFrame | None:
    """Annotate (add fuel_type) and return DataFrame."""
    if _annotate_and_save(file_path, fuel_label, output_path):
        try:
            return pd.read_csv(output_path, encoding="utf-8", on_bad_lines="skip")
        except Exception:
            return None
    return None


def _click_sidebar_filter_labels(driver, labels: list[str]) -> None:
    """
    Emergency fallback: click labels by substring inside #filterLayout.
    Do not combine with ID-based selection on the same checkboxes — each click toggles state.
    """
    if not labels:
        return
    try:
        fl = driver.find_element(By.ID, "filterLayout")
    except Exception:
        return
    for text in labels:
        if not text:
            continue
        esc = text.replace("'", "\\'")
        for xp in (
            f".//label[contains(normalize-space(.), '{esc}')]",
            f".//span[contains(normalize-space(.), '{esc}')]",
            f".//*[self::label or self::span][contains(., '{esc}')]",
        ):
            try:
                for el in fl.find_elements(By.XPATH, xp):
                    try:
                        if el.is_displayed() and _safe_click(driver, el):
                            time.sleep(0.14)
                            break
                    except Exception:
                        continue
            except Exception:
                continue


class VahanScraper:
    """Orchestrates the full Vahan scraping workflow."""

    PARALLEL_SAFE = True

    def __init__(self, output_base: Path, headless: bool = False):
        self.output_base = Path(output_base)
        self.headless = headless
        self.driver = None
        self.session_dir = None
        self.download_dir = None

    def _setup_session(self, state: str, year: int, window_layout_slot: int | None = None):
        """Create session and download directories (unique per parallel job: µs + slot)."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        safe_state = state.replace("/", "_").replace(":", "_")
        slot_part = (
            f"slot{window_layout_slot}_" if window_layout_slot is not None else ""
        )
        self.session_dir = (
            self.output_base / f"session_{safe_state}_{year}_{slot_part}{ts}"
        )
        self.download_dir = self.session_dir / "downloads"
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def run_state_year(
        self,
        state: str,
        year: int,
        rta: str | None = None,
        fuels: list[str] | None = None,
        refresh_to_download_wait: float | None = None,
        window_layout_slot: int | None = None,
        portal_filters: dict[str, str] | None = None,
    ) -> Path | None:
        """
        Run full scrape for one state and one year.
        portal_filters: optional keys y_axis, x_axis, year_type (visible labels); unset keys use BASE_FILTERS.
        Returns path to merged file, or None on failure.
        """
        self._setup_session(state, year, window_layout_slot)
        sess_log = _configure_scrape_logger(self.session_dir)
        ctx_token = _active_session_log.set(sess_log)
        try:
            if batch_stop_requested():
                print(f"  Skipped before browser (stop requested): {state} / {year}", flush=True)
                return None
            slot = (
                f" [parallel slot {window_layout_slot}]"
                if window_layout_slot is not None
                else ""
            )
            print(f"  Job:{slot} {state} / {year} - opening browser...", flush=True)
            _active_log().info(
                "Session %s / %s — log file: %s", state, year, self.session_dir / "scrape.log"
            )
            stagger = float(os.environ.get("VAHAN_PARALLEL_STAGGER_SEC", "2.5"))
            if window_layout_slot is not None and stagger > 0:
                delay_s = min(45.0, float(window_layout_slot) * stagger)
                if delay_s > 0:
                    time.sleep(delay_s)
            self.driver = create_driver(
                self.download_dir,
                headless=self.headless,
                window_layout_slot=window_layout_slot,
            )
            # Output merged file directly in vahan_data folder
            self.output_base.mkdir(parents=True, exist_ok=True)
            safe_state = state.replace("/", "_").replace(":", "_")
            merged_path = self.output_base / f"{safe_state}_{year}_merged.csv"
            all_dfs = []

            self._refresh_wait = (
                refresh_to_download_wait
                if refresh_to_download_wait is not None
                else REFRESH_TO_DOWNLOAD_WAIT
            )
            fuel_groups = (
                {k: v for k, v in FUEL_GROUP_CHECKBOX_IDS.items() if k in fuels}
                if fuels
                else FUEL_GROUP_CHECKBOX_IDS
            )

            try:
                # --- Step 1: Base filters (retry full page load; saves debug PNG/HTML on failure) ---
                base_ok = False
                for attempt in range(3):
                    try:
                        _load_report_page(self.driver)
                    except WebDriverException as nav_e:
                        print(f"  Navigation error: {nav_e}", flush=True)
                        if self.driver:
                            save_selenium_debug(
                                self.driver,
                                self.session_dir,
                                f"nav_fail_{attempt + 1}",
                            )
                        if attempt < 2:
                            time.sleep(min(WAIT_CAP_SEC, 1.5 * (attempt + 1)))
                            continue
                        raise
                    invalidate_main_hidden_select_cache(self.driver)
                    invalidate_sidebar_discovery_cache(self.driver)
                    WebDriverWait(self.driver, DEFAULT_WAIT).until(
                        EC.presence_of_element_located((By.TAG_NAME, "form"))
                    )
                    try:
                        WebDriverWait(
                            self.driver,
                            _long_wait_sec(
                                float(
                                    os.environ.get(
                                        "VAHAN_FORM_READY_WAIT_SEC",
                                        str(max(45.0, float(DEFAULT_WAIT) * 4)),
                                    )
                                )
                            ),
                        ).until(
                            EC.presence_of_element_located(
                                (By.ID, "masterLayout_formlogin")
                            )
                        )
                    except Exception:
                        pass
                    try:
                        WebDriverWait(
                            self.driver,
                            _long_wait_sec(
                                float(
                                    os.environ.get(
                                        "VAHAN_TOOLBAR_READY_WAIT_SEC",
                                        str(max(40.0, float(DEFAULT_WAIT) * 3)),
                                    )
                                )
                            ),
                        ).until(
                            EC.any_of(
                                EC.presence_of_element_located((By.ID, "yaxisVar_input")),
                                EC.presence_of_element_located(
                                    (By.CSS_SELECTOR, "div.ui-helper-hidden-accessible select")
                                ),
                            )
                        )
                    except Exception:
                        _active_log().warning(
                            "Toolbar hidden selects not ready in time — top bar may fail"
                        )
                    time.sleep(0.35)
                    _recover_view_expired(self.driver)
                    self.driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(0.15)

                    print(
                        "  Top bar: Type & Year type unchanged (defaults); State if not all-India; "
                        "Y-Axis, X-Axis, Year..."
                    )
                    if self._set_base_filters(state, year, rta, portal_filters=portal_filters):
                        base_ok = True
                        break
                    save_selenium_debug(
                        self.driver, self.session_dir, f"base_filters_fail_{attempt + 1}"
                    )
                    if attempt < 2:
                        print(
                            f"  Retrying base filters ({attempt + 2}/3) after full reload..."
                        )
                if not base_ok:
                    print("  Failed to set base filters - aborting")
                    return None
                time.sleep(REQUEST_DELAY)

                # --- Step 2: First main Refresh ---
                # Portal loads/refreshes west-pane filter markup (incl. Vehicle Category) on this submit;
                # #filterLayout stays collapsed until Step 3 — do not touch VhCatg before Refresh + expand.
                if not _click_main_refresh(self.driver):
                    print("  Failed to click main Refresh")
                    return None
                if not _wait_primefaces_quiet(self.driver):
                    _active_log().warning(
                        "Main refresh: overlay wait timed out before table ready"
                    )
                # Wait for AJAX (main Refresh updates VhCatg / fuel / VhClass in the DOM for the west pane)
                _wait_for_loading_finish(self.driver)
                # West-pane GIF / toggler often appears a moment after the data table is ready.
                time.sleep(0.12)

                # --- Step 3: Expand sidebar (red "click here" / clickhere.gif) ---
                if not _retry_op(
                    lambda: self._expand_sidebar(),
                    attempts=3,
                    delay=0.35,
                    label="expand_sidebar",
                ):
                    save_selenium_debug(self.driver, self.session_dir, "expand_sidebar_fail")
                    print("  Failed to expand sidebar")
                    return None
                # Wait for sidebar to be visible and content ready
                self._wait_for_sidebar_ready()
                time.sleep(0.06)

                # --- Step 4: Sidebar filters (continue even if partial fail) ---
                self._set_sidebar_filters()
                time.sleep(0.06)
                # Sidebar Refresh applies Vehicle Category / Class to the report before any fuel selection.
                if not _click_sidebar_refresh(self.driver):
                    _active_log().warning(
                        "Post–sidebar-filters: sidebar Refresh click failed (continuing)"
                    )
                if not _wait_primefaces_quiet(self.driver):
                    _active_log().warning(
                        "Post–sidebar-filters: overlay wait after sidebar refresh timed out"
                    )
                _wait_for_loading_finish(self.driver)
                time.sleep(self._refresh_wait)

                # --- Step 5: Fuel LOOP (annotate synchronously — avoids download-dir race with next fuel). ---
                all_dfs = []
                for idx, (fuel_label, fuel_ids) in enumerate(fuel_groups.items()):
                    if batch_stop_requested():
                        print(
                            "  Stop requested - closing after current state (partial merge if any data).",
                            flush=True,
                        )
                        break
                    is_first = idx == 0
                    print(
                        f"  {fuel_label}: "
                        f"{'fuel checkboxes -> sidebar Refresh -> Excel' if is_first else 'main Refresh -> sidebar -> filters -> sidebar Refresh -> fuel -> sidebar Refresh -> Excel'}"
                    )
                    try:
                        result = self._process_fuel_group(
                            fuel_label, fuel_ids, is_first_fuel=is_first
                        )
                        if not result:
                            print(f"    Skipped (no data)")
                            continue
                        downloaded_path, out_path = result
                        stable_src = downloaded_path
                        try:
                            safe_name = f"_src_{fuel_label.replace(' ', '_')}_{int(time.time() * 1000)}{downloaded_path.suffix}"
                            stable_src = self.download_dir / safe_name
                            shutil.move(str(downloaded_path), str(stable_src))
                        except Exception as copy_e:
                            _active_log().debug(
                                "Could not copy download to stable name: %s", copy_e
                            )
                        df = _annotate_and_get_df(stable_src, fuel_label, out_path)
                        if df is not None and not df.empty:
                            all_dfs.append(df)
                            print(f"    Download OK, {fuel_label}: {len(df)} rows")
                        else:
                            _active_log().warning("Annotation empty for fuel %s", fuel_label)
                    except Exception as e:
                        _active_log().exception("Fuel step failed: %s", fuel_label)
                        print(f"    Error: {e} (continuing to next fuel)")

                if not all_dfs:
                    print("  No data collected from any fuel group")
                    return None

                # --- Step 6: Merge all annotated files → output file ---
                merged = pd.concat(all_dfs, ignore_index=True)
                merged.to_csv(merged_path, index=False, encoding="utf-8")
                print(
                    f"  Output: {merged_path} ({len(all_dfs)} fuel groups, {len(merged)} rows)"
                )
                return merged_path

            except Exception as e:
                print(f"  Error: {e}")
                if self.driver:
                    save_selenium_debug(self.driver, self.session_dir, "exception")
                raise
            finally:
                if self.driver:
                    try:
                        self.driver.quit()
                    except Exception as quit_exc:
                        _active_log().debug("driver.quit() raised: %s", quit_exc)
                    self.driver = None
        finally:
            try:
                _active_session_log.reset(ctx_token)
            except (ValueError, TypeError, LookupError):
                pass

    def _set_base_filters(
        self,
        state: str,
        year: int,
        rta: str | None,
        portal_filters: dict[str, str] | None = None,
    ) -> bool:
        """
        Top bar (minimal touch): leave Type and Year type at portal defaults; do not change Type.
        State: only if not all-India aggregate (default row unchanged).
        Then Y-Axis Maker, X-Axis Month Wise, Year — via robust hidden-select path first.
        """
        pf = portal_filters or {}
        y_axis = (pf.get("y_axis") or "").strip() or BASE_FILTERS["y_axis"]
        x_axis = (pf.get("x_axis") or "").strip() or BASE_FILTERS["x_axis"]
        _active_log().info(
            "Top bar: skipping Type and Year type (defaults); state=%s all_india=%s",
            state,
            _is_aggregate_state_name(state),
        )
        time.sleep(0.12)
        # All India aggregate is default — do not open State when national scope is intended
        if not _is_aggregate_state_name(state):
            if not _apply_state_for_scrape(self.driver, state):
                print(f"    Failed: State '{state}'")
                return False
            time.sleep(0.35)  # State AJAX repaints RTO + Y-axis panels
            _wait_primefaces_quiet(self.driver)
            invalidate_main_hidden_select_cache(self.driver)
        else:
            time.sleep(0.1)
        if rta:
            print(f"    Top bar: RTA -> {rta!r}", flush=True)
            if not _select_by_text(self.driver, "rta_dropdown", rta):
                print(f"    Failed: RTA '{rta}'")
                return False
        for ek, ev in (
            ("y_axis_dropdown", y_axis),
            ("x_axis_dropdown", x_axis),
            ("year_dropdown", str(year)),
        ):
            print(f"    Top bar: {ek} -> {ev!r}", flush=True)
            if not _select_by_text(self.driver, ek, ev):
                print(f"    Failed: {ek} ({ev!r})")
                return False
            time.sleep(0.12)
        readback = _read_portal_main_filter_labels(self.driver)
        if readback:
            _active_log().info("Portal main form (hidden selects): %s", readback)
            yb = readback.get("y_axis", "")
            xb = readback.get("x_axis", "")
            stb = readback.get("state", "")
            if yb and y_axis not in yb:
                _active_log().warning("Y-Axis readback %r does not contain expected %r", yb, y_axis)
            if xb and x_axis not in xb:
                _active_log().warning("X-Axis readback %r does not contain expected %r", xb, x_axis)
            if (
                not _is_aggregate_state_name(state)
                and stb
                and not _state_option_matches(state, stb)
            ):
                _active_log().warning(
                    "State readback %r may not match requested %r — check portal / run discovery if ids drift.",
                    stb,
                    state,
                )
        return True

    def _wait_for_sidebar_ready(self) -> None:
        """
        After first main Refresh and expanding the west pane: wait until Vehicle Category
        checkboxes are actually visible. The portal does not expose usable VhCatg UI until
        both steps complete (pane may be display:none and markup stale before that).
        """
        try:
            WebDriverWait(self.driver, DEFAULT_WAIT).until(lambda _: self._sidebar_filter_panel_open())
        except Exception:
            _active_log().warning("filterLayout did not become visible within timeout")
        try:
            WebDriverWait(self.driver, DEFAULT_WAIT).until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, "#filterLayout input[id^='VhCatg']")
                )
            )
        except Exception:
            _active_log().warning("Vehicle Category inputs not visible in sidebar — may still be loading")
        try:
            if VEHICLE_CATEGORY_IDS:
                WebDriverWait(self.driver, DEFAULT_WAIT).until(
                    EC.element_to_be_clickable((By.ID, VEHICLE_CATEGORY_IDS[0]))
                )
            else:
                WebDriverWait(self.driver, DEFAULT_WAIT).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "#VhCatg label[for^='VhCatg:']")
                    )
                )
        except Exception:
            pass
        try:
            d = discover_sidebar_checkboxes(self.driver)
            setattr(self.driver, "_vahan_sidebar_checkbox_ids", d)
            _active_log().debug(
                "sidebar checkbox discovery: %s",
                {k: len(v) for k, v in d.items()},
            )
        except Exception as e:
            _active_log().debug("sidebar checkbox discovery skipped: %s", e)
        time.sleep(0.08)

    def _sidebar_filter_panel_open(self) -> bool:
        """True if west filter pane is expanded (layout open, not only display != none)."""
        try:
            return bool(
                self.driver.execute_script(
                    """
                    var e = document.getElementById('filterLayout');
                    if (!e) return false;
                    var s = window.getComputedStyle(e);
                    if (s.display === 'none' || s.visibility === 'hidden') return false;
                    var w = parseFloat(s.width) || 0;
                    return w > 40;
                    """
                )
            )
        except Exception:
            try:
                fl = self.driver.find_element(By.ID, "filterLayout")
                disp = (fl.value_of_css_property("display") or "").strip().lower()
                return disp != "none" and fl.is_displayed()
            except Exception:
                return False

    def _prepare_sidebar_toggle_viewport(self) -> None:
        """After Refresh, the click-here GIF sits on the left margin — bring it into view."""
        try:
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.12)
            for sid in ("tablePnl", "j_idt71", "j_idt74", "combTablePnl"):
                try:
                    el = self.driver.find_element(By.ID, sid)
                    self.driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", el
                    )
                    break
                except Exception:
                    continue
            time.sleep(0.22)
        except Exception:
            pass

    def _click_clickhere_gif(self) -> bool:
        """
        Legacy control: clickhere.gif (img id often j_idt69; older builds used j_idt72). jQuery Layout also exposes
        #filterLayout-toggler — prefer that in _expand_sidebar.
        """
        driver = self.driver
        for by, loc in (
            (By.CSS_SELECTOR, "img[src*='clickhere']"),
            (By.CSS_SELECTOR, "img[src*='clickhere.gif']"),
            (By.XPATH, "//img[contains(@src,'clickhere')]"),
        ):
            try:
                img = WebDriverWait(driver, DEFAULT_WAIT).until(EC.presence_of_element_located((by, loc)))
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center', inline: 'start'});", img
                )
                time.sleep(0.15)
                if _safe_click(driver, img):
                    return True
                try:
                    driver.execute_script(
                        "var n=arguments[0]; (n.parentElement||n).dispatchEvent("
                        "new MouseEvent('click',{bubbles:true,cancelable:true,view:window}));",
                        img,
                    )
                    return True
                except Exception:
                    pass
            except Exception:
                continue
        return False

    def _scroll_west_layout_controls_into_view(self) -> None:
        """Bring the 25px west resizer / toggler strip into view (left edge of table panel)."""
        driver = self.driver
        for sid in (
            "filterLayout-resizer",
            "filterLayout-toggler",
            "j_idt71",
            "j_idt74",
            "tablePnl",
        ):
            try:
                el = driver.find_element(By.ID, sid)
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center', inline: 'start'});", el
                )
                time.sleep(0.12)
                break
            except Exception:
                continue

    def _click_jquery_layout_west_expand(self) -> bool:
        """
        Primary open control on live portal: #filterLayout-toggler contains an expand <a>.
        Clicking the outer GIF (e.g. j_idt69) often fails if overlays or stacking context differ.
        """
        driver = self.driver
        selectors = (
            "#filterLayout-toggler a.ui-layout-unit-expand-icon",
            "#filterLayout-toggler a[href*='javascript']",
            "#filterLayout-toggler a",
            "#filterLayout-toggler span.ui-icon-arrow-4-diag",
        )
        for css in selectors:
            try:
                el = WebDriverWait(driver, SIDEBAR_WAIT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, css))
                )
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center', inline: 'start'});", el
                )
                time.sleep(0.12)
                if _safe_click(driver, el):
                    return True
                try:
                    driver.execute_script("arguments[0].click();", el)
                    return True
                except Exception:
                    pass
            except Exception:
                continue
        try:
            res = driver.find_element(By.ID, "filterLayout-resizer")
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center', inline: 'start'});", res
            )
            time.sleep(0.1)
            if _safe_click(driver, res):
                return True
            driver.execute_script("arguments[0].click();", res)
            return True
        except Exception:
            pass
        return False

    def _js_open_west_sidebar(self) -> bool:
        """Last resort: DOM clicks via JS (same targets as manual layout open)."""
        try:
            return bool(
                self.driver.execute_script(
                    """
                    var t = document.getElementById('filterLayout-toggler');
                    if (t) {
                      var a = t.querySelector('a.ui-layout-unit-expand-icon')
                        || t.querySelector('a[href*="javascript"]') || t.querySelector('a');
                      if (a) { a.click(); return true; }
                      t.click();
                      return true;
                    }
                    var r = document.getElementById('filterLayout-resizer');
                    if (r) { r.click(); return true; }
                    var img = document.querySelector('img[src*="clickhere"]');
                    if (img) { img.click(); return true; }
                    return false;
                    """
                )
            )
        except Exception:
            return False

    def _wait_west_layout_ready(self, timeout: float | None = None) -> None:
        """After Refresh, wait until jQuery Layout west controls exist."""
        t = _wait_s(float(timeout if timeout is not None else SIDEBAR_WAIT))
        try:
            WebDriverWait(self.driver, t).until(
                EC.presence_of_element_located((By.ID, "filterLayout-toggler"))
            )
        except Exception:
            _active_log().debug("filterLayout-toggler not found within %ss (will still try expand)", t)

    def _expand_sidebar(self) -> bool:
        """Open west filter sidebar after main Refresh. Prefer #filterLayout-toggler (jQuery Layout)."""
        driver = self.driver
        expand_id = SELECTORS.get("sidebar_expand") or "filterLayout-toggler"
        scraper_ref = self

        def wait_open_after_click() -> bool:
            time.sleep(0.22)
            try:
                WebDriverWait(driver, DEFAULT_WAIT).until(
                    lambda _: scraper_ref._sidebar_filter_panel_open()
                )
                return True
            except Exception:
                return scraper_ref._sidebar_filter_panel_open()

        if self._sidebar_filter_panel_open():
            return True

        self._prepare_sidebar_toggle_viewport()
        self._scroll_west_layout_controls_into_view()
        # Short beat: block UI from Refresh can still intercept the left-edge click
        _wait_primefaces_quiet(driver)
        self._wait_west_layout_ready()

        # 1) #filterLayout-toggler: JS expand first (reliable under overlays), then Selenium clicks
        if self._sidebar_filter_panel_open():
            return True
        if self._js_open_west_sidebar() and wait_open_after_click():
            return True
        if self._click_jquery_layout_west_expand() and wait_open_after_click():
            return True

        # 2) Legacy GIF / text / img id
        for fn in (
            self._click_clickhere_gif,
            lambda: _click_by_text(driver, "click here"),
            lambda: _click_by_text(driver, "Click here"),
        ):
            if self._sidebar_filter_panel_open():
                return True
            try:
                clicked = bool(fn())
            except Exception:
                clicked = False
            if clicked and wait_open_after_click():
                return True

        for iid in (expand_id, "filterLayout-toggler"):
            if self._sidebar_filter_panel_open():
                return True
            try:
                el = WebDriverWait(driver, DEFAULT_WAIT).until(EC.presence_of_element_located((By.ID, iid)))
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center', inline: 'start'});", el
                )
                time.sleep(0.12)
                if not _safe_click(driver, el):
                    try:
                        el.click()
                    except Exception:
                        pass
                if wait_open_after_click():
                    return True
            except Exception:
                continue

        return self._sidebar_filter_panel_open()

    def _set_sidebar_filters(self) -> None:
        """Set Vehicle Category, Vehicle Class. Requires prior main Refresh + expanded #filterLayout."""
        try:
            sidebar = self.driver.find_element(By.ID, "filterLayout")
            self.driver.execute_script("arguments[0].scrollTop = 0;", sidebar)
            time.sleep(0.05)
        except Exception:
            pass
        if VEHICLE_CATEGORY_TARGET_LABELS:
            _set_vehicle_categories_by_target_labels(self.driver, list(VEHICLE_CATEGORY_TARGET_LABELS))
        elif VEHICLE_CATEGORY_IDS:
            _set_checkboxes_scoped(
                self.driver, "VhCatg", VEHICLE_CATEGORY_IDS, wait_timeout=5.0, pause_s=0.04
            )
        time.sleep(0.04)
        # Scroll to Vehicle Class section (below Vehicle Category)
        try:
            el = self.driver.find_element(By.ID, "VhClass")
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block:'nearest', behavior:'instant'});", el
            )
            time.sleep(0.1)
        except Exception:
            self._scroll_sidebar(400)
        time.sleep(0.05)
        _set_vehicle_class_by_label(self.driver, list(VEHICLE_CLASS_LABELS))
        _set_checkboxes_scoped(self.driver, "VhClass", VEHICLE_CLASS_IDS, wait_timeout=5.0, pause_s=0.04)

    def _clear_fuel_checkboxes(self) -> None:
        """Deselect all fuel filters before selecting new group."""
        self._scroll_sidebar(350)  # Scroll to fuel section
        time.sleep(0.06)
        try:
            fuel_tbl = self.driver.find_element(By.ID, "fuel")
        except Exception:
            fuel_tbl = None
        for cid in ALL_FUEL_IDS:
            for _ in range(SELENIUM_ACTION_RETRIES):
                try:
                    cb = fuel_tbl.find_element(By.ID, cid) if fuel_tbl else self.driver.find_element(By.ID, cid)
                    if not cb.is_selected():
                        continue
                    if cb.is_selected():
                        if not _safe_click(self.driver, cb):
                            cb.click()
                    time.sleep(0.04)
                    break
                except StaleElementReferenceException:
                    time.sleep(0.1)
                except Exception:
                    continue

    def _scroll_sidebar(self, amount: int = 400):
        """Scroll down within sidebar (filterLayout) to reach fuel/VhClass checkboxes."""
        try:
            sidebar = self.driver.find_element(By.ID, "filterLayout")
            self.driver.execute_script("arguments[0].scrollTop += arguments[1]", sidebar, amount)
            time.sleep(0.06)
        except Exception:
            pass

    def _process_fuel_group(self, fuel_label: str, fuel_ids: list, is_first_fuel: bool = True) -> pd.DataFrame | None:
        """
        Per fuel: (2nd+: main Refresh → expand sidebar → sidebar filters → sidebar Refresh) →
        clear fuels → select group → sidebar Refresh → download.
        """
        if not is_first_fuel:
            # Full top-of-loop reset: main Refresh → open west pane → re-apply category/class → sidebar Refresh
            if not _click_main_refresh(self.driver):
                _active_log().warning("Fuel loop: main Refresh click failed")
            if not _wait_primefaces_quiet(self.driver):
                _active_log().warning("Fuel loop: overlay wait after main refresh timed out")
            _wait_for_loading_finish(self.driver)
            time.sleep(0.1)
            self._expand_sidebar()
            self._wait_for_sidebar_ready()
            self._set_sidebar_filters()
            time.sleep(0.05)
            if not _click_sidebar_refresh(self.driver):
                _active_log().warning(
                    "Fuel loop: sidebar Refresh after re-applying filters failed"
                )
            if not _wait_primefaces_quiet(self.driver):
                _active_log().warning(
                    "Fuel loop: overlay wait after sidebar refresh (post-filters) timed out"
                )
            _wait_for_loading_finish(self.driver)
            time.sleep(self._refresh_wait)

        # Deselect all fuel filters, then select current fuel
        self._clear_fuel_checkboxes()
        time.sleep(0.05)
        self._scroll_sidebar(200)
        time.sleep(0.05)
        _set_checkboxes_scoped(self.driver, "fuel", fuel_ids, pause_s=0.04)
        time.sleep(0.05)

        # Sidebar Refresh
        if not _click_sidebar_refresh(self.driver):
            _active_log().warning("Fuel loop: sidebar Refresh click failed")
        if not _wait_primefaces_quiet(self.driver):
            _active_log().warning("Fuel loop: overlay wait after sidebar refresh timed out")
        # Wait for table to render (Excel icon clickable)
        _wait_for_loading_finish(self.driver)
        time.sleep(self._refresh_wait)

        predownload = (
            {p.resolve() for p in self.download_dir.iterdir()}
            if self.download_dir and self.download_dir.exists()
            else set()
        )
        # Click Excel icon
        if not _click_excel_icon(self.driver):
            _click_by_text(self.driver, "Download") or _click_by_text(self.driver, "download")
        time.sleep(0.12)
        downloaded = _wait_for_download(
            self.download_dir,
            timeout=DOWNLOAD_WAIT,
            initial_paths=predownload,
        )
        if not downloaded:
            time.sleep(0.4)
            downloaded = _wait_for_download(
                self.download_dir,
                timeout=DOWNLOAD_WAIT,
                initial_paths=predownload,
            )

        if not downloaded:
            print(f"    No download for {fuel_label} - check if Vahan portal allows download")
            return None

        out_path = self.session_dir / f"{fuel_label.replace(' ', '_')}_annotated.csv"
        return (downloaded, out_path)




def run_scrape(
    states: list[str],
    years: list[int] | None = None,
    output_base: Path | None = None,
    headless: bool = False,
) -> list[Path]:
    """
    Run scraper for given states and years.
    Returns list of merged output file paths.
    """
    output_base = output_base or Path("output/vahan_data")
    years = years or list(range(YEAR_MIN, YEAR_MAX + 1))
    results = []

    for state in states:
        for year in years:
            print(f"\n--- {state} / {year} ---")
            scraper = VahanScraper(output_base, headless=headless)
            path = scraper.run_state_year(state, year, rta=None)
            if path:
                results.append(path)
                print(f"  Done. Output file: {path}")

    if results:
        print(f"\nAll output files: {results}")
    return results


if __name__ == "__main__":
    # Example: one state, one year (update SELECTORS in config first!)
    run_scrape(
        states=["Maharashtra"],  # Example - use actual state name from dropdown
        years=[2024],
        output_base=Path("output/vahan_data"),
        headless=False,
    )
