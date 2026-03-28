"""
Vahan scraping configuration.
Selectors: defaults below, overridden by config/discovered_selectors.json when present.
Maintain that JSON manually from `output/discovery/` (run `python scraper/discovery.py` to capture HTML + element_ids.txt).
"""

import json
from pathlib import Path
from typing import Union

# Vehicle category row: exact portal label, or tuple of alternates (try in order).
VehicleCategoryTarget = Union[str, tuple[str, ...]]

_CONFIG_DIR = Path(__file__).resolve().parent
_DISCOVERED_JSON = _CONFIG_DIR / "discovered_selectors.json"

# Base filters — MUST match portal option *visible text* (PrimeFaces selectOneMenu).
# Confirmed against output/discovery/reportview_page.html (see portal_filter_reference.md).
# Y=Maker + X=Month Wise — maker on rows, time on columns (sidebar Vehicle Class still filters body type).
BASE_FILTERS = {
    "type": "Actual Value",
    "rta": None,  # Not selected = all RTOs (leave default “All Vahan4 Running Office…”)
    "y_axis": "Maker",
    "x_axis": "Month Wise",
    "year_type": "Calendar Year",  # Set explicitly before Year
}

# Visible labels for PrimeFaces selectOneMenu (see portal_filter_reference.md). API/UI may override within these sets.
PORTAL_FILTER_CHOICES = {
    "y_axis": ["Maker", "Vehicle Class", "Fuel"],
    "x_axis": ["Month Wise", "Vehicle Category Group"],
    "year_type": ["Calendar Year"],
}

# Vehicle Category (#VhCatg): the portal shows a long list; `VhCatg:N` indices shift when rows are added.
# Selection is by **exact visible label** under `#VhCatg` (stable across reorders if text unchanged).
# Current portal rows (typical): TWO WHEELER…, THREE WHEELER…, then FOUR WHEELER (Invalid Carriage), …, LMV, LPV, …
# Labels must match live #VhCatg text (see output/discovery/reportview_page.html after discovery).
# Tuples = alternates when portal wording varies (e.g. suffix "(LPV)").
VEHICLE_CATEGORY_TARGET_LABELS: list[VehicleCategoryTarget] = [
    "FOUR WHEELER (Invalid Carriage)",
    "LIGHT MOTOR VEHICLE",
    (
        "LIGHT PASSENGER VEHICLE",
        "LIGHT PASSENGER VEHICLE (LPV)",
    ),
]

# Optional: fill after `python scraper/discovery.py` if you want ID clicks first (faster). Empty = labels only.
VEHICLE_CATEGORY_IDS: list[str] = []

# Vehicle Class: Motor Car, Motor Cab
VEHICLE_CLASS_IDS = ["VhClass:6", "VhClass:51"]
VEHICLE_CLASS_LABELS = ["Motor Car", "Motor Cab"]

NORMS = []  # Nothing selected

# Fuel groups: label → checkbox IDs
FUEL_GROUP_CHECKBOX_IDS = {
    "CNG": ["fuel:3", "fuel:22", "fuel:23", "fuel:29", "fuel:8"],
    "Petrol": ["fuel:21", "fuel:28", "fuel:24", "fuel:30", "fuel:31", "fuel:27"],
    "Diesel": ["fuel:4", "fuel:5", "fuel:7", "fuel:9"],
    "EV": ["fuel:10", "fuel:33"],
    "Strong Hybrid": ["fuel:35"],
}

# All fuel IDs (for clearing before each fuel group)
ALL_FUEL_IDS = [cid for ids in FUEL_GROUP_CHECKBOX_IDS.values() for cid in ids]

# Legacy: label → visible text (for fallback)
FUEL_GROUPS = {
    "CNG": ["CNG ONLY", "PETROL/CNG", "PETROL(E20)/CNG", "PETROL/HYBRID/CNG", "DUAL DIESEL/CNG"],
    "Petrol": ["PETROL", "PETROL/HYBRID", "PETROL(E20)/HYBRID", "PETROL/LPG", "PETROL/METHANOL", "PETROL/ETHANOL"],
    "Diesel": ["DIESEL", "DIESEL/HYBRID", "DUAL DIESEL/BIO CNG", "DUAL DIESEL/LNG"],
    "EV": ["ELECTRIC(BOV)", "PURE EV"],
    "Strong Hybrid": ["STRONG HYBRID EV"],
}

# Year range — merged Vahan analytics + research overlays use this window (calendar years).
YEAR_MIN = 2012
YEAR_MAX = 2026

# FY2011-12 is excluded from UI/API: only Jan–Mar 2012 fall in that FY while our window starts
# calendar 2012; full-year coverage is treated as starting FY2012-13.
# FY with start year >= YEAR_MAX (e.g. FY2026-27) is excluded: it needs Q1 of YEAR_MAX+1 outside scrape range.


def financial_year_labels_analytics() -> list[str]:
    """
    Indian FY labels (FYyyyy-yy) consistent with YEAR_MIN..YEAR_MAX for dropdowns and validation.

    Drops FY2011-12. Drops any FY whose April-start calendar year is >= YEAR_MAX (e.g. FY2026-27).
    """
    from .mappings import month_to_fy

    labels: set[str] = set()
    for y in range(YEAR_MIN, YEAR_MAX + 1):
        for m in range(1, 13):
            labels.add(month_to_fy(y, m))
    out: list[str] = []
    for label in sorted(labels):
        if label == "FY2011-12":
            continue
        try:
            start_y = int(label.replace("FY", "").split("-", 1)[0])
        except (ValueError, IndexError):
            continue
        if start_y >= YEAR_MAX:
            continue
        out.append(label)
    return out


# Main JSF form id — PrimeFaces.ab(..., f: ...) on top-bar selectOneMenus.
MAIN_REPORT_FORM_ID = "masterLayout_formlogin"

# Fallback PrimeFaces.ab "u" (space-separated client ids to re-render) when the native <select>
# has no inline onchange (see output/discovery/reportview_page.html). Keys = widget id (div id),
# same as SELECTORS *values* for j_idt* / selectedRto / yaxisVar / ...
PRIMEFACES_MENU_UPDATES: dict[str, str] = {
    # Widget ids drift on deploy — synced from live reportview.xhtml (Mar 2026).
    "j_idt28": "j_idt28",
    "j_idt37": "selectedRto yaxisVar",
    "selectedRto": "yaxisVar",
    "yaxisVar": "xaxisVar",
    "xaxisVar": "multipleYear",
    "selectedYearType": "selectedYear",
    "selectedYear": "selectedYear",
}


# --- SELECTORS (defaults; overridden by discovered_selectors.json) ---
SELECTORS = {
    "type_dropdown": "j_idt28",
    "state_dropdown": "j_idt37",
    "rta_dropdown": "selectedRto",
    "y_axis_dropdown": "yaxisVar",
    "x_axis_dropdown": "xaxisVar",
    "year_type_dropdown": "selectedYearType",
    "year_dropdown": "selectedYear",
    # Main toolbar Refresh (PrimeFaces commandButton id — was j_idt70 / j_idt68 on older builds)
    "refresh_btn_main": "j_idt67",
    # Red strip GIF to open west pane — was j_idt72 when that id was the image; do not use j_idt72 now (sidebar header Refresh)
    "sidebar_expand": "j_idt69",
    "filter_layout_toggler": "filterLayout-toggler",
    # Footer Refresh inside #filterLayout (was j_idt84)
    "refresh_btn_sidebar": "j_idt79",
    # Excel export (client id); live page often uses vchgroupTable:xls — groupingTable:xls tried in scraper as fallback
    "download_btn": "vchgroupTable:xls",
}

# Hidden state <select> uses short codes (value="MH"); visible text is e.g. "Maharashtra(59)".
# Used when select_by_visible_text is flaky or AJAX lags. Keys = casefold of UI/API state names.
STATE_PORTAL_OPTION_VALUE: dict[str, str] = {
    "andhra pradesh": "AP",
    "arunachal pradesh": "AR",
    "assam": "AS",
    "bihar": "BR",
    "chhattisgarh": "CG",
    "goa": "GA",
    "gujarat": "GJ",
    "haryana": "HR",
    "himachal pradesh": "HP",
    "jammu and kashmir": "JK",
    "jharkhand": "JH",
    "karnataka": "KA",
    "kerala": "KL",
    "madhya pradesh": "MP",
    "maharashtra": "MH",
    "manipur": "MN",
    "meghalaya": "ML",
    "mizoram": "MZ",
    "nagaland": "NL",
    "odisha": "OR",
    "punjab": "PB",
    "rajasthan": "RJ",
    "sikkim": "SK",
    "tamil nadu": "TN",
    "telangana": "TS",
    "tripura": "TR",
    "uttar pradesh": "UP",
    "uttarakhand": "UK",
    "west bengal": "WB",
    "delhi": "DL",
    "puducherry": "PY",
    "lakshadweep": "LD",
    "andaman and nicobar islands": "AN",
    "dadra and nagar haveli and daman and diu": "DD",
    "chandigarh": "CH",
    "ladakh": "LA",
}


def state_portal_option_value(requested_state_name: str) -> str | None:
    """Return VAHAN state <option value> for a state name from the API/UI, or None."""
    k = (requested_state_name or "").strip().casefold()
    return STATE_PORTAL_OPTION_VALUE.get(k)


def _load_discovered() -> dict:
    """Load discovered selectors from JSON if present."""
    if not _DISCOVERED_JSON.exists():
        return {}
    try:
        data = json.loads(_DISCOVERED_JSON.read_text(encoding="utf-8"))
        return data.get("selectors", {})
    except Exception:
        return {}


def apply_discovered_selectors(discovered: dict) -> None:
    """Merge discovered selectors into SELECTORS. Call after ensure_fresh_selectors()."""
    if discovered:
        SELECTORS.update(discovered)


# Load discovered on module import if file exists
apply_discovered_selectors(_load_discovered())
