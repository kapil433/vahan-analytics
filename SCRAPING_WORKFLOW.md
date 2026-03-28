# Vahan Scraping Workflow – Detailed Spec

## Loop Structure

```
FOR each State (or selected states):
  FOR each Year (2012–2026, or selected range):
    [OPTIONAL] FOR each RTA (if RTA filter selected for that state):
      Run single scrape session
      → Merge all fuel files → single state-year file → Python cleaner → analytics
```

**Default:** RTA = Not selected (all RTOs for that state)

---

## Single Session Flow (per State + Year)

### Step 1: Base Filters (main form)

Values below are **confirmed from the live portal DOM** (see `config/portal_filter_reference.md` and `output/discovery/reportview_page.html`).  
**Vehicle Class** + **Vehicle Category Group** is a different report layout (class on rows, category group on columns).

| Filter      | Value (current scraper / portal-aligned) |
|-------------|------------------------------------------|
| Type        | Actual Value                             |
| State       | Selected state                           |
| RTA         | Not selected (all)                       |
| Y-Axis      | **Maker**                                |
| X-Axis      | **Month Wise**                           |
| Year Type   | Calendar Year (set explicitly before Year) |
| Year        | Selected year (2012–2026)                |

→ **Click the main Refresh** (required before the sidebar).

**Important:** Vehicle Category and other west-pane filters are populated/usable only **after** this first main Refresh **and** after you **open the sidebar**. Do not try to use `#VhCatg` on the initial page before Refresh, or while `#filterLayout` is still collapsed.

---

### Step 2: Expand Sidebar

- Find **red icon** with text "click here"
- Click to expand the west pane (`#filterLayout`)

---

### Step 3: Sidebar Filters (scroll within sidebar only)

| Filter           | Value                                                                 |
|------------------|-----------------------------------------------------------------------|
| Vehicle Category | **FOUR WHEELER (Invalid Carriage)**, **LIGHT MOTOR VEHICLE**, **LIGHT PASSENGER VEHICLE** (exact labels under `#VhCatg`; see `VEHICLE_CATEGORY_TARGET_LABELS` in `config/scraping_config.py`) |
| Norms            | Nothing selected                                                      |
| Vehicle Class    | Motor Car, Motor Cab                                                  |
| Fuel             | Varies per fuel group (see below)                                     |

→ **Click sidebar Refresh** (west pane) so Vehicle Category / Class apply to the report **before** the first fuel selection.

---

### Step 4: Per-Fuel Iteration

For **fuel 1**: clear all fuel checkboxes → select one fuel group → **sidebar Refresh** → wait → **Excel download** → annotate rows with `fuel_type` → save in session.

For **fuel 2 onward**: **main Refresh** → expand sidebar → re-apply Vehicle Category + Vehicle Class → **sidebar Refresh** → then the same fuel steps (clear → select group → sidebar Refresh → download).

Each iteration:

1. **Select fuel options** (checkboxes under `#fuel`)
2. **Sidebar Refresh**
3. **Excel download** (`vchgroupTable:xls`)
4. **Add fuel label** column to every row
5. **Save** in session folder

#### Fuel Groups & Options

| Fuel Label     | Fuel Options to Select                                                                 |
|----------------|----------------------------------------------------------------------------------------|
| **CNG**        | CNG Only, Petrol/CNG, Petrol(E20)/CNG, PETROL/HYBRID/CNG, dual diesel/cng             |
| **Petrol**     | Petrol, Petrol/Hybrid, Petrol(E20)/Hybrid, Petrol/LPG, Petrol/Methanol, Petrol/Ethanol |
| **Diesel**     | Diesel, Diesel/Hybrid, dual diesel/bio cng, dual diesel/lng                           |
| **EV**         | Electric (BOV), Pure EV                                                                |
| **Strong Hybrid** | Strong Hybrid EV                                                                    |

**Order:** CNG → Petrol → Diesel → EV → Strong Hybrid

---

### Step 5: Merge & Output

- Merge all downloaded+annotated files for this state+year
- Output: **single state-year file** (e.g. `{state}_{year}.csv`)
- Pass to **Python cleaner** → **analytics platform**

---

## File Naming Convention

- Session folder: `session_{state}_{year}_{timestamp}/`
- Per-fuel: `{state}_{year}_{fuel}.csv` (or `.xlsx` if download is Excel)
- Merged: `{state}_{year}_merged.csv`

---

## Code map (implementation)

Verified against `scraper/vahan_scraper.py` → `VahanScraper.run_state_year`:

| Spec step | Implementation |
|-----------|----------------|
| Top-bar filters | `_set_base_filters` (Type, State, RTA, Y-Axis, X-Axis, year type, year) |
| Main Refresh | `_click_main_refresh` |
| Expand sidebar | `_expand_sidebar`, `_wait_for_sidebar_ready` |
| Category + class | `_set_sidebar_filters` → `_set_vehicle_categories_by_target_labels`, `_set_vehicle_class_by_label`, scoped `#VhClass` checkboxes |
| Sidebar Refresh (after filters) | `_click_sidebar_refresh` + `_wait_primefaces_quiet` + `_wait_for_loading_finish` |
| Per-fuel | `_process_fuel_group` (sidebar refresh, download, stable rename, `_annotate_and_get_df`) |
| Merge | `pd.concat(all_dfs)` → `{state}_{year}_merged.csv` |
| Stop / Ctrl+C | `batch_stop_requested()` in fuel loop; `scraper/batch_control.py`; API lifespan SIGINT; `POST /scrape/stop`. Non-browser checks: `python scripts/smoke_batch_control.py`, `python scripts/smoke_parallel_stop.py` |

Parallel jobs: `scraper/vahan_scraper_master.py` → `run_batch_parallel` (one browser per worker; `portal_filters` passed through).

---

## Selector discovery (when and how)

**When to re-run**

- After **Parivahan deploys** a new build (JSF ids like `j_idt70` often change).
- When scraper logs show missing elements (Refresh, sidebar toggler, `fuel:N` ids).
- When a **new fuel row** appears in `#fuel` — checkbox indices shift; update `FUEL_GROUP_CHECKBOX_IDS` in `config/scraping_config.py`.

**Commands (repo root)**

1. `python scraper/discovery.py` — opens Chrome, saves `output/discovery/reportview_page.html`, `reportview_screenshot.png`, and `element_ids.txt` (waits for Enter before closing). Use these to update `config/scraping_config.py` `SELECTORS` and/or hand-edit `config/discovered_selectors.json` (merged on import).
2. `python scripts/peek_vhcatg_labels.py` — prints all `VhCatg:*` label texts from the saved snapshot so you can align `VEHICLE_CATEGORY_TARGET_LABELS` (including alternates in tuples).

**Files**

- `config/discovered_selectors.json` — merged into `SELECTORS` in `config/scraping_config.py` on import (see `apply_discovered_selectors`).
- `config/portal_filter_reference.md` — human-readable table of main form fields.

**Manual checklist** (if discovery script misses something)

- Form ID, dropdown widget ids: Type, State, RTO, Y-Axis, X-Axis, Year Type, Year
- Main **Refresh** button id
- `#filterLayout-toggler` / clickhere / west resizer
- `#VhCatg`, `#VhClass`, `#fuel`, sidebar **Refresh**, Excel download id

---

## Offline merge test (no browser)

Not a portal simulator — only validates pandas merge + `fuel_type` annotation pattern:

```bash
python scripts/verify_merge_pipeline.py
```

Stub page (documentation): [http://localhost:8000/mock-portal](http://localhost:8000/mock-portal) when the API is running.
