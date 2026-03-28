# Portal filter reference (verified from DOM)

Source: `output/discovery/reportview_page.html` (run `python scraper/discovery.py` to refresh after site changes).

## Main form (`masterLayout_formlogin`)

| Field | Hidden `<select>` id | Example option `value` → visible label |
|-------|----------------------|----------------------------------------|
| Type | `j_idt27_input` (id changes) | `A` → Actual Value |
| State | `j_idt36_input` | `-1` → All Vahan4 Running States (36/36) |
| RTO | `selectedRto_input` | `-1` → All Vahan4 Running Office(1460/1465) |
| Y-Axis | `yaxisVar_input` | `Vehicle Class`, `Maker`, `Fuel`, … |
| X-Axis | `xaxisVar_input` | `VCG` → Vehicle Category Group; `Month Wise`; … |
| Year Type | `selectedYearType_input` | `C` → Calendar Year |
| Year | `selectedYear_input` | `2024`, … |

**Scraper default (`BASE_FILTERS`):** Y-Axis = **Maker**, X-Axis = **Month Wise**.

**Alternate report (captured snapshot):** Y-Axis = **Vehicle Class**, X-Axis = **Vehicle Category Group** (table title: “Vehicle Class Wise Vehicle Category Group Data …”).

## Portal ordering (west pane / Vehicle Category)

On [reportview.xhtml](https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml), **Vehicle Category** and the rest of the west filter sidebar are not reliably usable until:

1. **Main form Refresh** has run at least once after setting base filters (the submit refreshes partial UI targets such as `VhCatg`, `fuel`, `VhClass` in the same AJAX update as the table).
2. The **west sidebar** (`#filterLayout`) has been **expanded** (it is `display: none` while collapsed).

Automations must follow: **set base filters → main Refresh → wait for load → expand sidebar → then** tick Vehicle Category / Class / Fuel. The scraper implements this sequence.

### Opening the west sidebar (filters)

The live page uses **jQuery Layout**, not only the old `clickhere.gif`:

| Control | Element | Notes |
|---------|---------|--------|
| **Preferred** | `#filterLayout-toggler a.ui-layout-unit-expand-icon` | Title “Open”; actually expands `#filterLayout` |
| Fallback | `#filterLayout-resizer` | 25px west strip; often clickable |
| Legacy | `img[src*='clickhere']` / `j_idt72` | May sit outside the layout container and miss hits |

The scraper tries toggler → resizer → JS → GIF/text/`j_idt72`. If expansion still fails, re-run `python scraper/discovery.py` after a portal deploy (JSF ids drift).

## Sidebar — Vehicle Category (`#VhCatg`)

The portal shows a **long list** (e.g. TWO WHEELER…, THREE WHEELER…, **FOUR WHEELER (Invalid Carriage)**, …, **LIGHT MOTOR VEHICLE**, **LIGHT PASSENGER VEHICLE**, …).  
**`VhCatg:N` indices change** when rows are added — do not hard-code `:0`/`:1`/`:2` for “4W / LMV / LPV” without re-checking the DOM.

**Scraper:** uses `VEHICLE_CATEGORY_TARGET_LABELS` in `config/scraping_config.py` (exact label text after whitespace normalization).

| Typical selection (current product default) | Portal label (match) |
|---------------------------------------------|----------------------|
| Four-wheel invalid | `FOUR WHEELER (Invalid Carriage)` |
| LMV | `LIGHT MOTOR VEHICLE` |
| LPV | `LIGHT PASSENGER VEHICLE` |

**Older short list snapshot** (may not match live): `VhCatg:0`…`3` mapped to 4WIC/LMV/MMV/HMV — run `python scraper/discovery.py` if you need current ids.

## Sidebar — Vehicle Class (`#VhClass`) — cars / cabs

| Checkbox id | Label |
|---------------|--------|
| `VhClass:6` | MOTOR CAR |
| `VhClass:51` | MOTOR CAB |

## Fuels table (`#fuel`)

Checkbox ids are `fuel:0` … `fuel:35` with labels as in HTML (e.g. `fuel:3` CNG ONLY, `fuel:21` PETROL).
