# Vahan Parivahan Dashboard – Site Architecture & Scraper Design

## 1. Site Overview

**URL:** https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml

**Purpose:** Government of India vehicle registration analytics dashboard (VAHAN SEWA) providing vehicle registration data across states, RTOs, and time periods.

---

## 2. Technical Architecture

### 2.1 Technology Stack

| Component | Technology |
|-----------|------------|
| **Framework** | JSF (JavaServer Faces) |
| **File Format** | `.xhtml` (XHTML + JSF components) |
| **State Management** | `javax.faces.ViewState` (hidden form field) |
| **Rendering** | Server-side + dynamic client-side (JavaScript) |

### 2.2 Why This Matters for Scraping

- **JSF forms** use `javax.faces.ViewState` – a hidden field that must be extracted from each page and sent with every POST request. It acts like a CSRF token.
- **Dynamic content** – dropdowns, tables, and charts are often populated via JavaScript/AJAX after page load.
- **Form-based navigation** – button clicks and dropdown changes trigger POST requests, not simple GET links.
- **Simple HTTP requests** (e.g. `requests` + BeautifulSoup) will not work reliably; you need either:
  - **Browser automation** (Selenium, Playwright, Puppeteer), or
  - **Manual HTTP replication** (extract ViewState, mimic POST requests with correct form data).

---

## 3. Form Structure & Input Controls

### 3.1 Main Report Filters

| Filter | Type | Options / Range |
|--------|------|------------------|
| **Type** | Dropdown | Actual Value, In Crore, In Lakh, In Thousand |
| **State** | Dropdown | 35 states/UTs (with RTO count per state) |
| **RTO** | Dropdown | ~1,457–1,464 RTO offices |
| **Y-Axis** | Dropdown | State, Maker, Fuel, Norms, Vehicle Class, Vehicle Category |
| **X-Axis** | Dropdown | Month Wise, Calendar Year, Financial Year, Vehicle Category Group, Fuel, Norms, Vehicle Category |
| **Year Type** | Dropdown | Calendar Year, Financial Year, Select |
| **Year** | Dropdown | 2003–2026, Till Today, Select Year |

### 3.2 Additional Filters

| Filter | Options |
|--------|---------|
| **Norms** | Bharat Stage I–VI, Euro 1–6D, TREM stages |
| **Fuels** | Diesel, Petrol, CNG, Electric, Hybrid, LNG, Hydrogen, Bio-fuels |
| **Vehicle Class** | Motorcycles, Cars, Commercial vehicles, Tractors, Buses, Ambulances, etc. |

### 3.3 Data Output

- Tables with multilevel row headers (STATE, RTO, Vehicle Category, Vehicle Class, Fuel, Norm, Maker)
- Charts (likely rendered via JS)
- Export options (if available) – to be confirmed on the live site

---

## 4. Scraper Architecture Options

### Option A: Browser Automation (Recommended)

**Tools:** Selenium (Python) or Playwright (Python/Node)

**Flow:**
1. Launch headless browser.
2. Navigate to `reportview.xhtml`.
3. Wait for JSF/JS to load.
4. Select filters (State, RTO, Year, etc.) via dropdowns.
5. Click “Generate Report” / “Submit”.
6. Wait for table/chart to load.
7. Extract table data (e.g. via `BeautifulSoup` on `driver.page_source` or `driver.find_elements`).
8. Optionally export or scrape pagination.
9. Repeat for each combination of filters.

**Pros:** Handles JS, ViewState, and dynamic content automatically.  
**Cons:** Slower, more resource-heavy, may need anti-bot handling.

### Option B: HTTP Request Replication

**Tools:** `requests`, `BeautifulSoup`, `re` (for ViewState)

**Flow:**
1. GET `reportview.xhtml` → parse HTML.
2. Extract `javax.faces.ViewState` from hidden input.
3. Extract form `id` and other hidden fields.
4. Build POST body with selected dropdown values (JSF uses specific `j_idtXX:dropdownId`-style IDs).
5. POST to the same URL (or form action URL).
6. Parse response HTML for table data.

**Pros:** Faster, lighter, easier to schedule (e.g. cron).  
**Cons:** Requires reverse-engineering form IDs and parameter names; brittle if the site changes.

### Option C: Hybrid

- Use browser automation to discover exact form IDs, parameter names, and request flow.
- Then implement Option B for production runs if the site structure is stable.

---

## 5. Scraper Design for Monthly Runs

### 5.1 High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    MONTHLY SCRAPE JOB                            │
├─────────────────────────────────────────────────────────────────┤
│  1. Load config (which report type, states, RTOs, date range)    │
│  2. For each (State, RTO, Year, Month) combination:              │
│     a. Open reportview.xhtml                                     │
│     b. Set filters (Type, State, RTO, X-Axis, Y-Axis, Year)      │
│     c. Submit form                                               │
│     d. Wait for table/chart                                      │
│     e. Extract data → DataFrame / CSV / JSON                     │
│  3. Merge/aggregate outputs                                     │
│  4. Save to storage (local/cloud)                               │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Key Implementation Details

1. **Rate limiting:** Add delays (e.g. 2–5 seconds) between requests to avoid blocking.
2. **Session handling:** Reuse browser session or cookies where possible.
3. **Error handling:** Retry on timeout/503; log failed combinations for manual retry.
4. **Data format:** Output as CSV/Parquet with columns: `state`, `rto`, `year`, `month`, `vehicle_category`, `fuel`, `norm`, `count`, etc.
5. **Scheduling:** Use cron (Linux) or Task Scheduler (Windows) for monthly runs.

### 5.3 Dependencies (Python Example)

```
selenium>=4.0.0
webdriver-manager  # auto-manage ChromeDriver
pandas
beautifulsoup4
```

---

## 6. Next Steps

1. **Inspect the live site** (manually or with DevTools) to capture:
   - Exact form IDs and input names.
   - POST URL and method.
   - Structure of the results table (HTML/class names).
   - Whether export (Excel/CSV) is available.
2. **Define your exact report** – which filters to use (State, RTO, Year, Month, etc.).
3. **Choose Option A or B** based on stability vs. speed needs.
4. **Implement a minimal scraper** for one report type, then generalize.

---

## 7. References

- [GitHub: vahan_parivahan_scrape](https://github.com/abhishekzgithub/vahan_parivahan_scrape) – existing scraper for this dashboard
- [Stack Overflow: javax.faces.ViewState scraping](https://stackoverflow.com/questions/35923080/web-scraping-a-website-with-javax-faces-viewstate)
- Vahan Dashboard: https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml
