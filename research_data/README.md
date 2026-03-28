# Vahan Analytics — Research Data

Enriched datasets for vehicle registration analytics research.  
**Sources validated | State-wise | 2012–2026**

**FY vs calendar (platform / API):** Merged Vahan + overlays use **calendar years 2012–2026**. Indian FY pickers and `GET /options` → `financial_years` expose **FY2012-13** through **FY2025-26** only; **FY2011-12** and **FY2026-27** are omitted as partial relative to that calendar window (see `config.scraping_config.financial_year_labels_analytics`).

---

## Datasets

| File | Rows | Description |
|------|------|-------------|
| **population.csv** | 525 | State population (2012–2026). Census 2011 + MOHFW projections, interpolated. |
| **pci.csv** | 116 | Per capita income (₹) by state and FY. MOSPI/RBI NSDP. |
| **cng.csv** | 62 | CNG stations by state, year, month. PNGRB/PPAC. |
| **ev_chargers.csv** | 62 | EV charging stations by state and year. Ministry of Power. |

---

## Sources

- **Population:** Census India 2011, MOHFW Technical Group Projections 2011–2036
- **Per Capita Income:** MOSPI/RBI Per Capita NSDP (2011-12 series)
- **CNG Stations:** PNGRB CGD MIS, data.gov.in, PPAC
- **EV Chargers:** Ministry of Power, data.gov.in

---

## Usage

```python
import pandas as pd

pop = pd.read_csv("population.csv")
pci = pd.read_csv("pci.csv")
cng = pd.read_csv("cng.csv")
ev = pd.read_csv("ev_chargers.csv")

# Example: Registrations per capita (join with Vahan data)
# reg_per_capita = vahan_regs / population
```

---

## Regenerate

```bash
cd ../scripts
python fetch_population.py
python fetch_pci.py
python fetch_cng.py
python fetch_ev_chargers.py
python validate_data.py
python export_research_data.py
```

---

*Last updated: March 2025*
