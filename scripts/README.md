# Vahan Analytics - Data Fetch & Load Scripts

Scripts to fetch, validate, and load enriched datasets: Population, Per Capita Income, CNG Stations, EV Chargers.

**Research data is pre-populated** in `../research_data/` and `../vahan_research.db` (SQLite).

## Setup

```bash
cd scripts
pip install -r requirements.txt
```

## Data Sources (Validated)

| Dataset | Source | Update Cadence |
|---------|--------|----------------|
| **Population** | MOHFW Technical Group (NHM PDF) | One-time per Census |
| **Per Capita Income** | data.gov.in / RBI Handbook | Annual (Jun-Jul, Dec) |
| **CNG Stations** | PNGRB CGD MIS (PDF) | Monthly (5th) |
| **EV Chargers** | data.gov.in | Quarterly |

## Workflow

### 1. Population

```bash
# First run creates sample template
python fetch_population.py

# Download NHM report: https://main.mohfw.gov.in/reports-0
# Extract state-wise population table to CSV: state_name, year, population
# Save as data/population_raw.csv
python fetch_population.py
```

### 2. Per Capita Income

```bash
# Option A: Set API key for data.gov.in
export DATAGOVINDIA_API_KEY=your_key
python fetch_pci.py

# Option B: Manual CSV from data.gov.in
# Download from: https://data.gov.in/catalog/capita-net-state-domestic-product-current-prices
# Save as data/pci_raw.csv
python fetch_pci.py
```

### 3. CNG Stations

```bash
# Download CGD MIS PDF from https://pngrb.gov.in/eng-web/data-bank.html
# Save as data/20250630-CGD-MIS-Report.pdf (or similar)
python fetch_cng.py data/CGD-MIS-Report.pdf

# Or place PDF in data/ folder
python fetch_cng.py
```

### 4. EV Chargers

```bash
export DATAGOVINDIA_API_KEY=your_key
python fetch_ev_chargers.py

# Or manual CSV from data.gov.in
# Save as data/ev_chargers_raw.csv
python fetch_ev_chargers.py
```

### 5. Validate

```bash
python validate_data.py
```

### 6. Load to PostgreSQL

```bash
# Run migration first
psql $DATABASE_URL -f ../migrations/001_create_enriched_tables.sql

# Load all validated CSVs
export DATABASE_URL=postgresql://localhost/vahan_analytics
python load_to_db.py --all

# Or load individually
python load_to_db.py --population --pci --cng --ev
```

## data.gov.in API Key

Get a free API key from: https://data.gov.in/

Set as environment variable: `DATAGOVINDIA_API_KEY`

## Lab export (Parquet)

After PostgreSQL is loaded:

```bash
cd ..
pip install pyarrow sqlalchemy
python scripts/export_snapshot.py
```

Writes `exports/vahan_panel_<date>.parquet` and `exports/manifest.json` for the **complexity-lab** repo.

## Wholesale (optional)

1. Add CSVs under `data/wholesale/` (see column hints in `clean_wholesale.py`).
2. Run `python scripts/clean_wholesale.py` → `output/wholesale_cleaned/wholesale_long.csv`.
3. Apply `migrations/004_wholesale_sales.sql`, then `python scripts/load_wholesale_to_db.py`.

## File Layout

```
scripts/
├── config.py           # State mapping, paths
├── fetch_population.py
├── fetch_pci.py
├── fetch_cng.py
├── fetch_ev_chargers.py
├── validate_data.py
├── load_to_db.py
├── requirements.txt
├── data/               # Created on first run
│   ├── population_raw.csv
│   ├── population_validated.csv
│   ├── pci_raw.csv
│   ├── pci_validated.csv
│   ├── cng_validated.csv
│   └── ev_chargers_validated.csv
└── README.md
```
