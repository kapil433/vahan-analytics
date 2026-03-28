#!/usr/bin/env python3
"""
Fetch state-wise population data (2011-2036).
Source: MOHFW Technical Group Population Projections
PDF: https://main.mohfw.gov.in/sites/default/files/Population%20Projection%20Report%202011-2036.pdf

This script expects a CSV file with columns: state_name, year, population
If you have the NHM PDF, extract tables to CSV manually or use pdfplumber.
Alternative: Use data.gov.in API if dataset available.
"""
import csv
import re
from pathlib import Path

import pandas as pd

from config import DATA_DIR, STATE_MAP, normalize_state


# Sample structure from NHM report - Table: Projected Total Population by State
# Columns vary by table; adjust as needed
EXPECTED_COLUMNS = ["state_name", "year", "population"]


def load_population_csv(path: Path) -> pd.DataFrame:
    """Load and validate population CSV."""
    df = pd.read_csv(path)
    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    required = ["state_name", "year", "population"]
    for r in required:
        if r not in df.columns:
            raise ValueError(f"Missing column: {r}. Found: {list(df.columns)}")
    return df


def validate_row(row: dict) -> list[str]:
    """Return list of validation errors for a row."""
    errs = []
    state = row.get("state_name", "")
    year = row.get("year")
    pop = row.get("population")

    if not state or pd.isna(state):
        errs.append("Empty state_name")
    elif not normalize_state(str(state).strip()):
        errs.append(f"Unknown state: {state}")

    if pd.isna(year):
        errs.append("Missing year")
    else:
        try:
            y = int(float(year))
            if y < 2011 or y > 2036:
                errs.append(f"Year {y} out of range 2011-2036")
        except (ValueError, TypeError):
            errs.append(f"Invalid year: {year}")

    if pd.isna(pop):
        errs.append("Missing population")
    else:
        try:
            p = int(float(pop))
            if p <= 0:
                errs.append(f"Population must be positive: {p}")
        except (ValueError, TypeError):
            errs.append(f"Invalid population: {pop}")

    return errs


def transform_to_db_format(df: pd.DataFrame) -> list[dict]:
    """Transform to DB insert format with validation."""
    rows = []
    errors = []
    for _, r in df.iterrows():
        row = r.to_dict()
        errs = validate_row(row)
        if errs:
            errors.append((row, errs))
            continue
        mapped = normalize_state(str(row["state_name"]).strip())
        if not mapped:
            continue
        code, name = mapped
        rows.append({
            "state_code": code,
            "state_name": name,
            "year": int(float(row["year"])),
            "population": int(float(row["population"])),
            "reference_date": "1st March",
            "source": "MOHFW_Technical_Group_2019",
        })
    if errors:
        print(f"Validation errors ({len(errors)} rows):")
        for row, errs in errors[:5]:
            print(f"  {row}: {errs}")
        if len(errors) > 5:
            print(f"  ... and {len(errors) - 5} more")
    return rows


def create_sample_csv(path: Path) -> None:
    """Create a sample CSV template for manual population data entry."""
    # Sample from NHM report - India total and a few states (illustrative)
    sample = [
        {"state_name": "India", "year": 2011, "population": 1210854977},
        {"state_name": "India", "year": 2021, "population": 1380004385},
        {"state_name": "India", "year": 2031, "population": 1518830000},
        {"state_name": "Maharashtra", "year": 2011, "population": 112374333},
        {"state_name": "Maharashtra", "year": 2021, "population": 124352000},
        {"state_name": "Gujarat", "year": 2011, "population": 60439692},
        {"state_name": "Gujarat", "year": 2021, "population": 70500000},
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["state_name", "year", "population"])
        w.writeheader()
        w.writerows(sample)
    print(f"Created sample: {path}")


def main():
    csv_path = DATA_DIR / "population_raw.csv"
    out_path = DATA_DIR / "population_validated.csv"

    if not csv_path.exists():
        create_sample_csv(csv_path)
        print("\nNext steps:")
        print("1. Download NHM report: https://main.mohfw.gov.in/reports-0")
        print("2. Extract state-wise population table to CSV with columns: state_name, year, population")
        print("3. Save as data/population_raw.csv")
        print("4. Run this script again")
        return

    df = load_population_csv(csv_path)
    rows = transform_to_db_format(df)

    # Filter India/All India if present (store state-level only)
    rows = [r for r in rows if r["state_code"] not in ("ALL", None)]

    # Interpolate missing years (2012-2026) for research continuity
    df = pd.DataFrame(rows)
    all_years = set(range(2012, 2027))
    existing_years = set(df["year"])
    if existing_years and all_years - existing_years:
        filled = []
        for (sc, sn), grp in df.groupby(["state_code", "state_name"]):
            grp = grp.sort_values("year")
            yrs = grp["year"].tolist()
            pops = grp["population"].tolist()
            for y in all_years:
                if y in grp["year"].values:
                    filled.append({"state_code": sc, "state_name": sn, "year": y, "population": int(grp[grp["year"] == y]["population"].iloc[0]), "reference_date": "1st March", "source": "MOHFW_Technical_Group_2019"})
                elif yrs:
                    # Linear interpolate
                    if y < min(yrs):
                        p = pops[0]
                    elif y > max(yrs):
                        p = pops[-1]
                    else:
                        for i in range(len(yrs) - 1):
                            if yrs[i] <= y <= yrs[i + 1]:
                                t = (y - yrs[i]) / (yrs[i + 1] - yrs[i])
                                p = pops[i] + t * (pops[i + 1] - pops[i])
                                break
                        else:
                            p = pops[-1]
                    filled.append({"state_code": sc, "state_name": sn, "year": y, "population": int(p), "reference_date": "1st March", "source": "MOHFW_Technical_Group_2019"})
        df = pd.DataFrame(filled)
    else:
        df["reference_date"] = "1st March"
        df["source"] = "MOHFW_Technical_Group_2019"

    df.to_csv(out_path, index=False)
    print(f"Validated {len(df)} rows -> {out_path}")
    print(f"States: {sorted(set(df['state_code']))}")
    print(f"Years: {sorted(set(df['year']))}")


if __name__ == "__main__":
    main()
