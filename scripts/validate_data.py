#!/usr/bin/env python3
"""
Validate enriched datasets: Population, PCI, CNG, EV.
Cross-checks and sanity validations.
"""
import sys
from pathlib import Path

import pandas as pd

from config import DATA_DIR, STATE_MAP


def validate_population(path: Path) -> list[str]:
    """Validate population CSV."""
    errs = []
    df = pd.read_csv(path)
    if "state_code" not in df.columns or "year" not in df.columns or "population" not in df.columns:
        errs.append("Missing required columns: state_code, year, population")
        return errs

    # Duplicates
    dupes = df[df.duplicated(subset=["state_code", "year"], keep=False)]
    if len(dupes) > 0:
        errs.append(f"Duplicate (state_code, year): {len(dupes)} rows")

    # Range checks
    bad_pop = df[(df["population"] <= 0) | (df["population"] > 2e9)]
    if len(bad_pop) > 0:
        errs.append(f"Invalid population range: {len(bad_pop)} rows")

    bad_year = df[(df["year"] < 2011) | (df["year"] > 2036)]
    if len(bad_year) > 0:
        errs.append(f"Year out of range 2011-2036: {len(bad_year)} rows")

    # YoY growth sanity (only when consecutive years)
    df_s = df.sort_values(["state_code", "year"])
    df_s["prev_pop"] = df_s.groupby("state_code")["population"].shift(1)
    df_s["prev_year"] = df_s.groupby("state_code")["year"].shift(1)
    df_s["year_diff"] = df_s["year"] - df_s["prev_year"]
    df_s["yoy_pct"] = (df_s["population"] - df_s["prev_pop"]) / df_s["prev_pop"] * 100
    # Only flag when consecutive years (diff=1) and growth > 10% (allows projection jumps)
    consecutive = df_s[df_s["year_diff"] == 1]
    extreme = consecutive[(consecutive["yoy_pct"].abs() > 10) & (consecutive["prev_pop"].notna())]
    if len(extreme) > 0:
        errs.append(f"Extreme YoY growth (>10% for consecutive years): {len(extreme)} rows - review")

    return errs


def validate_pci(path: Path) -> list[str]:
    """Validate per capita income CSV."""
    errs = []
    df = pd.read_csv(path)
    if "state_code" not in df.columns or "fy" not in df.columns or "pci_rs" not in df.columns:
        errs.append("Missing required columns: state_code, fy, pci_rs")
        return errs

    dupes = df[df.duplicated(subset=["state_code", "fy"], keep=False)]
    if len(dupes) > 0:
        errs.append(f"Duplicate (state_code, fy): {len(dupes)} rows")

    bad_pci = df[(df["pci_rs"] <= 0) | (df["pci_rs"] > 1e7)]
    if len(bad_pci) > 0:
        errs.append(f"Invalid PCI range: {len(bad_pci)} rows")

    return errs


def validate_cng(path: Path) -> list[str]:
    """Validate CNG stations CSV."""
    errs = []
    df = pd.read_csv(path)
    required = ["state_code", "year", "month", "station_count"]
    for c in required:
        if c not in df.columns:
            errs.append(f"Missing column: {c}")
            return errs

    dupes = df[df.duplicated(subset=["state_code", "year", "month"], keep=False)]
    if len(dupes) > 0:
        errs.append(f"Duplicate (state_code, year, month): {len(dupes)} rows")

    bad_month = df[(df["month"] < 1) | (df["month"] > 12)]
    if len(bad_month) > 0:
        errs.append(f"Invalid month: {len(bad_month)} rows")

    bad_count = df[df["station_count"] < 0]
    if len(bad_count) > 0:
        errs.append(f"Negative station count: {len(bad_count)} rows")

    return errs


def validate_ev(path: Path) -> list[str]:
    """Validate EV chargers CSV."""
    errs = []
    df = pd.read_csv(path)
    required = ["state_code", "year", "charger_count"]
    for c in required:
        if c not in df.columns:
            errs.append(f"Missing column: {c}")
            return errs

    dupes = df[df.duplicated(subset=["state_code", "year"], keep=False)]
    if len(dupes) > 0:
        errs.append(f"Duplicate (state_code, year): {len(dupes)} rows")

    bad_count = df[df["charger_count"] < 0]
    if len(bad_count) > 0:
        errs.append(f"Negative charger count: {len(bad_count)} rows")

    return errs


def main():
    checks = [
        ("population_validated.csv", validate_population),
        ("pci_validated.csv", validate_pci),
        ("cng_validated.csv", validate_cng),
        ("ev_chargers_validated.csv", validate_ev),
    ]
    all_ok = True
    for fname, validator in checks:
        path = DATA_DIR / fname
        if not path.exists():
            print(f"SKIP {fname}: file not found")
            continue
        errs = validator(path)
        if errs:
            print(f"FAIL {fname}:")
            for e in errs:
                print(f"  - {e}")
            all_ok = False
        else:
            print(f"OK   {fname}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
