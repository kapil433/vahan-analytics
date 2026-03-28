#!/usr/bin/env python3
"""
Fetch state-wise Per Capita Income (Per Capita NSDP) data.
Sources:
  - data.gov.in: https://data.gov.in/catalog/capita-net-state-domestic-product-current-prices
  - RBI Handbook of Statistics (Excel) - manual download
  - AP data.gov: State/UT-wise Per Capita NSDP 2017-18 to 2022-23

Requires DATAGOVINDIA_API_KEY for data.gov.in API.
Or use manual CSV download.
"""
import csv
import os
import re
from pathlib import Path

import pandas as pd
import requests

from config import DATA_DIR, normalize_state


def fetch_datagov_resource(resource_id: str, api_key: str, limit: int = 10000) -> list[dict]:
    """Fetch records from data.gov.in API."""
    url = "https://api.data.gov.in/resource/{resource_id}".format(resource_id=resource_id)
    params = {"api-key": api_key, "format": "json", "limit": limit, "offset": 0}
    all_records = []
    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        records = data.get("records", [])
        if not records:
            break
        all_records.extend(records)
        if len(records) < limit:
            break
        params["offset"] += limit
    return all_records


def parse_fy(fy_str: str) -> str | None:
    """Normalize FY string to YYYY-YY format."""
    if pd.isna(fy_str):
        return None
    s = str(fy_str).strip()
    # Match 2012-13, 2012-2013, FY 2012-13, etc.
    m = re.search(r"(\d{4})[-_](\d{2,4})", s)
    if m:
        y1, y2 = m.group(1), m.group(2)
        if len(y2) == 2:
            return f"{y1}-{y2}"
        return f"{y1}-{y2[-2:]}"
    return None


def validate_pci_row(row: dict) -> list[str]:
    errs = []
    state = row.get("state_name") or row.get("state") or row.get("stateut") or row.get("state_ut")
    if not state or pd.isna(state):
        errs.append("Missing state")
    elif not normalize_state(str(state).strip()):
        errs.append(f"Unknown state: {state}")

    fy = row.get("fy") or row.get("financial_year") or row.get("year")
    if not parse_fy(str(fy) if fy is not None else ""):
        errs.append(f"Invalid FY: {fy}")

    pci = row.get("pci_rs") or row.get("per_capita_nsdp") or row.get("nsdp") or row.get("value")
    if pci is None or pd.isna(pci):
        errs.append("Missing PCI value")
    else:
        try:
            v = float(pci)
            if v <= 0:
                errs.append(f"PCI must be positive: {v}")
        except (ValueError, TypeError):
            errs.append(f"Invalid PCI: {pci}")

    return errs


def transform_pci(rows: list[dict]) -> list[dict]:
    """Transform to DB format."""
    out = []
    for r in rows:
        state_key = "state_name" if "state_name" in r else ("state" if "state" in r else "stateut")
        state = r.get(state_key, "")
        fy = parse_fy(str(r.get("fy") or r.get("financial_year") or r.get("year", "")))
        pci_raw = r.get("pci_rs") or r.get("per_capita_nsdp") or r.get("nsdp") or r.get("value")
        if not state or not fy or pci_raw is None:
            continue
        mapped = normalize_state(str(state).strip())
        if not mapped:
            continue
        code, name = mapped
        try:
            pci_val = float(pci_raw)
        except (ValueError, TypeError):
            continue
        if pci_val <= 0:
            continue
        out.append({
            "state_code": code,
            "state_name": name,
            "fy": fy,
            "pci_rs": round(pci_val, 2),
            "source": "MOSPI",
        })
    return out


def load_from_csv(path: Path) -> list[dict]:
    """Load PCI from CSV (manual download from data.gov.in)."""
    df = pd.read_csv(path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df.to_dict("records")


def main():
    api_key = os.getenv("DATAGOVINDIA_API_KEY", "")
    csv_path = DATA_DIR / "pci_raw.csv"
    out_path = DATA_DIR / "pci_validated.csv"

    rows = []

    if api_key:
        # Try data.gov.in API - resource IDs vary; check catalog
        # Example: https://data.gov.in/resource/stateut-wise-details-capita-net-state-domestic-product-current-prices-base-year-2011-12
        resource_id = "36ec39af-cc90-4d0e-8665-a1704b14058c"  # Example - replace with actual
        try:
            records = fetch_datagov_resource(resource_id, api_key)
            if records:
                rows = records
                print(f"Fetched {len(rows)} records from data.gov.in")
        except Exception as e:
            print(f"API fetch failed: {e}. Using CSV fallback.")

    if not rows and csv_path.exists():
        rows = load_from_csv(csv_path)
        print(f"Loaded {len(rows)} rows from {csv_path}")

    if not rows:
        # Create sample template
        sample = [
            {"state_name": "Maharashtra", "fy": "2017-18", "pci_rs": 198762},
            {"state_name": "Maharashtra", "fy": "2022-23", "pci_rs": 258000},
            {"state_name": "Gujarat", "fy": "2017-18", "pci_rs": 215654},
            {"state_name": "Gujarat", "fy": "2022-23", "pci_rs": 275000},
        ]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["state_name", "fy", "pci_rs"])
            w.writeheader()
            w.writerows(sample)
        print(f"Created sample: {csv_path}")
        print("Download PCI CSV from data.gov.in and save as data/pci_raw.csv")
        return

    validated = transform_pci(rows)
    out_df = pd.DataFrame(validated)
    out_df.to_csv(out_path, index=False)
    print(f"Validated {len(validated)} rows -> {out_path}")
    print(f"FY range: {min(r['fy'] for r in validated)} - {max(r['fy'] for r in validated)}")


if __name__ == "__main__":
    main()
