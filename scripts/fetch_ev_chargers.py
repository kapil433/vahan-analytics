#!/usr/bin/env python3
"""
Fetch state-wise EV charging station counts from data.gov.in.
Source: https://www.data.gov.in/resource/stateut-wise-number-electric-vehicle-ev-charging-stations-installed-01-03-2025

Requires DATAGOVINDIA_API_KEY or manual CSV download.
"""
import os
from pathlib import Path

import pandas as pd
import requests

from config import DATA_DIR, normalize_state


# data.gov.in resource IDs (check catalog for latest)
EV_CHARGERS_RESOURCE_IDS = [
    "stateut-wise-number-electric-vehicle-ev-charging-stations-installed-01-03-2025",
    "year-wise-details-public-electric-vehicle-ev-charging-stations-pcs-deployed-country-31st",
]


def fetch_datagov_csv(resource_id: str, api_key: str) -> list[dict]:
    """Fetch CSV from data.gov.in resource."""
    # API format: https://api.data.gov.in/resource/{id}?api-key=xxx&format=csv
    url = f"https://api.data.gov.in/resource/{resource_id}"
    params = {"api-key": api_key, "format": "csv", "limit": 10000}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    # Parse CSV from response
    from io import StringIO
    df = pd.read_csv(StringIO(r.text))
    return df.to_dict("records")


def load_ev_from_csv(path: Path) -> list[dict]:
    """Load EV charger data from manually downloaded CSV."""
    df = pd.read_csv(path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df.to_dict("records")


def transform_ev_rows(rows: list[dict], default_year: int = None, default_month: int = None) -> list[dict]:
    """Transform to DB format."""
    from datetime import datetime
    default_year = default_year or datetime.now().year
    default_month = default_month or 12

    out = []
    for r in rows:
        state_keys = ["state", "state_name", "state_ut", "stateut", "name_of_state"]
        state = None
        for k in state_keys:
            if k in r and pd.notna(r.get(k)):
                state = str(r[k]).strip()
                break
        if not state:
            continue
        mapped = normalize_state(state)
        if not mapped:
            continue
        code, name = mapped

        count_keys = ["charger_count", "number_of_charging_stations", "charging_stations", "total", "count", "value"]
        count = None
        for k in count_keys:
            if k in r and pd.notna(r.get(k)):
                try:
                    count = int(float(r[k]))
                    break
                except (ValueError, TypeError):
                    pass
        if count is None:
            continue
        if count < 0:
            continue

        year = int(r.get("year") or default_year)
        month = int(r.get("month") or default_month)

        out.append({
            "state_code": code,
            "state_name": name,
            "year": year,
            "month": month,
            "charger_count": count,
            "charger_type": "total",
            "source": "data.gov.in",
        })
    return out


def main():
    api_key = os.getenv("DATAGOVINDIA_API_KEY", "")
    csv_path = DATA_DIR / "ev_chargers_raw.csv"
    out_path = DATA_DIR / "ev_chargers_validated.csv"

    rows = []

    if api_key:
        for rid in EV_CHARGERS_RESOURCE_IDS:
            try:
                records = fetch_datagov_csv(rid, api_key)
                if records:
                    rows = records
                    print(f"Fetched {len(rows)} records from data.gov.in ({rid})")
                    break
            except Exception as e:
                print(f"API fetch {rid} failed: {e}")

    if not rows and csv_path.exists():
        rows = load_ev_from_csv(csv_path)
        print(f"Loaded {len(rows)} rows from {csv_path}")

    if not rows:
        # Create sample
        sample = [
            {"state_name": "Karnataka", "charger_count": 6096},
            {"state_name": "Maharashtra", "charger_count": 4166},
            {"state_name": "Delhi", "charger_count": 1957},
            {"state_name": "Tamil Nadu", "charger_count": 1780},
            {"state_name": "Rajasthan", "charger_count": 1515},
            {"state_name": "Kerala", "charger_count": 1389},
        ]
        pd.DataFrame(sample).to_csv(csv_path, index=False)
        print(f"Created sample: {csv_path}")
        print("Download EV chargers CSV from data.gov.in and save as data/ev_chargers_raw.csv")
        print("Or set DATAGOVINDIA_API_KEY for API fetch")
        return

    validated = transform_ev_rows(rows)
    pd.DataFrame(validated).to_csv(out_path, index=False)
    print(f"Validated {len(validated)} rows -> {out_path}")
    print(f"Total chargers: {sum(r['charger_count'] for r in validated)}")


if __name__ == "__main__":
    main()
