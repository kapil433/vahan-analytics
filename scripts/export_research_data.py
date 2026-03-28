#!/usr/bin/env python3
"""
Export all validated data to a single research-ready folder.
Creates consolidated CSV/JSON for further research without needing a database.
"""
import json
from pathlib import Path

import pandas as pd

from config import DATA_DIR

RESEARCH_DIR = Path(__file__).resolve().parent.parent / "research_data"
RESEARCH_DIR.mkdir(exist_ok=True)


def main():
    datasets = [
        ("population", "population_validated.csv"),
        ("pci", "pci_validated.csv"),
        ("cng", "cng_validated.csv"),
        ("ev_chargers", "ev_chargers_validated.csv"),
    ]

    manifest = {"datasets": [], "sources": {}}

    for name, fname in datasets:
        path = DATA_DIR / fname
        if not path.exists():
            print(f"SKIP {name}: {fname} not found")
            continue
        df = pd.read_csv(path)
        # Export CSV
        out_csv = RESEARCH_DIR / f"{name}.csv"
        df.to_csv(out_csv, index=False)
        # Export JSON
        out_json = RESEARCH_DIR / f"{name}.json"
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(df.to_dict(orient="records"), f, indent=2)
        manifest["datasets"].append({
            "name": name,
            "rows": len(df),
            "columns": list(df.columns),
            "csv": str(out_csv.name),
            "json": str(out_json.name),
        })
        print(f"Exported {name}: {len(df)} rows -> {out_csv.name}")

    manifest["sources"] = {
        "population": "Census India 2011 + MOHFW Technical Group Projections 2011-2036",
        "pci": "MOSPI/RBI Per Capita NSDP (2011-12 series)",
        "cng": "PNGRB/PPAC CGD MIS, data.gov.in",
        "ev_chargers": "Ministry of Power, data.gov.in",
    }

    with open(RESEARCH_DIR / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nResearch data exported to: {RESEARCH_DIR}")
    print("Files: " + ", ".join(f.name for f in RESEARCH_DIR.iterdir()))


if __name__ == "__main__":
    main()
