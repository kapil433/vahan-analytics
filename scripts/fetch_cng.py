#!/usr/bin/env python3
"""
Fetch state-wise CNG station counts from PNGRB CGD MIS reports.
Source: https://pngrb.gov.in/eng-web/data-bank.html
Reports are PDF; this script parses a downloaded PDF or CSV export.

Manual: Download CGD-MIS-Report.pdf from pngrb.gov.in/data-bank/
Then run: python fetch_cng.py data/CGD-MIS-Report.pdf
Or place PDF in data/ and run without args.
"""
import re
import sys
from pathlib import Path

import pdfplumber

from config import DATA_DIR, normalize_state


def extract_tables_from_pdf(pdf_path: Path) -> list[list]:
    """Extract all tables from PDF using pdfplumber."""
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_tables = page.extract_tables()
            if page_tables:
                tables.extend(page_tables)
    return tables


def find_state_cng_in_tables(tables: list[list]) -> list[dict]:
    """
    PNGRB MIS format varies. Look for tables with state names and CNG counts.
    Returns list of {state_name, station_count, year, month}.
    """
    results = []
    state_pattern = re.compile(
        r"^(Andhra Pradesh|Arunachal Pradesh|Assam|Bihar|Chhattisgarh|Goa|"
        r"Gujarat|Haryana|Himachal Pradesh|Jammu|Jharkhand|Karnataka|Kerala|"
        r"Madhya Pradesh|Maharashtra|Manipur|Meghalaya|Mizoram|Nagaland|"
        r"Odisha|Punjab|Rajasthan|Sikkim|Tamil Nadu|Telangana|Tripura|"
        r"Uttar Pradesh|Uttarakhand|West Bengal|Delhi|Puducherry|"
        r"Lakshadweep|Andaman|Chandigarh|Dadra|Daman|Diu)", re.I
    )
    for table in tables:
        if not table or len(table) < 2:
            continue
        headers = [str(c).strip().lower() if c else "" for c in table[0]]
        for row in table[1:]:
            if not row:
                continue
            first_cell = str(row[0]).strip() if row[0] else ""
            if not state_pattern.search(first_cell):
                continue
            state_name = first_cell
            # Find numeric column (CNG stations)
            count = None
            for i, cell in enumerate(row[1:], 1):
                if cell is None:
                    continue
                s = str(cell).replace(",", "").strip()
                if s.isdigit():
                    count = int(s)
                    break
            if count is not None and normalize_state(state_name):
                results.append({
                    "state_name": state_name,
                    "station_count": count,
                })
    return results


def parse_cng_from_csv(csv_path: Path) -> list[dict]:
    """Parse CNG data from CSV (e.g. OGD export)."""
    import pandas as pd
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    results = []
    state_col = next((c for c in df.columns if "state" in c or "state_ut" in c), None)
    count_col = next((c for c in df.columns if "cng" in c or "station" in c or "number" in c), None)
    if not state_col or not count_col:
        raise ValueError(f"Need state and count columns. Found: {list(df.columns)}")
    year_col = next((c for c in df.columns if c == "year"), None)
    month_col = next((c for c in df.columns if c == "month"), None)
    from datetime import datetime
    now = datetime.now()
    for _, r in df.iterrows():
        state = r.get(state_col)
        if pd.isna(state):
            continue
        mapped = normalize_state(str(state).strip())
        if not mapped:
            continue
        code, name = mapped
        try:
            cnt = int(float(r[count_col]))
        except (ValueError, TypeError):
            continue
        year = int(r[year_col]) if year_col and pd.notna(r.get(year_col)) else now.year
        month = int(r[month_col]) if month_col and pd.notna(r.get(month_col)) else 12
        results.append({
            "state_code": code,
            "state_name": name,
            "year": year,
            "month": month,
            "station_count": cnt,
            "source": "PNGRB_CGD_MIS",
        })
    return results


def main():
    if len(sys.argv) > 1:
        input_path = Path(sys.argv[1])
    else:
        # Look for CSV first
        csv_path = DATA_DIR / "cng_raw.csv"
        if csv_path.exists():
            input_path = csv_path
        else:
            # Look for latest PDF in data/
            pdfs = list(DATA_DIR.glob("*.pdf"))
            if not pdfs:
                pdfs = list(DATA_DIR.glob("**/CGD*.pdf"))
            input_path = pdfs[0] if pdfs else None

    if not input_path or not input_path.exists():
        print("Usage: python fetch_cng.py <path-to-CGD-MIS-Report.pdf>")
        print("Or: place CGD-MIS-Report.pdf in data/ folder")
        print("Download from: https://pngrb.gov.in/eng-web/data-bank.html")
        return

    out_path = DATA_DIR / "cng_validated.csv"

    if input_path.suffix.lower() == ".csv":
        rows = parse_cng_from_csv(input_path)
    else:
        tables = extract_tables_from_pdf(input_path)
        raw = find_state_cng_in_tables(tables)
        # Add year/month from filename or default to current
        from datetime import datetime
        now = datetime.now()
        year = now.year
        month = now.month
        # Try extract from filename e.g. 20250630-CGD-MIS-Report.pdf
        m = re.search(r"(\d{4})(\d{2})\d{2}", input_path.name)
        if m:
            year, month = int(m.group(1)), int(m.group(2))
        rows = []
        for r in raw:
            mapped = normalize_state(r["state_name"])
            if mapped:
                code, name = mapped
                rows.append({
                    "state_code": code,
                    "state_name": name,
                    "year": year,
                    "month": month,
                    "station_count": r["station_count"],
                    "source": "PNGRB_CGD_MIS",
                })

    if not rows:
        print("No CNG data extracted. Check PDF structure.")
        return

    import pandas as pd
    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"Extracted {len(rows)} state records -> {out_path}")
    print(f"Total stations: {sum(r['station_count'] for r in rows)}")


if __name__ == "__main__":
    main()
