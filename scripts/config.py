"""
Vahan Analytics - Data Fetch Configuration
"""
import os
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# DB (optional - for direct insert)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/vahan_analytics")

# data.gov.in API (get free key from https://data.gov.in)
DATAGOV_API_KEY = os.getenv("DATAGOVINDIA_API_KEY", "")

# State name normalization: source_name -> (state_code, canonical_name)
STATE_MAP = {
    "Andhra Pradesh": ("AP", "Andhra Pradesh"),
    "Arunachal Pradesh": ("AR", "Arunachal Pradesh"),
    "Assam": ("AS", "Assam"),
    "Bihar": ("BR", "Bihar"),
    "Chhattisgarh": ("CH", "Chhattisgarh"),
    "Goa": ("GA", "Goa"),
    "Gujarat": ("GJ", "Gujarat"),
    "Haryana": ("HR", "Haryana"),
    "Himachal Pradesh": ("HP", "Himachal Pradesh"),
    "Jammu and Kashmir": ("JK", "Jammu and Kashmir"),
    "Jammu & Kashmir": ("JK", "Jammu and Kashmir"),
    "Jharkhand": ("JH", "Jharkhand"),
    "Karnataka": ("KA", "Karnataka"),
    "Kerala": ("KL", "Kerala"),
    "Madhya Pradesh": ("MP", "Madhya Pradesh"),
    "Maharashtra": ("MH", "Maharashtra"),
    "Manipur": ("MN", "Manipur"),
    "Meghalaya": ("ML", "Meghalaya"),
    "Mizoram": ("MZ", "Mizoram"),
    "Nagaland": ("NL", "Nagaland"),
    "Odisha": ("OR", "Odisha"),
    "Orissa": ("OR", "Odisha"),
    "Punjab": ("PB", "Punjab"),
    "Rajasthan": ("RJ", "Rajasthan"),
    "Sikkim": ("SK", "Sikkim"),
    "Tamil Nadu": ("TN", "Tamil Nadu"),
    "Telangana": ("TG", "Telangana"),
    "Tripura": ("TR", "Tripura"),
    "Uttar Pradesh": ("UP", "Uttar Pradesh"),
    "Uttarakhand": ("UK", "Uttarakhand"),
    "Uttaranchal": ("UK", "Uttarakhand"),
    "West Bengal": ("WB", "West Bengal"),
    "Delhi": ("DL", "Delhi"),
    "NCT of Delhi": ("DL", "Delhi"),
    "Puducherry": ("PY", "Puducherry"),
    "Pondicherry": ("PY", "Puducherry"),
    "Lakshadweep": ("LD", "Lakshadweep"),
    "Andaman and Nicobar Islands": ("AN", "Andaman and Nicobar Islands"),
    "Andaman & Nicobar Islands": ("AN", "Andaman and Nicobar Islands"),
    "Dadra and Nagar Haveli and Daman and Diu": ("DN", "Dadra and Nagar Haveli and Daman and Diu"),
    "Chandigarh": ("CHD", "Chandigarh"),
    "Ladakh": ("LA", "Ladakh"),
    "All Vahan4 Running States (36/36)": ("ALL", "All India"),
    "All Vahan4 Running States (36_36)": ("ALL", "All India"),
    "All Vahan4 Running States (36 36)": ("ALL", "All India"),  # after _ -> space in filename parse
    "All India": ("ALL", "All India"),
}


def normalize_state(name: str):
    """Return (state_code, canonical_name) or None if unknown."""
    cleaned = str(name).strip()
    if not cleaned:
        return None
    hit = STATE_MAP.get(cleaned) or STATE_MAP.get(cleaned.replace("  ", " "))
    if hit:
        return hit
    low = cleaned.lower()
    for k, v in STATE_MAP.items():
        if k.lower() == low:
            return v
    return None
