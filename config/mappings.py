"""
Master mappings for Vahan pipeline: OEM (maker), fuel, and Indian financial year.

Used by scripts/clean_vahan_data.py and api/master_bundle.py so all states share one path.
Dashboard + legacy HTML must use the same FUEL_CANONICAL and OEM short labels.

Fuel: only CNG, Petrol, EV, Diesel, Strong Hybrid (LPG/PNG → CNG; unknown → Petrol).
OEM: portal names normalize to canonical UPPER; oem_short_display() → short UI label (Maruti, Tata, …).
"""

from __future__ import annotations

import re
import unicodedata

# Exact order used in bundle + charts (matches dashboard FUEL_CANONICAL_ORDER).
FUEL_CANONICAL: tuple[str, ...] = ("CNG", "Petrol", "EV", "Diesel", "Strong Hybrid")
FUEL_CANONICAL_SET: frozenset[str] = frozenset(FUEL_CANONICAL)

# Raw / portal variants → one of FUEL_CANONICAL (never LPG/Unknown as outputs).

FUEL_MAP: dict[str, str] = {
    # CNG / gas
    "CNG": "CNG",
    "cng": "CNG",
    "LPG": "CNG",
    "lpg": "CNG",
    "PNG": "CNG",
    # Petrol
    "PETROL": "Petrol",
    "Petrol": "Petrol",
    "petrol": "Petrol",
    "GASOLINE": "Petrol",
    "MS": "Petrol",
    "MOTOR SPIRIT": "Petrol",
    # Diesel
    "DIESEL": "Diesel",
    "Diesel": "Diesel",
    "diesel": "Diesel",
    "HSD": "Diesel",
    # EV / electric
    "EV": "EV",
    "ELECTRIC VEHICLE(EV)": "EV",
    "ELECTRIC VEHICLE (EV)": "EV",
    "ELECTRIC VEHICLE": "EV",
    "ELECTRIC(EV)": "EV",
    "ELECTRIC (EV)": "EV",
    "ELECTRIC": "EV",
    "Electric": "EV",
    "ELECTRIC VEHICLES": "EV",
    "BATTERY ELECTRIC VEHICLE": "EV",
    "BEV": "EV",
    "PURE EV": "EV",
    "PURE ELECTRIC": "EV",
    # Strong hybrid / hybrid (portal variants)
    "STRONG HYBRID EV": "Strong Hybrid",
    "STRONG HYBRID": "Strong Hybrid",
    "Strong Hybrid": "Strong Hybrid",
    "HYBRID": "Strong Hybrid",
    "Hybrid": "Strong Hybrid",
    "HYBRID EV": "Strong Hybrid",
    "MILD HYBRID": "Strong Hybrid",
    "MILD HYBRID EV": "Strong Hybrid",
    "PLUG-IN HYBRID": "Strong Hybrid",
    "PLUGIN HYBRID": "Strong Hybrid",
    "PLUG IN HYBRID": "Strong Hybrid",
    "PHEV": "Strong Hybrid",
    "SELF CHARGING HYBRID": "Strong Hybrid",
    # Unmapped / junk → Petrol (single ICE bucket for odd portal values)
    "UNKNOWN": "Petrol",
    "Unknown": "Petrol",
    "OTHERS": "Petrol",
    "OTHER": "Petrol",
    "NA": "Petrol",
    "N/A": "Petrol",
    "-": "Petrol",
}

# Compact uppercase alphanumerics -> canonical (for noisy portal strings)
FUEL_COMPACT_MAP: dict[str, str] = {
    "CNG": "CNG",
    "LPG": "CNG",
    "PNG": "CNG",
    "PETROL": "Petrol",
    "DIESEL": "Diesel",
    "ELECTRICVEHICLEEV": "EV",
    "ELECTRICVEHICLE": "EV",
    "STRONGHYBRIDEV": "Strong Hybrid",
    "STRONGHYBRID": "Strong Hybrid",
    "HYBRID": "Strong Hybrid",
    "MILDHYBRID": "Strong Hybrid",
    "PLUGINHYBRID": "Strong Hybrid",
    "PLUG_INHYBRID": "Strong Hybrid",
}


def _fuel_synonyms_extra() -> None:
    """Register case/space variants that are cheap to derive from base keys."""
    extras: dict[str, str] = {}
    for k, v in list(FUEL_MAP.items()):
        extras[k.upper()] = v
        extras[k.lower()] = v
        collapsed = re.sub(r"\s+", " ", k.strip())
        if collapsed != k:
            extras[collapsed] = v
    FUEL_MAP.update(extras)


_fuel_synonyms_extra()

# --- OEM: alias (uppercase key) -> canonical DB-style uppercase name (then oem_short_display → UI short) ---

MAKER_MAP: dict[str, str] = {
    # Maruti
    "MARUTI UDYOG LTD": "MARUTI SUZUKI INDIA LTD",
    "MARUTI UDYOG LIMITED": "MARUTI SUZUKI INDIA LTD",
    "MARUTI SUZUKI INDIA LIMITED": "MARUTI SUZUKI INDIA LTD",
    "SUZUKI MOTOR GUJARAT PRIVATE LIMITED": "MARUTI SUZUKI INDIA LTD",
    "SUZUKI MOTOR GUJARAT PVT LTD": "MARUTI SUZUKI INDIA LTD",
    # Tata
    "TATA MOTORS LIMITED": "TATA MOTORS LTD",
    "TATA MOTORS LTD.": "TATA MOTORS LTD",
    "TATA MOTORS PV LTD": "TATA MOTORS PASSENGER VEHICLES LTD",
    # Hyundai
    "HYUNDAI MOTOR INDIA LIMITED": "HYUNDAI MOTOR INDIA LTD",
    "HYUNDAI MOTORS INDIA LTD": "HYUNDAI MOTOR INDIA LTD",
    # Mahindra
    "MAHINDRA & MAHINDRA LTD": "MAHINDRA & MAHINDRA LIMITED",
    "M&M LIMITED": "MAHINDRA & MAHINDRA LIMITED",
    "M & M LIMITED": "MAHINDRA & MAHINDRA LIMITED",
    # Honda
    "HONDA CARS INDIA LIMITED": "HONDA CARS INDIA LTD",
    "HONDA SIEL CARS INDIA LTD": "HONDA CARS INDIA LTD",
    # Toyota / Kia
    "TOYOTA KIRLOSKAR MOTOR PRIVATE LIMITED": "TOYOTA KIRLOSKAR MOTOR PVT LTD",
    "KIA INDIA PVT LTD": "KIA INDIA PRIVATE LIMITED",
    # MG
    "MG MOTOR INDIA PVT LTD": "JSW MG MOTOR INDIA PVT LTD",
    "MG MOTOR INDIA PRIVATE LIMITED": "JSW MG MOTOR INDIA PVT LTD",
    "JSW MG MOTOR INDIA PRIVATE LIMITED": "JSW MG MOTOR INDIA PVT LTD",
    # Renault–Nissan
    "RENAULT NISSAN AUTOMOTIVE INDIA PVT LTD": "RENAULT NISSAN AUTOMOTIVE INDIA PRIVATE LIMITED",
    # VW group
    "VW GROUP SALES INDIA PVT LTD": "SKODA AUTO VOLKSWAGEN INDIA PVT LTD",
    "VOLKSWAGEN GROUP SALES INDIA PRIVATE LIMITED": "SKODA AUTO VOLKSWAGEN INDIA PVT LTD",
    "AUDI INDIA PRIVATE LIMITED": "AUDI AG",
    "AUDI INDIA PVT LTD": "AUDI AG",
    # Mercedes
    "MERCEDES -BENZ AG": "MERCEDES-BENZ INDIA PVT LTD",
    "MERCEDES-BENZ AG": "MERCEDES-BENZ INDIA PVT LTD",
    "MERCEDES BENZ INDIA PVT LTD": "MERCEDES-BENZ INDIA PVT LTD",
    "MERCEDES-BENZ INDIA PRIVATE LIMITED": "MERCEDES-BENZ INDIA PVT LTD",
    "DAIMLER INDIA COMMERCIAL VEHICLES PVT LTD": "MERCEDES-BENZ INDIA PVT LTD",
    # Stellantis / legacy FCA
    "FCA INDIA AUTOMOBILES PVT LTD": "STELLANTIS AUTOMOBILES INDIA PVT LTD",
    "FIAT INDIA AUTOMOBILES PVT LTD": "STELLANTIS AUTOMOBILES INDIA PVT LTD",
    "JEEP INDIA": "STELLANTIS AUTOMOBILES INDIA PVT LTD",
    "CITROEN INDIA": "STELLANTIS AUTOMOBILES INDIA PVT LTD",
    # GM / Chevrolet (often still in historical rows)
    "GENERAL MOTORS INDIA PVT LTD": "CHEVROLET SALES INDIA PVT LTD",
    "GENERAL MOTORS INDIA PRIVATE LIMITED": "CHEVROLET SALES INDIA PVT LTD",
    "GM INDIA": "CHEVROLET SALES INDIA PVT LTD",
    "CHEVROLET": "CHEVROLET SALES INDIA PVT LTD",
    # Two-wheeler / CV names that appear in some state portals
    "FORCE MOTORS LTD": "FORCE MOTORS LTD",
    "FORCE MOTORS LIMITED": "FORCE MOTORS LTD",
    "ASHOK LEYLAND LTD": "ASHOK LEYLAND LTD",
    "ASHOK LEYLAND LIMITED": "ASHOK LEYLAND LTD",
    "EICHER MOTORS LIMITED": "EICHER MOTORS LIMITED",
    "EICHER MOTORS LTD": "EICHER MOTORS LIMITED",
    "VE COMMERCIAL VEHICLES LIMITED": "EICHER MOTORS LIMITED",
    "TVS MOTOR COMPANY LTD": "TVS MOTOR COMPANY LTD",
    "TVS MOTOR COMPANY LIMITED": "TVS MOTOR COMPANY LTD",
    "HERO MOTOCORP LTD": "HERO MOTOCORP LTD",
    "HERO MOTOCORP LIMITED": "HERO MOTOCORP LTD",
    "BAJAJ AUTO LTD": "BAJAJ AUTO LTD",
    "BAJAJ AUTO LIMITED": "BAJAJ AUTO LTD",
    "ROYAL ENFIELD": "ROYAL ENFIELD",
    "ROYAL ENFIELD LTD": "ROYAL ENFIELD",
    # Identity rows for common canonical OEM strings (no alias)
    "MARUTI SUZUKI INDIA LTD": "MARUTI SUZUKI INDIA LTD",
    "TATA MOTORS LTD": "TATA MOTORS LTD",
    "TATA MOTORS PASSENGER VEHICLES LTD": "TATA MOTORS PASSENGER VEHICLES LTD",
    "TATA PASSENGER ELECTRIC MOBILITY LTD": "TATA PASSENGER ELECTRIC MOBILITY LTD",
    "HYUNDAI MOTOR INDIA LTD": "HYUNDAI MOTOR INDIA LTD",
    "HYUNDAI MOTORS LTD, SOUTH KOREA": "HYUNDAI MOTORS LTD, SOUTH KOREA",
    "MAHINDRA & MAHINDRA LIMITED": "MAHINDRA & MAHINDRA LIMITED",
    "MAHINDRA ELECTRIC MOBILITY LIMITED": "MAHINDRA ELECTRIC MOBILITY LIMITED",
    "MAHINDRA LAST MILE MOBILITY LTD": "MAHINDRA LAST MILE MOBILITY LTD",
    "MAHINDRA ELECTRIC AUTOMOBILE LTD": "MAHINDRA ELECTRIC AUTOMOBILE LTD",
    "HONDA CARS INDIA LTD": "HONDA CARS INDIA LTD",
    "TOYOTA KIRLOSKAR MOTOR PVT LTD": "TOYOTA KIRLOSKAR MOTOR PVT LTD",
    "KIA INDIA PRIVATE LIMITED": "KIA INDIA PRIVATE LIMITED",
    "KIA MOTORS CORPN": "KIA MOTORS CORPN",
    "JSW MG MOTOR INDIA PVT LTD": "JSW MG MOTOR INDIA PVT LTD",
    "MG CAR COMPANY LIMITED": "MG CAR COMPANY LIMITED",
    "RENAULT INDIA PVT LTD": "RENAULT INDIA PVT LTD",
    "RENAULT NISSAN AUTOMOTIVE INDIA PRIVATE LIMITED": "RENAULT NISSAN AUTOMOTIVE INDIA PRIVATE LIMITED",
    "VOLKSWAGEN INDIA PVT LTD": "VOLKSWAGEN INDIA PVT LTD",
    "SKODA AUTO VOLKSWAGEN INDIA PVT LTD": "SKODA AUTO VOLKSWAGEN INDIA PVT LTD",
    "SKODA AUTO INDIA PVT LTD": "SKODA AUTO INDIA PVT LTD",
    "MERCEDES-BENZ INDIA PVT LTD": "MERCEDES-BENZ INDIA PVT LTD",
    "BMW INDIA PVT LTD": "BMW INDIA PVT LTD",
    "AUDI AG": "AUDI AG",
    "FORD INDIA PVT LTD": "FORD INDIA PVT LTD",
    "NISSAN MOTOR INDIA PVT LTD": "NISSAN MOTOR INDIA PVT LTD",
    "STELLANTIS INDIA PVT LTD": "STELLANTIS INDIA PVT LTD",
    "STELLANTIS AUTOMOBILES INDIA PVT LTD": "STELLANTIS AUTOMOBILES INDIA PVT LTD",
    "ISUZU MOTORS INDIA PVT LTD": "ISUZU MOTORS INDIA PVT LTD",
    "VOLVO AUTO INDIA PVT LTD": "VOLVO AUTO INDIA PVT LTD",
    "PIAGGIO VEHICLES PVT LTD": "PIAGGIO VEHICLES PVT LTD",
    "CHEVROLET SALES INDIA PVT LTD": "CHEVROLET SALES INDIA PVT LTD",
    "OTHERS": "OTHERS",
}

# Canonical UPPER (post-normalize_maker) → short UI label (top-10 + Others collapse in master_bundle).
# Keep in sync with dashboard MAKER_MAP_JS values.
_CANONICAL_TO_SHORT: dict[str, str] = {
    "MARUTI SUZUKI INDIA LTD": "Maruti",
    "TATA MOTORS LTD": "Tata",
    "TATA MOTORS PASSENGER VEHICLES LTD": "Tata",
    "TATA PASSENGER ELECTRIC MOBILITY LTD": "Tata",
    "HYUNDAI MOTOR INDIA LTD": "Hyundai",
    "HYUNDAI MOTORS LTD, SOUTH KOREA": "Hyundai",
    "MAHINDRA & MAHINDRA LIMITED": "Mahindra",
    "MAHINDRA ELECTRIC MOBILITY LIMITED": "Mahindra",
    "MAHINDRA LAST MILE MOBILITY LTD": "Mahindra",
    "MAHINDRA ELECTRIC AUTOMOBILE LTD": "Mahindra",
    "HONDA CARS INDIA LTD": "Honda",
    "TOYOTA KIRLOSKAR MOTOR PVT LTD": "Toyota",
    "KIA INDIA PRIVATE LIMITED": "Kia",
    "KIA MOTORS CORPN": "Kia",
    "JSW MG MOTOR INDIA PVT LTD": "MG",
    "MG CAR COMPANY LIMITED": "MG",
    "RENAULT INDIA PVT LTD": "Renault",
    "RENAULT NISSAN AUTOMOTIVE INDIA PRIVATE LIMITED": "Renault",
    "VOLKSWAGEN INDIA PVT LTD": "VW",
    "SKODA AUTO VOLKSWAGEN INDIA PVT LTD": "VW",
    "SKODA AUTO INDIA PVT LTD": "VW",
    "MERCEDES-BENZ INDIA PVT LTD": "Mercedes",
    "BMW INDIA PVT LTD": "BMW",
    "AUDI AG": "Audi",
    "FORD INDIA PVT LTD": "Ford",
    "CHEVROLET SALES INDIA PVT LTD": "Chevy",
    "NISSAN MOTOR INDIA PVT LTD": "Nissan",
    "STELLANTIS INDIA PVT LTD": "Stellantis",
    "STELLANTIS AUTOMOBILES INDIA PVT LTD": "Stellantis",
    "ISUZU MOTORS INDIA PVT LTD": "Isuzu",
    "VOLVO AUTO INDIA PVT LTD": "Volvo",
    "PIAGGIO VEHICLES PVT LTD": "Piaggio",
    "ASHOK LEYLAND LTD": "Ashok Leyland",
    "BAJAJ AUTO LTD": "Bajaj",
    "FORCE MOTORS LTD": "Force",
    "EICHER MOTORS LIMITED": "Eicher",
    "TVS MOTOR COMPANY LTD": "TVS",
    "HERO MOTOCORP LTD": "Hero",
    "ROYAL ENFIELD": "Royal Enfield",
    "OTHERS": "Others",
}


def _build_oem_upper_to_short() -> dict[str, str]:
    out: dict[str, str] = {}
    for canon, short in _CANONICAL_TO_SHORT.items():
        out[canon.upper()] = short
    for raw, canon in MAKER_MAP.items():
        short = _CANONICAL_TO_SHORT.get(canon)
        if short:
            out[raw.upper()] = short
    return out


OEM_UPPER_TO_SHORT: dict[str, str] = _build_oem_upper_to_short()


def _strip_noise(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_fuel(raw: str) -> str:
    """Map portal fuel strings to exactly one of FUEL_CANONICAL."""
    if raw is None or (isinstance(raw, float) and str(raw) == "nan"):
        return "Petrol"
    s = _strip_noise(str(raw))
    if not s:
        return "Petrol"
    if s in FUEL_MAP:
        v = FUEL_MAP[s]
        return v if v in FUEL_CANONICAL_SET else "Petrol"
    u = s.upper()
    if u in FUEL_MAP:
        v = FUEL_MAP[u]
        return v if v in FUEL_CANONICAL_SET else "Petrol"
    compact = re.sub(r"[^A-Z0-9]", "", u)
    if compact in FUEL_COMPACT_MAP:
        v = FUEL_COMPACT_MAP[compact]
        return v if v in FUEL_CANONICAL_SET else "Petrol"
    for k, v in FUEL_MAP.items():
        if k.upper() == u:
            return v if v in FUEL_CANONICAL_SET else "Petrol"
    return "Petrol"


def normalize_maker(raw: str) -> str:
    """Map OEM aliases to a single canonical uppercase name."""
    if raw is None or (isinstance(raw, float) and str(raw) == "nan"):
        return ""
    s = _strip_noise(str(raw))
    if not s:
        return ""
    upper = s.upper()
    if upper in MAKER_MAP:
        return MAKER_MAP[upper]
    compact = re.sub(r"[^A-Z0-9]", "", upper)
    for k, v in MAKER_MAP.items():
        if re.sub(r"[^A-Z0-9]", "", k) == compact:
            return v
    return upper


def oem_short_display(normalized_upper: str) -> str:
    """Map normalize_maker() output (UPPER) to dashboard short label; unknown → Others."""
    u = (normalized_upper or "").strip().upper()
    if not u:
        return "Others"
    if u in OEM_UPPER_TO_SHORT:
        return OEM_UPPER_TO_SHORT[u]
    for key, short in OEM_UPPER_TO_SHORT.items():
        if len(key) >= 12 and (key in u or u in key):
            return short
    return "Others"


def maker_strings_for_ui_short(label: str) -> set[str]:
    """
    Expand a dashboard OEM filter (short label like 'Maruti') to all DB / portal spellings
    for SQL IN (...). 'Others' is not expandable here.
    """
    s = (label or "").strip()
    if not s or s == "Others":
        return set()
    acc: set[str] = set()
    for key, short in OEM_UPPER_TO_SHORT.items():
        if short == s:
            acc.add(key)
            nm = normalize_maker(key)
            if nm:
                acc.add(nm)
    for raw, canon in MAKER_MAP.items():
        if oem_short_display(canon) == s:
            acc.add(raw)
            acc.add(canon)
    return acc


def fy_start_year(calendar_year: int, month: int) -> int:
    """India FY April–March: FY starts in April of fy_start_year."""
    if month >= 4:
        return calendar_year
    return calendar_year - 1


def month_to_fy(calendar_year: int, month: int) -> str:
    """Label like FY2024-25 for Indian financial year containing this calendar month."""
    start = fy_start_year(calendar_year, month)
    end_short = str(start + 1)[-2:]
    return f"FY{start}-{end_short}"
