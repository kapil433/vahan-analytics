"""
Clean Vahan merged CSV files and output normalized data.

Raw format: Repeated sections per fuel with headers, Maker, JAN-DEC, TOTAL.
Issues: Comma in numbers ("1,036"), repeated headers, Unnamed columns.

Output: Clean CSV with columns:
  state_code, state_name, year, fy, fuel_type, maker, month, count

Standardized for ANY state dump (including All India) - all scraped files
go through this same pipeline with master mappings (maker, fuel, FY).

Also supports portal **Excel** exports under ``f1/<state folder>/`` (FUELWISE layout:
row 0 = Maker, FUEL, JAN..DEC; data rows = S No, Maker, fuel, month counts).
"""

import re
import sys
from pathlib import Path

import pandas as pd

# Add project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.config import STATE_MAP, normalize_state
from config.mappings import (
    FUEL_CANONICAL,
    FUEL_MAP,
    MAKER_MAP,
    month_to_fy,
    normalize_fuel,
    normalize_maker,
)

MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

# Canonical fuel types from scraper (used to detect fuel sections)
CANONICAL_FUELS = set(FUEL_CANONICAL)


def _merged_csv_kind(df: pd.DataFrame) -> str:
    """Detect portal export shape (title is often in header row → pandas column names)."""
    if len(df.columns) == 0:
        return "empty"
    col_blob = " ".join(str(c) for c in df.columns if pd.notna(c))
    row0 = ""
    if len(df) > 0:
        row0 = " ".join(str(x) for x in df.iloc[0].tolist() if pd.notna(x) and str(x).strip())
    blob = f"{col_blob} {row0}"
    if "Maker Month Wise" in blob:
        return "maker_monthwise"
    if "Vehicle Class Wise" in blob and "Maker Month Wise" not in blob:
        return "vehicle_class_only"
    return "unknown"


def _try_split_month_row_records(row: pd.Series, state_name: str, year: int) -> list[dict] | None:
    """
    Some merged CSVs concatenate tables; maker rows use:
      col1=Maker, col2-6=JAN-MAY, col7=fuel, col8=S No, col9-15=JUN-DEC, col16=TOTAL
    Returns None if this row is not in that layout.
    """
    if len(row) < 17:
        return None
    f_raw = str(row.iloc[7]).strip()
    if f_raw not in CANONICAL_FUELS and f_raw not in FUEL_MAP:
        return None
    if pd.isna(row.iloc[9]):
        return None
    maker_raw = str(row.iloc[1]).strip()
    if not maker_raw or "Maker" in maker_raw or "Month Wise" in maker_raw:
        return None
    fuel = normalize_fuel(f_raw)
    maker = normalize_maker(maker_raw)
    month_cols = list(range(2, 7)) + list(range(9, 16))
    if len(month_cols) != 12:
        return None
    out: list[dict] = []
    for month_num, ci in enumerate(month_cols, start=1):
        cnt = parse_count(row.iloc[ci])
        out.append(
            {
                "state_code": get_state_code(state_name),
                "state_name": get_canonical_state_name(state_name),
                "year": year,
                "fy": month_to_fy(year, month_num),
                "fuel_type": fuel,
                "maker": maker,
                "month": month_num,
                "count": cnt,
            }
        )
    return out


def _is_seven_col_partial_year_layout(df: pd.DataFrame) -> bool:
    """Portal sometimes exports only JAN–MAR + TOTAL in 7 columns (year in progress)."""
    if len(df.columns) != 7:
        return False
    return str(df.columns[-1]).strip().lower() == "fuel_type"


def _try_seven_col_partial_month_records(row: pd.Series, state_name: str, year: int) -> list[dict] | None:
    """
    Layout C: col0=S No, col1=Maker, col2-4=JAN..MAR, col5=TOTAL, col6=fuel_type.
    """
    if len(row) < 7:
        return None
    f_raw = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ""
    if not f_raw or (f_raw not in CANONICAL_FUELS and f_raw not in FUEL_MAP):
        return None
    col0_raw = row.iloc[0]
    if pd.isna(col0_raw):
        return None
    col0 = str(col0_raw).strip()
    if col0.upper() in ("S NO", "S NO.", "NAN"):
        return None
    try:
        int(float(str(col0).replace(",", "")))
    except (ValueError, TypeError):
        return None
    maker_raw = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
    if not maker_raw or "Maker" in maker_raw or maker_raw.upper() == "NAN":
        return None
    fuel = normalize_fuel(f_raw)
    maker = normalize_maker(maker_raw)
    out: list[dict] = []
    for month_num, ci in enumerate((2, 3, 4), start=1):
        cnt = parse_count(row.iloc[ci])
        out.append(
            {
                "state_code": get_state_code(state_name),
                "state_name": get_canonical_state_name(state_name),
                "year": year,
                "fy": month_to_fy(year, month_num),
                "fuel_type": fuel,
                "maker": maker,
                "month": month_num,
                "count": cnt,
            }
        )
    return out


def parse_count(val) -> int:
    """Parse count: '1,036' -> 1036, '5' -> 5."""
    if pd.isna(val):
        return 0
    s = str(val).strip().replace(",", "").replace(" ", "")
    try:
        return int(float(s)) if s else 0
    except (ValueError, TypeError):
        return 0


def get_state_code(state_name: str) -> str:
    """Map state name to code. All Vahan4 -> ALL, All India -> ALL."""
    if "All Vahan4" in state_name or "All India" in state_name:
        return "ALL"
    result = normalize_state(state_name)
    return result[0] if result else state_name.replace(" ", "_")[:10]


def get_canonical_state_name(state_name: str) -> str:
    """Map to canonical state name for display."""
    if "All Vahan4" in state_name or "All India" in state_name:
        return "All India"
    result = normalize_state(state_name)
    return result[1] if result else state_name


def clean_merged_csv(csv_path: Path, state_name: str, year: int) -> pd.DataFrame:
    """
    Parse a merged Vahan CSV and return long-format DataFrame.

    Layout A (usual): col0=S No, col1=Maker, col2-13=JAN..DEC, col14=TOTAL, col15=fuel_type
    Layout B (wide/concatenated): col1=Maker, col2-6=JAN-MAY, col7=fuel, col8=S No, col9-15=JUN-DEC, col16=TOTAL
    Layout C (partial year): 7 columns — JAN..MAR, TOTAL, fuel_type (common for in-progress calendar years).
    Vehicle Class Wise-only files (no maker × month grid) are skipped (empty DataFrame).
    """
    df = read_vahan_csv_flexible(csv_path)
    if df.empty:
        return pd.DataFrame()

    kind = _merged_csv_kind(df)
    if kind == "vehicle_class_only":
        return pd.DataFrame()

    if _is_seven_col_partial_year_layout(df):
        rows_c: list[dict] = []
        for _, row in df.iterrows():
            recs = _try_seven_col_partial_month_records(row, state_name, year)
            if recs is not None:
                rows_c.extend(recs)
        if not rows_c:
            return pd.DataFrame()
        out_c = pd.DataFrame(rows_c)
        out_c = out_c.groupby(
            ["state_code", "state_name", "year", "fy", "fuel_type", "maker", "month"],
            as_index=False,
        )["count"].sum()
        return out_c

    min_cols = 17 if kind == "maker_monthwise" and len(df.columns) >= 17 else 16
    if len(df.columns) < min_cols:
        return pd.DataFrame()

    rows = []
    current_fuel = None

    for _, row in df.iterrows():
        # Col 15 = fuel_type (from scraper annotation) — Layout A
        fuel_val = row.iloc[15] if len(row) > 15 else ""
        if pd.notna(fuel_val):
            fuel_str = str(fuel_val).strip()
            if fuel_str in CANONICAL_FUELS or fuel_str in FUEL_MAP:
                current_fuel = normalize_fuel(fuel_str)

        col0_raw = row.iloc[0] if len(row) > 0 else None
        col0 = "" if pd.isna(col0_raw) else str(col0_raw).strip()
        col1 = str(row.iloc[1]).strip() if len(row) > 1 else ""

        if col0 in ("S No", "S No.", "nan") or "Maker" in col1 or "Month Wise" in col1:
            continue

        # Layout B: leading S No empty, fuel at col7, months split 2-6 and 9-15
        split_recs = _try_split_month_row_records(row, state_name, year)
        if split_recs is not None:
            rows.extend(split_recs)
            continue

        if col0 in ("", "nan"):
            continue

        f15 = str(row.iloc[15]).strip() if len(row) > 15 else ""
        fuel_at_7 = str(row.iloc[7]).strip() if len(row) > 7 else ""
        fuel7_ok = fuel_at_7 in CANONICAL_FUELS or fuel_at_7 in FUEL_MAP
        f15_ok = f15 in CANONICAL_FUELS or f15 in FUEL_MAP
        # Vehicle-class aggregate rows (fuel in col7, no Layout A fuel in col15) are not maker×month
        if fuel7_ok and not f15_ok:
            continue

        # Data row: col0 numeric (S No) — Layout A
        try:
            int(float(str(col0).replace(",", "")))
        except (ValueError, TypeError):
            continue

        raw_maker = col1
        if not raw_maker or raw_maker.upper() in ("MAKER", "NAN"):
            continue

        if not current_fuel:
            continue

        maker = normalize_maker(raw_maker)

        # Cols 2-13 = JAN(1) to DEC(12)
        for m, _month_name in enumerate(MONTHS):
            month_num = m + 1
            idx = 2 + m
            if idx < len(row):
                count = parse_count(row.iloc[idx])
                fy = month_to_fy(year, month_num)
                rows.append({
                    "state_code": get_state_code(state_name),
                    "state_name": get_canonical_state_name(state_name),
                    "year": year,
                    "fy": fy,
                    "fuel_type": current_fuel,
                    "maker": maker,
                    "month": month_num,
                    "count": count,
                })

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out = out.groupby(
        ["state_code", "state_name", "year", "fy", "fuel_type", "maker", "month"],
        as_index=False,
    )["count"].sum()
    return out


_YEAR_TOKEN_RE = re.compile(r"(20[0-2]\d)")


def _year_from_text(s: str) -> int | None:
    found = _YEAR_TOKEN_RE.findall(s)
    if not found:
        return None
    return int(found[-1])


_STATE_CODE_TO_CANONICAL: dict[str, str] | None = None


def _state_code_to_canonical() -> dict[str, str]:
    global _STATE_CODE_TO_CANONICAL
    if _STATE_CODE_TO_CANONICAL is None:
        m: dict[str, str] = {}
        for _name, (code, canon) in STATE_MAP.items():
            if code == "ALL":
                continue
            cu = code.upper()
            if cu not in m:
                m[cu] = canon
        _STATE_CODE_TO_CANONICAL = m
    return _STATE_CODE_TO_CANONICAL


def _portal_folder_state_guess(key: str) -> str | None:
    """
    Strip portal suffixes from f1 folder names, e.g.
    'Arunachal Pradesh(29) FUELWISE12' -> 'Arunachal Pradesh',
    'Uttar Pradesh(77) 12' -> 'Uttar Pradesh'.
    """
    s = key.replace("_", " ").strip()
    s = re.sub(r"\(\d+\)", "", s)
    s = re.sub(r"\s*FUELWISE\s*\d+\s*$", "", s, flags=re.I).strip()
    s = re.sub(r"\s+\d{2}\s*$", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    pair = normalize_state(s)
    if pair:
        return pair[1]
    pair = normalize_state(s.title())
    return pair[1] if pair else None


def _state_name_from_folder_label(folder: str) -> str | None:
    """Resolve f1 subfolder name (full name, code, or 01_State) to canonical state name."""
    raw = folder.strip()
    if not raw or (raw.isdigit() and len(raw) == 4):
        return None
    raw = re.sub(r"^\d+[_\s-]+", "", raw)
    raw = raw.replace("_", " ").strip()
    if not raw:
        return None
    portal = _portal_folder_state_guess(raw)
    if portal:
        return portal
    pair = normalize_state(raw)
    if pair:
        return pair[1]
    titled = raw.title() if raw != raw.title() else None
    if titled:
        pair = normalize_state(titled)
        if pair:
            return pair[1]
    code_key = raw.upper().replace(" ", "")
    cmap = _state_code_to_canonical()
    if code_key in cmap:
        return cmap[code_key]
    return None


def parse_state_year_from_filename(name: str) -> tuple[str, int] | None:
    """
    Extract state and year from filename.

    Examples:
        'Andhra Pradesh_2014_merged.csv' -> ('Andhra Pradesh', 2014)
        'All Vahan4 Running States (36_36)_2024_merged.csv' -> ('All Vahan4 Running States (36/36)', 2024)
        'All_Vahan4_Running_States_36_36_2025_merged.csv' -> ('All Vahan4 Running States (36/36)', 2025)
        'Punjab-2023.csv' -> ('Punjab', 2023)  (f1-style without _merged)
    """
    stem = Path(name).stem
    if "_merged" in stem:
        stem = stem.replace("_merged", "").strip()

    # --- Standard: {State}_{Year} ---
    parts = stem.rsplit("_", 1)
    if len(parts) == 2:
        state_part, year_part = parts
        try:
            year = int(year_part)
        except ValueError:
            year = None
        else:
            if 2000 <= year <= 2100:
                state_part = state_part.replace("_", " ")
                if "All Vahan4" in state_part or "All India" in state_part:
                    state_part = "All Vahan4 Running States (36/36)"
                elif "36 36" in state_part or "36/36" in state_part:
                    state_part = "All Vahan4 Running States (36/36)"
                return (state_part, year)

    # --- Alternate: year anywhere in stem, state before first digit chunk (f1 exports) ---
    y = _year_from_text(stem)
    if y is None:
        return None
    head = _YEAR_TOKEN_RE.split(stem)[0].strip(" _-.")
    if not head:
        return None
    head = head.replace("_", " ").strip()
    pair = normalize_state(head)
    if pair:
        return (pair[1], y)
    pair = normalize_state(head.title())
    if pair:
        return (pair[1], y)
    return (head, y)


def parse_state_year_from_path(path: Path, raw_root: Path) -> tuple[str, int] | None:
    """
    When filename alone is not enough, use f1/<state-folder>/... layout.

    Supported:
      f1/Maharashtra/2024.csv
      f1/MH/Maharashtra_2024_export.csv
      f1/MH/2024/anything.csv
    """
    try:
        rel = path.resolve().relative_to(raw_root.resolve())
    except ValueError:
        return None
    parts = rel.parts
    if len(parts) < 2 or parts[0] != "f1":
        return None

    stem_year = _year_from_text(path.stem)
    parent = parts[-2] if len(parts) >= 2 else ""
    grand = parts[-3] if len(parts) >= 3 else ""

    year: int | None = stem_year
    state_folder = ""

    if year is not None:
        if len(parts) == 3:
            state_folder = parts[1]
        elif len(parts) > 3:
            state_folder = parts[-2]
    if year is None and parent.isdigit() and len(parent) == 4:
        year = int(parent)
        state_folder = grand if len(parts) >= 4 else (parts[1] if len(parts) >= 3 else "")

    if year is None or not state_folder:
        return None
    state_name = _state_name_from_folder_label(state_folder)
    if not state_name:
        return None
    return (state_name, year)


def parse_state_year_for_raw_file(path: Path, raw_root: Path) -> tuple[str, int] | None:
    """Try filename rules first, then f1 path layout."""
    hit = parse_state_year_from_filename(path.name)
    if hit:
        return hit
    return parse_state_year_from_path(path, raw_root)


def parse_year_from_f1_xlsx_stem(stem: str) -> int | None:
    """CY from '...FUELWISE26' or '..._26' suffix (2000+two-digit year)."""
    m = re.search(r"FUELWISE\s*(\d{2})", stem, re.I)
    if m:
        return 2000 + int(m.group(1))
    m2 = re.search(r"_(\d{2})$", stem)
    if m2:
        return 2000 + int(m2.group(1))
    return None


def parse_state_year_from_f1_xlsx(path: Path, raw_root: Path) -> tuple[str, int] | None:
    if path.suffix.lower() != ".xlsx":
        return None
    try:
        rel = path.resolve().relative_to(raw_root.resolve())
    except ValueError:
        return None
    parts = rel.parts
    if len(parts) < 3 or parts[0] != "f1":
        return None
    state_folder = parts[1]
    state_name = _state_name_from_folder_label(state_folder)
    if not state_name:
        return None
    year = parse_year_from_f1_xlsx_stem(path.stem)
    if year is None or not (2000 <= year <= 2100):
        return None
    return (state_name, year)


_MONTH_HDR_TO_NUM = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def _fuelwise_cell_upper(val) -> str:
    if pd.isna(val):
        return ""
    return str(val).strip().upper()


def detect_fuelwise_table_layout(df: pd.DataFrame) -> tuple[int, int, int, dict[int, int]] | None:
    """
    Locate the header row and columns for portal FUELWISE exports.

    Two shapes:
    - Full CY: Maker, FUEL, JAN..DEC, Total (16+ columns).
    - In-progress CY (e.g. Mar): Maker, FUEL, JAN, FEB, MAR, Total (7 columns).

    Returns (header_row_index, maker_col, fuel_col, {calendar_month: col_index}).
    """
    max_scan = min(8, len(df))
    for hi in range(max_scan):
        row = df.iloc[hi]
        ncols = len(row)
        vals = [_fuelwise_cell_upper(row.iloc[ci]) for ci in range(ncols)]
        try:
            maker_ci = next(i for i, v in enumerate(vals) if v == "MAKER")
            fuel_ci = next(i for i, v in enumerate(vals) if v == "FUEL")
        except StopIteration:
            continue
        if maker_ci >= fuel_ci:
            continue
        month_map: dict[int, int] = {}
        for ci, v in enumerate(vals):
            if v in _MONTH_HDR_TO_NUM:
                month_map[_MONTH_HDR_TO_NUM[v]] = ci
        if not month_map:
            continue
        if min(month_map.values()) <= fuel_ci:
            continue
        return hi, maker_ci, fuel_ci, month_map
    return None


def clean_fuelwise_xlsx(xlsx_path: Path, state_name: str, year: int) -> pd.DataFrame:
    """
    Parse f1 FUELWISE workbooks by detecting the header row and which month columns exist.

    Skips spacer rows (e.g. fuel banner with no Maker) and emits rows only for months
    present in the file (full 12 or partial JAN–MAR for current year).
    """
    df = pd.read_excel(xlsx_path, header=None, engine="openpyxl")
    if df.empty or len(df.columns) < 4:
        return pd.DataFrame()

    layout = detect_fuelwise_table_layout(df)
    if not layout:
        return pd.DataFrame()

    header_i, maker_ci, fuel_ci, month_col_map = layout
    rows: list[dict] = []

    for ri in range(header_i + 1, len(df)):
        row = df.iloc[ri]
        if maker_ci >= len(row) or fuel_ci >= len(row):
            continue
        maker_raw = str(row.iloc[maker_ci]).strip() if pd.notna(row.iloc[maker_ci]) else ""
        fuel_raw = str(row.iloc[fuel_ci]).strip() if pd.notna(row.iloc[fuel_ci]) else ""
        if not maker_raw or maker_raw.upper() in ("MAKER", "NAN"):
            continue
        if not fuel_raw or fuel_raw.upper() in ("FUEL", "NAN"):
            continue

        fuel = normalize_fuel(fuel_raw)
        maker = normalize_maker(maker_raw)

        for month_num in sorted(month_col_map.keys()):
            ci = month_col_map[month_num]
            cnt = parse_count(row.iloc[ci]) if ci < len(row) else 0
            rows.append(
                {
                    "state_code": get_state_code(state_name),
                    "state_name": get_canonical_state_name(state_name),
                    "year": year,
                    "fy": month_to_fy(year, month_num),
                    "fuel_type": fuel,
                    "maker": maker,
                    "month": month_num,
                    "count": cnt,
                }
            )

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out = out.groupby(
        ["state_code", "state_name", "year", "fy", "fuel_type", "maker", "month"],
        as_index=False,
    )["count"].sum()
    return out


def read_vahan_csv_flexible(csv_path: Path, *, nrows: int | None = None) -> pd.DataFrame:
    """Read portal CSV with common encodings (incl. BOM); last resort latin-1."""
    last_err: Exception | None = None
    kw: dict = {"encoding": "utf-8", "on_bad_lines": "skip", "header": 0}
    if nrows is not None:
        kw["nrows"] = nrows
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        kw["encoding"] = enc
        try:
            df = pd.read_csv(csv_path, **kw)
            if df.shape[1] >= 1 and df.shape[0] >= 0:
                return df
        except Exception as e:
            last_err = e
            continue
    kw["encoding"] = "latin-1"
    try:
        return pd.read_csv(csv_path, **kw)
    except Exception:
        if last_err:
            raise last_err
        raise


def iter_merged_csv_files(raw_dir: Path, *, recursive: bool = True) -> list[Path]:
    """All *_merged.csv under raw_dir (optionally recursive); skips _verify* names."""
    pattern = "*_merged.csv"
    if recursive:
        found = [p for p in raw_dir.rglob(pattern) if p.is_file() and not p.name.startswith("_verify")]
    else:
        found = [p for p in raw_dir.glob(pattern) if p.is_file() and not p.name.startswith("_verify")]
    return sorted(found, key=lambda p: str(p.relative_to(raw_dir).as_posix()))


def iter_raw_vahan_csv_files(
    raw_dir: Path, *, recursive: bool = True, f1_only: bool = False
) -> list[Path]:
    """
    Portal files to clean. Default: *_merged.csv (flat + subdirs) plus any *.csv / *.xlsx
    under f1/ (FUELWISE workbooks). De-duplicated by path.

    When f1_only=True, only ``raw_dir/f1/**/*.csv`` and ``raw_dir/f1/**/*.xlsx`` are used
    (no top-level merged scrapes / All-Vahan aggregates).
    """
    raw_dir = raw_dir.resolve()
    seen: set[Path] = set()
    ordered: list[Path] = []

    def add(p: Path) -> None:
        rp = p.resolve()
        if not rp.is_file() or rp.name.startswith("_verify"):
            return
        if rp in seen:
            return
        seen.add(rp)
        ordered.append(rp)

    if not f1_only:
        if recursive:
            for p in raw_dir.rglob("*_merged.csv"):
                add(p)
        else:
            for p in raw_dir.glob("*_merged.csv"):
                add(p)

    f1 = raw_dir / "f1"
    if f1.is_dir():
        for p in f1.rglob("*.csv"):
            add(p)
        for p in f1.rglob("*.xlsx"):
            add(p)

    return sorted(ordered, key=lambda p: str(p.relative_to(raw_dir).as_posix()))


def cleaned_csv_basename(raw_root: Path, merged_file: Path) -> str:
    """Unique stem for per-file cleaned output (avoids collisions across f1 subfolders)."""
    stem = merged_file.stem.replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
    try:
        rel = merged_file.relative_to(raw_root)
    except ValueError:
        return f"{stem}_cleaned.csv"
    if rel.parent == Path("."):
        return f"{stem}_cleaned.csv"
    parent = "__".join(rel.parent.as_posix().split("/")).replace(" ", "_")
    return f"{parent}__{stem}_cleaned.csv"


def clean_all(
    input_dir: Path,
    output_dir: Path,
    *,
    recursive: bool = True,
    f1_only: bool = False,
) -> list[Path]:
    """
    Clean portal exports in input_dir, write to output_dir.
    When recursive=True and not f1_only, includes top-level *_merged.csv and f1/**.
    When f1_only=True, only input_dir/f1/** is scanned.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    files = iter_raw_vahan_csv_files(input_dir, recursive=recursive, f1_only=f1_only)
    if not files and input_dir.name != "vahan_data":
        vahan = input_dir / "vahan_data" if (input_dir / "vahan_data").exists() else input_dir.parent / "vahan_data"
        if vahan.is_dir():
            files = iter_raw_vahan_csv_files(vahan, recursive=recursive, f1_only=f1_only)

    all_dfs = []
    for f in files:
        if f.suffix.lower() == ".xlsx":
            parsed = parse_state_year_from_f1_xlsx(f, input_dir)
            if not parsed:
                print(f"  Skip (bad xlsx path/name): {f}")
                continue
            state_name, year = parsed
            try:
                df = clean_fuelwise_xlsx(f, state_name, year)
            except Exception as e:
                print(f"  Skip (xlsx error): {f} ({e})")
                continue
        else:
            parsed = parse_state_year_for_raw_file(f, input_dir)
            if not parsed:
                print(f"  Skip (bad name): {f}")
                continue
            state_name, year = parsed
            df = clean_merged_csv(f, state_name, year)
        if df.empty:
            print(f"  Skip (empty): {f}")
            continue
        all_dfs.append(df)
        out_name = cleaned_csv_basename(input_dir, f)
        out_path = output_dir / out_name
        df.to_csv(out_path, index=False, encoding="utf-8")
        try:
            disp = f.relative_to(input_dir)
        except ValueError:
            disp = f
        print(f"  Cleaned: {disp} -> {len(df)} rows")

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined_path = output_dir / "vahan_registrations_cleaned.csv"
        combined.to_csv(combined_path, index=False, encoding="utf-8")
        print(f"  Combined: {combined_path} ({len(combined)} rows)")
        return [combined_path]

    return []


if __name__ == "__main__":
    import argparse

    base = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser(
        description=(
            "Clean Vahan exports into long-format CSVs. "
            "Default: all *_merged.csv under raw-dir (recursive) plus raw-dir/f1/**/*.csv and *.xlsx."
        ),
    )
    ap.add_argument(
        "--raw-dir",
        type=Path,
        default=base / "output" / "vahan_data",
        help="Root folder (e.g. output/vahan_data): merged CSVs + f1/ subtree",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=base / "output" / "vahan_data_cleaned",
        help="Output folder for per-file and combined cleaned CSVs",
    )
    ap.add_argument(
        "--no-recurse",
        action="store_true",
        help="Only scan raw-dir top level (no subfolders)",
    )
    ap.add_argument(
        "--f1-only",
        action="store_true",
        help="Only ingest output/vahan_data/f1/** (skip top-level *_merged.csv and All-Vahan files)",
    )
    args = ap.parse_args()
    clean_all(
        args.raw_dir.resolve(),
        args.output_dir.resolve(),
        recursive=not args.no_recurse,
        f1_only=args.f1_only,
    )
