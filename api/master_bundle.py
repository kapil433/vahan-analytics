"""Build legacy `vahan_master.json`-shaped payload for GET /data/vahan_master_compat."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from api.data_policy import append_exclude_state_codes_sql
from config.mappings import FUEL_CANONICAL, normalize_fuel, normalize_maker, month_to_fy, oem_short_display

API_DIR = Path(__file__).resolve().parent
OVERLAY_PATH = API_DIR / "static" / "dashboard" / "legacy_overlay.json"

# Dashboard deep-dive + matrix click targets (see top15_oems / top10_oems in bundle).
OEM_TOP_N = 15
OEM_LEGACY_TOP_N = 10


def _fy_short_label(calendar_year: int, month: int) -> str:
    """Row fy slot uses `2024-25` (matches legacy JSON), not `FY2024-25`."""
    full = month_to_fy(int(calendar_year), int(month))
    return full[2:] if full.startswith("FY") else full


def _fy_sort_key(label: str) -> tuple[int, int]:
    parts = str(label).strip().split("-", 1)
    if len(parts) != 2:
        return (0, 0)
    try:
        return (int(parts[0]), int(parts[1]))
    except ValueError:
        return (0, 0)


def _load_overlay() -> dict[str, Any]:
    if not OVERLAY_PATH.is_file():
        return {}
    with open(OVERLAY_PATH, encoding="utf-8") as f:
        return json.load(f)


def _exec_sql(cur: Any, dialect: str, q: str, params: list) -> None:
    if dialect == "sqlite":
        q = q.replace("%s", "?")
    cur.execute(q, params)


def build_vahan_master_bundle(conn: Any, dialect: str = "postgres") -> dict[str, Any]:
    overlay = _load_overlay()
    raw_maker_map = overlay.get("maker_map") or {}
    maker_overlay = {str(k).strip().upper(): v for k, v in raw_maker_map.items()}
    raw_fuel_map = overlay.get("fuel_map") or {}
    fuel_overlay = {str(k).strip().upper(): v for k, v in raw_fuel_map.items()}

    cur = conn.cursor()
    try:
        q = """
            SELECT state_code, state_name, year, month, fuel_type, maker, SUM(count) AS cnt
            FROM vahan_registrations
            WHERE 1=1
        """
        params: list = []
        q = append_exclude_state_codes_sql(q, params)
        q += " GROUP BY state_code, state_name, year, month, fuel_type, maker"
        if dialect == "postgres":
            q = q.replace("SUM(count)", "SUM(count)::bigint")
        _exec_sql(cur, dialect, q, params)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    def maker_label(raw: str) -> str:
        s = str(raw or "").strip()
        if not s:
            return "Others"
        upper_raw = s.upper()
        if upper_raw in maker_overlay:
            return maker_overlay[upper_raw]
        canon = normalize_maker(s)
        cu = canon.upper() if canon else ""
        if cu in maker_overlay:
            return maker_overlay[cu]
        for key, disp in maker_overlay.items():
            if len(key) >= 12 and (key in cu or cu in key):
                return disp
        short = oem_short_display(canon)
        return short if short else "Others"

    def fuel_label(raw: str) -> str:
        base = normalize_fuel(raw)
        ru = str(raw or "").strip().upper()
        return fuel_overlay.get(ru, fuel_overlay.get(base.upper(), base))

    region_names: set[str] = set()
    maker_names: set[str] = set()
    fuel_names: set[str] = set()
    cal_years: set[int] = set()
    fy_labels: set[str] = set()
    merged: dict[tuple[str, int, int, str, str, str], int] = defaultdict(int)

    for r in rows:
        sc = str(r.get("state_code") or "").strip().upper()
        sn = str(r.get("state_name") or "").strip()
        if sc == "ALL":
            region = "All India"
        else:
            region = sn or sc or "Unknown"
        region_names.add(region)

        y = int(r["year"])
        m = int(r["month"])
        cal_years.add(y)
        fy_s = _fy_short_label(y, m)
        fy_labels.add(fy_s)

        mk = maker_label(str(r.get("maker") or ""))
        maker_names.add(mk)

        fl = fuel_label(str(r.get("fuel_type") or ""))
        if fl not in FUEL_CANONICAL:
            fl = normalize_fuel(r.get("fuel_type"))
        fuel_names.add(fl)

        cnt = int(r.get("cnt") or 0)
        key = (region, y, m, fy_s, mk, fl)
        merged[key] += cnt

    sorted_regions = ["All India"] + sorted(x for x in region_names if x != "All India")
    region_index = {n: i for i, n in enumerate(sorted_regions)}

    fuels_ordered = [f for f in FUEL_CANONICAL if f in fuel_names]
    for f in sorted(fuel_names):
        if f not in fuels_ordered:
            fuels_ordered.append(f)
    fuel_index = {n: i for i, n in enumerate(fuels_ordered)}

    makers_sorted = sorted(m for m in maker_names if m != "Others")
    if "Others" in maker_names:
        makers_sorted.append("Others")
    maker_index = {n: i for i, n in enumerate(makers_sorted)}

    financial_years = sorted(fy_labels, key=_fy_sort_key)
    ai_fy = [y for y in financial_years if y != "2011-12"]
    ap_fy = [y for y in financial_years if y != "2011-12"]

    encoded: list[list] = []
    maker_totals: defaultdict[str, int] = defaultdict(int)
    for (region, y, m, fy_s, mk, fl), cnt in merged.items():
        if cnt <= 0:
            continue
        encoded.append(
            [
                region_index[region],
                y,
                m,
                fy_s,
                maker_index[mk],
                fuel_index[fl],
                cnt,
            ]
        )
        maker_totals[mk] += cnt

    ranked = sorted(maker_totals.items(), key=lambda x: -x[1])
    top15_oems = [name for name, _ in ranked[:OEM_TOP_N] if name != "Others"]
    if len(top15_oems) < OEM_TOP_N:
        for name, _ in ranked:
            if name not in top15_oems and name != "Others":
                top15_oems.append(name)
            if len(top15_oems) >= OEM_TOP_N:
                break

    cal_sorted = sorted(cal_years)
    today = date.today().isoformat()
    meta = {
        "regions": sorted_regions,
        "cal_years": cal_sorted,
        "financial_years": financial_years,
        "ai_financial_years": ai_fy,
        "ap_financial_years": ap_fy,
        "fuels": fuels_ordered,
        "makers": makers_sorted,
        "total_records": len(encoded),
        "partial_cal_years": [],
        "partial_fy": [],
        "last_updated": today,
    }

    bundle: dict[str, Any] = {
        "meta": meta,
        "regions": sorted_regions,
        "makers": makers_sorted,
        "fuels": fuels_ordered,
        "data": encoded,
        "top15_oems": top15_oems,
        "top10_oems": top15_oems[:OEM_LEGACY_TOP_N],
    }

    for key, val in overlay.items():
        if key in ("maker_map", "fuel_map"):
            bundle[key] = val
        elif key not in bundle:
            bundle[key] = val

    return bundle
