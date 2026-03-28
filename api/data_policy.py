"""State-level exclusions for analytics SQL (national KPIs, platform context, master bundle)."""

from __future__ import annotations

# ISO-like state codes to drop from aggregates (empty = include all).
EXCLUDED_STATE_CODES: frozenset[str] = frozenset()

EXCLUSION_REASON = (
    "No state codes are excluded in this build. Add codes to EXCLUDED_STATE_CODES "
    "if certain regions should be omitted from All-India-style totals."
)


def append_exclude_state_codes_sql(q: str, params: list) -> str:
    """Append `AND state_code NOT IN (...)` when EXCLUDED_STATE_CODES is non-empty."""
    if not EXCLUDED_STATE_CODES:
        return q
    codes = sorted(EXCLUDED_STATE_CODES)
    placeholders = ",".join(["%s"] * len(codes))
    q += f" AND state_code NOT IN ({placeholders})"
    params.extend(codes)
    return q
