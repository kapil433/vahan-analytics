"""
National / all-states aggregate labels (no Selenium dependency).
Used by API and vahan_scraper without importing the full scraper stack.
"""

from __future__ import annotations

# Portal wording varies by build — match vahan_scraper.ALL_INDIA_STATES
ALL_INDIA_STATES = (
    "All Vahan4 Running States (36/36)",
    "All Vahan4 Running State",
    "All India",
    "All Vahan4",
)


def is_aggregate_state_name(state: str) -> bool:
    """True if ``state`` is the national / all-offices aggregate (not a single state/UT)."""
    s = (state or "").strip()
    if not s:
        return True
    if s in ALL_INDIA_STATES:
        return True
    sl = s.casefold()
    if "all vahan4" in sl:
        return True
    if sl in ("all india", "all states"):
        return True
    return False


# Backward-compatible name for scraper internals
_is_aggregate_state_name = is_aggregate_state_name
