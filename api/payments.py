"""
api/payments.py — Compatibility stub.

The original Pro / Freemium tier system was removed in the v2 UI overhaul.
Vahan Intelligence is now free for everyone: every state, every year, every
chart, no signup, no paywall.

This file is kept as a stub so any older import sites don't break. It exposes:

  - router          : an empty FastAPI router (no routes mounted)
  - tier_from_request(request) -> dict   : always returns the free-everywhere tier

If you want to bring paid tiers back, restore from git history:
    git log -- api/payments.py
"""

from fastapi import APIRouter, Request

router = APIRouter()

# A single permissive tier — every caller gets full access.
FREE_TIER = {
    "tier": "free",
    "states": "all",
    "max_years": "all",
    "can_export": True,
    "can_api": True,
    "plan": "free-for-everyone",
}


def tier_from_request(request: Request) -> dict:  # noqa: ARG001
    """Return the universal free-for-everyone tier. Kept for old call sites."""
    return dict(FREE_TIER)


__all__ = ["router", "tier_from_request", "FREE_TIER"]
