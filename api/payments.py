"""
Razorpay Freemium Integration  —  api/payments.py
==================================================
Freemium model:
  FREE tier   → All India overview, top-level fuel/OEM charts, last 2 years
  PRO tier    → All 37 states, full historical (2012–present), data exports, API access

Environment variables required:
  RAZORPAY_KEY_ID        Your Razorpay Key ID  (rzp_live_xxxxx or rzp_test_xxxxx)
  RAZORPAY_KEY_SECRET    Your Razorpay Key Secret
  JWT_SECRET             Random 32+ char string for signing session tokens
  RAZORPAY_WEBHOOK_SECRET  Webhook secret set in Razorpay dashboard

Plans (set prices in INR):
  PLAN_MONTHLY   ₹499/month
  PLAN_ANNUAL    ₹3,999/year   (save ~33%)

Usage in main.py:
  from api.payments import router as payments_router
  app.include_router(payments_router, prefix="/api/payments")

Frontend calls:
  POST /api/payments/create-order   → returns {order_id, amount, currency, key_id}
  POST /api/payments/verify         → verifies signature, issues JWT
  GET  /api/payments/me             → returns current user tier from JWT
  POST /api/payments/webhook        → Razorpay webhook (subscription events)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Header, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ── JWT (stdlib only, no PyJWT dependency) ────────────────────────────────────
import base64
import struct


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    pad = 4 - len(s) % 4
    return base64.urlsafe_b64decode(s + "=" * (pad % 4))


def _sign_jwt(payload: dict, secret: str) -> str:
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = _b64url(json.dumps(payload).encode())
    msg = f"{header}.{body}".encode()
    sig = hmac.new(secret.encode(), msg, hashlib.sha256).digest()
    return f"{header}.{body}.{_b64url(sig)}"


def _verify_jwt(token: str, secret: str) -> dict | None:
    try:
        h, b, s = token.split(".")
        msg = f"{h}.{b}".encode()
        expected = _b64url(hmac.new(secret.encode(), msg, hashlib.sha256).digest())
        if not hmac.compare_digest(s, expected):
            return None
        payload = json.loads(_b64url_decode(b))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None


# ── Config ────────────────────────────────────────────────────────────────────
RAZORPAY_KEY_ID = os.environ.get("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET", "")
JWT_SECRET = os.environ.get("JWT_SECRET", "change-this-to-a-random-secret-in-production")
WEBHOOK_SECRET = os.environ.get("RAZORPAY_WEBHOOK_SECRET", "")

PLAN_ANNUAL_PAISE  = 10000   # ₹100  / year
PLAN_LIFETIME_PAISE = 49900  # ₹499  lifetime
JWT_EXPIRY_DAYS_ANNUAL   = 365
JWT_EXPIRY_DAYS_LIFETIME = 36500  # ~100 years = effectively lifetime

# ── Always-free states (never gated regardless of tier) ──────────────────────
FREE_STATES = {
    "All India",
    "All Vahan4 Running States (36/36)",
    "Andhra Pradesh",
    "Chhattisgarh",
    "Odisha",        # also known as Orissa
}

# ── Tier definitions (used by dashboard to gate features) ────────────────────
FREE_TIER = {
    "tier": "free",
    "free_states": sorted(FREE_STATES),  # Always accessible for everyone
    "max_years": 0,                      # No year restriction on free states
    "full_map": True,                    # Full India map is always free
    "can_export": False,
    "can_api": False,
    "all_states": False,
}

PRO_TIER = {
    "tier": "pro",
    "free_states": sorted(FREE_STATES),
    "states": "__all__",             # All 37 states
    "max_years": 0,                  # Unlimited (2012–present)
    "full_map": True,
    "can_export": True,
    "can_api": True,
    "all_states": True,
}

# ── Router ────────────────────────────────────────────────────────────────────
router = APIRouter(tags=["payments"])


# ── Request / Response models ─────────────────────────────────────────────────

class CreateOrderRequest(BaseModel):
    plan: str = "annual"        # "annual" (₹100/yr) or "lifetime" (₹499)
    email: str = ""


class VerifyPaymentRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    plan: str = "annual"
    email: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _razorpay_client():
    """Return Razorpay client or raise if not configured."""
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        raise HTTPException(503, "Razorpay not configured. Set RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET.")
    try:
        import razorpay
        return razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
    except ImportError:
        raise HTTPException(503, "razorpay package not installed. Run: pip install razorpay")


def _issue_token(email: str, plan: str, order_id: str) -> str:
    days = JWT_EXPIRY_DAYS_LIFETIME if plan == "lifetime" else JWT_EXPIRY_DAYS_ANNUAL
    payload = {
        "sub": email,
        "plan": plan,
        "order_id": order_id,
        "iat": int(time.time()),
        "exp": int(time.time()) + days * 86400,
    }
    return _sign_jwt(payload, JWT_SECRET)


def _get_token_from_header(authorization: str | None) -> dict | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    return _verify_jwt(authorization[7:], JWT_SECRET)


def _get_tier(claims: dict | None) -> dict:
    if not claims:
        return {**FREE_TIER}
    plan = claims.get("plan", "free")
    if plan in ("annual", "lifetime", "pro"):
        return {**PRO_TIER, "email": claims.get("sub", ""), "plan": plan}
    return {**FREE_TIER}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/plans")
def get_plans():
    """Return available plan details for the pricing page."""
    return {
        "free_states": sorted(FREE_STATES),
        "plans": [
            {
                "id": "free",
                "name": "Free",
                "price_inr": 0,
                "features": [
                    "Full India Map (all states visible)",
                    "All India, Andhra Pradesh, Chhattisgarh & Odisha — always free",
                    "Full history 2012–present for free states",
                    "Fuel mix, OEM charts, month-wise trends",
                    "No signup required",
                ],
                "limits": {"free_states": sorted(FREE_STATES), "export": False, "api": False},
            },
            {
                "id": "annual",
                "name": "Pro — 1 Year",
                "price_inr": 100,
                "price_display": "₹100 / year",
                "badge": "Most Popular",
                "features": [
                    "All 37 states + UTs",
                    "Full 2012–present history for every state",
                    "State drill-downs, compare, forecast",
                    "OEM + fuel deep dives",
                    "CSV data export",
                    "API access",
                ],
                "limits": {"states": "all", "years": "all", "export": True, "api": True},
            },
            {
                "id": "lifetime",
                "name": "Pro — Lifetime",
                "price_inr": 499,
                "price_display": "₹499 one-time",
                "badge": "Best Value",
                "features": [
                    "Everything in Pro Annual",
                    "Never pay again",
                    "All future updates included",
                    "Priority support",
                ],
                "limits": {"states": "all", "years": "all", "export": True, "api": True},
            },
        ],
        "key_id": RAZORPAY_KEY_ID,
    }


@router.post("/create-order")
def create_order(body: CreateOrderRequest):
    """Create a Razorpay order. Returns order_id + amount for the checkout widget."""
    client = _razorpay_client()
    amount = PLAN_LIFETIME_PAISE if body.plan == "lifetime" else PLAN_ANNUAL_PAISE
    try:
        order = client.order.create({
            "amount": amount,
            "currency": "INR",
            "receipt": f"vahan_{body.plan}_{int(time.time())}",
            "notes": {"plan": body.plan, "email": body.email},
        })
    except Exception as e:
        raise HTTPException(502, f"Razorpay order creation failed: {e}")
    return {
        "order_id": order["id"],
        "amount": order["amount"],
        "currency": order["currency"],
        "key_id": RAZORPAY_KEY_ID,
        "plan": body.plan,
    }


@router.post("/verify")
def verify_payment(body: VerifyPaymentRequest):
    """
    Verify Razorpay payment signature. On success, issue a JWT access token.
    Frontend stores this token in localStorage and sends it as Authorization: Bearer <token>.
    """
    # Signature verification
    msg = f"{body.razorpay_order_id}|{body.razorpay_payment_id}".encode()
    expected = hmac.new(RAZORPAY_KEY_SECRET.encode(), msg, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, body.razorpay_signature):
        raise HTTPException(400, "Invalid payment signature. Contact support if amount was deducted.")

    token = _issue_token(body.email, body.plan, body.razorpay_order_id)
    tier = _get_tier({"sub": body.email, "plan": body.plan})
    return {
        "success": True,
        "token": token,
        "tier": tier,
        "expires_days": JWT_EXPIRY_DAYS_LIFETIME if body.plan == "lifetime" else JWT_EXPIRY_DAYS_ANNUAL,
        "message": "Payment verified. You now have Pro access.",
    }


@router.get("/me")
def get_me(authorization: str | None = Header(default=None)):
    """
    Return current user's tier based on JWT in Authorization header.
    Dashboard calls this on load to decide which features to unlock.
    """
    claims = _get_token_from_header(authorization)
    tier = _get_tier(claims)
    return {
        "authenticated": claims is not None,
        "email": claims.get("sub", "") if claims else "",
        "plan": claims.get("plan", "free") if claims else "free",
        "tier": tier,
        "token_expires": datetime.fromtimestamp(
            claims["exp"], tz=timezone.utc
        ).isoformat() if claims else None,
    }


@router.post("/webhook")
async def razorpay_webhook(request: Request):
    """
    Handle Razorpay webhooks (subscription renewal, payment failed, refund, etc.).
    Set the webhook URL in Razorpay Dashboard → Settings → Webhooks.
    """
    body = await request.body()

    # Verify webhook signature
    if WEBHOOK_SECRET:
        sig = request.headers.get("X-Razorpay-Signature", "")
        expected = hmac.new(WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            raise HTTPException(400, "Invalid webhook signature")

    try:
        event = json.loads(body)
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    event_type = event.get("event", "")

    # ── Handle subscription events ─────────────────────────────────────────
    if event_type == "payment.captured":
        payment = event.get("payload", {}).get("payment", {}).get("entity", {})
        # TODO: Store payment record in DB (email → plan, expires_at)
        print(f"[Webhook] Payment captured: {payment.get('id')} email={payment.get('email')}")

    elif event_type == "subscription.charged":
        sub = event.get("payload", {}).get("subscription", {}).get("entity", {})
        print(f"[Webhook] Subscription charged: {sub.get('id')}")

    elif event_type in ("subscription.cancelled", "subscription.expired"):
        sub = event.get("payload", {}).get("subscription", {}).get("entity", {})
        print(f"[Webhook] Subscription ended: {sub.get('id')}")
        # TODO: Mark user as reverted to free tier in DB

    elif event_type == "payment.failed":
        payment = event.get("payload", {}).get("payment", {}).get("entity", {})
        print(f"[Webhook] Payment failed: {payment.get('id')}")

    return {"status": "ok"}


# ── Middleware helper: add to FastAPI app ────────────────────────────────────

def tier_from_request(request: Request) -> dict:
    """
    Call from any endpoint to get the requesting user's tier.
    Usage:
        from api.payments import tier_from_request
        tier = tier_from_request(request)
        if tier["tier"] == "free" and state != "All India":
            raise HTTPException(402, "Upgrade to Pro for state-level data")
    """
    auth = request.headers.get("Authorization")
    claims = _get_token_from_header(auth)
    return _get_tier(claims)
