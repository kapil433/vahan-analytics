"""
HTTP security headers, light fingerprint reduction, and optional per-IP rate limits.
"""

from __future__ import annotations

import os
import time
from collections import defaultdict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


def _csp_value() -> str:
    """Allow inline scripts/styles (dashboard is a single HTML file with inline JS/CSS)."""
    return (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com data:; "
        "img-src 'self' data: blob: https:; "
        "connect-src 'self' https: http:; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self' mailto:;"
    )


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault(
            "Permissions-Policy",
            "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()",
        )
        response.headers.setdefault("Content-Security-Policy", _csp_value())
        if os.getenv("ENABLE_HSTS", "").strip().lower() in ("1", "true", "yes"):
            response.headers.setdefault(
                "Strict-Transport-Security",
                "max-age=31536000; includeSubDomains",
            )
        for h in ("server", "Server", "x-powered-by", "X-Powered-By"):
            if h in response.headers:
                del response.headers[h]
        return response


# path prefix -> max requests per window
_default_limits: dict[str, tuple[int, float]] = {
    "/data/vahan_master_compat": (45, 60.0),
    "/data/vahan_master.json": (45, 60.0),
    "/scrape": (8, 60.0),
}


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple fixed-window counter per client IP (best-effort; use Redis in multi-node prod)."""

    def __init__(self, app, limits: dict[str, tuple[int, float]] | None = None):
        super().__init__(app)
        self._limits = limits or _default_limits
        self._hits: dict[str, list[float]] = defaultdict(list)

    def _client_ip(self, request: Request) -> str:
        if request.client:
            return request.client.host
        return "unknown"

    def _prune(self, key: str, window: float, now: float) -> None:
        lst = self._hits[key]
        cutoff = now - window
        self._hits[key] = [t for t in lst if t > cutoff]

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        rule = None
        for prefix, spec in self._limits.items():
            if path == prefix or path.startswith(prefix + "/"):
                rule = spec
                break
        if rule is None or request.method == "OPTIONS":
            return await call_next(request)
        max_n, window = rule
        now = time.monotonic()
        ip = self._client_ip(request)
        key = f"{ip}:{path.split('/')[1] if '/' in path else path}"
        self._prune(key, window, now)
        if len(self._hits[key]) >= max_n:
            return JSONResponse(
                {"detail": "Too many requests. Please wait and try again."},
                status_code=429,
                headers={"Retry-After": str(int(window))},
            )
        self._hits[key].append(now)
        return await call_next(request)
