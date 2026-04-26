"""
HTTP security headers, light fingerprint reduction, and optional per-IP rate limits.
"""

from __future__ import annotations

import os
import time
from collections import defaultdict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response


def _csp_value() -> str:
    """
    Allow inline scripts/styles (the dashboard is a single HTML with inline JS/CSS).
    Tightened in v2 launch: removed unused unpkg + jsdelivr origins, removed http:
    from connect-src, restricted connect-src to known own/mirror origins, added
    object-src 'none' and upgrade-insecure-requests.
    """
    return (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://subscribe-forms.beehiiv.com https://www.googletagmanager.com https://*.googletagmanager.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com data:; "
        "img-src 'self' data: blob: https:; "
        "connect-src 'self' https://www.vahanintelligence.in https://kapil433.github.io https://vahan-intelligence-api.onrender.com https://subscribe-forms.beehiiv.com https://www.google-analytics.com https://*.analytics.google.com https://*.google-analytics.com https://stats.g.doubleclick.net; "
        "frame-src https://subscribe-forms.beehiiv.com https://www.googletagmanager.com; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self' mailto:; "
        "object-src 'none'; "
        "upgrade-insecure-requests;"
    )


class ApexToWwwRedirectMiddleware(BaseHTTPMiddleware):
    """301 apex → www for the public marketing host (SEO canonical). Disable with APEX_WWW_REDIRECT=0."""

    def __init__(self, app, apex_host: str = "vahanintelligence.in", www_host: str = "www.vahanintelligence.in"):
        super().__init__(app)
        self._apex = apex_host.lower()
        self._www = www_host.lower()

    async def dispatch(self, request: Request, call_next):
        if os.getenv("APEX_WWW_REDIRECT", "1").strip().lower() in ("0", "false", "no"):
            return await call_next(request)
        host = (request.headers.get("host") or "").split(":")[0].lower()
        if host == self._apex:
            path = request.url.path or "/"
            q = request.url.query
            loc = f"https://{self._www}{path}"
            if q:
                loc = f"{loc}?{q}"
            return RedirectResponse(loc, status_code=301)
        return await call_next(request)


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
        # Cross-origin isolation hardening (cheap, no UX impact for a static dashboard).
        response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
        response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
        response.headers.setdefault("X-DNS-Prefetch-Control", "off")
        response.headers.setdefault("Content-Security-Policy", _csp_value())
        # HSTS is now ON by default (set DISABLE_HSTS=1 to opt out — useful only for
        # pre-prod envs running on plain http). 1 year + subdomains + preload-eligible.
        if os.getenv("DISABLE_HSTS", "").strip().lower() not in ("1", "true", "yes"):
            response.headers.setdefault(
                "Strict-Transport-Security",
                "max-age=31536000; includeSubDomains; preload",
            )
        for h in ("server", "Server", "x-powered-by", "X-Powered-By"):
            if h in response.headers:
                del response.headers[h]
        return response


# path prefix -> max requests per window
_default_limits: dict[str, tuple[int, float]] = {
    # Tightened in v9 anti-scraping pass: the dashboard makes 1 bundle request
    # per page load — anything above 5/min from a single IP is bot territory.
    "/data/vahan_master_compat": (5, 60.0),
    "/data/vahan_master.json":   (5, 60.0),
    "/scrape": (8, 60.0),
}


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple fixed-window counter per client IP (best-effort; use Redis in multi-node prod)."""

    def __init__(self, app, limits: dict[str, tuple[int, float]] | None = None):
        super().__init__(app)
        self._limits = limits or _default_limits
        self._hits: dict[str, list[float]] = defaultdict(list)

    def _client_ip(self, request: Request) -> str:
        # Honour Cloudflare / Render forward headers when present.
        for h in ("cf-connecting-ip", "x-forwarded-for", "x-real-ip"):
            v = request.headers.get(h)
            if v:
                return v.split(",")[0].strip()
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


# ── Anti-scraping: Origin / Referer check on /data/* endpoints ───────────────
#
# Real users hit /data/vahan_master_compat from one of:
#   - https://www.vahanintelligence.in   (the GitHub Pages site)
#   - https://vahan-intelligence-api.onrender.com  (the FastAPI service itself)
#   - https://kapil433.github.io  (the optional GH Pages mirror)
#
# Direct curl/python `requests` calls don't send Origin or Referer matching any
# of those. This middleware lets them through CORS preflight (already handled
# upstream) but blocks the actual GET if both Origin AND Referer are missing or
# foreign. Determined scrapers can spoof headers, but this raises the bar for
# casual `curl URL > out.json` style scraping.
class DataReferrerGuardMiddleware(BaseHTTPMiddleware):
    ALLOWED_HOSTS = (
        "www.vahanintelligence.in",
        "vahanintelligence.in",
        "vahan-intelligence-api.onrender.com",
        "kapil433.github.io",
        "localhost",
        "127.0.0.1",
    )
    GUARDED_PREFIXES = ("/data/",)

    def _is_guarded(self, path: str) -> bool:
        return any(path.startswith(p) for p in self.GUARDED_PREFIXES)

    def _host_allowed(self, value: str | None) -> bool:
        if not value:
            return False
        try:
            from urllib.parse import urlparse
            netloc = urlparse(value).netloc.lower().split(":")[0]
        except Exception:
            return False
        return any(netloc == h or netloc.endswith("." + h) for h in self.ALLOWED_HOSTS)

    async def dispatch(self, request: Request, call_next):
        if request.method == "OPTIONS" or not self._is_guarded(request.url.path):
            return await call_next(request)
        # Allow when env var is set (useful for CI / local testing)
        if os.getenv("DISABLE_REFERER_GUARD", "").strip().lower() in ("1", "true", "yes"):
            return await call_next(request)
        origin = request.headers.get("origin")
        referer = request.headers.get("referer")
        if self._host_allowed(origin) or self._host_allowed(referer):
            return await call_next(request)
        # Block: missing or foreign origin/referer on a guarded data endpoint.
        return JSONResponse(
            {"detail": "Direct data fetches require a valid Origin or Referer. "
                       "See https://www.vahanintelligence.in/#about for terms of use."},
            status_code=403,
        )
