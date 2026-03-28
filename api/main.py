"""
Vahan Scraper API — Dashboard at GET /, scraper UI at GET /scraper (api/static/index.html), Swagger.
Uses vahan_scraper_master for batch scrape. Output: output/vahan_data/.
"""

import os
import sys
from pathlib import Path

# Project root + Windows console UTF-8 before any other imports (avoids cp1252 scrape crashes).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from scraper.console_win import configure_stdio_utf8

configure_stdio_utf8()

import json
import re
import signal
import sqlite3
from contextlib import asynccontextmanager

os.environ.setdefault("VAHAN_SCRAPER_BACKEND", "selenium")

from typing import Annotated, Any, Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, Response
from starlette.middleware.gzip import GZipMiddleware

from api.middleware_security import ApexToWwwRedirectMiddleware, RateLimitMiddleware, SecurityHeadersMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from config.mappings import MAKER_MAP, maker_strings_for_ui_short, month_to_fy, normalize_maker
from api.data_policy import (
    EXCLUDED_STATE_CODES,
    EXCLUSION_REASON,
    append_exclude_state_codes_sql,
)
from api.master_bundle import build_vahan_master_bundle
from config.scraping_config import (
    BASE_FILTERS,
    FUEL_GROUP_CHECKBOX_IDS,
    PORTAL_FILTER_CHOICES,
    YEAR_MIN,
    YEAR_MAX,
    financial_year_labels_analytics,
)
from scraper.state_aggregate import is_aggregate_state_name

# States: national aggregate (wording varies by portal build) + states/UTs (portal exact names).
# First option must match the live State dropdown label (see output/discovery/reportview_page.html after discovery).
AVAILABLE_STATES = [
    "All Vahan4 Running States (36/36)",
    "All Vahan4 Running State",
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jammu and Kashmir",
    "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra",
    "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab",
    "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal", "Delhi", "Puducherry",
    "Lakshadweep", "Andaman and Nicobar Islands",
    "Dadra and Nagar Haveli and Daman and Diu", "Chandigarh", "Ladakh",
]

AVAILABLE_FUELS = list(FUEL_GROUP_CHECKBOX_IDS.keys())
AVAILABLE_YEARS = list(range(YEAR_MIN, YEAR_MAX + 1))
_PLATFORM_ANALYTICS_FY_LABELS: frozenset[str] | None = None


def _platform_analytics_fy_allowed() -> frozenset[str]:
    """FY labels allowed for platform_context `fy` (matches UI: no FY2011-12, no FY2026-27 when YEAR_MAX=2026)."""
    global _PLATFORM_ANALYTICS_FY_LABELS
    if _PLATFORM_ANALYTICS_FY_LABELS is None:
        _PLATFORM_ANALYTICS_FY_LABELS = frozenset(financial_year_labels_analytics())
    return _PLATFORM_ANALYTICS_FY_LABELS

def _normalize_explicit_states(states: list[str]) -> list[str]:
    """
    Drop all-India aggregate entries when one or more real states are also selected.

    The HTML multiselect used to pre-select the aggregate; users often add a state with
    Ctrl+click without clearing the default, which queued All-India jobs alongside the
    intended state and left the dashboard on national scope for those jobs.
    """
    if len(states) <= 1:
        return states
    specifics = [s for s in states if not is_aggregate_state_name(s)]
    return specifics if specifics else states


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_BASE = PROJECT_ROOT / "output" / "vahan_data"
RESEARCH_DIR = PROJECT_ROOT / "research_data"
SQLITE_LOCAL = PROJECT_ROOT / "data" / "vahan_local.db"
# Bundled demo dataset when no DB (fresh Render / local API without PostgreSQL or SQLite load).
STATIC_MASTER_JSON = PROJECT_ROOT / "docs" / "data" / "vahan_master.json"

# In-memory cache for GET /data/vahan_master_compat: serialized JSON bytes + fingerprint.
# Invalidates when row count or latest loaded_at changes (reload / upsert).
_vahan_bundle_cache_fp: tuple[int, str] | None = None
_vahan_bundle_cache_body: bytes | None = None

# Expose background scrape errors to UI
last_scrape_error: str | None = None

# Last PostgreSQL connection error (for 503 detail when DB is down / misconfigured)
_last_pg_connect_error: str | None = None


def _database_unavailable_detail() -> str:
    base = (
        "No database available. Either: (1) Start PostgreSQL, set DATABASE_URL, run migrations/, "
        "then python scripts/load_vahan_to_db.py — or (2) run python scripts/setup_local_sqlite.py "
        "to create data/vahan_local.db (no PostgreSQL)."
    )
    if _last_pg_connect_error:
        return f"{base} PostgreSQL error: {_last_pg_connect_error}"
    return base


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Chain SIGINT/SIGBREAK so Ctrl+C reaches the scrape batch (thread-safe stop event)
    and still lets uvicorn shut down. Without this, scrape runs on a worker thread and
    never sees the signal; a non-daemon thread then blocks process exit.
    """
    from scraper.batch_control import request_batch_stop
    from scraper.sigint_bridge import chain_sigint

    prev_int = signal.getsignal(signal.SIGINT)
    prev_break = signal.getsignal(signal.SIGBREAK) if hasattr(signal, "SIGBREAK") else None
    try:
        signal.signal(signal.SIGINT, chain_sigint(prev_int))
        if prev_break is not None:
            signal.signal(signal.SIGBREAK, chain_sigint(prev_break))
    except ValueError:
        yield
        return
    yield
    request_batch_stop()
    try:
        signal.signal(signal.SIGINT, prev_int)
        if prev_break is not None:
            signal.signal(signal.SIGBREAK, prev_break)
    except ValueError:
        pass


app = FastAPI(
    title="Vahan Scraper API",
    description="Scrape vehicle registration data from Vahan Parivahan. Select states, fuels, years.",
    version="1.0.0",
    docs_url=None,  # Custom /docs below with alternate CDN (unpkg) if jsdelivr blocked
    redoc_url="/redoc",
    lifespan=lifespan,
)


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui():
    """Swagger UI using unpkg CDN (fallback if cdn.jsdelivr.net is blocked)."""
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        swagger_js_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css",
    )
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1_000)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(ApexToWwwRedirectMiddleware)

STATIC_DIR = Path(__file__).resolve().parent / "static"
DASHBOARD_DIR = STATIC_DIR / "dashboard"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/robots.txt", include_in_schema=False)
def serve_robots_txt():
    p = STATIC_DIR / "robots.txt"
    if p.is_file():
        return FileResponse(str(p), media_type="text/plain")
    return Response("User-agent: *\nAllow: /\n", media_type="text/plain")


@app.get("/sitemap.xml", include_in_schema=False)
def serve_sitemap_xml():
    p = STATIC_DIR / "sitemap.xml"
    if p.is_file():
        return FileResponse(str(p), media_type="application/xml")
    return Response(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>',
        media_type="application/xml",
    )


@app.get("/google5332b27a4f971584.html", include_in_schema=False)
def serve_google_search_console_verification():
    """Google Search Console HTML file verification (root URL)."""
    p = STATIC_DIR / "google5332b27a4f971584.html"
    if p.is_file():
        return FileResponse(str(p), media_type="text/html")
    return Response(status_code=404)


@app.get("/health")
def health():
    """Quick health check for UI to verify API is reachable."""
    conn, kind = _connect_data()
    db_ok = conn is not None
    if conn:
        conn.close()
    out: dict = {"ok": True, "database_reachable": db_ok, "database_backend": kind}
    if not db_ok and _last_pg_connect_error:
        out["database_error"] = _last_pg_connect_error
    if not db_ok and SQLITE_LOCAL.is_file():
        out["sqlite_file_exists"] = True
        out["hint"] = "SQLite file present but unreadable — check permissions or run setup_local_sqlite.py again."
    elif not db_ok:
        out["hint"] = "Run: python scripts/setup_local_sqlite.py"
    return out


@app.get("/")
def serve_root_dashboard():
    """Canonical public URL: full analytics dashboard (same HTML as legacy /dashboard path)."""
    dash = DASHBOARD_DIR / "index.html"
    if dash.is_file():
        return FileResponse(
            dash,
            media_type="text/html",
            headers={"Cache-Control": "no-cache, no-store"},
        )
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(
            index_path,
            media_type="text/html",
            headers={"Cache-Control": "no-cache, no-store"},
        )
    return {"message": "UI not found. Use /docs for Swagger."}


@app.get("/scraper", include_in_schema=False)
def serve_scraper_ui():
    """Vahan multiselect scraper UI (moved from / for SEO — canonical dashboard is /)."""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(
            index_path,
            media_type="text/html",
            headers={"Cache-Control": "no-cache, no-store"},
        )
    raise HTTPException(404, "scraper UI not found")


class ScrapeRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    states: list[str] = Field(
        default_factory=list,
        description="State names. Empty = all states. If both All-India aggregate and a state appear, only the state(s) are used.",
    )
    fuels: list[str] = Field(default_factory=list, description="Fuel types. Empty = all (CNG, Petrol, Diesel, EV, Strong Hybrid).")
    years: list[int] = Field(default_factory=list, description="Years 2012-2026. Empty = all years.")
    y_axis: str | None = Field(
        default=None,
        validation_alias=AliasChoices("y_axis", "yAxis"),
        description="Portal top-bar Y-Axis visible label (e.g. Maker). Omit = scraper default.",
    )
    x_axis: str | None = Field(
        default=None,
        validation_alias=AliasChoices("x_axis", "xAxis"),
        description="Portal X-Axis visible label (e.g. Month Wise). Omit = scraper default.",
    )
    year_type: str | None = Field(
        default=None,
        validation_alias=AliasChoices("year_type", "yearType"),
        description="Portal Year type (e.g. Calendar Year). Omit = scraper default.",
    )
    parallel: bool = Field(
        default=True,
        validation_alias=AliasChoices("parallel", "use_parallel"),
        description="Use multiple browser windows when there are multiple state×year jobs",
    )
    max_workers: int = Field(
        default=8,
        ge=1,
        le=12,
        validation_alias=AliasChoices("max_workers", "maxWorkers"),
        description="Max parallel browser windows (capped by number of state×year jobs)",
    )

    @field_validator("parallel", mode="before")
    @classmethod
    def _coerce_parallel(cls, v: object) -> bool:
        """Accept true/false from JSON and loose string/int forms (some clients send strings)."""
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v != 0
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "on", "y")
        return bool(v)


class PortalOptionsOut(BaseModel):
    """Defaults and allowed labels for Vahan reportview top-bar (matches scraper BASE_FILTERS / portal)."""

    y_axis_default: str
    x_axis_default: str
    year_type_default: str
    y_axis_options: list[str]
    x_axis_options: list[str]
    year_type_options: list[str]


class OptionsResponse(BaseModel):
    states: list[str]
    fuels: list[str]
    years: list[int]
    calendar_year_min: int
    calendar_year_max: int
    financial_years: list[str]
    portal: PortalOptionsOut


class ScrapeResponse(BaseModel):
    message: str
    output_dir: str
    output_files: list[str]
    total: int
    parallel: bool
    max_workers: int
    batch_jobs: int
    concurrent_cap: int


def _portal_filters_from_scrape_request(req: ScrapeRequest) -> dict[str, str] | None:
    """Non-empty override dict for scraper top-bar (keys y_axis, x_axis, year_type)."""
    out: dict[str, str] = {}
    if req.y_axis is not None and str(req.y_axis).strip():
        out["y_axis"] = str(req.y_axis).strip()
    if req.x_axis is not None and str(req.x_axis).strip():
        out["x_axis"] = str(req.x_axis).strip()
    if req.year_type is not None and str(req.year_type).strip():
        out["year_type"] = str(req.year_type).strip()
    return out or None


def _validate_portal_filters(pf: dict[str, str] | None) -> None:
    if not pf:
        return
    for key, val in pf.items():
        allowed = PORTAL_FILTER_CHOICES.get(key)
        if allowed is not None and val not in allowed:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid portal filter {key}={val!r}. Allowed: {allowed}",
            )


def _run_scrape_impl(states, years, fuels, parallel, max_workers, portal_filters):
    """Run scrape in-process (direct call to master scraper)."""
    global last_scrape_error
    import traceback

    from scraper.console_win import configure_stdio_utf8

    configure_stdio_utf8()

    last_scrape_error = None
    n_states, n_years = len(states), len(years)
    print(
        f"\n[Scrape] Background thread started: {n_states} states x {n_years} years, "
        f"parallel={parallel}, max_workers={max_workers}. Watch this console for progress.\n",
        flush=True,
    )
    try:
        from scraper.vahan_scraper_master import run_batch_parallel, run_batch_sequential
        if parallel:
            run_batch_parallel(
                states,
                years,
                fuels,
                OUTPUT_BASE,
                headless=False,
                max_workers=max_workers,
                portal_filters=portal_filters,
            )
        else:
            run_batch_sequential(
                states,
                years,
                fuels,
                OUTPUT_BASE,
                headless=False,
                portal_filters=portal_filters,
            )
    except Exception as e:
        last_scrape_error = f"{type(e).__name__}: {e}"
        print(f"[Scrape] Failed: {e}")
        traceback.print_exc()


def run_scrape_task(
    states: list[str],
    years: list[int],
    fuels: list[str],
    parallel: bool,
    max_workers: int,
    portal_filters: dict[str, str] | None,
):
    """Start scrape in background thread (daemon so Ctrl+C + uvicorn exit can terminate the process)."""
    import threading

    t = threading.Thread(
        target=_run_scrape_impl,
        args=(states, years, fuels, parallel, max_workers, portal_filters),
        daemon=True,
        name="vahan-scrape",
    )
    t.start()


@app.get("/scrape/status")
def scrape_status():
    """Return last scrape error if any (for UI polling)."""
    return {"error": last_scrape_error} if last_scrape_error else {"status": "ok"}


@app.post("/scrape/stop")
def stop_scrape():
    """
    Cooperative stop for a running batch (works when Ctrl+C does not, e.g. focus on Chrome).
    Sets a shared flag; parallel workers exit at their next batch_stop check and the pool cancels pending work.
    """
    from scraper.batch_control import request_batch_stop

    request_batch_stop()
    return {
        "ok": True,
        "message": "Stop requested. Running jobs will stop at the next checkpoint; pending jobs are cancelled.",
    }


@app.get("/options", response_model=OptionsResponse)
def get_options():
    """Multiselect options (states, fuels, calendar years) + `financial_years` for platform FY mode + portal scraper defaults."""
    return OptionsResponse(
        states=AVAILABLE_STATES,
        fuels=AVAILABLE_FUELS,
        years=AVAILABLE_YEARS,
        calendar_year_min=YEAR_MIN,
        calendar_year_max=YEAR_MAX,
        financial_years=financial_year_labels_analytics(),
        portal=PortalOptionsOut(
            y_axis_default=BASE_FILTERS["y_axis"],
            x_axis_default=BASE_FILTERS["x_axis"],
            year_type_default=BASE_FILTERS.get("year_type", "Calendar Year"),
            y_axis_options=list(PORTAL_FILTER_CHOICES["y_axis"]),
            x_axis_options=list(PORTAL_FILTER_CHOICES["x_axis"]),
            year_type_options=list(PORTAL_FILTER_CHOICES["year_type"]),
        ),
    )


def _scraper_runtime_available() -> bool:
    """False on slim installs (e.g. requirements-render.txt) where Selenium is omitted."""
    try:
        import selenium  # noqa: F401
    except ImportError:
        return False
    return True


@app.post("/scrape", response_model=ScrapeResponse)
def start_scrape(req: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Start scrape. Runs in background. Output files saved to output/vahan_data/.
    Each state-year produces: {state}_{year}_merged.csv
    """
    if not _scraper_runtime_available():
        raise HTTPException(
            501,
            "Batch scraping is not available on this server (no Selenium/Chrome). "
            "Run scrapes locally with full requirements.txt, or use PostgreSQL/SQLite data only.",
        )
    states = req.states if req.states else AVAILABLE_STATES
    fuels = req.fuels if req.fuels else AVAILABLE_FUELS
    years = req.years if req.years else AVAILABLE_YEARS
    if req.states:
        states = _normalize_explicit_states(states)

    # Validate
    invalid_states = [s for s in states if s not in AVAILABLE_STATES]
    if invalid_states:
        raise HTTPException(400, f"Invalid states: {invalid_states}")
    invalid_fuels = [f for f in fuels if f not in AVAILABLE_FUELS]
    if invalid_fuels:
        raise HTTPException(400, f"Invalid fuels: {invalid_fuels}")
    invalid_years = [y for y in years if y < YEAR_MIN or y > YEAR_MAX]
    if invalid_years:
        raise HTTPException(400, f"Years must be {YEAR_MIN}-{YEAR_MAX}. Invalid: {invalid_years}")

    portal_filters = _portal_filters_from_scrape_request(req)
    _validate_portal_filters(portal_filters)

    total = len(states) * len(years)
    safe = lambda s: s.replace("/", "_").replace(":", "_")
    output_files = [f"{safe(s)}_{y}_merged.csv" for s in states for y in years]

    use_parallel = bool(req.parallel)
    workers = max(1, int(req.max_workers))
    concurrent_cap = min(workers, total) if use_parallel else 1
    # Keyword args so a future arg-order mistake cannot swap parallel vs max_workers.
    background_tasks.add_task(
        run_scrape_task,
        states,
        years,
        fuels,
        use_parallel,
        workers,
        portal_filters,
    )

    if total <= 1:
        mode_note = " One state x year job - only one browser will open."
    elif use_parallel:
        mode_note = (
            f" Parallel on: requested {workers} windows; effective concurrency is "
            f"{concurrent_cap} (min of that and {total} job(s))."
        )
    else:
        mode_note = " Sequential mode - one browser, jobs run one after another."

    return ScrapeResponse(
        message=(
            f"Scrape started: {len(states)} states x {len(years)} years = {total} job(s).{mode_note} "
            f"Check output/vahan_data/ and the terminal."
        ),
        output_dir=str(OUTPUT_BASE),
        output_files=output_files,
        total=total,
        parallel=use_parallel,
        max_workers=workers,
        batch_jobs=total,
        concurrent_cap=concurrent_cap,
    )


@app.get("/outputs")
def list_outputs():
    """List raw registration CSVs: *_merged.csv plus any *.csv under output/vahan_data/f1/."""
    from scripts.clean_vahan_data import iter_raw_vahan_csv_files

    if not OUTPUT_BASE.exists():
        return {"files": [], "output_dir": str(OUTPUT_BASE)}
    files = sorted(
        str(p.relative_to(OUTPUT_BASE).as_posix()) for p in iter_raw_vahan_csv_files(OUTPUT_BASE, recursive=True)
    )
    return {"files": files, "output_dir": str(OUTPUT_BASE)}


@app.get("/platform", include_in_schema=False)
def serve_analytics_platform():
    """Vahan Analytics Platform dashboard (legacy-style HTML + Chart.js)."""
    path = STATIC_DIR / "platform.html"
    if path.exists():
        return FileResponse(
            path,
            media_type="text/html",
            headers={"Cache-Control": "no-cache, no-store"},
        )
    raise HTTPException(404, "platform.html not found")


@app.get("/dashboard", include_in_schema=False)
def redirect_dashboard_to_canonical_root():
    """301 to / — dashboard HTML is served at the site root for a single canonical URL."""
    return RedirectResponse(url="/", status_code=301)


@app.get("/data/vahan_master_compat")
def vahan_master_compat():
    """
    Legacy vahan_master.json-shaped bundle for the dashboard (regions, makers, fuels, encoded data rows).
    Merges static analytics overlay from api/static/dashboard/legacy_overlay.json for Intelligence pages.

    Responses are cached in-process until ``vahan_registrations`` row count or ``MAX(loaded_at)``
    changes, and gzip-compressed when large (see GZipMiddleware).
    """
    global _vahan_bundle_cache_fp, _vahan_bundle_cache_body
    conn, dialect = _connect_data()
    if conn:
        try:
            fp = _vahan_registrations_fingerprint(conn, dialect)
            if (
                fp is not None
                and fp == _vahan_bundle_cache_fp
                and _vahan_bundle_cache_body is not None
            ):
                return Response(
                    content=_vahan_bundle_cache_body,
                    media_type="application/json; charset=utf-8",
                    headers={
                        "X-Vahan-Data-Source": "database-cache",
                        "Cache-Control": "public, max-age=120",
                    },
                )
            bundle = build_vahan_master_bundle(conn, dialect=dialect)
            body = json.dumps(bundle, ensure_ascii=False).encode("utf-8")
            if fp is not None:
                _vahan_bundle_cache_fp = fp
                _vahan_bundle_cache_body = body
            return Response(
                content=body,
                media_type="application/json; charset=utf-8",
                headers={
                    "X-Vahan-Data-Source": "database",
                    "Cache-Control": "public, max-age=120",
                },
            )
        except Exception as e:
            raise HTTPException(500, str(e))
        finally:
            conn.close()
    if STATIC_MASTER_JSON.is_file():
        return FileResponse(
            str(STATIC_MASTER_JSON),
            media_type="application/json",
            headers={"X-Vahan-Data-Source": "static-docs-data"},
        )
    raise HTTPException(503, _database_unavailable_detail())


@app.get("/data/vahan_master.json")
def vahan_master_json_file():
    """Same bundle as compat static fallback; helps relative fetch(`data/vahan_master.json`) from `/` or `/dashboard/`."""
    if STATIC_MASTER_JSON.is_file():
        return FileResponse(
            str(STATIC_MASTER_JSON),
            media_type="application/json",
            headers={"X-Vahan-Data-Source": "static-docs-data"},
        )
    raise HTTPException(503, _database_unavailable_detail())


@app.get("/mock-portal", include_in_schema=False)
def serve_mock_portal_stub():
    """Static note + pointers for offline merge verification (not a PrimeFaces simulator)."""
    path = STATIC_DIR / "mock_portal.html"
    if path.exists():
        return FileResponse(
            path,
            media_type="text/html",
            headers={"Cache-Control": "no-cache, no-store"},
        )
    raise HTTPException(404, "mock_portal.html not found")


# --- Data API for Vahan Analytics Dashboard ---

def _try_postgres():
    """Return psycopg2 connection with RealDictCursor, or None."""
    global _last_pg_connect_error
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        _last_pg_connect_error = "psycopg2 is not installed"
        return None

    url = os.getenv("DATABASE_URL", "postgresql://localhost/vahan_analytics")
    try:
        conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
        _last_pg_connect_error = None
        return conn
    except Exception as e:
        line = str(e).strip().split("\n")[0]
        _last_pg_connect_error = (line[:280] + "…") if len(line) > 280 else line
        return None


def _connect_data():
    """
    Prefer PostgreSQL; if unavailable, use data/vahan_local.db when present (see scripts/setup_local_sqlite.py).
    Returns (connection, 'postgres'|'sqlite') or (None, None).
    """
    pg = _try_postgres()
    if pg is not None:
        return pg, "postgres"
    if SQLITE_LOCAL.is_file():
        try:
            c = sqlite3.connect(str(SQLITE_LOCAL), check_same_thread=False)
            c.row_factory = sqlite3.Row
            c.execute("SELECT 1 FROM vahan_registrations LIMIT 1")
            return c, "sqlite"
        except sqlite3.Error as e:
            _last_pg_connect_error = f"SQLite {SQLITE_LOCAL.name}: {e}"
            return None, None
    return None, None


def _exec(cur, dialect: str, q: str, params: list) -> None:
    if dialect == "sqlite":
        q = q.replace("%s", "?")
    cur.execute(q, params)


def _vahan_registrations_fingerprint(conn: Any, dialect: str) -> tuple[int, str] | None:
    """Cheap cache token: changes when data is loaded or updated."""
    try:
        cur = conn.cursor()
        try:
            q = "SELECT COUNT(*) AS c, MAX(loaded_at) AS m FROM vahan_registrations"
            _exec(cur, dialect, q, [])
            row = cur.fetchone()
            if row is None:
                return None
            if isinstance(row, sqlite3.Row):
                c = int(row["c"] or 0)
                m = row["m"]
            elif isinstance(row, dict):
                c = int(row.get("c") or 0)
                m = row.get("m")
            else:
                c = int(row[0] or 0)
                m = row[1] if len(row) > 1 else None
            m_s = str(m) if m is not None else ""
            return (c, m_s)
        finally:
            cur.close()
    except Exception:
        return None




# Indian FY in SQL — matches config.mappings.month_to_fy (works without physical `fy` column)
_VR = "vahan_registrations"
_SQL_FY_LABEL = (
    f"('FY' || (CASE WHEN {_VR}.month >= 4 THEN {_VR}.year ELSE {_VR}.year - 1 END)::text || '-' || "
    f"RIGHT((CASE WHEN {_VR}.month >= 4 THEN {_VR}.year + 1 ELSE {_VR}.year END)::text, 2))"
)


def _row_with_fy(row: dict) -> dict:
    d = dict(row)
    if d.get("fy"):
        return d
    y, m = d.get("year"), d.get("month")
    if y is not None and m is not None:
        try:
            d["fy"] = month_to_fy(int(y), int(m))
        except (TypeError, ValueError):
            pass
    return d


def _add_fy_filter(q: str, params: list, fy: str | None, dialect: str) -> str:
    if not fy:
        return q
    if dialect == "sqlite":
        q += " AND fy = %s"
        params.append(fy)
    else:
        q += f" AND {_SQL_FY_LABEL} = %s"
        params.append(fy)
    return q


def _add_maker_filter(q: str, params: list, maker: str | None) -> str:
    if not maker or not str(maker).strip():
        return q
    q += " AND lower(maker) LIKE lower(%s)"
    params.append(f"%{str(maker).strip()}%")
    return q


def _append_fuel_filters(
    q: str,
    params: list,
    fuel_type: str | None,
    fuel_types: list[str] | None,
) -> str:
    fts = [str(x).strip() for x in (fuel_types or []) if x and str(x).strip()]
    if fts:
        ph = ",".join(["%s"] * len(fts))
        q += f" AND fuel_type IN ({ph})"
        params.extend(fts)
        return q
    if fuel_type and str(fuel_type).strip():
        q += " AND fuel_type = %s"
        params.append(str(fuel_type).strip())
    return q


def _expand_maker_values_for_in_clause(makers: list[str]) -> list[str]:
    """UI sends short OEM labels (Maruti, Tata, …) or raw portal strings; expand for SQL IN."""
    out: set[str] = set()
    for m in makers:
        s = str(m).strip()
        if not s:
            continue
        expanded = maker_strings_for_ui_short(s)
        if expanded:
            out.update(expanded)
            continue
        if s == "Others":
            continue
        canon = normalize_maker(s) or s
        out.add(s)
        out.add(canon)
        for raw, mapped in MAKER_MAP.items():
            if mapped == canon:
                out.add(raw)
                out.add(mapped)
    return list(out)


def _aggregate_maker_totals(rows: list[dict]) -> list[dict]:
    from collections import defaultdict

    acc: defaultdict[str, int] = defaultdict(int)
    for r in rows:
        raw = str(r.get("maker") or "")
        key = normalize_maker(raw) or raw
        acc[key] += int(r.get("total") or 0)
    out = [{"maker": k, "total": v} for k, v in acc.items()]
    out.sort(key=lambda x: -x["total"])
    return out


def _sql_year_predicate(year_list: list[int]) -> tuple[str, list]:
    ys = sorted({int(y) for y in year_list})
    if len(ys) == 1:
        return "year = %s", [ys[0]]
    ph = ",".join(["%s"] * len(ys))
    return f"year IN ({ph})", ys


def _append_maker_filters(
    q: str,
    params: list,
    maker: str | None,
    makers: list[str] | None,
) -> str:
    mks = [str(x).strip() for x in (makers or []) if x and str(x).strip()]
    if mks:
        expanded = _expand_maker_values_for_in_clause(mks)
        if not expanded:
            return q
        ph = ",".join(["%s"] * len(expanded))
        q += f" AND maker IN ({ph})"
        params.extend(expanded)
        return q
    return _add_maker_filter(q, params, maker)


def _append_month_filter(q: str, params: list, months: list[int] | None) -> str:
    if not months:
        return q
    clean: list[int] = []
    for m in months:
        try:
            mi = int(m)
        except (TypeError, ValueError):
            continue
        if 1 <= mi <= 12:
            clean.append(mi)
    if not clean:
        return q
    ph = ",".join(["%s"] * len(clean))
    q += f" AND month IN ({ph})"
    params.extend(clean)
    return q


# --- Research JSON (population, PCI, CNG, EV) — server-side merge for /data/platform_context ---
_research_json_mtime: dict[str, float] = {}
_research_json_data: dict[str, list] = {}


def _load_research_array(name: str) -> list:
    """Load research_data/{name}.json with simple mtime cache."""
    path = RESEARCH_DIR / f"{name}.json"
    if not path.is_file():
        return []
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return []
    if _research_json_mtime.get(name) != mtime:
        with open(path, encoding="utf-8") as f:
            _research_json_data[name] = json.load(f)
        _research_json_mtime[name] = mtime
    return _research_json_data.get(name, [])


def _nearest_calendar_year(years: set[int], target: int) -> int | None:
    if not years:
        return None
    if target in years:
        return target
    return min(years, key=lambda y: abs(y - target))


def _platform_pick_population(rows: list, state_code: str | None, year: int) -> dict | None:
    if not state_code or not rows:
        return None
    cand = [r for r in rows if r.get("state_code") == state_code and int(r.get("year") or 0) <= year]
    if not cand:
        return None
    cand.sort(key=lambda r: int(r.get("year") or 0), reverse=True)
    return cand[0]


def _platform_pick_pci(rows: list, state_code: str | None, year: int, fy: str | None) -> dict | None:
    if not state_code or not rows:
        return None
    if fy:
        short = str(fy).replace("FY", "", 1).strip()
        for r in rows:
            if r.get("state_code") == state_code and str(r.get("fy") or "") == short:
                return r
    target_fy = month_to_fy(year, 9).replace("FY", "", 1)
    for r in rows:
        if r.get("state_code") == state_code and str(r.get("fy") or "") == target_fy:
            return r
    cand = [r for r in rows if r.get("state_code") == state_code]
    cand.sort(key=lambda r: str(r.get("fy") or ""), reverse=True)
    return cand[0] if cand else None


def _platform_aggregate_population_national(rows: list, year: int) -> dict | None:
    """Sum latest population (per state, year ≤ anchor) for All India KPI."""
    if not rows:
        return None
    by_state: dict[str, dict] = {}
    for r in rows:
        sc = r.get("state_code")
        if not sc:
            continue
        try:
            y = int(r.get("year") or 0)
        except (TypeError, ValueError):
            continue
        if y > year:
            continue
        prev = by_state.get(sc)
        if prev is None or y > int(prev.get("year") or 0):
            by_state[sc] = r
    if not by_state:
        return None
    total = sum(int(r.get("population") or 0) for r in by_state.values())
    ref_y = max(int(r.get("year") or 0) for r in by_state.values())
    return {
        "state_code": None,
        "state_name": "All India (sum of state populations, year ≤ " + str(year) + ")",
        "population": total,
        "year": ref_y,
        "source": "research_data/population.json (aggregated)",
    }


def _platform_aggregate_pci_national(rows: list, year: int, fy: str | None) -> dict | None:
    """Mean state PCI for one FY row (All India context)."""
    if not rows:
        return None
    if fy:
        short = str(fy).replace("FY", "", 1).strip()
    else:
        short = month_to_fy(year, 9).replace("FY", "", 1)
    vals: list[float] = []
    for r in rows:
        if str(r.get("fy") or "").strip() != short:
            continue
        p = r.get("pci_rs")
        if p is None:
            continue
        try:
            vals.append(float(p))
        except (TypeError, ValueError):
            continue
    if not vals:
        return None
    avg = sum(vals) / len(vals)
    return {
        "state_code": None,
        "state_name": "All India (unweighted mean of state PCI)",
        "fy": short,
        "pci_rs": round(avg, 2),
        "source": "research_data/pci.json (aggregated)",
        "states_in_mean": len(vals),
    }


def _platform_cng_ev_series(
    cng_rows: list,
    ev_rows: list,
    year: int,
    state_code: str | None,
) -> dict:
    def ev_scope(r):
        if r.get("charger_type") and r.get("charger_type") != "total":
            return None
        return int(r.get("year") or 0), r.get("state_code")

    c_years = {int(r["year"]) for r in cng_rows if r.get("year") is not None}
    e_years = set()
    for r in ev_rows:
        t = ev_scope(r)
        if t:
            e_years.add(t[0])
    cy = _nearest_calendar_year(c_years, year)
    ey = _nearest_calendar_year(e_years, year)

    def filt_cng(r):
        if cy is None or int(r.get("year") or -1) != cy:
            return False
        if state_code:
            return r.get("state_code") == state_code
        sc = r.get("state_code")
        return sc not in EXCLUDED_STATE_CODES

    def filt_ev(r):
        t = ev_scope(r)
        if t is None:
            return False
        if ey is None or t[0] != ey:
            return False
        if state_code:
            return r.get("state_code") == state_code
        sc = r.get("state_code")
        return sc not in EXCLUDED_STATE_CODES

    c_part = [r for r in cng_rows if filt_cng(r)]
    e_part = [r for r in ev_rows if filt_ev(r)]

    if state_code:
        c_part.sort(key=lambda r: (int(r.get("month") or 0), int(r.get("year") or 0)))
        e_part.sort(key=lambda r: (int(r.get("month") or 0), int(r.get("year") or 0)))
        c_end = int(c_part[-1].get("station_count") or 0) if c_part else 0
        e_end = int(e_part[-1].get("charger_count") or 0) if e_part else 0
    else:
        from collections import defaultdict

        cm: dict[int, int] = defaultdict(int)
        em: dict[int, int] = defaultdict(int)
        for r in c_part:
            m = int(r.get("month") or 0)
            cm[m] += int(r.get("station_count") or 0)
        for r in e_part:
            m = int(r.get("month") or 0)
            em[m] += int(r.get("charger_count") or 0)
        c_part = [{"month": m, "station_count": v, "year": cy} for m, v in sorted(cm.items())]
        e_part = [{"month": m, "charger_count": v, "year": ey} for m, v in sorted(em.items())]
        c_end = int(c_part[-1]["station_count"]) if c_part else 0
        e_end = int(e_part[-1]["charger_count"]) if e_part else 0

    notes = []
    if cy is not None and cy != year:
        notes.append(f"CNG data uses nearest available calendar year {cy} (requested {year}).")
    if ey is not None and ey != year:
        notes.append(f"EV charger data uses nearest available calendar year {ey} (requested {year}).")

    return {
        "cng_calendar_year_used": cy,
        "ev_calendar_year_used": ey,
        "cng_monthly": c_part,
        "ev_monthly": e_part,
        "cng_year_end_count": c_end,
        "ev_year_end_count": e_end,
        "alignment_notes": notes,
    }


def build_platform_research_bundle(year: int, state_code: str | None, fy: str | None) -> dict:
    """Population, PCI, CNG/EV series from JSON files (no DB)."""
    pop_all = _load_research_array("population")
    pci_all = _load_research_array("pci")
    cng_all = _load_research_array("cng")
    ev_all = _load_research_array("ev_chargers")
    if state_code:
        pop = _platform_pick_population(pop_all, state_code, year)
        pci = _platform_pick_pci(pci_all, state_code, year, fy)
    else:
        pop = _platform_aggregate_population_national(pop_all, year)
        pci = _platform_aggregate_pci_national(pci_all, year, fy)
    infra = _platform_cng_ev_series(cng_all, ev_all, year, state_code)
    return {
        "population": pop,
        "pci": pci,
        "infrastructure": infra,
        "sources_hint": "research_data/*.json — see GET /research/manifest",
    }


def _compute_kpis_payload(
    cur,
    dialect: str,
    year_list: list[int],
    state_code: str | None,
    fuel_type: str | None,
    fy: str | None,
    maker: str | None = None,
    fuel_types: list[str] | None = None,
    makers: list[str] | None = None,
    months: list[int] | None = None,
) -> dict:
    ys = sorted({int(y) for y in year_list})
    if not ys:
        raise ValueError("year_list must be non-empty")
    y_pred, y_params = _sql_year_predicate(ys)
    base = f" FROM vahan_registrations WHERE {y_pred}"
    params: list = list(y_params)
    if state_code:
        base += " AND state_code = %s"
        params.append(state_code)
    base = append_exclude_state_codes_sql(base, params)
    base = _append_fuel_filters(base, params, fuel_type, fuel_types)
    base = _add_fy_filter(base, params, fy, dialect)
    base = _append_maker_filters(base, params, maker, makers)
    base = _append_month_filter(base, params, months)
    _exec(cur, dialect, "SELECT COALESCE(SUM(count), 0) AS total" + base, params)
    total = int(dict(cur.fetchone())["total"])
    _exec(
        cur,
        dialect,
        "SELECT fuel_type, SUM(count) AS subtotal" + base + " GROUP BY fuel_type ORDER BY subtotal DESC",
        params,
    )
    by_fuel = [dict(r) for r in cur.fetchall()]
    ev_total = next((int(r["subtotal"]) for r in by_fuel if r["fuel_type"] == "EV"), 0)
    ev_share_pct = round(100.0 * ev_total / total, 2) if total else 0.0

    yoy_pct = None
    prev_total = None
    fy_note = ""
    if len(ys) == 1:
        y = ys[0]
        prev_params: list = [y - 1]
        prev_base = " FROM vahan_registrations WHERE year = %s"
        if state_code:
            prev_base += " AND state_code = %s"
            prev_params.append(state_code)
        prev_base = append_exclude_state_codes_sql(prev_base, prev_params)
        prev_base = _append_fuel_filters(prev_base, prev_params, fuel_type, fuel_types)
        prev_base = _add_fy_filter(prev_base, prev_params, fy, dialect)
        prev_base = _append_maker_filters(prev_base, prev_params, maker, makers)
        prev_base = _append_month_filter(prev_base, prev_params, months)
        _exec(cur, dialect, "SELECT COALESCE(SUM(count), 0) AS total" + prev_base, prev_params)
        prev_total = int(dict(cur.fetchone())["total"])
        if prev_total > 0:
            yoy_pct = round(100.0 * (total - prev_total) / prev_total, 2)
        fy_labels = sorted({month_to_fy(y, m) for m in range(1, 13)})
        fy_note = (
            f"Calendar year {y} spans " + " & ".join(fy_labels)
            if len(fy_labels) <= 2
            else f"Calendar year {y} spans multiple FYs (Apr–Mar); e.g. {fy_labels[0]} … {fy_labels[-1]}"
        )
    else:
        fy_note = (
            f"Aggregated across {len(ys)} calendar years ({ys[0]}–{ys[-1]}); "
            "YoY not shown for multi-year slices."
        )

    return {
        "years": ys,
        "year": ys[-1],
        "state_code": state_code,
        "fuel_type": fuel_type,
        "fuel_types": fuel_types,
        "fy": fy,
        "maker_contains": maker.strip() if maker and str(maker).strip() else None,
        "makers_exact": makers,
        "months": months,
        "total_registrations": total,
        "ev_share_pct": ev_share_pct,
        "by_fuel": by_fuel,
        "yoy_growth_pct": yoy_pct,
        "prior_year_total": prev_total,
        "fy_note": fy_note,
    }


def _fetch_top_makers_data(
    cur,
    dialect: str,
    year_list: list[int],
    state_code: str | None,
    fuel_type: str | None,
    fy: str | None,
    maker: str | None,
    limit: int,
    fuel_types: list[str] | None = None,
    makers: list[str] | None = None,
    months: list[int] | None = None,
) -> list:
    lim = max(1, min(50, limit))
    y_pred, y_params = _sql_year_predicate(year_list)
    q = f"""
            SELECT maker, SUM(count) AS total
            FROM vahan_registrations
            WHERE {y_pred}
            """
    params: list = list(y_params)
    q = append_exclude_state_codes_sql(q, params)
    if state_code:
        q += " AND state_code = %s"
        params.append(state_code)
    q = _append_fuel_filters(q, params, fuel_type, fuel_types)
    q = _add_fy_filter(q, params, fy, dialect)
    q = _append_maker_filters(q, params, maker, makers)
    q = _append_month_filter(q, params, months)
    q += " GROUP BY maker"
    _exec(cur, dialect, q, params)
    merged = _aggregate_maker_totals([dict(r) for r in cur.fetchall()])
    return merged[:lim]


def _fetch_monthly_data(
    cur,
    dialect: str,
    year_list: list[int],
    state_code: str | None,
    fuel_type: str | None,
    fy: str | None,
    maker: str | None,
    fuel_types: list[str] | None = None,
    makers: list[str] | None = None,
    months: list[int] | None = None,
) -> list:
    y_pred, y_params = _sql_year_predicate(year_list)
    if dialect == "sqlite":
        q = f"""
                SELECT year, month, SUM(count) AS total, MAX(fy) AS fy
                FROM vahan_registrations
                WHERE {y_pred}
                """
    else:
        q = f"""
                SELECT year, month, SUM(count) AS total, MAX({_SQL_FY_LABEL}) AS fy
                FROM vahan_registrations
                WHERE {y_pred}
                """
    params: list = list(y_params)
    q = append_exclude_state_codes_sql(q, params)
    if state_code:
        q += " AND state_code = %s"
        params.append(state_code)
    q = _append_fuel_filters(q, params, fuel_type, fuel_types)
    q = _add_fy_filter(q, params, fy, dialect)
    q = _append_maker_filters(q, params, maker, makers)
    q = _append_month_filter(q, params, months)
    q += " GROUP BY year, month ORDER BY year, month"
    _exec(cur, dialect, q, params)
    out = []
    for r in cur.fetchall():
        d = dict(r)
        y = int(d["year"])
        m = int(d["month"])
        if not d.get("fy"):
            d["fy"] = month_to_fy(y, m)
        out.append(d)
    return out


@app.get("/data/states")
def data_states():
    """
    State list for dashboard geography filters.

    Merges (1) STATE_MAP canonical names with (2) every distinct ``state_name`` actually
    present in ``vahan_registrations`` (f1 / merged ingest). The master bundle uses DB
    ``state_name`` as ``region``; without (2), filters built only from the map often do
    not match row keys and charts look empty.
    """
    from scripts.config import STATE_MAP, normalize_state

    seen_lower: set[str] = set()
    names: list[str] = []

    def add_name(nm: str) -> None:
        s = str(nm).strip()
        if not s:
            return
        low = s.lower()
        if low in ("all india", "all vahan4 running states (36/36)"):
            return
        if low in seen_lower:
            return
        seen_lower.add(low)
        names.append(s)

    for _portal_name, (code, cname) in STATE_MAP.items():
        if code in EXCLUDED_STATE_CODES or code == "ALL":
            continue
        add_name(cname)

    conn, dialect = _connect_data()
    if conn:
        try:
            cur = conn.cursor()
            q = (
                "SELECT DISTINCT state_name FROM vahan_registrations WHERE 1=1"
                " AND UPPER(TRIM(COALESCE(state_code, ''))) != 'ALL'"
                " AND state_name IS NOT NULL AND TRIM(state_name) != ''"
            )
            params: list = []
            q = append_exclude_state_codes_sql(q, params)
            _exec(cur, dialect, q, params)
            for row in cur.fetchall():
                d = dict(row)
                add_name(d.get("state_name") or "")
        finally:
            conn.close()

    names.sort(key=lambda x: x.lower())
    out: list[dict[str, str]] = []
    for nm in names:
        pair = normalize_state(nm)
        code = pair[0] if pair else ""
        out.append({"state_code": code, "state_name": nm})
    return {
        "states": out,
        "excluded_from_analytics": [
            {"state_code": c, "reason": EXCLUSION_REASON}
            for c in sorted(EXCLUDED_STATE_CODES)
        ],
    }


@app.get("/data/registrations")
def get_registrations(
    state_code: str | None = None,
    year: int | None = None,
    fy: str | None = None,
    fuel_type: str | None = None,
    maker: str | None = None,
    limit: int = 10000,
):
    """
    Fetch vahan_registrations for dashboard.
    Filter by state_code, year, fy, fuel_type, maker.
    fy is computed in SQL so this works even before migrations/003_vahan_fy_column.sql.
    """
    conn, dialect = _connect_data()
    if not conn:
        raise HTTPException(503, _database_unavailable_detail())
    try:
        cur = conn.cursor()
        try:
            if dialect == "sqlite":
                q = "SELECT state_code, state_name, year, fuel_type, maker, month, count, fy FROM vahan_registrations WHERE 1=1"
            else:
                q = f"SELECT state_code, state_name, year, fuel_type, maker, month, count, {_SQL_FY_LABEL} AS fy FROM vahan_registrations WHERE 1=1"
            params = []
            q = append_exclude_state_codes_sql(q, params)
            if state_code:
                q += " AND state_code = %s"
                params.append(state_code)
            if year:
                q += " AND year = %s"
                params.append(year)
            q = _add_fy_filter(q, params, fy, dialect)
            if fuel_type:
                q += " AND fuel_type = %s"
                params.append(fuel_type)
            if maker:
                q += " AND lower(maker) LIKE lower(%s)"
                params.append(f"%{maker}%")
            q += f" ORDER BY year, state_code, fuel_type, maker, month LIMIT {limit}"
            _exec(cur, dialect, q, params)
            rows = cur.fetchall()
            norm = [dict(r) for r in rows]
            return {"data": [_row_with_fy(r) for r in norm], "count": len(norm)}
        finally:
            cur.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        conn.close()


@app.get("/data/aggregates")
def get_aggregates(
    year: int | None = None,
    state_code: str | None = None,
    fy: str | None = None,
):
    """
    Aggregated counts: year, state, fuel_type -> total.
    For dashboard charts.
    """
    conn, dialect = _connect_data()
    if not conn:
        raise HTTPException(503, _database_unavailable_detail())
    try:
        cur = conn.cursor()
        try:
            q = """
            SELECT state_code, state_name, year, fuel_type, SUM(count) as total
            FROM vahan_registrations
            WHERE 1=1
            """
            params = []
            q = append_exclude_state_codes_sql(q, params)
            if year:
                q += " AND year = %s"
                params.append(year)
            if state_code:
                q += " AND state_code = %s"
                params.append(state_code)
            q = _add_fy_filter(q, params, fy, dialect)
            q += " GROUP BY state_code, state_name, year, fuel_type ORDER BY year, state_code, fuel_type"
            _exec(cur, dialect, q, params)
            rows = cur.fetchall()
            return {"data": [dict(r) for r in rows]}
        finally:
            cur.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        conn.close()


@app.get("/data/kpis")
def get_kpis(
    year: int,
    state_code: str | None = None,
    fuel_type: str | None = None,
    fy: str | None = None,
    maker: str | None = None,
    fuel_types: Annotated[list[str] | None, Query()] = None,
    makers: Annotated[list[str] | None, Query()] = None,
    months: Annotated[list[int] | None, Query()] = None,
):
    """
    Summary KPIs: total registrations, EV share %, fuel breakdown, optional YoY vs previous calendar year.
    Optional fy narrows to that Indian FY within the selected calendar year (e.g. year=2024 + fy=FY2024-25 → Apr–Dec 2024).
    Optional maker: case-insensitive substring. Optional makers: exact OEM names (repeat query key).
    fuel_types: repeat param for multiselect fuels. months: 1–12 subset.
    """
    conn, dialect = _connect_data()
    if not conn:
        raise HTTPException(503, _database_unavailable_detail())
    try:
        cur = conn.cursor()
        try:
            return _compute_kpis_payload(
                cur,
                dialect,
                [year],
                state_code,
                fuel_type,
                fy,
                maker,
                fuel_types=fuel_types or None,
                makers=makers or None,
                months=months or None,
            )
        finally:
            cur.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        conn.close()


@app.get("/data/makers")
def get_top_makers(
    year: int,
    state_code: str | None = None,
    fuel_type: str | None = None,
    fy: str | None = None,
    maker: str | None = None,
    fuel_types: Annotated[list[str] | None, Query()] = None,
    makers: Annotated[list[str] | None, Query()] = None,
    months: Annotated[list[int] | None, Query()] = None,
    limit: int = 12,
):
    """Top OEMs by registration count for filters."""
    conn, dialect = _connect_data()
    if not conn:
        raise HTTPException(503, _database_unavailable_detail())
    lim = max(1, min(50, limit))
    try:
        cur = conn.cursor()
        try:
            rows = _fetch_top_makers_data(
                cur,
                dialect,
                [year],
                state_code,
                fuel_type,
                fy,
                maker,
                lim,
                fuel_types=fuel_types or None,
                makers=makers or None,
                months=months or None,
            )
            return {"data": rows}
        finally:
            cur.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        conn.close()


@app.get("/data/monthly")
def get_monthly(
    year: int,
    state_code: str | None = None,
    fuel_type: str | None = None,
    fy: str | None = None,
    maker: str | None = None,
    fuel_types: Annotated[list[str] | None, Query()] = None,
    makers: Annotated[list[str] | None, Query()] = None,
    months: Annotated[list[int] | None, Query()] = None,
):
    """Month-wise totals (1–12) with Indian FY label per month."""
    conn, dialect = _connect_data()
    if not conn:
        raise HTTPException(503, _database_unavailable_detail())
    try:
        cur = conn.cursor()
        try:
            out = _fetch_monthly_data(
                cur,
                dialect,
                [year],
                state_code,
                fuel_type,
                fy,
                maker,
                fuel_types=fuel_types or None,
                makers=makers or None,
                months=months or None,
            )
            return {"data": out}
        finally:
            cur.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        conn.close()


def _pci_fy_key_for_calendar_year(calendar_year: int) -> str:
    """Match state_per_capita_income.fy (e.g. 2024-25) from a calendar year."""
    return month_to_fy(calendar_year, 9).replace("FY", "", 1)


@app.get("/data/enriched")
def get_data_enriched(year: int, state_code: str | None = None):
    """
    Per-state totals for one calendar year joined with population, PCI, CNG stations (sum), EV chargers (year snapshot).
    Excludes ALL aggregate rows. Requires migrations/001 enriched tables populated.
    """
    conn, dialect = _connect_data()
    if not conn:
        raise HTTPException(503, _database_unavailable_detail())
    if dialect == "sqlite":
        raise HTTPException(
            503,
            "Enriched data requires PostgreSQL with migration 001 tables (population, PCI, CNG, EV). "
            "SQLite mode serves /data/kpis and the dashboard at / only.",
        )
    pci_fy = _pci_fy_key_for_calendar_year(year)
    try:
        cur = conn.cursor()
        try:
            q = """
            WITH reg AS (
                SELECT state_code, state_name, year, SUM(count)::bigint AS total_registrations
                FROM vahan_registrations
                WHERE year = %s AND state_code <> 'ALL'
            """
            params: list = [year]
            q = append_exclude_state_codes_sql(q, params)
            if state_code:
                q += " AND state_code = %s"
                params.append(state_code)
            q += """
                GROUP BY state_code, state_name, year
            )
            SELECT
                reg.state_code,
                reg.state_name,
                reg.year,
                reg.total_registrations,
                sp.population,
                pci.pci_rs,
                CASE
                    WHEN sp.population IS NOT NULL AND sp.population > 0
                    THEN reg.total_registrations::float / sp.population::float
                    ELSE NULL
                END AS registrations_per_capita,
                cng.cng_stations_year,
                ev.ev_chargers_year
            FROM reg
            LEFT JOIN state_population sp
                ON sp.state_code = reg.state_code AND sp.year = reg.year
            LEFT JOIN state_per_capita_income pci
                ON pci.state_code = reg.state_code AND pci.fy = %s
            LEFT JOIN LATERAL (
                SELECT COALESCE(SUM(station_count), 0)::bigint AS cng_stations_year
                FROM cng_stations c
                WHERE c.state_code = reg.state_code AND c.year = reg.year
            ) cng ON true
            LEFT JOIN LATERAL (
                SELECT COALESCE(MAX(charger_count), 0)::bigint AS ev_chargers_year
                FROM ev_chargers e
                WHERE e.state_code = reg.state_code AND e.year = reg.year
            ) ev ON true
            ORDER BY reg.state_name
            """
            params.append(pci_fy)
            cur.execute(q, params)
            rows = cur.fetchall()
            return {
                "year": year,
                "pci_fy_matched": pci_fy,
                "data": [dict(r) for r in rows],
            }
        finally:
            cur.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        conn.close()


@app.get("/data/platform_context")
def get_platform_context(
    year: Optional[int] = Query(default=None),
    years: Optional[list[int]] = Query(default=None),
    state_code: Optional[str] = Query(default=None),
    fuel_type: Optional[str] = Query(default=None),
    fy: Optional[str] = Query(default=None),
    maker: Optional[str] = Query(default=None),
    fuel_types: Optional[list[str]] = Query(default=None),
    makers: Optional[list[str]] = Query(default=None),
    months: Optional[list[int]] = Query(default=None),
):
    """
    One response for the analytics platform: Vahan KPIs (when DB is up) plus research JSON
    (population, PCI, CNG/EV infra) with **derived** links (regs/capita, regs per charger, etc.).
    Pass `year` or repeat `years` for a multi-year aggregate (calendar years only; do not combine with `fy`).
    `fy` must be one of `GET /options` `financial_years` (FY2012-13 … FY2025-26 for YEAR_MAX=2026).
    Multiselect: fuel_types, makers, months.
    OEM names in `makers` use config.mappings (canonical + alias expansion in SQL).
    """
    if years is not None and len(years) > 0:
        year_list = sorted({int(x) for x in years})
    elif year is not None:
        year_list = [int(year)]
    else:
        raise HTTPException(
            400,
            "Provide query param `year` or repeat `years` (e.g. years=2022&years=2023).",
        )
    if len(year_list) > 1 and fy:
        raise HTTPException(
            400,
            "Indian FY filter (`fy`) applies to a single calendar year only; omit `fy` when using multiple `years`.",
        )
    if fy and str(fy).strip() and str(fy).strip() not in _platform_analytics_fy_allowed():
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported fy={fy!r}. Allowed labels are those in GET /options "
                f"`financial_years` (FY2012-13 through FY{YEAR_MAX - 1}-{str(YEAR_MAX)[-2:]} for this build)."
            ),
        )
    fts = fuel_types if fuel_types else None
    mks = makers if makers else None
    mos = months if months else None
    anchor_year = max(year_list)
    fy_research = fy if len(year_list) == 1 else None
    research_core = build_platform_research_bundle(anchor_year, state_code, fy_research)
    conn, dialect = _connect_data()
    vahan: dict | None = None
    database_reachable = False
    chart_makers: list = []
    chart_monthly: list = []
    if conn:
        try:
            cur = conn.cursor()
            try:
                vahan = _compute_kpis_payload(
                    cur,
                    dialect,
                    year_list,
                    state_code,
                    fuel_type,
                    fy,
                    maker,
                    fuel_types=fts,
                    makers=mks,
                    months=mos,
                )
                database_reachable = True
                chart_makers = _fetch_top_makers_data(
                    cur,
                    dialect,
                    year_list,
                    state_code,
                    fuel_type,
                    fy,
                    maker,
                    10,
                    fuel_types=fts,
                    makers=mks,
                    months=mos,
                )
                chart_monthly = _fetch_monthly_data(
                    cur,
                    dialect,
                    year_list,
                    state_code,
                    fuel_type,
                    fy,
                    maker,
                    fuel_types=fts,
                    makers=mks,
                    months=mos,
                )
            finally:
                cur.close()
        except Exception as e:
            vahan = {"error": str(e)}
        finally:
            conn.close()

    derived: dict = {}
    total = None
    if isinstance(vahan, dict) and vahan.get("error") is None:
        total = vahan.get("total_registrations")
        if total is not None:
            total = int(total)
    pop_o = research_core.get("population")
    pop_n = (
        int(pop_o["population"])
        if pop_o and pop_o.get("population") is not None
        else None
    )
    if total is not None and pop_n and pop_n > 0:
        derived["registrations_per_capita"] = round(total / float(pop_n), 8)
    pci_o = research_core.get("pci")
    if (
        total is not None
        and pci_o
        and pci_o.get("pci_rs") is not None
        and float(pci_o["pci_rs"]) > 0
    ):
        derived["registrations_per_thousand_pci"] = round(
            1000.0 * float(total) / float(pci_o["pci_rs"]), 6
        )
    infra = research_core.get("infrastructure") or {}
    ev_n = infra.get("ev_year_end_count")
    if total is not None and ev_n is not None and int(ev_n) > 0:
        derived["registrations_per_ev_charger"] = round(
            float(total) / float(ev_n), 4
        )

    research_out = {**research_core, "derived": derived}

    return {
        "years": year_list,
        "year": anchor_year,
        "state_code": state_code,
        "fuel_type": fuel_type,
        "fuel_types": fts,
        "fy": fy,
        "maker_contains": maker.strip() if maker and str(maker).strip() else None,
        "makers": mks,
        "months": mos,
        "filters_applied": {
            "years": year_list,
            "year": year_list[0] if len(year_list) == 1 else None,
            "state_code": state_code,
            "fuel_type": fuel_type,
            "fuel_types": fts,
            "fy": fy,
            "maker_substring": maker.strip() if maker and str(maker).strip() else None,
            "makers_exact": mks,
            "months": mos,
        },
        "database_reachable": database_reachable,
        "database_message": None
        if database_reachable
        else _database_unavailable_detail(),
        "vahan": vahan,
        "research": research_out,
        "charts": {
            "makers": chart_makers,
            "monthly": chart_monthly,
        },
    }


@app.get("/research/manifest")
def research_manifest():
    """Dataset index for population, PCI, CNG, EV chargers (JSON research layer)."""
    path = RESEARCH_DIR / "manifest.json"
    if not path.is_file():
        raise HTTPException(404, "Research manifest not found")
    return FileResponse(path, media_type="application/json")


@app.get("/research/{name}.json", include_in_schema=False)
def research_dataset_json(name: str):
    """Serve a research JSON bundle (population, pci, cng, ev_chargers)."""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", name):
        raise HTTPException(400, "Invalid dataset name")
    path = RESEARCH_DIR / f"{name}.json"
    if not path.is_file():
        raise HTTPException(404, f"No dataset: {name}")
    return FileResponse(path, media_type="application/json")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
