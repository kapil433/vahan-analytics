"""
Vahan Master Scraper - Batch orchestration for state x year x fuel scraping.

- Parallel/sequential via run_batch_parallel, run_batch_sequential
- Called by api/main.py (POST /scrape) and run_api.py HTML UI
- All India: state filter skipped (default on portal)
"""

import signal
import sys
import threading
import traceback
from pathlib import Path
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

try:
    from selenium.common.exceptions import WebDriverException
except ImportError:

    class WebDriverException(Exception):  # type: ignore[no-redef]
        pass


# Project root must be on path before any `scraper.*` imports (CLI cwd may differ).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Windows: before any other scraper import (printing, logging). Avoids cp1252 UnicodeEncodeError.
from scraper.console_win import configure_stdio_utf8

configure_stdio_utf8()

from scraper.batch_control import batch_stop_requested, clear_batch_stop, request_batch_stop


def _is_main_thread() -> bool:
    """Signal handlers can only be registered in the main thread."""
    return threading.current_thread() is threading.main_thread()


from scraper.sigint_bridge import chain_sigint as _chain_sigint

from scraper.backend import get_vahan_scraper_class

VahanScraper = get_vahan_scraper_class()

# Master timing: All India is default so less wait needed (reduced for faster fuel-to-fuel)
REFRESH_TO_DOWNLOAD_WAIT_MASTER = 0.35


def _parallel_pool_ok() -> bool:
    """Playwright sync driver cannot be used from multiple worker threads."""
    return bool(getattr(VahanScraper, "PARALLEL_SAFE", True))


def _prewarm_chromedriver_if_selenium() -> None:
    """Resolve chromedriver once before workers start (avoids serial bottleneck on the install lock)."""
    if getattr(VahanScraper, "__module__", "") != "scraper.vahan_scraper":
        return
    try:
        from scraper.vahan_scraper import resolve_chromedriver_path
        resolve_chromedriver_path()
        print("[Scrape] ChromeDriver path ready - workers can start browsers in parallel.", flush=True)
    except ImportError:
        pass
    except Exception as e:
        print(f"[Scrape] ChromeDriver pre-resolve failed (workers will retry): {e}", flush=True)


def _run_tasks_sequential_pool(tasks: list) -> list[Path]:
    """Run each task in the current thread (required for Playwright sync API)."""
    results: list[Path] = []
    for t in tasks:
        if batch_stop_requested():
            break
        state, year = t[0], t[1]
        print(f"\n--- {state} / {year} ---", flush=True)
        try:
            path = _run_one_state_year(t)
            if path:
                results.append(path)
                print(f"  Done: {state} / {year} -> {path}", flush=True)
            else:
                print(f"  Failed: {state} / {year}", flush=True)
        except WebDriverException as e:
            if not batch_stop_requested():
                msg = (str(e) or type(e).__name__).split("\n")[0].strip()
                if len(msg) > 220:
                    msg = msg[:217] + "..."
                print(f"  Error {state} / {year}: {msg}", flush=True)
        except Exception as e:
            if not batch_stop_requested():
                print(f"  Error {state} / {year}: {repr(e)}", flush=True)
                traceback.print_exc()
    return results


def _run_one_state_year(args: tuple) -> Path | None:
    """Worker for ThreadPoolExecutor. args: (state, year, fuels, output_base, headless, window_layout_slot, portal_filters)."""
    configure_stdio_utf8()
    state, year, fuels, output_base, headless, window_layout_slot, portal_filters = args
    if batch_stop_requested():
        print(f"  Skipped (stop requested): {state} / {year}", flush=True)
        return None
    output_base = Path(output_base)
    scraper = VahanScraper(output_base, headless=headless)
    return scraper.run_state_year(
        state=state,
        year=year,
        rta=None,
        fuels=fuels if fuels else None,
        refresh_to_download_wait=REFRESH_TO_DOWNLOAD_WAIT_MASTER,
        window_layout_slot=window_layout_slot,
        portal_filters=portal_filters,
    )


def run_batch_parallel(
    states: list[str],
    years: list[int],
    fuels: list[str] | None = None,
    output_base: Path | None = None,
    headless: bool = False,
    max_workers: int = 8,
    portal_filters: dict[str, str] | None = None,
) -> list[Path]:
    """
    Run scrape for multiple state-year combos in parallel (each job: own browser + scraper instance).
    Backends may set PARALLEL_SAFE=False to force sequential execution in this process.
    """
    output_base = output_base or Path("output/vahan_data")
    output_base.mkdir(parents=True, exist_ok=True)

    ob = str(output_base.resolve())
    tasks = [
        (s, y, fuels, ob, headless, idx, portal_filters)
        for idx, (s, y) in enumerate((s, y) for s in states for y in years)
    ]
    n_jobs = len(tasks)
    ys = sorted(set(years))
    y_preview = f"{ys[:8]}{'...' if len(ys) > 8 else ''}"
    print(
        f"[Scrape] Input: {len(states)} state(s) x {len(years)} year(s) -> {n_jobs} job(s). "
        f"Years: {y_preview}",
        flush=True,
    )
    results = []

    clear_batch_stop()
    previous_sigint = None
    previous_sigbreak = None
    if _is_main_thread():
        previous_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, _chain_sigint(previous_sigint))
        if hasattr(signal, "SIGBREAK"):
            previous_sigbreak = signal.getsignal(signal.SIGBREAK)
            signal.signal(signal.SIGBREAK, _chain_sigint(previous_sigbreak))

    if not _parallel_pool_ok():
        print(
            "[Scrape] Backend PARALLEL_SAFE=False - "
            f"running {len(tasks)} job(s) sequentially (max_workers ignored).",
            flush=True,
        )
        try:
            results = _run_tasks_sequential_pool(tasks)
        except KeyboardInterrupt:
            request_batch_stop()
        finally:
            if _is_main_thread() and previous_sigint is not None:
                signal.signal(signal.SIGINT, previous_sigint)
                if previous_sigbreak is not None:
                    signal.signal(signal.SIGBREAK, previous_sigbreak)
        return results

    future_to_task: dict = {}
    pending: set = set()
    mw_requested = max(1, int(max_workers))
    # Never exceed job count (no idle threads); never exceed user's cap (slow sites: keep max_windows low).
    pool_workers = min(mw_requested, n_jobs)
    if mw_requested > n_jobs:
        print(
            f"[Scrape] Requested {mw_requested} windows but only {n_jobs} job(s) - "
            f"using pool size {pool_workers}.",
            flush=True,
        )
    print(
        f"[Scrape] Queued {n_jobs} job(s); max concurrent Chrome windows = {pool_workers} "
        f"(capped by max_workers={mw_requested}). Remaining jobs wait in queue.",
        flush=True,
    )
    print(
        "[Scrape] Log order may interleave across years - that is normal for parallel jobs.",
        flush=True,
    )
    _prewarm_chromedriver_if_selenium()
    print(
        f"[Scrape] ThreadPoolExecutor starting with max_workers={pool_workers}.",
        flush=True,
    )
    executor = ThreadPoolExecutor(max_workers=pool_workers)
    try:
        future_to_task = {executor.submit(_run_one_state_year, t): t for t in tasks}
        pending = set(future_to_task.keys())
        # Do not use as_completed() alone: it blocks until a job finishes, so Ctrl+C is ignored for minutes.
        while pending:
            if batch_stop_requested():
                break
            # Shorter timeout so Ctrl+C / request_batch_stop() is picked up quickly (main-thread SIGINT sets the flag).
            done, pending = wait(pending, timeout=0.25, return_when=FIRST_COMPLETED)
            for fut in done:
                task = future_to_task[fut]
                state, year = task[0], task[1]
                try:
                    path = fut.result()
                    if path:
                        results.append(path)
                        print(f"  Done: {state} / {year} -> {path}", flush=True)
                    else:
                        print(f"  Failed: {state} / {year}", flush=True)
                except WebDriverException as e:
                    if not batch_stop_requested():
                        msg = (str(e) or type(e).__name__).split("\n")[0].strip()
                        if len(msg) > 220:
                            msg = msg[:217] + "..."
                        print(f"  Error {state} / {year}: {msg}", flush=True)
                except Exception as e:
                    if not batch_stop_requested():
                        print(f"  Error {state} / {year}: {repr(e)}", flush=True)
                        traceback.print_exc()
    except KeyboardInterrupt:
        request_batch_stop()
    finally:
        if batch_stop_requested():
            for f in pending:
                f.cancel()
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=False)
            except Exception:
                pass
        else:
            try:
                executor.shutdown(wait=True)
            except Exception:
                pass
        if _is_main_thread() and previous_sigint is not None:
            signal.signal(signal.SIGINT, previous_sigint)
            if previous_sigbreak is not None:
                signal.signal(signal.SIGBREAK, previous_sigbreak)

    return results


def run_batch_sequential(
    states: list[str],
    years: list[int],
    fuels: list[str] | None = None,
    output_base: Path | None = None,
    headless: bool = False,
    portal_filters: dict[str, str] | None = None,
) -> list[Path]:
    """Run scrape sequentially."""
    clear_batch_stop()
    previous_sigint = None
    previous_sigbreak = None
    if _is_main_thread():
        previous_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, _chain_sigint(previous_sigint))
        if hasattr(signal, "SIGBREAK"):
            previous_sigbreak = signal.getsignal(signal.SIGBREAK)
            signal.signal(signal.SIGBREAK, _chain_sigint(previous_sigbreak))

    output_base = output_base or Path("output/vahan_data")
    results = []
    try:
        for state in states:
            if batch_stop_requested():
                break
            for year in years:
                if batch_stop_requested():
                    break
                configure_stdio_utf8()
                print(f"\n--- {state} / {year} ---")
                scraper = VahanScraper(output_base, headless=headless)
                path = scraper.run_state_year(
                    state=state,
                    year=year,
                    rta=None,
                    fuels=fuels if fuels else None,
                    refresh_to_download_wait=REFRESH_TO_DOWNLOAD_WAIT_MASTER,
                    window_layout_slot=None,
                    portal_filters=portal_filters,
                )
                if path:
                    results.append(path)
    except KeyboardInterrupt:
        request_batch_stop()
    except Exception as e:
        print(f"  Sequential scrape error: {repr(e)}", flush=True)
        traceback.print_exc()
    finally:
        if _is_main_thread() and previous_sigint is not None:
            signal.signal(signal.SIGINT, previous_sigint)
            if previous_sigbreak is not None:
                signal.signal(signal.SIGBREAK, previous_sigbreak)
    return results
