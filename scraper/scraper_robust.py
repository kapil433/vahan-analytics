"""Retries, download polling, debug dumps for browser scrapers (issues under our control)."""

from __future__ import annotations

import functools
import logging
import os
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

T = TypeVar("T")

log = logging.getLogger("vahan.scraper")

try:
    from selenium.common.exceptions import (
        ElementNotInteractableException,
        NoSuchElementException,
        StaleElementReferenceException,
    )
except ImportError:

    class StaleElementReferenceException(Exception):  # type: ignore[no-redef]
        pass

    class ElementNotInteractableException(Exception):  # type: ignore[no-redef]
        pass

    class NoSuchElementException(Exception):  # type: ignore[no-redef]
        pass


def retry(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    delay: float = 0.75,
    backoff: float = 1.6,
    label: str = "",
) -> T | None:
    """
    Run fn until it returns truthy or attempts exhausted.
    fn should return None or False on failure.
    """
    wait = delay
    last = None
    for i in range(attempts):
        try:
            last = fn()
            if last:
                return last
        except KeyboardInterrupt:
            raise
        except SystemExit:
            raise
        except MemoryError:
            raise
        except Exception as e:
            log.warning(
                "[retry:%s] attempt %s/%s: %s: %s",
                label,
                i + 1,
                attempts,
                type(e).__name__,
                e,
            )
        if i < attempts - 1:
            if label:
                print(f"    retry ({label}): attempt {i + 2}/{attempts} after {wait:.1f}s")
            time.sleep(wait)
            wait *= backoff
    return last if last else None


def retry_stale(*, max_attempts: int = 4, delay: float = 0.35):
    """Retry callable on StaleElementReferenceException (DOM replaced by AJAX)."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for i in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except (
                    StaleElementReferenceException,
                    ElementNotInteractableException,
                    NoSuchElementException,
                ) as e:
                    last_exc = e
                    if i < max_attempts - 1:
                        time.sleep(delay * (i + 1))
            if last_exc:
                raise last_exc
            return None

        return wrapper

    return decorator


def _clamped_download_timeout(sec: float | None) -> float:
    """Download polling uses VAHAN_DOWNLOAD_WAIT_SEC / VAHAN_DOWNLOAD_MAX_SEC (not VAHAN_WAIT_CAP_SEC)."""
    hi = float(os.environ.get("VAHAN_DOWNLOAD_MAX_SEC", "300"))
    if sec is None:
        sec = float(os.environ.get("VAHAN_DOWNLOAD_WAIT_SEC", "60"))
    return max(0.5, min(float(sec), hi))


def wait_for_chrome_download(
    download_dir: Path,
    *,
    timeout: float | None = None,
    poll_s: float = 0.14,
    initial_paths: set | None = None,
) -> Path | None:
    """
    Wait for Chrome to finish a download: ignores .crdownload until it disappears,
    then returns a new .csv/.xlsx/.xls (or newest qualifying file not in initial snapshot).

    Pass ``initial_paths`` (set of resolved Paths) captured **before** clicking download
    so only files created after the click count as this export.
    """
    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    timeout = _clamped_download_timeout(timeout)
    if initial_paths is not None:
        initial = {Path(p).resolve() for p in initial_paths}
    else:
        initial = {p.resolve() for p in download_dir.iterdir()}
    deadline = time.time() + timeout
    paths: list = []

    def _is_data_file(p: Path) -> bool:
        if not p.is_file():
            return False
        n = p.name.lower()
        if ".crdownload" in n or n.endswith(".tmp"):
            return False
        suf = p.suffix.lower()
        if suf in (".csv", ".xlsx", ".xls"):
            return True
        return False

    while time.time() < deadline:
        time.sleep(poll_s)
        paths = list(download_dir.iterdir())
        if any(".crdownload" in p.name.lower() for p in paths):
            continue
        new_files = [p for p in paths if p.resolve() not in initial and _is_data_file(p)]
        if new_files:
            return max(new_files, key=lambda p: p.stat().st_mtime)
        for p in sorted(paths, key=lambda x: x.stat().st_mtime, reverse=True):
            if p.resolve() not in initial and _is_data_file(p):
                return p
    paths_final = list(download_dir.iterdir())
    new_files = [p for p in paths_final if p.resolve() not in initial and _is_data_file(p)]
    if new_files:
        return max(new_files, key=lambda p: p.stat().st_mtime)
    return None


def save_selenium_debug(driver, out_dir: Path, tag: str) -> None:
    """Screenshot + HTML for post-mortem when selectors or timing fail."""
    if driver is None:
        return
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    safe = tag.replace(" ", "_").replace("/", "-")[:80]
    ts = datetime.now().strftime("%H%M%S_%f")[:-3]
    png = out_dir / f"debug_{safe}_{ts}.png"
    html = out_dir / f"debug_{safe}_{ts}.html"
    try:
        driver.save_screenshot(str(png))
    except Exception:
        pass
    try:
        html.write_text(driver.page_source or "", encoding="utf-8", errors="replace")
    except Exception:
        pass
    msg = f"Debug saved: {png.name}, {html.name} in {out_dir}"
    print(f"  {msg}", flush=True)
    logging.getLogger("vahan.scraper").warning("%s", msg)
