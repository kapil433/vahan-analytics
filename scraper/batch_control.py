"""Thread-safe stop flag for batch scrapes (Ctrl+C, API shutdown). Imported by scrapers without cycles."""

from __future__ import annotations

import threading

_stop = threading.Event()


def batch_stop_requested() -> bool:
    return _stop.is_set()


def clear_batch_stop() -> None:
    _stop.clear()


def request_batch_stop() -> None:
    """Idempotent: safe from signal handler, /scrape/stop, and repeated Ctrl+C."""
    if _stop.is_set():
        return
    _stop.set()
    print(
        "\n  Stop requested - parallel jobs exit at next wait/checkpoint. "
        "If run via API, you can also POST /scrape/stop. Second Ctrl+C may exit the server.",
        flush=True,
    )
