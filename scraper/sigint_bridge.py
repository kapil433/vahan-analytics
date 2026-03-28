"""
Chain SIGINT → batch stop without importing vahan_scraper_master (avoids Selenium import chain at startup).
"""

from __future__ import annotations

import signal

from scraper.batch_control import request_batch_stop


def chain_sigint(previous):
    """Return handler that requests batch stop then delegates to ``previous`` or raises KeyboardInterrupt."""

    def _handler(signum, frame):
        request_batch_stop()
        if callable(previous) and previous not in (signal.SIG_DFL, signal.SIG_IGN):
            previous(signum, frame)
        else:
            raise KeyboardInterrupt

    return _handler
