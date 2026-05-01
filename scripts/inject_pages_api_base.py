#!/usr/bin/env python3
"""Insert window.__VAHAN_API_BASE__ as the first line inside <head> (valid HTML)."""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--api-base", required=True, help="Origin only, no trailing slash")
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--output", type=Path, action="append", required=True)
    args = ap.parse_args()

    api = args.api_base.strip().rstrip("/")
    src = args.input.read_text(encoding="utf-8")
    needle = "<head>"
    idx = src.find(needle)
    if idx == -1:
        print("error: no <head> in input", file=sys.stderr)
        return 1

    # Build-time cache buster — appended as ?v= to the data-bundle URL so each
    # deploy invalidates the Cloudflare + browser cache automatically. Without
    # this, users hit max-age=600 stale data after a refresh until cache expires.
    cache_buster = (os.environ.get("GITHUB_SHA") or _dt.datetime.utcnow().strftime("%Y%m%d%H%M"))[:12]

    # Apex → www for custom domain on static hosts (GitHub Pages has no server redirect).
    apex_redirect = (
        "<script>(function(){var h=location.hostname;"
        "if(h==='vahanintelligence.in'){"
        "location.replace('https://www.vahanintelligence.in'+location.pathname+location.search+location.hash);"
        "}})();</script>\n"
    )
    inject = (
        apex_redirect
        + f"<script>window.__VAHAN_API_BASE__={json.dumps(api)};"
          f"window.__VAHAN_DATA_VERSION__={json.dumps(cache_buster)};</script>\n"
    )
    insert_at = idx + len(needle)
    out = src[:insert_at] + inject + src[insert_at:]

    # Rewrite the data-bundle preload href so the browser fetches from the actual
    # API origin on Pages (relative path /data/... 404s here — data lives on Render).
    relative_preload = '<link rel="preload" href="/data/vahan_master_compat"'
    absolute_preload = f'<link rel="preload" href="{api}/data/vahan_master_compat?v={cache_buster}"'
    if relative_preload in out:
        out = out.replace(relative_preload, absolute_preload)

    for p in args.output:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(out, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
