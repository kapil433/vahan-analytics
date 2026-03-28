#!/usr/bin/env python3
"""Insert window.__VAHAN_API_BASE__ as the first line inside <head> (valid HTML)."""
from __future__ import annotations

import argparse
import json
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
    inject = f"<script>window.__VAHAN_API_BASE__={json.dumps(api)};</script>\n"
    insert_at = idx + len(needle)
    out = src[:insert_at] + inject + src[insert_at:]

    for p in args.output:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(out, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
