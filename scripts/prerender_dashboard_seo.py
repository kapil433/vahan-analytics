#!/usr/bin/env python3
"""
Build-time SEO fallback: inject a <noscript> block with structured text from the Blog
and About sections so simple crawlers and no-JS clients see indexable copy.

Run after inject_pages_api_base.py on the built dashboard HTML (CI or locally).
Idempotent: replaces content between SEO-PRERENDER markers if present.
"""
from __future__ import annotations

import argparse
import html as html_lib
import re
import sys
from pathlib import Path

MARK_START = "<!-- SEO-PRERENDER-START -->"
MARK_END = "<!-- SEO-PRERENDER-END -->"


def _strip_tags(fragment: str) -> str:
    t = re.sub(r"<[^>]+>", " ", fragment)
    return html_lib.unescape(re.sub(r"\s+", " ", t).strip())


def _extract_blog_section(html: str) -> str:
    m = re.search(
        r'<div class="page" id="page-blog"[^>]*>(.*?)<div class="page" id="page-info">',
        html,
        re.DOTALL | re.IGNORECASE,
    )
    return m.group(1) if m else ""


def _extract_about_section(html: str) -> str:
    m = re.search(
        r'<div class="page" id="page-about"[^>]*>\s*<div class="static-page">(.*?)</div>\s*</div>',
        html,
        re.DOTALL | re.IGNORECASE,
    )
    return m.group(1) if m else ""


def _articles_from_blog(blob: str) -> list[tuple[str, list[str]]]:
    out: list[tuple[str, list[str]]] = []
    for am in re.finditer(r"<article\b[^>]*>(.*?)</article>", blob, re.DOTALL | re.IGNORECASE):
        inner = am.group(1)
        hm = re.search(r"<h2\b[^>]*>(.*?)</h2>", inner, re.DOTALL | re.IGNORECASE)
        if not hm:
            continue
        title = _strip_tags(hm.group(1))
        paras: list[str] = []
        for pm in re.finditer(r"<p\b([^>]*)>(.*?)</p>", inner, re.DOTALL | re.IGNORECASE):
            attrs, body = pm.group(1), pm.group(2)
            if "blog-sources" in attrs:
                continue
            text = _strip_tags(body)
            if text:
                paras.append(text)
        if title:
            out.append((title, paras))
    return out


def _static_page_text(blob: str) -> str:
    parts: list[str] = []
    for hm in re.finditer(r"<h[23]\b[^>]*>(.*?)</h[23]>", blob, re.DOTALL | re.IGNORECASE):
        parts.append(_strip_tags(hm.group(1)))
    for pm in re.finditer(r"<p\b[^>]*>(.*?)</p>", blob, re.DOTALL | re.IGNORECASE):
        t = _strip_tags(pm.group(1))
        if t:
            parts.append(t)
    for lm in re.finditer(r"<li\b[^>]*>(.*?)</li>", blob, re.DOTALL | re.IGNORECASE):
        t = _strip_tags(lm.group(1))
        if t:
            parts.append("• " + t)
    return "\n".join(parts)


def build_noscript_inner(html: str) -> str:
    blog_blob = _extract_blog_section(html)
    articles = _articles_from_blog(blog_blob)
    about_blob = _extract_about_section(html)
    about_text = _static_page_text(about_blob) if about_blob else ""

    chunks: list[str] = [
        '<noscript id="seo-static-fallback">',
        "<main>",
        "<h1>Vahan Intelligence — India passenger vehicle registrations (VAHAN-class)</h1>",
        "<p>Interactive dashboard: use a JavaScript-enabled browser for charts and filters.</p>",
        "<section aria-label=\"Blog articles\">",
        "<h2>Blog</h2>",
    ]
    for title, paras in articles:
        chunks.append(f"<h3>{html_lib.escape(title)}</h3>")
        for p in paras:
            chunks.append(f"<p>{html_lib.escape(p)}</p>")
    chunks.append("</section>")
    if about_text:
        chunks.append('<section aria-label="About">')
        chunks.append("<h2>About</h2>")
        for line in about_text.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("•"):
                chunks.append(f"<p>{html_lib.escape(line)}</p>")
            elif len(line) < 80 and not line.endswith("."):
                chunks.append(f"<h3>{html_lib.escape(line)}</h3>")
            else:
                chunks.append(f"<p>{html_lib.escape(line)}</p>")
        chunks.append("</section>")
    chunks.append("</main>")
    chunks.append("</noscript>")
    return "\n".join(chunks)


def inject(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    inner = build_noscript_inner(text)
    block = f"{MARK_START}\n{inner}\n{MARK_END}"

    if MARK_START in text and MARK_END in text:
        text = re.sub(
            re.escape(MARK_START) + r".*?" + re.escape(MARK_END),
            block,
            text,
            count=1,
            flags=re.DOTALL,
        )
    else:
        if "</body>" not in text:
            print("error: no </body>", file=sys.stderr)
            sys.exit(1)
        text = text.replace("</body>", f"{block}\n</body>", 1)

    path.write_text(text, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", "-i", type=Path, required=True)
    ap.add_argument("--output", "-o", type=Path, required=True)
    args = ap.parse_args()
    if args.input.resolve() != args.output.resolve():
        args.output.write_text(args.input.read_text(encoding="utf-8"), encoding="utf-8")
        inject(args.output)
    else:
        inject(args.input)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
