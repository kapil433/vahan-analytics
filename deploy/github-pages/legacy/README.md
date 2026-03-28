# Legacy static dashboard

This folder holds the **previous** GitHub Pages build (the older single-file / static analytics UI).

## What to do

1. Move your **old** repository files here, for example:
   - Previous root `index.html`
   - `lander/` (if you had it)
   - Any static `data/` bundles that belonged to the old site only

2. Add an `index.html` inside **`legacy/`** if the old site was at repo root — so visitors can open **`/legacy/`** and see the archived UI.

3. The **current** product is the dashboard served from **Render** (`/dashboard`) or a copy of `api/static/dashboard/index.html` with `window.__VAHAN_API_BASE__` set (see **`DEPLOY.md`** in the main analytics repo).
