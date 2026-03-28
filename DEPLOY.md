# Deploy: Render (API) + GitHub Pages (public site)

## 1. Backend on [Render](https://dashboard.render.com/)

1. Push this repo to GitHub.
2. **New +** → **Web Service** → connect the repo (or **Blueprint** → import `render.yaml`).
3. Render uses **`requirements-render.txt`** (no Selenium/Playwright — smaller image, faster builds).
4. **Start command** (if not using Blueprint):  
   `uvicorn api.main:app --host 0.0.0.0 --port $PORT`  
   **Health check:** `/health`
5. **Database**
   - **SQLite:** run `python scripts/setup_local_sqlite.py` locally, add a **persistent disk** on Render mounted at `/app/data`, upload `vahan_local.db` or rebuild in a **release command** (see Render docs). Ephemeral disk alone loses the DB on redeploy.
   - **PostgreSQL (recommended):** create **PostgreSQL** on Render → set env **`DATABASE_URL`** to the **Internal** URL → run migrations / `scripts/load_vahan_to_db.py` as appropriate.
6. Live app: **`https://<service>.onrender.com/`** (analytics dashboard). **`/dashboard`** redirects to **`/`** (301). Scraper UI: **`/scraper`**.

**Note:** `POST /scrape` returns **501** on this slim install (no Chrome). Use full `requirements.txt` locally for scraping.

**CORS:** By default the API allows browser `fetch` from **`https://www.vahanintelligence.in`**, apex **`https://vahanintelligence.in`**, **`https://kapil433.github.io`**, and local dev (`localhost` / `127.0.0.1` on port 8000). To allow another GitHub Pages origin or staging host, set env **`CORS_ALLOW_ORIGINS`** to a comma-separated list (no spaces), e.g. `https://www.example.com,https://staging.example.com`. Use **`CORS_ALLOW_ORIGINS=*`** only for debugging (not recommended in production).

**GitHub Pages vs headers:** Pages cannot set `Content-Security-Policy`, `Strict-Transport-Security`, `X-Frame-Options`, etc. as HTTP headers. The dashboard ships a **CSP `<meta>`** and **`referrer`** meta for partial mitigation; for full header control use **Cloudflare** (or another reverse proxy) in front of Pages or serve the app from **Render** where `SecurityHeadersMiddleware` applies.

**Apex → `www` on static hosting:** The built HTML includes a tiny inline script (also prepended by `scripts/inject_pages_api_base.py`) that replaces `https://vahanintelligence.in/…` with `https://www.vahanintelligence.in/…` in the browser, so GitHub Pages visitors hitting the apex hostname still land on `www` for canonical SEO. The Render API also applies `ApexToWwwRedirectMiddleware` when traffic hits the service directly.

---

## 2. Full install locally (scraping + API)

```bash
pip install -r requirements.txt
python run_api.py
```

---

## 3. Public GitHub repo ([ALL-India-Vahan-Analytics-Dashboard](https://github.com/kapil433/ALL-India-Vahan-Analytics-Dashboard))

### Automated sync (recommended)

On **this** (`vahan-analytics`) repository, add **Secrets**:

| Secret | Purpose |
|--------|--------|
| `PUBLIC_DASHBOARD_TOKEN` | PAT with `contents:write` on `kapil433/ALL-India-Vahan-Analytics-Dashboard` |
| `VAHAN_API_BASE_URL` | Your Render origin, e.g. `https://your-app.onrender.com` (no trailing slash) |

Workflow **Sync public dashboard** (`.github/workflows/sync-public-dashboard.yml`) builds:

- **`index.html`** at site root — same full dashboard as `dashboard/index.html` (canonical public URL is `/`)
- **`dashboard/index.html`** — same UI (duplicate path for relative asset links); injected `window.__VAHAN_API_BASE__` and `<noscript>` SEO extract
- **`og-image.png`** at site root (Open Graph preview; source `api/static/og-image.png`)
- **`robots.txt`** / **`sitemap.xml`** at site root (from `api/static/`)
- `legacy/README.md`, optional `docs/data/vahan_master.json`

**Removing old `welcome.html`:** If the public repo still has `welcome.html` from an earlier sync, delete it in that repo (or run a one-off commit) so crawlers do not find a duplicate entry page. New syncs no longer publish `welcome.html`.

Push to `main` / `master` (or run **Actions → Sync public dashboard → Run workflow**).

### Pages on this same repo

If you use **GitHub Pages** on `vahan-analytics`, set a **repository variable** `VAHAN_API_BASE_URL` to your Render URL. Workflow **Deploy GitHub Pages** builds the same layout under `_site`.

### Legacy

Move the old static site into **`legacy/`** on the public repo (previous `index.html`, `lander/`, etc.).

---

## 4. Docker

`Dockerfile` installs **`requirements-render.txt`**. For a full scraper image, switch the `RUN pip install` line back to `requirements.txt` and add Chrome.

---

## 5. Custom domain and **www** (DNS + CNAME)

Canonical host: **`https://www.vahanintelligence.in`**. The dashboard HTML uses absolute `https://www…` URLs for canonical, Open Graph, and sitemap.

### GitHub Pages (`deploy/github-pages/CNAME`)

The repo ships **`deploy/github-pages/CNAME`** with:

```text
www.vahanintelligence.in
```

1. In the **GitHub Pages** repository (or this repo if Pages is enabled here): **Settings → Pages → Custom domain** → enter **`www.vahanintelligence.in`**, save, wait for DNS check, enable **Enforce HTTPS**.
2. At your **DNS provider**, create:
   - **`www`** → **CNAME** → **`<your-github-username>.github.io`** (exact target is shown in GitHub’s custom-domain UI for your account).
   - **Apex** **`vahanintelligence.in`**: either  
     - **A** records to GitHub Pages’ current IPs (see [Managing a custom domain for your GitHub Pages site](https://docs.github.com/en/pages/configuring-a-custom-domain-for-your-github-pages-site/managing-a-custom-domain-for-your-github-pages-site)), **or**  
     - Prefer **redirect apex → www** using your DNS/registrar “redirect” / **ALIAS** to `www` / a **Cloudflare** single redirect rule, so only **`www`** serves the site.

After DNS propagates, **`https://www.vahanintelligence.in/`** should load the Pages site; avoid linking the apex if it still shows duplicate content without a redirect.

### Render (API)

**Settings → Custom Domains:** add **`www.vahanintelligence.in`** (and apex only if you need it). The app’s **`ApexToWwwRedirectMiddleware`** returns **301** from **`vahanintelligence.in`** → **`www.vahanintelligence.in`** when the request hits this service (disable with `APEX_WWW_REDIRECT=0` if needed).

---

## 6. SEO checklist (robots, sitemap, Search Console)

**Already in this repo**

- `api/static/robots.txt` and `api/static/sitemap.xml` — served at **`/robots.txt`** and **`/sitemap.xml`** by the FastAPI app (`api/main.py`).
- The same files are copied to the **site root** in GitHub Actions for **Sync public dashboard** and **Deploy GitHub Pages** (`_public/` / `_site/`).
- Mirrors for visibility in git: `deploy/github-pages/robots.txt`, `deploy/github-pages/sitemap.xml` (keep in sync with `api/static/` when URLs change).
- Google Search Console **HTML file** verification: `api/static/google5332b27a4f971584.html` → served at **`/google5332b27a4f971584.html`** on the API origin and copied to the static site root in CI.
- Open Graph image: `api/static/og-image.png` — served at **`/og-image.png`** on the API (`api/main.py`) and copied to the static site root in CI; referenced in dashboard `<meta property="og:image" …>`.

**Google Search Console (you complete in the browser)**

1. Add a **URL-prefix** or **Domain** property for **`https://www.vahanintelligence.in/`** (preferred canonical). Apex **`https://vahanintelligence.in/`** should 301 to `www` when served by the API (`ApexToWwwRedirectMiddleware`). For `*.github.io`, use that origin if applicable.
2. Verify using one of:
   - **HTML file:** Download `google….html` from Google and place it as `api/static/google….html` (same filename). The API serves it at `https://<your-origin>/google….html`; GitHub Actions also copies it to the Pages site root. Then click **Verify** in Search Console.
   - **HTML tag:** In `api/static/dashboard/index.html`, add inside `<head>`:  
     `<meta name="google-site-verification" content="PASTE_TOKEN_HERE"/>`
3. Commit and deploy so the file or meta is live, then click **Verify** in Search Console.
4. In Search Console → **Sitemaps**, submit: **`https://www.vahanintelligence.in/sitemap.xml`** (must match `robots.txt`).

---

## 7. Cloudflare (optional) — security headers at the edge

If you **proxy** `vahanintelligence.in` through [Cloudflare](https://www.cloudflare.com/) (free plan):

1. Move DNS nameservers to Cloudflare and ensure the orange-cloud proxy is on for `A`/`CNAME` to GitHub Pages or Render, as appropriate.
2. **SSL/TLS** → set mode to **Full (strict)** when the origin serves HTTPS.
3. Add **Transform Rules** → **Modify response header** (or **Rules** → **Configuration** depending on UI) to append security headers the static host may not send, for example:
   - `Strict-Transport-Security` = `max-age=31536000; includeSubDomains; preload`
   - `X-Frame-Options` = `DENY`
   - `X-Content-Type-Options` = `nosniff`
   - `Referrer-Policy` = `strict-origin-when-cross-origin`
   - `Permissions-Policy` = `geolocation=(), microphone=(), camera=()`

The SPA also ships a **CSP** `<meta>` tag for static hosting; the API adds **Content-Security-Policy** via `SecurityHeadersMiddleware`. Align Cloudflare-added headers with those policies so you do not duplicate conflicting CSPs unless intentional.

---

## 8. Build-time SEO prerender (dashboard)

`scripts/prerender_dashboard_seo.py` injects a `<noscript>` block with the Blog and About text so crawlers that execute little or no JavaScript still see indexable copy. It runs automatically in **Sync public dashboard** and **Deploy GitHub Pages** after `inject_pages_api_base.py`. To refresh locally after editing the dashboard:

```bash
python scripts/prerender_dashboard_seo.py -i api/static/dashboard/index.html -o api/static/dashboard/index.html
```
