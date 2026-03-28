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
6. Live app: **`https://<service>.onrender.com/dashboard`**

**Note:** `POST /scrape` returns **501** on this slim install (no Chrome). Use full `requirements.txt` locally for scraping.

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

- Landing: `deploy/github-pages/index.html`
- **`dashboard/index.html`** — full UI with `window.__VAHAN_API_BASE__` injected from the secret
- `legacy/README.md`, optional `docs/data/vahan_master.json`

Push to `main` / `master` (or run **Actions → Sync public dashboard → Run workflow**).

### Pages on this same repo

If you use **GitHub Pages** on `vahan-analytics`, set a **repository variable** `VAHAN_API_BASE_URL` to your Render URL. Workflow **Deploy GitHub Pages** builds the same layout under `_site`.

### Legacy

Move the old static site into **`legacy/`** on the public repo (previous `index.html`, `lander/`, etc.).

---

## 4. Docker

`Dockerfile` installs **`requirements-render.txt`**. For a full scraper image, switch the `RUN pip install` line back to `requirements.txt` and add Chrome.

---

## 5. Custom domain

- **Render:** Web Service → **Custom Domains**.
- **GitHub Pages:** keep **`CNAME`**; configure DNS as GitHub documents.
