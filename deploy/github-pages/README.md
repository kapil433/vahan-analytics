# Files for [ALL-India-Vahan-Analytics-Dashboard](https://github.com/kapil433/ALL-India-Vahan-Analytics-Dashboard)

## One-time setup

1. **Legacy**  
   Create folder **`legacy/`** on the default branch. Move the **old** site into it (previous `index.html`, `lander/`, etc.). Keep this **`legacy/README.md`** for context.

2. **Automated layout (recommended)**  
   Workflow **Sync public dashboard** in the main **`vahan-analytics`** repo publishes the **full dashboard** as **`index.html`** at the site root (plus **`dashboard/index.html`**, **`og-image.png`**, robots/sitemap). You do **not** maintain a separate launcher page here.

3. **GitHub Pages**  
   Use **root** `/` or **`/docs`** as configured in the Pages repo. Keep **`CNAME`** for **`www.vahanintelligence.in`** and follow **`DEPLOY.md` § 5** in the main `vahan-analytics` repo for DNS (www CNAME + apex redirect).

## Optional: full dashboard on Pages (manual)

Copy `api/static/dashboard/index.html` from the main repo and add **above** its main `<script>`:

```html
<script>window.__VAHAN_API_BASE__='https://your-app.onrender.com';</script>
```

See **`DEPLOY.md`** in the `vahan-analytics` repository.
