# Files for [ALL-India-Vahan-Analytics-Dashboard](https://github.com/kapil433/ALL-India-Vahan-Analytics-Dashboard)

## One-time setup

1. **Legacy**  
   Create folder **`legacy/`** on the default branch. Move the **old** site into it (previous `index.html`, `lander/`, etc.). Keep this **`legacy/README.md`** for context.

2. **New home page**  
   Copy **`index.html`** from this folder to the **repository root** (replace the old root `index.html`).

3. **Point to Render**  
   In that `index.html`, replace `https://YOUR-SERVICE.onrender.com/dashboard` with your real Render URL, or set:
   ```html
   <script>window.RENDER_DASHBOARD_URL='https://your-app.onrender.com/dashboard';</script>
   ```
   before the closing `</body>` (the template already reads this).

4. **GitHub Pages**  
   Use **root** `/` or **`/docs`** as today; **`CNAME`** can stay for your custom domain.

## Optional: full dashboard on Pages

Copy `api/static/dashboard/index.html` from the main repo and add **above** its main `<script>`:

```html
<script>window.__VAHAN_API_BASE__='https://your-app.onrender.com';</script>
```

See **`DEPLOY.md`** in the `vahan-analytics` repository.
