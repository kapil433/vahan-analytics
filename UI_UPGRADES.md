# UI Upgrade Recommendations — Color Contrast & Accessibility

## Applied: Vahan Scraper UI (`api/static/index.html`)

- **Dark theme** with WCAG AA–friendly contrast
- **Text:** `#f1f5f9` on `#0f172a` (~14:1)
- **Muted text:** `#94a3b8` on `#0f172a` (~7:1)
- **Accent:** `#38bdf8` (sky blue) on dark
- **Success/Error:** High-contrast green/red on dark backgrounds
- **Focus rings** for keyboard navigation
- **Selected options** clearly highlighted

---

## Recommendations for ALL-India-Vahan-Analytics-Dashboard

### 1. Color Contrast (WCAG AA)

| Element | Current (example) | Recommended | Ratio |
|---------|------------------|-------------|-------|
| Body text | `#e2e8f0` on `#0b0e17` | `#f1f5f9` on `#0f172a` | ≥4.5:1 |
| Muted/dim text | `#64748b` | `#94a3b8` | ≥4.5:1 |
| KPI values | `var(--text)` | Ensure ≥4.5:1 on card background |
| Chart labels | `#94a3b8` | `#a8b8cc` or lighter | ≥4.5:1 |

### 2. Fuel Colors (Improve Distinction)

| Fuel | Current | Suggested | Notes |
|------|---------|-----------|-------|
| Petrol | `#818cf8` | `#a78bfa` or `#c4b5fd` | Brighter purple |
| Diesel | `#64748b` | `#94a3b8` | Lighter gray |
| CNG | `#34d399` | `#34d399` ✓ | Keep |
| EV | `#22d3ee` | `#22d3ee` ✓ | Keep |
| Strong Hybrid | `#fbbf24` | `#fbbf24` ✓ | Keep |

Ensure each fuel color has ≥3:1 contrast against chart background.

### 3. Interactive Elements

- **Buttons:** Min 3:1 contrast vs background; focus ring 2px+ visible
- **Selects/dropdowns:** Border visible; hover/focus state distinct
- **Links:** Underline or ≥3:1 contrast; visible focus

### 4. KPI Cards

- Card background vs page: subtle but clear separation
- KPI value: large, bold, high contrast
- Delta badges (up/down): ensure green/red distinguishable for colorblind users (add ↑↓ icons)

### 5. Charts

- Grid lines: lighter but visible (`rgba(255,255,255,0.08)`)
- Legend: text contrast ≥4.5:1
- Tooltips: dark bg, light text, or vice versa with sufficient contrast

### 6. Quick Wins

```css
/* Bump muted text contrast */
--text-dim: #94a3b8;  /* was #64748b */

/* Stronger focus for accessibility */
*:focus-visible { outline: 2px solid var(--blue); outline-offset: 2px; }

/* Reduce motion for users who prefer it */
@media (prefers-reduced-motion: reduce) {
  * { animation: none !important; transition: none !important; }
}
```

---

*Reference: [WCAG 2.1 Contrast Guidelines](https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum.html)*
