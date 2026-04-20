## 2026-04-20 - Focus visibility on hover-only elements
**Learning:** Elements hidden with `opacity-0` and revealed on hover (`group-hover:opacity-100`) are invisible to keyboard navigators unless explicit focus styles are applied.
**Action:** Always include `focus-visible:opacity-100 focus-visible:ring-2` when using the hover-opacity reveal pattern to ensure keyboard accessibility.
