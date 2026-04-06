
## 2024-05-18 - Intl.NumberFormat caching vs toLocaleString
**Learning:** In JavaScript/React contexts, calling `Number.prototype.toLocaleString()` inside frequent operations or render loops (like table cell formatting) severely degrades performance by repeatedly instantiating new locale formatters.
**Action:** Always instantiate and cache `Intl.NumberFormat` objects outside of the loop for repeated use.
