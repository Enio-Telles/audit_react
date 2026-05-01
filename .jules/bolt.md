## 2026-05-01 - Optimizing React Re-renders and Single-Pass Loops
**Learning:** Found multiple independent `.reduce()` and `.filter()` arrays calling an O(N) evaluation inside `DossieContatoDetalhe.tsx`. These iterations cause redundant calculation passes (O(5*N) instead of O(N)).
**Action:** Replaced the independent `.reduce` and `.filter` combinations with a single `for...of` loop to calculate all metrics, halving the calls to the expensive function and eliminating multiple iterations.
