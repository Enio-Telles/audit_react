## 2025-04-29 - O(N) Array Aggregation Optimization
**Learning:** Consolidating multiple chained `.filter()` and `.reduce()` iterations into a single `for...of` loop is critical when calculating independent totals on the same array structure, especially when calculations rely on an expensive evaluation (like `calcular_status_entidade`).
**Action:** Always search for opportunities to merge chained collection methods that iterate over the exact same array to perform scalar aggregations (like sums or counts) to avoid redundant iterations and re-evaluations.
