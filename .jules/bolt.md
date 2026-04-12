## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Avoid repetitive regex parsing in DataTable render
**Learning:** In React component loops evaluating highlight rules per table cell, calculating `.toLowerCase()` and parsing floats per cell forces redundant `O(N*M)` allocations and hurts performance.
**Action:** Always pre-compile the rule configurations inside a `useMemo` hook (caching parsed numbers and `.toLowerCase()` strings) rather than computing them directly inside the render loop.
