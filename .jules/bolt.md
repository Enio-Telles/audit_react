## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.
## 2026-04-14 - Prevent O(N*C) object allocations in filter loops
**Learning:** In JavaScript/React filtering operations, using `Object.values(obj).some(...)` or `Object.entries(obj).every(...)` inside frequent operations or hooks creates intermediate arrays causing unnecessary O(N*C) object allocations.
**Action:** Use a `for...in` or `for...of` loop with an early `break` instead, to avoid allocating intermediate arrays for optimal performance.
