## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.

## 2025-04-30 - Optimize Array Aggregations
**Learning:** Chaining array methods like `filter` and `reduce` in React render or memo functions creates massive performance bottlenecks (O(M*N)) on large tabular datasets, specifically when callbacks contain expensive entity lookups (`calcular_status_entidade`).
**Action:** Consolidate chained generic array methods into a single-pass `for...of` loop when dealing with datasets shared across tables, making sure to cache intermediate functional evaluations (like entity status) within the loop iteration itself.
