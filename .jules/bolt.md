## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.

## 2024-04-30 - Bottleneck on Aggregation UI Rendering
**Learning:** Polars/UI components evaluating `calcular_status_entidade` inside multiple chained `.filter()` or `.reduce()` calls significantly slowed down array iterations (O(N) * 5 passes per group).
**Action:** When evaluating computationally expensive functions, group iteration logic and cache the result within a single-pass `for...of` loop instead of recalculating it across multiple independent calculations.
