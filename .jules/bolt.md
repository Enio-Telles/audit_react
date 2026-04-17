## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.

## 2024-05-15 - Optimize multiple array iterations in JS
**Learning:** Using chained array methods (like `.reduce` and `.filter`) sequentially on the same array triggers redundant O(N) memory traversals, which becomes a bottleneck in views aggregating large records like DossieContatoDetalhe.
**Action:** Always consolidate multiple aggregations on the same array into a single `for...of` loop or single `reduce()` pass to traverse the data only once.
