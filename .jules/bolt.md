## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.
## 2026-04-14 - Cache expensive evaluation during single-pass array aggregation
**Learning:** When evaluating computationally expensive functions (like `calcular_status_entidade`) or calculating multiple aggregations on the same dataset arrays in the frontend, using multiple chained `.filter()` and `.reduce()` passes forces redundant dataset traversals and double-evaluation of the expensive rule per entity.
**Action:** Consolidate the aggregations into a single `for...of` loop and cache the result of the expensive evaluation inside the loop to avoid redundant O(N) traversals and repeated function calls.
