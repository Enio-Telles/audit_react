## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.

## 2026-05-02 - Consolidate Multiple Array Iterations with Expensive Function Calls
**Learning:** The frontend function `resumir_grupos` performed 5 separate array passes (`.reduce` and `.filter`) per group, and called the expensive `calcular_status_entidade` twice per entity during the `.filter` calls. Consolidating into a single pass and caching the expensive result eliminates redundant operations.
**Action:** Always evaluate whether multiple array operations over the same data can be consolidated into a single pass, especially when the operations involve evaluating complex status calculations.
