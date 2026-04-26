## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.
## 2026-04-14 - Consolidate iterative aggregations over large objects
**Learning:** Evaluating multiple iterations via `.reduce()` and `.filter()` over the same array with computationally expensive internal checks (like `calcular_status_entidade`) compounds execution time severely (O(M*N)).
**Action:** When extracting multiple summary statistics from an array, calculate them simultaneously in a single `for...of` loop pass, and cache expensive evaluations locally inside the loop to avoid redundant recalculation.
