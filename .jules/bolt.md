## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.
## 2026-04-14 - Consolidate iterative aggregations to avoid duplicated logic
**Learning:** Using chained `.reduce()` and `.filter()` array passes calculates aggregations multiple times. In cases where the logic relies on an expensive function like `calcular_status_entidade()`, doing this inside multiple `.filter()` conditions forces redundant O(N) evaluations that calculate set intersections repeatedly per entity.
**Action:** Consolidate these iterative loops into a single `for...of` pass, caching the result of expensive helper functions (like status calculations) and avoiding redundant array traversals.
