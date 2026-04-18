## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.
## 2026-04-18 - Optimize multiple aggregations over the same Javascript array
**Learning:** When calculating multiple metrics (like counts based on a complex function evaluation) over the same large array, chaining multiple `.filter(...).length` and `.reduce()` calls forces redundant O(N) traversals per metric and repeated evaluations of the underlying function.
**Action:** Replace the chained array methods with a single pass loop (e.g., `for...of`) to calculate all metrics simultaneously and evaluate expensive underlying functions only once per item.
