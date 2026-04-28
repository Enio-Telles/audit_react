## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.
## 2026-04-14 - Optimize repetitive React component loop evaluations
**Learning:** In React components like `DossieContatoDetalhe`, using multiple chained array iterations (`.reduce()` and `.filter()`) to evaluate a computationally expensive function repeatedly per item causes redundant O(N) evaluations, significantly slowing down renders when the data set is large.
**Action:** Consolidate multiple array iteration passes into a single `for...of` loop and cache the result of the expensive evaluation (e.g. `const status = calcular_status_entidade(entidade);`) to calculate all aggregated values concurrently.
