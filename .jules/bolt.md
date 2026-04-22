## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.
## 2024-05-18 - Concurrent LazyFrame Evaluation in Polars
**Learning:** Polars does not cache intermediate `LazyFrame` evaluations. Consequently, evaluating `.select(pl.len().alias("n")).collect().item()` and then `.slice(...).collect()` forces the underlying Parquet file to be redundantly scanned twice.
**Action:** Always compute multiple aggregations or branches of the same underlying `LazyFrame` concurrently using `pl.collect_all()` to leverage Common Subplan Elimination (CSE), which drastically reduces disk I/O and evaluation overhead for large out-of-core datasets.
