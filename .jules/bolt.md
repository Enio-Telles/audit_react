## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.
## 2024-04-27 - Polars LazyFrame Multiple Collects Anti-Pattern
**Learning:** In Polars, calling `.collect()` multiple times on the same `LazyFrame` (e.g., once for `select(pl.len())` and once for `limit()`) forces redundant full scans of the underlying file because Polars does not cache intermediate `LazyFrame` results by default. This causes a significant performance regression (redundant I/O), especially for large Parquet files.
**Action:** Consolidate multiple queries on the same lazy data source using `pl.collect_all([lazyframe.query1(), lazyframe.query2()])` to enable Common Subplan Elimination (CSE) and avoid double-scanning files.
