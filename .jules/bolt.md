## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2023-10-27 - Polars Conditional Unique Count Excludes Nulls
**Learning:** In Polars, `n_unique()` treats `null` (or `None`) as a distinct value. When consolidating operations using conditional expressions (e.g., `pl.when(...).then(...).otherwise(pl.lit(None)).n_unique()`), this will artificially inflate the unique count by 1 if there are any un-matched rows.
**Action:** Explicitly chain `.drop_nulls()` before `.n_unique()` when calculating unique non-null values dynamically.
