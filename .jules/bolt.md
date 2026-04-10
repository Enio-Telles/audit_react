## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
