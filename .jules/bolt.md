## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.

## 2026-04-11 - Optimize Polars multiple counts on same DataFrame in backend/routers/estoque.py
**Learning:** In Polars, calculating multiple metrics (like unique counts, filtered unique counts, and sums) sequentially by chaining multiple `df.select(...).item()` calls forces redundant scans over the dataset, leading to poor performance on large files.
**Action:** Use a single `df.select()` combining multiple aggregations into a list of expressions (e.g., using `pl.col('codigo_produto').filter(valid_mask).n_unique().alias('total_produtos')`) and then extract all results at once via `.row(0)`, processing all metrics concurrently in one pass.
