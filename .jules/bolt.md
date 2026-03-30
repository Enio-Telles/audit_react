## 2026-03-26 - React Derived State Performance
**Learning:** Derived state like `Array.filter` applied to large arrays (e.g. `PRODUTOS_EXEMPLO`) directly in render causes O(N) operations on every render, lagging UI when unrelated state updates (e.g. selecting a row or typing).
**Action:** Always wrap expensive list operations (filters, sorts, sets) in `useMemo` and hoist string operations like `toLowerCase()` out of the iteration loop.

## 2026-03-27 - Optimize O(N) DataFrame filtering in nested loop
**Learning:** Performing `pl.col()` filtering on DataFrames inside a large nested `for` loop (like `df.filter(pl.col("id_produto") == id_prod)`) drastically hurts performance because the DataFrame has to be queried sequentially each iteration (causing O(N*M) time complexity).
**Action:** When doing a look-up that only requires simple equivalence matching inside loops, convert the required DataFrame columns to a native Python dictionary beforehand (`dict(zip(keys, values))`). Using `dict.get()` reduces the time complexity to O(1) for lookups and speeds up processing significantly.

## 2026-03-27 - Lazy Parquet Evaluation with Polars
**Learning:** Calling `pl.read_parquet()` eager-loads the entire Parquet file into memory before applying operations like filters or slices, causing unnecessary I/O and memory bloat, especially for APIs supporting pagination.
**Action:** When querying Parquet files where only a subset of rows is returned (e.g., via `.slice()` or `.filter()`), always use `pl.scan_parquet()` to create a `LazyFrame`. Apply operations, and finally call `.collect()`. This allows Polars to push down the optimizations to the storage layer, dramatically improving performance. Use `lf.collect_schema().names()` to inspect columns lazily.
