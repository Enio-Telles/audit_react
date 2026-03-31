## 2026-03-26 - React Derived State Performance
**Learning:** Derived state like `Array.filter` applied to large arrays (e.g. `PRODUTOS_EXEMPLO`) directly in render causes O(N) operations on every render, lagging UI when unrelated state updates (e.g. selecting a row or typing).
**Action:** Always wrap expensive list operations (filters, sorts, sets) in `useMemo` and hoist string operations like `toLowerCase()` out of the iteration loop.

## 2026-03-27 - Optimize O(N) DataFrame filtering in nested loop
**Learning:** Performing `pl.col()` filtering on DataFrames inside a large nested `for` loop (like `df.filter(pl.col("id_produto") == id_prod)`) drastically hurts performance because the DataFrame has to be queried sequentially each iteration (causing O(N*M) time complexity).
**Action:** When doing a look-up that only requires simple equivalence matching inside loops, convert the required DataFrame columns to a native Python dictionary beforehand (`dict(zip(keys, values))`). Using `dict.get()` reduces the time complexity to O(1) for lookups and speeds up processing significantly.

## 2025-03-03 - Use Polars LazyFrames for Parquet file generation
**Learning:** Polars `pl.read_parquet()` eagerly loads the whole dataset into memory, which can cause high memory usage for large generated Parquet files. `pl.scan_parquet()` provides a lazy DataFrame where operations (like `group_by`, `filter`, and length checking) can be pushed down to the reader level. This prevents full dataset loading and improves I/O by executing only upon `.collect()`. Furthermore, an empty length check can also be evaluated lazily via `lf.select(pl.len()).collect().item() == 0` without allocating rows.
**Action:** When performing aggregate metrics or basic checks from a Parquet source in generator scripts, start with `pl.scan_parquet()`, chain the processing logic, and call `.collect()` at the very end to maximize Polars query optimizer's efficiency.
