## 2026-03-26 - React Derived State Performance
**Learning:** Derived state like `Array.filter` applied to large arrays (e.g. `PRODUTOS_EXEMPLO`) directly in render causes O(N) operations on every render, lagging UI when unrelated state updates (e.g. selecting a row or typing).
**Action:** Always wrap expensive list operations (filters, sorts, sets) in `useMemo` and hoist string operations like `toLowerCase()` out of the iteration loop.

## 2026-03-26 - Parquet Read Performance with FastAPI
**Learning:** Using `pl.read_parquet` in FastAPI endpoints to filter and sort large Parquet files causes full loads into memory, resulting in high RAM usage and slower response times.
**Action:** Replace `pl.read_parquet()` with `pl.scan_parquet()` to leverage Polars' LazyFrame API. Chaining `.filter()` and `.sort()` operations before `.collect()` allows Polars to push down these operations to the Parquet reader level, optimizing memory and IO. Use `.collect_schema()` to perform column checks without loading data.
