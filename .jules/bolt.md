## 2026-03-26 - React Derived State Performance
**Learning:** Derived state like `Array.filter` applied to large arrays (e.g. `PRODUTOS_EXEMPLO`) directly in render causes O(N) operations on every render, lagging UI when unrelated state updates (e.g. selecting a row or typing).
**Action:** Always wrap expensive list operations (filters, sorts, sets) in `useMemo` and hoist string operations like `toLowerCase()` out of the iteration loop.

## 2024-03-31 - Lazy Parquet Metadata Extraction
**Learning:** Extracting basic metadata like row counts and column schemas by reading the entire Parquet dataset into memory (`pl.read_parquet()`) causes massive memory spikes and slow response times in APIs.
**Action:** When only schema names or row counts are needed from a Parquet file, always use Polars' lazy evaluation: `schema = pl.read_parquet_schema(arquivo)` and `lf.select(pl.len()).collect().item()`. This allows the engine to push optimizations down to the reader level without allocating rows. In tests with mocked polars, `pl.read_parquet(arquivo)` can be used as fallback.

## 2024-05-20 - Lazy evaluation for reading Parquet metadata
**Learning:** Eager loading a Parquet file just to fetch its schema, columns, or record count using `pl.read_parquet` can cause severe memory and performance bottlenecks, especially for large datasets.
**Action:** Always use `lf = pl.scan_parquet` combined with `lf.collect_schema()` to get columns/schema and `lf.select(pl.len()).collect().item()` to get the total number of records without loading the entire dataframe into memory.
