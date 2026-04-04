## 2026-03-26 - React Derived State Performance
**Learning:** Derived state like `Array.filter` applied to large arrays (e.g. `PRODUTOS_EXEMPLO`) directly in render causes O(N) operations on every render, lagging UI when unrelated state updates (e.g. selecting a row or typing).
**Action:** Always wrap expensive list operations (filters, sorts, sets) in `useMemo` and hoist string operations like `toLowerCase()` out of the iteration loop.

## 2024-03-31 - Lazy Parquet Metadata Extraction
**Learning:** Extracting basic metadata like row counts and column schemas by reading the entire Parquet dataset into memory (`pl.read_parquet()`) causes massive memory spikes and slow response times in APIs.
**Action:** When only schema names or row counts are needed from a Parquet file, always use Polars' lazy evaluation: `schema = pl.read_parquet_schema(arquivo)` and `lf.select(pl.len()).collect().item()`. This allows the engine to push optimizations down to the reader level without allocating rows. In tests with mocked polars, `pl.read_parquet(arquivo)` can be used as fallback.

## 2024-05-20 - Lazy evaluation for reading Parquet metadata
**Learning:** Eager loading a Parquet file just to fetch its schema, columns, or record count using `pl.read_parquet` can cause severe memory and performance bottlenecks, especially for large datasets.
**Action:** Always use `lf = pl.scan_parquet` combined with `lf.collect_schema()` to get columns/schema and `lf.select(pl.len()).collect().item()` to get the total number of records without loading the entire dataframe into memory.
## 2026-04-01 - Lazy Evaluation for Polars Filters
**Learning:** Using `pl.read_parquet()` loads the entire parquet into memory before applying `.filter()`, leading to large memory overhead. Instead, `pl.scan_parquet().filter().collect()` pushes the filter execution down to the file level, bringing only the necessary rows into memory.
**Action:** Always prefer lazy evaluation with Polars when loading Parquet files, chaining operations like filters and sorts before `.collect()`.

## 2026-03-31 - Memoizing Reference Table Loading
**Learning:** Loading static reference data (like NCM, CEST) via eager `pl.read_parquet()` repeatedly on every lookup or validation call causes severe disk I/O and memory instantiation overhead (N+1 queries).
**Action:** Always wrap data-loading functions for small, frequently-accessed reference tables with `functools.lru_cache(maxsize=1)` so the underlying dataframe is read from disk only once.

## 2026-04-01 - Fast JSON Array Parsing in Polars
**Learning:** Using regex string replacements (`str.replace_all`) and `.str.split()` to extract arrays of elements from JSON strings is incredibly slow.
**Action:** For expanding JSON string columns containing lists in Polars, always use `.str.json_decode(pl.List(TYPE)).explode()` to leverage vectorized performance and avoid slow Python-level row iteration or slow string manipulation.
