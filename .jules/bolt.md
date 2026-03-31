## 2026-03-26 - React Derived State Performance
**Learning:** Derived state like `Array.filter` applied to large arrays (e.g. `PRODUTOS_EXEMPLO`) directly in render causes O(N) operations on every render, lagging UI when unrelated state updates (e.g. selecting a row or typing).
**Action:** Always wrap expensive list operations (filters, sorts, sets) in `useMemo` and hoist string operations like `toLowerCase()` out of the iteration loop.

## 2026-03-27 - Optimize O(N) DataFrame filtering in nested loop
**Learning:** Performing `pl.col()` filtering on DataFrames inside a large nested `for` loop (like `df.filter(pl.col("id_produto") == id_prod)`) drastically hurts performance because the DataFrame has to be queried sequentially each iteration (causing O(N*M) time complexity).
**Action:** When doing a look-up that only requires simple equivalence matching inside loops, convert the required DataFrame columns to a native Python dictionary beforehand (`dict(zip(keys, values))`). Using `dict.get()` reduces the time complexity to O(1) for lookups and speeds up processing significantly.

## 2026-03-28 - Optimize full Parquet reads using Polars LazyFrame in FastAPI
**Learning:** Using `pl.read_parquet(arquivo)` to eagerly load Parquet files into memory before applying pagination (`slice`) and simple text filters creates a major memory bottleneck. When data sets are large, reading the entire DataFrame eagerly and executing `.len()` or subsetting locally forces all chunks into RAM, blocking resources for other requests.
**Action:** Use `pl.scan_parquet(arquivo)` to leverage the Lazy API. Operations like `.filter()` and `.slice()` can be pushed down the query plan, allowing Polars to intelligently skip Parquet row groups or read only the needed partitions. Use `lf.collect_schema().names()` to validate available columns before applying operations, and dynamically calculate row totals efficiently with `lf.select(pl.len()).collect().item()`.
