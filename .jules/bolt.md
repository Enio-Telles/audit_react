## 2026-03-26 - React Derived State Performance
**Learning:** Derived state like `Array.filter` applied to large arrays (e.g. `PRODUTOS_EXEMPLO`) directly in render causes O(N) operations on every render, lagging UI when unrelated state updates (e.g. selecting a row or typing).
**Action:** Always wrap expensive list operations (filters, sorts, sets) in `useMemo` and hoist string operations like `toLowerCase()` out of the iteration loop.

## 2026-03-27 - Optimize O(N) DataFrame filtering in nested loop
**Learning:** Performing `pl.col()` filtering on DataFrames inside a large nested `for` loop (like `df.filter(pl.col("id_produto") == id_prod)`) drastically hurts performance because the DataFrame has to be queried sequentially each iteration (causing O(N*M) time complexity).
**Action:** When doing a look-up that only requires simple equivalence matching inside loops, convert the required DataFrame columns to a native Python dictionary beforehand (`dict(zip(keys, values))`). Using `dict.get()` reduces the time complexity to O(1) for lookups and speeds up processing significantly.

## 2026-03-29 - O(N*M) iterative filtering bottleneck
**Learning:** Performing iterative filtering (`df.filter(pl.col(name) == val)`) inside a Python loop (e.g. iterating over unique categories) is an O(N*M) anti-pattern in Polars, where N is the number of rows and M is the number of unique categories. The DataFrame evaluates sequentially each iteration.
**Action:** Always replace iterative filtering loops with `df.group_by(column, maintain_order=True)`. Note that iterating over `group_by` may yield a tuple of keys or a scalar depending on the Polars version, so handle it robustly with `key = key_tuple[0] if isinstance(key_tuple, tuple) else key_tuple`.
