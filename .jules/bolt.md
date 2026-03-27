## 2026-03-26 - React Derived State Performance
**Learning:** Derived state like `Array.filter` applied to large arrays (e.g. `PRODUTOS_EXEMPLO`) directly in render causes O(N) operations on every render, lagging UI when unrelated state updates (e.g. selecting a row or typing).
**Action:** Always wrap expensive list operations (filters, sorts, sets) in `useMemo` and hoist string operations like `toLowerCase()` out of the iteration loop.

## 2026-03-27 - Optimize O(N) DataFrame filtering in nested loop
**Learning:** Performing `pl.col()` filtering on DataFrames inside a large nested `for` loop (like `df.filter(pl.col("id_produto") == id_prod)`) drastically hurts performance because the DataFrame has to be queried sequentially each iteration (causing O(N*M) time complexity).
**Action:** When doing a look-up that only requires simple equivalence matching inside loops, convert the required DataFrame columns to a native Python dictionary beforehand (`dict(zip(keys, values))`). Using `dict.get()` reduces the time complexity to O(1) for lookups and speeds up processing significantly.
