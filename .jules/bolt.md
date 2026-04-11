## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.

## 2026-04-11 - Pre-compile generic rules before O(N*M) render loops
**Learning:** Inside `DataTable.tsx`, running `v.toLowerCase()` or `parseFloat()` directly inside an evaluation function (`matchesRule`) executed for every cell in a large virtualized/paginated table creates massive redundant allocations and parser overhead.
**Action:** When evaluating dynamic rules across thousands of elements, hoist string manipulation and number parsing into a `compileRule` phase outside the render/evaluation loop, caching properties like `_vLower` and `_vFloat` on a pre-compiled rule object.
