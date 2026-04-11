## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.

## 2026-04-11 - O(N^2) Complexity in List Comprehension
**Learning:** Checking for existence in a sublist via list comprehension (`[item for i, item in enumerate(lst) if item not in lst[:i]]`) creates an O(N^2) complexity bottleneck.
**Action:** Replace the list comprehension with `list(dict.fromkeys(lst))` to achieve O(N) deduplication while preserving the original insertion order.
