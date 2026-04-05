## 2025-02-12 - Polars String map_elements bottleneck
**Learning:** Using `map_elements` with Python functions for string manipulation (like removing accents via `unicodedata`) breaks Polars vectorization and severely degrades performance. The installed Polars version lacks `replace_many()`.
**Action:** Always prefer native chained Polars string operations (`.str.replace_all()`, `.str.to_uppercase()`) via regexes to leverage Rust's underlying optimizations and parallelization when cleaning or mapping string elements.
