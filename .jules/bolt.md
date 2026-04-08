
## 2024-05-24 - Number.prototype.toLocaleString vs Intl.NumberFormat performance
**Learning:** Calling `Number.prototype.toLocaleString()` inside a render loop (like a table cell formatter) repeatedly allocates locale data and parses the options object, causing significant performance degradation in JS execution times. In a test with 80k iterations, `toLocaleString()` took ~7.5 seconds, while a cached `Intl.NumberFormat` instance took ~65 milliseconds, making it over 100x faster.
**Action:** Always instantiate and cache `Intl.NumberFormat` objects outside of render loops or frequent operations when formatting large amounts of data in the frontend.

## 2024-05-25 - Polars string processing performance with map_elements
**Learning:** Using `.map_elements()` with custom Python functions (like string manipulation and regex replacements) in Polars breaks vectorization, causing significant performance bottlenecks on large DataFrames. Operations are executed iteratively in Python space instead of utilizing the optimized Rust backend.
**Action:** Always prefer native Polars string operations (e.g., chained `.str.replace_all()`, `.str.to_uppercase()`) for vectorized processing when cleaning or normalizing text columns.

## 2024-05-26 - Polars vectorization for complex accent removal
**Learning:** Using Python functions for complex accent removal inside `.map_elements()` breaks Polars vectorization and severely degrades performance. A fully vectorized approach using chained `.str.replace_all()` with regex groups (e.g., `r"[ÁÀÂÃÄ]", "A"`) and `.str.to_uppercase()` performs over 3x faster than the Python-based counterpart.
**Action:** Always refactor text normalization functions that remove accents or normalize spacing to use native Polars string manipulation methods to fully utilize the Rust backend optimizations.
