## 2026-04-10 - Optimize Polars multiple counts on same DataFrame
**Learning:** In Polars, calculating multiple counts or aggregates on the same DataFrame by chaining `df.filter(...).height` forces redundant dataset scans.
**Action:** Use a single `df.select()` containing multiple `.sum()` aggregations on boolean expressions (e.g., `(pl.col('col') == val).sum().alias('count')`) to process all metrics concurrently in a single pass.
## 2026-04-12 - Pre-compile React UI rules into useMemo hook
**Learning:** When applying dynamic rules across large datasets in React (e.g., evaluating highlight rules per table cell), executing `.toLowerCase()` and `parseFloat()` directly inside the render loop causes redundant O(N*M) allocations.
**Action:** Pre-compile the rule configurations inside a `useMemo` hook to cache parsed variables before iterating the dataset.
## 2024-05-18 - Avoid adding test dependencies without instruction
**Learning:** During validation, if test dependencies like `@testing-library/react` are missing and tests fail, do not use `pnpm install` to add them unless explicitly instructed. Doing so violates strict constraints against modifying `package.json` or `pnpm-lock.yaml`.
**Action:** Revert any accidental `package.json` modifications using `git restore` and proceed with available validation tools like `pnpm lint` or `tsc --noEmit`, accepting pre-existing test failures due to missing environment setup rather than attempting to fix them.
