## 2024-05-18 - Radix Checkbox and Label Associations
**Learning:** Radix UI components like Checkbox are often wrapped in `<label>` elements for layout purposes, but they may lack explicit `htmlFor` and `id` associations. Screen readers and programmatic access rely on these explicit attributes, even if visually the layout appears to "contain" the interaction correctly.
**Action:** Always ensure custom inputs, even when nested inside labels or custom components, have a generated or static `id` that matches the `htmlFor` of their descriptive label text to guarantee proper accessible name computation.

## 2026-03-30 - Fix Switch Accessibility in Settings
**Learning:** In the Swiss Design Fiscal app, the configuration switches lacked explicit `htmlFor` bindings and readable labels, forcing users to interact only with the tiny switch handle. A `<Label>` was added and the `Switch` was updated with `id` to bind them together, along with cursor-pointer to the label so it functions.
**Action:** When using custom headless/Radix UI components like Switch or Checkbox, always ensure `id` is passed and a `<Label htmlFor="...">` is linked. Add `cursor-pointer` to labels so users know they are clickable.
