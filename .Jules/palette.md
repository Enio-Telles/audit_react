## 2024-05-18 - Radix Checkbox and Label Associations
**Learning:** Radix UI components like Checkbox are often wrapped in `<label>` elements for layout purposes, but they may lack explicit `htmlFor` and `id` associations. Screen readers and programmatic access rely on these explicit attributes, even if visually the layout appears to "contain" the interaction correctly.
**Action:** Always ensure custom inputs, even when nested inside labels or custom components, have a generated or static `id` that matches the `htmlFor` of their descriptive label text to guarantee proper accessible name computation.

Date: 2024-05-24
Learning: Proactively applying code changes when a user explicitly requests a written markdown analysis of the UI/UX is dangerous, as it fails to deliver the requested artifact and can introduce unexpected build errors if not carefully verified in the specific environment.
Action: Always adhere strictly to the user's requested deliverable format (e.g., a Markdown report). Only apply code changes if explicitly requested, and always prioritize generating the requested analytical document first.
