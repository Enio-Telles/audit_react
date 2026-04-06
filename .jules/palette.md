## 2025-04-06 - Accessible Tab Navigation
**Learning:** Custom UI tabs built without primitive elements lack intrinsic accessibility; explicitly defining `role="tablist"`, `role="tab"`, and `role="tabpanel"` along with ID-linking via `aria-controls`/`aria-labelledby` is essential for screen reader context and compliance.
**Action:** When building tabbed interfaces, immediately apply full WAI-ARIA tab pattern roles and focus states, not just active classes.
