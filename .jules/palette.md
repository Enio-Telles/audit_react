## 2025-04-06 - Accessible Tab Navigation
**Learning:** When applying WAI-ARIA tab patterns to navigation elements without full arrow-key event listener implementation, a roving tabindex breaks native Tab-key navigation.
**Action:** Apply `role="tablist"`, `role="tab"`, and `role="tabpanel"` along with `aria-selected` and `aria-controls`, but omit `tabIndex={-1}` on inactive tabs to preserve standard keyboard accessibility.
