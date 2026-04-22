## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.

## 2024-05-24 - Missing focus on hover-only UI elements
**Learning:** Elements styled with `opacity-0 group-hover:opacity-100` are invisible to keyboard users who navigate via `Tab` unless explicitly styled to show on focus.
**Action:** Always include `focus-visible:opacity-100` along with visual focus indicators like `focus-visible:ring-2` whenever hiding an interactive element until hover.
