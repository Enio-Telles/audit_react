## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.

## 2024-04-21 - Keyboard Accessibility for Hover-only UI
**Learning:** Elements styled with `opacity-0 group-hover:opacity-100` are invisible and inaccessible to keyboard-only users who tab to them.
**Action:** Always include `focus-visible:opacity-100` alongside visual focus indicators (like `focus-visible:ring-2 focus-visible:outline-none`) to ensure hover-only elements reveal themselves on keyboard focus.
