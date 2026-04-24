## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.
## 2024-04-24 - Hover-Only Element Accessibility
**Learning:** The app frequently uses `opacity-0 group-hover:opacity-100` on icon buttons (like settings or delete actions). This pattern makes these elements completely invisible to keyboard-only users navigating via the `Tab` key, breaking WCAG focus visibility guidelines.
**Action:** Whenever using `opacity-0 group-hover:opacity-100` to hide secondary actions, always include `focus-visible:opacity-100` alongside visual focus indicators (e.g., `focus-visible:ring-2 focus-visible:ring-blue-400`) to ensure they reveal themselves on keyboard focus.
