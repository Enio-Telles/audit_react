## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.
## 2024-04-28 - Missing keyboard visibility on hover-only actions
**Learning:** Found a specific accessibility issue pattern where UI elements styled with `opacity-0 group-hover:opacity-100` are completely invisible to keyboard-only users navigating via Tab.
**Action:** Always pair `group-hover:opacity-100` with `focus-visible:opacity-100` and visual focus indicators like `focus-visible:ring-2` so actions reveal themselves when focused.
