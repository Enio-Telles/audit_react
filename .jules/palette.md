## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.

## 2025-04-27 - Keyboard Focus Visibility on Hover-Only Elements
**Learning:** Elements visually hidden with `opacity-0` and revealed via `group-hover:opacity-100` remain invisible when navigated to via keyboard, rendering them inaccessible.
**Action:** Always pair `opacity-0 group-hover:opacity-100` with `focus-visible:opacity-100` and explicit focus rings (`focus-visible:ring-2`) to guarantee keyboard users can discover and interact with the elements.
