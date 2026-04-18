## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.
## 2024-04-18 - Keyboard Accessibility for Hover-only Buttons
**Learning:** Elements that use `opacity-0 group-hover:opacity-100` become completely invisible to keyboard users when they receive focus, breaking keyboard navigation and accessibility.
**Action:** Always pair `opacity-0 group-hover:opacity-100` with `focus-visible:opacity-100` and a focus ring (e.g., `focus-visible:ring-2`) so the element reveals itself when navigated to via the keyboard.
