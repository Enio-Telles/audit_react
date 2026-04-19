## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.
## 2024-04-19 - Keyboard Accessibility for Hover-Only Actions
**Learning:** In Tailwind, using `opacity-0 group-hover:opacity-100` creates a complete keyboard accessibility barrier because the elements remain invisible when focused via the `Tab` key.
**Action:** Always pair `opacity-0 group-hover:opacity-100` with `focus-visible:opacity-100` and a focus ring (e.g., `focus-visible:ring-2 focus-visible:outline-none`) to ensure keyboard users can discover and interact with the actions.
