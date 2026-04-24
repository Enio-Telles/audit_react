## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.
## 2024-03-24 - Keyboard accessibility for hover-revealed UI
**Learning:** Elements styled with `opacity-0 group-hover:opacity-100` are invisible to keyboard users who navigate via `Tab`.
**Action:** Always include `focus-visible:opacity-100` and clear focus ring indicators (e.g., `focus-visible:ring-2`) when hiding elements visually but keeping them in the DOM and tab order.
