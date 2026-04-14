## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.
## 2024-05-24 - Map async states and focus-visible utilities to action buttons
**Learning:** Shared button styling constants (e.g., `btnCls`) often omit essential `focus-visible` ring styles and `disabled:` opacity states. Furthermore, primary async buttons lacked `disabled` state protection during pipeline processing, degrading both keyboard navigation and usability.
**Action:** When implementing or refining action buttons in the frontend, ensure primary asynchronous buttons are disabled during processing by mapping to their underlying mutation states (e.g., `isPending`), utilize `aria-busy` for screen reader loading context, and include `focus-visible` utilities (e.g., `focus-visible:ring-2`) in shared button classes for proper keyboard accessibility.
