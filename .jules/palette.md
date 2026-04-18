## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.

## 2024-04-13 - Primary Pipeline Action Buttons
**Learning:** The main action buttons in the sidebar lack disabled states during asynchronous polling and are missing `aria-busy` properties. Moreover, generic shared button classes lack essential accessibility features like disabled styling and focus-visible outlines.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons, conditionally disable them when missing required data or during polling, and update shared utility classes (like `btnCls`) to include `disabled:opacity-50 disabled:cursor-not-allowed focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-400 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-900`.
