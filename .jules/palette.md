## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.
## 2024-05-24 - Action Button Accessibility
**Learning:** Shared button classes across tabs often lack keyboard focus indicators (`focus-visible:ring-2`), and async actions like pipeline execution fail to communicate loading states (`aria-busy`) or disable themselves during processing, leading to poor keyboard and screen reader UX.
**Action:** Always include `focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-400 disabled:opacity-50 disabled:cursor-not-allowed` in shared `btnCls` strings, and map `isPending` or polling states to both `disabled` and `aria-busy` on async buttons.
