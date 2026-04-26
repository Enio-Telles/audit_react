## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.
## 2025-04-26 - Keyboard Accessibility for Hover-Revealed Actions
**Learning:** UI elements in this application styled to appear only on hover (e.g., using `opacity-0 group-hover:opacity-100`) become invisible and unusable for keyboard-only users who navigate via `Tab`.
**Action:** When implementing hover-revealed elements, always add `focus-visible:opacity-100` along with visual focus indicators (like `focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[color]-400 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-900`) to ensure they are discoverable and accessible via keyboard navigation.
