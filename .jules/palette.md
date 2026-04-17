## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.

## 2024-04-17 - Custom Background Ring Offsets
**Learning:** Left panel buttons lacked clear keyboard focus outlines because the default outline clashed or was obscured. Furthermore, because the panel uses a custom hex background (`#0d1f3c`) instead of a standard Tailwind token, generic ring offsets look out of place.
**Action:** Use arbitrary value syntax for `focus-visible:ring-offset-[#0d1f3c]` to perfectly match custom background colors when adding focus rings.
