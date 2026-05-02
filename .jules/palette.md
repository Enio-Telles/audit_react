## 2024-04-12 - Accessible Asynchronous Actions
**Learning:** Asynchronous action buttons need `aria-busy` to indicate loading state to screen readers, and modals need proper ARIA attributes to announce their behavior.
**Action:** Always add `aria-busy={isLoading}` to asynchronous buttons and `aria-haspopup="dialog"` / `aria-expanded={isOpen}` to buttons opening modals.
## 2024-05-02 - Textarea Accessibility and Async Buttons
**Learning:** Raw `<textarea>` elements without an associated `<label>` are skipped or announced poorly by screen readers. Furthermore, adding an explicit `type="button"` prevents accidental form submissions if the component is ever wrapped in a form.
**Action:** Always add `aria-label` to textareas when a visual `<label>` is not present. Always add `type="button"` to non-submit buttons and `aria-busy` to buttons executing async mutations.
