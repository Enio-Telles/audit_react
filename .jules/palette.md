## 2025-04-07 - Add ARIA labels to icon-only modal close buttons
**Learning:** Found multiple modals missing `aria-label` on their "✕" / "x" close buttons, which creates accessibility issues for screen reader users trying to dismiss dialogues.
**Action:** Always ensure `aria-label="Fechar modal"` (or equivalent) is present on all icon-only modal close buttons.

## 2025-04-07 - Add WAI-ARIA attributes to progress bars
**Learning:** Visual progress bars implemented with generic tags (like `<div>`) without semantic meaning are inaccessible to screen readers.
**Action:** When creating or updating visual progress bars that use non-semantic HTML tags, always explicitly add WAI-ARIA accessibility attributes including `role="progressbar"`, `aria-valuenow`, `aria-valuemin`, and `aria-valuemax` to ensure screen reader accessibility.
## 2024-04-08 - Add WAI-ARIA roles to custom tab interfaces
**Learning:** Custom tabbed navigation interfaces implemented without semantic HTML lack accessibility for screen readers, as they aren't announced as tabs or tab panels.
**Action:** When creating or modifying custom UI components like tabbed interfaces in the frontend, always apply proper WAI-ARIA roles (`role="tablist"`, `role="tab"`, `role="tabpanel"`) and attributes (`aria-selected`, `aria-controls`, `aria-label`) to ensure keyboard and screen reader accessibility, while strictly avoiding `tabIndex={-1}` if it breaks native Tab navigation.
