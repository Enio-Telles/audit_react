## 2025-04-07 - Add ARIA labels to icon-only modal close buttons
**Learning:** Found multiple modals missing `aria-label` on their "✕" / "x" close buttons, which creates accessibility issues for screen reader users trying to dismiss dialogues.
**Action:** Always ensure `aria-label="Fechar modal"` (or equivalent) is present on all icon-only modal close buttons.
