
## 2026-04-06 - Accessible Icon-Only Buttons in Lists
**Learning:** Found an accessibility issue pattern where small, dynamically rendered icon-only buttons inside lists (like the "✕" to remove active highlight filters) lack `aria-label`s and visible focus states, making them difficult for screen reader users and keyboard navigators to identify or interact with.
**Action:** When reviewing dynamic list items with icon-only actions, always ensure `aria-label` is provided and add `focus-visible:ring` styles to guarantee keyboard accessibility.
