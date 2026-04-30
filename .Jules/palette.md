## 2024-04-30 - Keyboard Accessibility for Hover-only Elements
**Learning:** Elements styled with `opacity-0 group-hover:opacity-100` become completely inaccessible to keyboard users because they cannot see the element even when it receives focus. This is a common pattern for secondary actions like "Settings" or "Delete" icons.
**Action:** Always pair `opacity-0 group-hover:opacity-100` with `focus-visible:opacity-100` and visual focus indicators (`focus-visible:ring-2`) so the elements reveal themselves during keyboard navigation (via `Tab`).
