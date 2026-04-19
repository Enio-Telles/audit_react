## 2026-04-19 - Keyboard Accessibility on Hover-only Elements
**Learning:** UI elements styled to be visible only on hover using Tailwind's `opacity-0 group-hover:opacity-100` become invisible to keyboard users tabbing through the interface, breaking discoverability.
**Action:** Always pair `group-hover:opacity-100` with `focus-visible:opacity-100` and a clear focus indicator like `focus-visible:ring-2` so the element reveals itself during keyboard navigation.
