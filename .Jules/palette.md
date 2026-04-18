## 2024-05-24 - Initial Journal
**Learning:** Initial Palette memory check.
**Action:** Starting the project.
## 2024-05-24 - Keyboard accessibility for hover-revealed actions
**Learning:** Elements relying on `opacity-0 group-hover:opacity-100` to hide visual clutter remain hidden to keyboard users (via Tab) unless `focus-visible:opacity-100` is explicitly provided.
**Action:** Always pair `opacity-0 group-hover:opacity-100` with `focus-visible:opacity-100` and visual focus indicators (`focus-visible:ring-2`) in Tailwind for complete keyboard accessibility, using custom offsets (e.g. `ring-offset-[#0d1f3c]`) when over custom hex backgrounds.
