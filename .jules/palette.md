## 2024-03-31 - DataTable Integration Customizations

**Learning:** When using AG Grid within `ag-grid-react`, replacing HTML tables requires careful mapping of callbacks like `onSelectionChanged` and usage of custom `cellRenderer` templates to maintain application logic. If this is forgotten, the grid will display simple text arrays but user interactions (checkboxes, inline buttons) will be lost.

**Action:** Upgraded the `<DataTable />` wrapper component to support optionally receiving `customColumnDefs` extending `ColDef[]`. This allows defining nested cell renderers and preserving original specific components (like `CheckCircle2` for booleans or DOCX generation buttons) inside AG Grid cells natively.

## 2024-04-04 - ARIA labels for Icon Buttons

**Learning:** When using Radix UI/Tailwind `Button` components that only contain a single icon (e.g. `<X />`, `<Trash />`), they often lack an `aria-label`, severely reducing accessibility for screen reader users and leaving the button's action unexplained visually for mouse hover if `title` is also omitted.

**Action:** Added `aria-label="Remover CNPJ da DSF"` and `title="Remover"` directly to the close `X` button in the `Relatorios.tsx` DSF section. Future icon-only buttons should be inspected to ensure they possess a localized `aria-label` and, if appropriate, a `title` or tooltip.
