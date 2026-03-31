## 2024-03-31 - DataTable Integration Customizations

**Learning:** When using AG Grid within `ag-grid-react`, replacing HTML tables requires careful mapping of callbacks like `onSelectionChanged` and usage of custom `cellRenderer` templates to maintain application logic. If this is forgotten, the grid will display simple text arrays but user interactions (checkboxes, inline buttons) will be lost.

**Action:** Upgraded the `<DataTable />` wrapper component to support optionally receiving `customColumnDefs` extending `ColDef[]`. This allows defining nested cell renderers and preserving original specific components (like `CheckCircle2` for booleans or DOCX generation buttons) inside AG Grid cells natively.
