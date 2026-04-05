
## 2024-04-05 - Melhora acessibilidade na barra de filtros
**Learning:** Itens de formulário e botões sem labels explícitos diminuem a acessibilidade do app.
**Action:** Sempre usar `<label>` associado a inputs via `htmlFor` e fornecer `aria-label` para botões que contenham apenas ícones.
## 2024-04-05 - Use explicitly associated <label> for main UI inputs
**Learning:** Found that layout components (like LeftPanel) were using generic `<div>` or `<span>` elements as "labels" for critical inputs (e.g., CPF/CNPJ, Data limite). This means screen readers will not announce the purpose of the input.
**Action:** Always replace purely visual text labels next to inputs with a proper `<label>` element containing an `htmlFor` attribute that matches the input's `id`. This ensures both visual and assistive technology users understand the input's purpose without affecting layout styles.
## 2024-05-24 - Improve DataTable pagination and selection accessibility
**Learning:** Found missing aria labels for checkboxes and pagination buttons. Added `aria-label` and `title` to them, and improved visual feedback with `disabled:cursor-not-allowed` for disabled buttons.
**Action:** Always provide `aria-label` or `title` for icon-only elements like `«` and `»` pagination buttons or checkboxes to ensure screen readers can announce them. Also add visual cues like `disabled:cursor-not-allowed` for disabled states.
## 2024-05-30 - Add WAI-ARIA roles to custom tabs and aria-labels to inline filters
**Learning:** Custom tabbed interfaces lack inherent semantics, causing screen readers to misinterpret their structure. Similarly, inline filter inputs without explicit labels are inaccessible to screen reader users.
**Action:** Always apply `role="tablist"`, `role="tab"`, `aria-selected`, `aria-controls`, and `role="tabpanel"` to custom tab implementations. Ensure all inputs, including inline filters, have `aria-label` or explicit `<label>` associations.
