
## 2024-04-05 - Melhora acessibilidade na barra de filtros
**Learning:** Itens de formulário e botões sem labels explícitos diminuem a acessibilidade do app.
**Action:** Sempre usar `<label>` associado a inputs via `htmlFor` e fornecer `aria-label` para botões que contenham apenas ícones.

## 2026-04-05 - DataTable Accessibility and Feedback
**Learning:** The custom DataTable lacked ARIA labels for icon-only pagination buttons and select-all/row checkboxes, making them inaccessible to screen readers. It also lacked keyboard focus indicators (`focus-visible`) and disabled button feedback.
**Action:** Add explicit `aria-label`/`title` for icon-only controls and implement consistent `focus-visible:ring-2` for keyboard navigation on custom interactive table elements.
