
## 2024-04-05 - Melhora acessibilidade na barra de filtros
**Learning:** Itens de formulário e botões sem labels explícitos diminuem a acessibilidade do app.
**Action:** Sempre usar `<label>` associado a inputs via `htmlFor` e fornecer `aria-label` para botões que contenham apenas ícones.

## 2026-04-05 - Botões com apenas ícones
**Learning:** Icon-only buttons sem ARIA labels tornam a interface inacessível para leitores de tela e a falta de focus visível prejudica a navegação por teclado.
**Action:** Sempre adicione 'aria-label' e classes de 'focus-visible' em botões que exibem apenas ícones (como 'X' para remover).
