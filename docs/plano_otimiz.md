# Plano de Limpeza, Otimização e Reorganização (Fiscal Parquet)

Este documento propõe uma reestruturação profunda do projeto para melhorar a manutenibilidade, escalabilidade e performance.

## Revisão do Usuário Requerida

> [!IMPORTANT]
> - **Reorganização de Pastas**: Propomos remover/consolidar mais de 10 pastas temporárias e redundantes.
> - **Modularização**: Arquivos core como `EstoqueTab.tsx` e `movimentacao_estoque.py` serão fragmentados em sub-módulos.
> - **Performance**: Foco em transformar operações Polars de `Eager` para `Lazy` onde possível.

---

## 1. Proposta de Reorganização de Pastas

Atualmente, o projeto apresenta poluição visual com pastas temporárias e redundantes.

### 1.1 Limpeza de Pastas Redundantes
| Ação | Alvo | Motivo |
|---|---|---|
| **[DELETE]** | `.pytest_tmp`, `tmp_pytest_run`, `_tmp_testes` | Consolidar em `.tmp/` ou usar pastas padrão do pytest. |
| **[DELETE]** | `testes/` | Mover conteúdo relevante para `tests/` e deletar a pasta duplicada. |
| **[RENAME]** | `modelo/` -> `resources/templates/` | Nome mais semântico para modelos de documentos (DOCX/TXT). |
| **[CLEAN]** | `.validacao_*`, `validacao_tmp` | Mover lógica de validação para `scripts/validation/`. |
| **[MOVE]** | `app_react.py`, `orquestrador_pipeline.py` | Mover para `src/` para limpar a raiz do projeto. |

### 1.2 Estrutura Sugerida
```bash
c:\Sistema_react\
├── src/                # Soul do projeto (Python)
├── backend/            # API FastAPI
├── frontend/           # Interface React
├── sql/                # Queries SQL (Modulares)
├── tests/              # Testes unitários e integração
├── docs/               # Documentação Didática
├── resources/          # Templates e ativos estáticos
└── dados/              # Datasets e Referências (Parquet)
```

---

## 2. Modularização de Códigos Extensos

Reduzir arquivos que excedem 20KB para melhorar a legibilidade.

### 2.1 Frontend (React)
- **`EstoqueTab.tsx`**: Extrair `EstoqueSubTab`, `BlocoHSummaryCards` e funções utilitárias (`exportTable`, `openInNewTab`) para arquivos separados.
- **`DataTable.tsx`**: Modularizar sub-componentes (Headers, Rows, Pagination) e extrair hooks de virtualização.
- **`GerenciarCnpjModal.tsx`**: Separar a lógica de formulário da lógica de exibição de status.

### 2.2 Backend/ETL (Python)
- **`movimentacao_estoque_pkg`**: Já está em formato de pacote, mas `movimentacao_estoque.py` deve ser quebrado em `regras_calculo.py`, `filtros_sped.py` e `composicao_final.py`.
- **`calculos_mensais.py`**: Separar lógica de impostos (ICMS, IPI) de lógica de estoque.

---

## 3. Melhorias de Processamento e Performance

### 3.1 Otimização Polars
- **Lazy vs Eager**: Substituir `pl.read_parquet` por `pl.scan_parquet` em todo o fluxo ETL.
- **Predicate Pushdown**: Garantir que filtros (`.filter()`) sejam aplicados antes de joins pesados.
- **Deduplicação**: Otimizar o processo de `deduplicate` para rodar em paralelo usando `unique(maintain_order=False)`.

### 3.2 SQL
- **Modularização de `Fisconforme.sql`**: Quebrar a query de 48KB em arquivos SQL menores por CTE (Common Table Expressions) ou por etapa de processamento.

---

## 4. Documentação Didática

Criar arquivos `.md` explicativos em `docs/`:
- `PIPELINE_FLOW.md`: Diagrama do fluxo de dados desde o Oracle até o React.
- `DATA_MODEL.md`: Explicação dos campos principais das tabelas Parquet calculadas.
- `GETTING_STARTED.md`: Guia passo a passo para configurar o ambiente e rodar o pipeline.

---

## TODO List Detalhado

### Fase 1: Limpeza e Reorganização
- [ ] Criar diretório `.tmp/` e atualizar `.gitignore`.
- [ ] Consolidar pastas de teste (`testes` -> `tests`).
- [ ] Remover duplicata `01_item_unidades.py`.
- [ ] Renomear `modelo/` para `resources/templates/`.
- [ ] Limpar arquivos temporários na raiz.

### Fase 2: Modularização Frontend
- [ ] Criar `frontend/src/utils/tableExport.ts` e mover funções de exportação.
- [ ] Fragmentar `EstoqueTab.tsx` em componentes menores.
- [ ] Fragmentar `DataTable.tsx`.

### Fase 3: Modularização Backend
- [ ] Refatorar `movimentacao_estoque.py` em sub-módulos funcionais.
- [ ] Implementar `LazyFrame` em 100% dos scripts de transformação.

### Fase 4: Documentação e SQL
- [ ] Quebrar `Fisconforme.sql` em partes gerenciáveis.
- [ ] Escrever `ARCHITECTURE.md` e `PIPELINE_FLOW.md`.

---

## Plano de Verificação

### Testes Automatizados
- Executar `pytest tests/` para garantir que a mudança de caminhos não quebrou as importações.
- Rodar `tsc --noEmit` no frontend para validar refs de componentes movidos.

### Manual
- Verificar se o pipeline ETL completa a execução com os novos caminhos de script.
- Validar se a interface React continua carregando todas as abas corretamente.
