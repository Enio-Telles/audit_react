# PLANO_SQL - Checklist Executivo do Dossie

- `[x]` 1. **Modulo Guia**: `Agent_SQL.md` criado com regras de bind variables, isolamento de SQL e orientacoes para o legado XML.
- `[x]` 2. **Fragmentacao do Repositorio SQL**:
  - `[x]` Enderecos
  - `[x]` Historico de situacao
  - `[x]` Regime de pagamento
  - `[x]` Atividades
  - `[x]` Contador
  - `[x]` Historico FAC
  - `[x]` Vistorias
  - `[x]` Historico de socios
  - `[x]` Cadastro mantido via reuso de `dados_cadastrais.sql`
- `[/]` 3. **Motor de Extracao**:
  - `[x]` Criar `src/interface_grafica/services/dossie_extraction_service.py`
  - `[x]` Resolver secao via `dossie_resolution.py`
  - `[x]` Executar SQL com binds filtrados pelo `SqlService`
  - `[x]` Persistir cache em Parquet
  - `[ ]` Suportar composicao com multiplos `sql_ids` por secao
  - `[ ]` Retornar metadados mais auditaveis da execucao
- `[x]` 4. **API FastAPI**:
  - `[x]` Expor rota POST de sincronizacao
  - `[x]` Alinhar o contrato real da rota com a documentacao
  - `[x]` Preservar compatibilidade com o alias legado previsto no plano
  - `[x]` Adicionar teste automatizado para o POST de sync
- `[x]` 5. **Mapeamento Logico**:
  - `[x]` Atualizar `dossie_catalog.py`
  - `[x]` Atualizar `dossie_aliases.py`
- `[x]` 6. **Integracao React**:
  - `[x]` Criar `dossieApi.syncSecao(...)` em `frontend/src/api/client.ts`
  - `[x]` Adicionar botao de sincronizacao no `DossieTab.tsx`
  - `[x]` Implementar `useMutation` com invalidacao de cache
  - `[x]` Exibir feedback visual por secao durante execucao
- `[ ]` 7. **Validacao Final**:
  - `[x]` Cobrir o roteamento de sync com testes automatizados
  - `[ ]` Executar dry-run do POST de sync com consulta real
  - `[ ]` Confirmar criacao do parquet com cache canonico em execucao real
  - `[ ]` Validar atualizacao da UI sem reload manual em execucao real

## Referencia

- Status detalhado e riscos: `docs/plano_sql.md`
