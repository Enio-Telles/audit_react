# Plano de Otimização Estrutural - Fase 3 (Ressarcimento & Rede)

Este plano foca em estender a arquitetura Lazy (Polars `scan_parquet`) para o pacote de Ressarcimento ST e consolidar as correções de estabilidade de rede aplicadas ao frontend.

## User Review Required

> [!IMPORTANT]
> A refatoração do `ler_parquet_opcional` em `base.py` afetará todos os geradores do pacote Ressarcimento. Garantiremos que cada gerador finalize com `.collect()` para manter a compatibilidade com a função `salvar_df`.

## Proposed Changes

---

### [Component] Frontend API & Estabilidade
#### [MODIFY] [client.ts](file:///c:/sist_react_02/frontend/src/api/client.ts)
- [x] Remover função `buscarTodasAsPaginas` que causava flood de requisições `page_size=1` no backend.
- [x] Ajustar endpoints para utilizar paginação nativa do servidor, evitando picos de carga em tabelas com milhões de registros.

---

### [Component] Backend ETL - Ressarcimento ST (`ressarcimento_st_pkg`)

#### [MODIFY] [base.py](file:///c:/sist_react_02/src/transformacao/ressarcimento_st_pkg/base.py)
- [ ] Implementar `scan_parquet_opcional` que retorna `LazyFrame`.
- [ ] Versão otimizada do schema alignment para operar em modo Lazy.

#### [MODIFY] [gerador.py (item)](file:///c:/sist_react_02/src/transformacao/ressarcimento_st_pkg/ressarcimento_st_item/gerador.py)
- [ ] Converter joins de `c176_xml`, `credito_icms` e bases Oracle para Lazy.
- [ ] Otimizar filtros de validação.

#### [MODIFY] [gerador.py (mensal)](file:///c:/sist_react_02/src/transformacao/ressarcimento_st_pkg/ressarcimento_st_mensal/gerador.py)
- [ ] Integrar `scan_parquet` no agrupamento de `mes_ref`.

#### [MODIFY] [gerador.py (conciliacao)](file:///c:/sist_react_02/src/transformacao/ressarcimento_st_pkg/ressarcimento_st_conciliacao/gerador.py)
- [ ] Migrar joins finais para Lazy.

---

## Open Questions

- Nenhuma no momento. A estratégia Lazy validada na Fase 2 será replicada aqui.

## Verification Plan

### Automated Tests
- `PYTHONPATH=src python -m pytest tests/` (se houver testes de ressarcimento).
- Verificação de tipos: `cd frontend && pnpm exec tsc --noEmit`.

### Manual Verification
- Executar pipeline completo de Ressarcimento para o CNPJ `84654326000394`.
- Monitorar logs do backend para garantir que não há mais flood de requisições paginadas.
