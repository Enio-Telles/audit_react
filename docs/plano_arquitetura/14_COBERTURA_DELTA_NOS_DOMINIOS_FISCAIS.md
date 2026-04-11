# Cobertura Delta nos domínios fiscais

## Objetivo

Este passo fecha a cobertura de leitura adaptativa nos quatro domínios do backend fiscal novo.

---

## O que faltava

Depois da leitura adaptativa ter entrado em Documentos, Análise e no endpoint genérico, ainda restavam dois pontos presos em `pl.read_parquet()`:

- `backend/routers/fiscal_efd.py`
- `backend/routers/fiscal_fiscalizacao.py`

Isso deixava a migração para Delta incompleta do ponto de vista do backend funcional.

---

## O que foi implementado

### EFD

- resolução automática de caminho materializado;
- leitura de `C170`, `C176`, `Bloco H`, `K200` e `C197` em Parquet ou Delta;
- probes e paginação funcionando sobre o formato realmente encontrado.

### Fiscalização

- leitura adaptativa de `dados_cadastrais` e `malhas`;
- leitura do primeiro registro de cadastro usando helper de storage;
- manutenção da mesma API externa, sem exigir alteração imediata no frontend.

---

## Estado atual

Agora os quatro domínios do módulo fiscal novo estão cobertos:

- EFD
- Documentos Fiscais
- Fiscalização
- Análise Fiscal

Além disso, o endpoint genérico `/api/parquet/query` e o `ParquetService` também acompanham a transição.

---

## Próximo passo recomendado

Com a cobertura funcional do backend fechada, o próximo passo mais valioso passa a ser operacional:

1. executar uma rodada real do pipeline com Delta seletivo;
2. validar leitura nos quatro domínios;
3. confirmar o aparecimento dos datasets no fluxo de navegação e inspeção;
4. revisar nomes/caminhos de datasets da primeira onda para evitar divergência entre `tb_documentos`, `mov_estoque`, `aba_mensal` e variantes legadas.

### Configuração inicial sugerida

```bash
export DATA_LAKE_FORMAT=delta
export DELTA_ENABLED_TABLES=tb_documentos,mov_estoque,aba_mensal,c170_xml,bloco_h,malhas,dados_cadastrais
export DELTA_WRITE_MODE=overwrite
```
