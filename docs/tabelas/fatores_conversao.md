# Fatores de Conversao

## Papel da Tabela

A `fatores_conversao` padroniza compra, venda e unidade de referencia de cada `id_agrupado`. Ela e a base para converter quantidades documentais para uma unidade comparavel no estoque e na trilha ST.

## Origem e Dependencias

- Origem principal: `produtos_agrupados.parquet`
- Fontes auxiliares:
  - `silver/item_unidades.parquet`
  - `extraidos/reg0220.parquet`
  - `storage/CNPJ/{cnpj}/edicoes/fatores.json`
- Dependencias do pipeline: `produtos_agrupados`

## Regras Principais

Precedencia atual:

1. edicao manual do auditor
2. fator fiscal vindo do `reg0220`
3. inferencia por preco medio relativo entre unidades
4. fallback `1.0`

Outras regras:

- `unid_ref` tende a privilegiar a unidade de venda e, na falta dela, a de compra
- quando compra e venda divergem e nenhum fator confiavel foi encontrado, o status pode ficar pendente
- a edicao manual sempre prevalece sobre qualquer calculo automatico

## Campos Criticos

- `id_agrupado`, `descricao_padrao`
- `unid_compra`, `unid_venda`, `unid_ref`
- `fator_compra_ref`, `fator_venda_ref`
- `origem_fator`: `manual`, `reg0220`, `preco_medio` ou fallback
- `status`, `editado_em`

## Uso Operacional

- alimenta `produtos_final`
- e consumida por `nfe_entrada`, `mov_estoque` e `st_itens`
- sustenta a tela de conversao e o reprocessamento em cascata

## Limitacoes e Observacoes

- pode sair vazia legitimamente se `produtos_agrupados` vier vazia
- a inferencia por preco medio depende da qualidade da movimentacao em `silver/item_unidades`
- o comportamento documentado aqui reflete o pipeline atual, que e mais simples que o fluxo desktop do projeto externo

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/fatores_conversao.parquet
```
