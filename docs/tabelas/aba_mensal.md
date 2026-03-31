# Fechamento Mensal

## Papel da Tabela

A `aba_mensal` e o resumo mensal do estoque por `id_agrupado`. Ela deriva exclusivamente da trilha ja materializada em `mov_estoque`.

## Origem e Dependencias

- Origem principal: `mov_estoque.parquet`
- Dependencias do pipeline: `mov_estoque`
- Nao usa documentos brutos diretamente

## Regras Principais

- deriva o mes pela chave `YYYY-MM` a partir de `data`
- ordena os movimentos para preservar `first` e `last` do periodo
- calcula por mes:
  - `saldo_inicial`
  - `entradas`
  - `saidas`
  - `saldo_final`
  - `custo_medio`
  - `valor_estoque`
  - `qtd_movimentos`
- marca `omissao` quando `saldo_final` fica negativo

## Campos Criticos

- `id_agrupado`, `descricao`, `mes`
- `saldo_inicial`, `entradas`, `saidas`, `saldo_final`
- `custo_medio`, `valor_estoque`
- `qtd_movimentos`
- `omissao`

## Uso Operacional

- alimenta `aba_anual`
- e consumida pela tela `Estoque` para leitura mensal
- simplifica comparacoes periodicas sem reabrir a trilha documental

## Limitacoes e Observacoes

- depende integralmente da qualidade de `mov_estoque`
- documenta o comportamento atual do pipeline, que trata omissao a partir de saldo final negativo

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/aba_mensal.parquet
```
