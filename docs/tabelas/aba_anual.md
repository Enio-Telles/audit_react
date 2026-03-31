# Fechamento Anual

## Papel da Tabela

A `aba_anual` e o fechamento anual do estoque por `id_agrupado`. Ela deriva exclusivamente de `aba_mensal`, sem recomputar regras documentais.

## Origem e Dependencias

- Origem principal: `aba_mensal.parquet`
- Dependencias do pipeline: `aba_mensal`
- Nao usa `extraidos` nem `silver` diretamente

## Regras Principais

- deriva `ano` a partir do campo `mes`
- ordena os resumos mensais para preservar a sequencia anual
- consolida por ano:
  - `saldo_inicial_ano`
  - `total_entradas`
  - `total_saidas`
  - `saldo_final_ano`
  - `custo_medio_anual`
  - `valor_estoque_final`
  - `meses_com_omissao`
- calcula `total_omissao` quando o saldo anual final fica negativo

## Campos Criticos

- `id_agrupado`, `descricao`, `ano`
- `saldo_inicial_ano`, `total_entradas`, `total_saidas`, `saldo_final_ano`
- `custo_medio_anual`, `valor_estoque_final`
- `meses_com_omissao`, `total_omissao`

## Uso Operacional

- e consumida pela tela `Estoque` para leitura anual
- apoia comparacoes entre exercicios sem depender da trilha de item

## Limitacoes e Observacoes

- herda a qualidade e as limitacoes de `aba_mensal`
- o total anual de omissao no pipeline atual e uma medida derivada do saldo final anual negativo

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/aba_anual.parquet
```
