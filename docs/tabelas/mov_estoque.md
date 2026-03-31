# Movimentacao de Estoque

## Papel da Tabela

A `mov_estoque` e a trilha cronologica e auditavel do estoque no `audit_react`. Ela consolida entradas, saidas e inventario em uma sequencia unica por `id_agrupado`.

## Origem e Dependencias

- Origem principal:
  - `nfe_entrada.parquet`
  - `silver/fontes_produtos.parquet`
- Fontes auxiliares:
  - `produtos.parquet`
  - `id_agrupados.parquet`
  - `produtos_agrupados.parquet`
  - `produtos_final.parquet`
- Dependencias do pipeline:
  - `nfe_entrada`
  - `produtos_final`
  - `id_agrupados`

## Regras Principais

- entradas chegam da `nfe_entrada`
- saidas e inventario saem de `silver/fontes_produtos`
- o mapeamento para `id_agrupado` reutiliza a heuristica do catalogo de produtos
- a ordem logica e cronologica por `id_agrupado`, `data` e tipo
- o calculo sequencial atual faz:
  - entrada aumenta saldo e saldo financeiro
  - saida baixa saldo pelo custo medio vigente
  - inventario registra a linha sem recalcular saldo

## Campos Criticos

- `id_agrupado`, `descricao`
- `tipo`: `ENTRADA`, `SAIDA` ou `INVENTARIO`
- `data`
- `quantidade`, `valor_total`
- `saldo`
- `custo_medio`
- `cfop`, `origem`

## Uso Operacional

- alimenta `aba_mensal`
- e a principal tabela da tela `Estoque`
- sustenta conciliacao operacional e exportacao de movimentos

## Limitacoes e Observacoes

- o comportamento atual e mais enxuto que o do projeto externo: ele nao documenta ainda todas as neutralizacoes e linhas sinteticas do desktop
- ainda assim, o papel da tabela como trilha cronologica do estoque ja esta consolidado
- pode sair vazia legitimamente se nao houver entradas, saidas ou inventario mapeados

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/mov_estoque.parquet
```
