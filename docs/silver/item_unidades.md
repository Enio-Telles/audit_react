# Item Unidades

## Papel da Tabela na Camada Silver

A `item_unidades` consolida movimentos por produto-origem e unidade. Ela e a principal base intermediaria para decidir unidades predominantes de compra e venda antes da formacao do catalogo gold.

## Origem em `extraidos`

- `extraidos/nfe.parquet`
- `extraidos/nfce.parquet`
- `extraidos/c170.parquet`
- `extraidos/bloco_h.parquet`

A tabela e derivada da consolidacao de `silver/fontes_produtos.parquet`.

## Regras Principais de Normalizacao

- agrupa por `codigo_fonte`, `codigo_produto`, descricao padronizada, classificacoes fiscais e `unidade`
- separa totais de compras e vendas com base em `tipo_movimento`
- totaliza valores e quantidades por unidade
- concatena as fontes que efetivamente movimentaram aquela combinacao de produto e unidade

## Campos Criticos

- `codigo_fonte`, `codigo_produto`
- `descricao`, `descricao_normalizada`
- `ncm`, `cest`, `gtin`
- `unidade`
- `compras`, `vendas`
- `qtd_compras`, `qtd_vendas`
- `fontes`

## Tabelas Gold que Consomem a Tabela

- `produtos_unidades`
- `fatores_conversao`

## Limitacoes e Observacoes

- pode sair vazia legitimamente se `fontes_produtos` nao tiver movimentos para o CNPJ
- a qualidade da unidade predominante depende da consistencia documental das quantidades nas extracoes
- a tabela e interna ao pipeline e nao e o contrato publico principal da UI

## Saida Gerada

```text
storage/CNPJ/{cnpj}/silver/item_unidades.parquet
```
