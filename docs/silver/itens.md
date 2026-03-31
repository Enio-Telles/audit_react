# Itens

## Papel da Tabela na Camada Silver

A `itens` consolida cada produto-origem em uma linha intermediaria antes da agregacao. Ela resume unidades vistas, volumes de compra e venda e a presenca em diferentes fontes documentais.

## Origem em `extraidos`

- origem indireta em `extraidos/nfe.parquet`
- origem indireta em `extraidos/nfce.parquet`
- origem indireta em `extraidos/c170.parquet`
- origem indireta em `extraidos/bloco_h.parquet`

A tabela e derivada de `silver/item_unidades.parquet`.

## Regras Principais de Normalizacao

- agrupa todas as linhas de `item_unidades` por `codigo_fonte` e `codigo_produto`
- transforma as unidades observadas em `lista_unidades`
- soma valores e quantidades totais de compra e venda por produto-origem
- preserva descricao padronizada e classificacoes fiscais para uso nas etapas seguintes

## Campos Criticos

- `codigo_fonte`, `codigo_produto`
- `descricao`, `descricao_normalizada`
- `ncm`, `cest`, `gtin`
- `lista_unidades`
- `valor_total_compras`, `valor_total_vendas`
- `qtd_total_compras`, `qtd_total_vendas`
- `fontes`

## Tabelas Gold que Consomem a Tabela

- Nenhuma tabela gold consome `itens` diretamente na versao atual.
- Ela alimenta `silver/descricao_produtos` e ajuda no diagnostico do catalogo antes da agregacao.

## Limitacoes e Observacoes

- pode sair vazia legitimamente se `item_unidades` vier vazia
- e uma consolidacao intermediaria; nao substitui o catalogo gold publico
- sua funcao principal e reduzir dispersao de produto antes da fase de agregacao

## Saida Gerada

```text
storage/CNPJ/{cnpj}/silver/itens.parquet
```
