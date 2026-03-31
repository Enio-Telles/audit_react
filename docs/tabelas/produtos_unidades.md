# Produtos por Unidades

## Papel da Tabela

A `produtos_unidades` e a primeira tabela gold publica do fluxo de produtos. Ela consolida cada `codigo_fonte` em um registro auditavel, preservando descricao, classificacao fiscal, unidades predominantes e totais de compra e venda.

## Origem e Dependencias

- Origem principal: `silver/item_unidades.parquet`
- Dependencias do pipeline: nenhuma tabela gold anterior
- Nao usa edicoes manuais diretamente

## Regras Principais

- parte da consolidacao por `codigo_fonte`, que representa o produto antes do agrupamento;
- escolhe `unid_compra` pela maior movimentacao de compra;
- escolhe `unid_venda` pela maior movimentacao de venda;
- gera `id_produto` sequencial publico para o catalogo interno do `audit_react`;
- agrega compras, vendas e quantidades sem perder a separacao operacional entre entrada e saida.

## Campos Criticos

- `id_produto`: identificador publico canonico do produto individual
- `descricao`, `ncm`, `cest`, `gtin`: atributos principais de classificacao
- `unid_compra`, `unid_venda`: base para fatores e estoque
- `qtd_nfe_compra`, `qtd_nfe_venda`, `valor_total_compra`, `valor_total_venda`: intensidade de movimentacao

## Uso Operacional

- alimenta `produtos`
- participa do fio de ouro do produto:
  origem documental -> `codigo_fonte` -> `id_produto` -> `id_agrupado` -> `descricao_padrao`
- e consultada pela API em `GET /api/tabelas/{cnpj}/produtos_unidades`

## Limitacoes e Observacoes

- pode sair vazia legitimamente quando `silver/item_unidades` nao existir ou vier vazia
- nao representa ainda o grupo mestre; esse papel passa para `produtos_agrupados`

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/produtos_unidades.parquet
```
