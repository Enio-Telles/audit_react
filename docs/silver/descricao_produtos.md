# Descricao Produtos

## Papel da Tabela na Camada Silver

A `descricao_produtos` materializa a dimensao padronizada de descricoes fiscais na camada `silver`. Ela organiza a base textual dos produtos antes das heuristicas de agrupamento e do catalogo gold final.

## Origem em `extraidos`

- origem indireta em `extraidos/nfe.parquet`
- origem indireta em `extraidos/nfce.parquet`
- origem indireta em `extraidos/c170.parquet`
- origem indireta em `extraidos/bloco_h.parquet`

A tabela e extraida de `silver/itens.parquet`.

## Regras Principais de Normalizacao

- seleciona de `itens` apenas os atributos descritivos e classificatorios
- preserva `descricao_normalizada` para comparacao estavel no pipeline
- mantem `codigo_fonte` e `codigo_produto` como ponte entre descricao e origem documental

## Campos Criticos

- `codigo_fonte`, `codigo_produto`
- `descricao`, `descricao_normalizada`
- `ncm`, `cest`, `gtin`

## Tabelas Gold que Consomem a Tabela

- Nenhuma tabela gold consome `descricao_produtos` diretamente na versao atual.
- Ela apoia entendimento do catalogo e pode ser usada em diagnosticos de agregacao.

## Limitacoes e Observacoes

- pode sair vazia legitimamente se `itens` vier vazia
- nao define agrupamento por si so; ela apenas preserva a dimensao textual normalizada
- por ser contrato interno, pode receber colunas novas se o pipeline precisar refinar a etapa de produto

## Saida Gerada

```text
storage/CNPJ/{cnpj}/silver/descricao_produtos.parquet
```
