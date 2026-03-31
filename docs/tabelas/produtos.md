# Produtos

## Papel da Tabela

A `produtos` e o catalogo publico consolidado do produto individual. Ela elimina a duplicidade por unidade e mantem um unico registro por `id_produto`, ainda antes da agregacao para `id_agrupado`.

## Origem e Dependencias

- Origem principal: `produtos_unidades.parquet`
- Dependencias do pipeline: `produtos_unidades`
- Nao usa edicoes manuais diretamente

## Regras Principais

- consolida um unico produto publico por `id_produto`
- escolhe `unidade_principal` a partir da referencia operacional predominante
- totaliza a movimentacao documental em `qtd_total_nfe` e `valor_total`
- classifica o tipo do produto conforme o padrao de compra, venda ou ambos

## Campos Criticos

- `id_produto`: chave publica do item antes do agrupamento
- `descricao`, `ncm`, `cest`: base para agregacao e rastreabilidade
- `unidade_principal`: unidade operacional dominante
- `qtd_total_nfe`, `valor_total`, `tipo`: intensidade e perfil de uso

## Uso Operacional

- alimenta `produtos_agrupados`
- serve de base para o mapeamento de fontes documentais em `nfe_entrada`, `mov_estoque` e `st_itens`
- e exibida diretamente na UI de consulta e nas rotinas de agregacao

## Limitacoes e Observacoes

- continua sendo uma visao individual; nao representa ainda o produto mestre consolidado
- o agrupamento automatico e manual acontece depois desta tabela

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/produtos.parquet
```
