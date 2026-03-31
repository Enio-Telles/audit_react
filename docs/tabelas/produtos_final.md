# Produtos Finais

## Papel da Tabela

A `produtos_final` e o catalogo mestre final do produto no `audit_react`. Ela combina agrupacao e conversao e expõe o conjunto minimo publico para estoque, consulta e trilhas complementares.

## Origem e Dependencias

- Origem principal:
  - `produtos_agrupados.parquet`
  - `fatores_conversao.parquet`
- Dependencias do pipeline:
  - `produtos_agrupados`
  - `fatores_conversao`

## Regras Principais

- herda a identidade mestre de `produtos_agrupados`
- injeta `unid_ref`, `fator_compra_ref` e `fator_venda_ref`
- preserva `status_agregacao` e `status_conversao`
- nao refaz regras documentais; apenas consolida o catalogo final

## Campos Criticos

- `id_agrupado`, `descricao_padrao`
- `ncm_padrao`, `cest_padrao`
- `unid_ref`
- `fator_compra_ref`, `fator_venda_ref`
- `ids_membros`, `qtd_membros`
- `status_agregacao`, `status_conversao`

## Uso Operacional

- alimenta `nfe_entrada`, `mov_estoque`, `produtos_selecionados` e `st_itens`
- e a principal referencia de produto consolidado para a UI
- representa a parte final do fio de ouro do catalogo de produto

## Limitacoes e Observacoes

- depende integralmente da qualidade de agregacao e conversao anteriores
- nao substitui a trilha documental; ela apenas padroniza o produto mestre

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/produtos_final.parquet
```
