# Produtos Selecionados

## Papel da Tabela

A `produtos_selecionados` e uma visao operacional de apoio para analise detalhada. Ela marca produtos mestres que ficam prontos para consumo em telas e filtros de estoque.

## Origem e Dependencias

- Origem principal: `produtos_final.parquet`
- Dependencias do pipeline: `produtos_final`
- Nao usa edicoes manuais diretamente

## Regras Principais

- replica o catalogo mestre com foco em uso analitico
- marca todos os produtos como `selecionado = true` no estado atual
- preserva um campo `motivo` para evolucoes ou filtros futuros

## Campos Criticos

- `id_agrupado`
- `descricao_padrao`
- `ncm_padrao`
- `selecionado`
- `motivo`

## Uso Operacional

- e usada como base para selecoes de analise detalhada
- ajuda a UI a trabalhar com o catalogo final ja filtrado para estoque

## Limitacoes e Observacoes

- no estado atual, a selecao e ampla e nao representa uma malha de priorizacao complexa
- a tabela pode ser refinada no futuro sem alterar o catalogo mestre

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/produtos_selecionados.parquet
```
