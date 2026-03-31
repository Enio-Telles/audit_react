# Produtos Agrupados

## Papel da Tabela

A `produtos_agrupados` materializa o grupo mestre do produto no pipeline analitico. Ela e a tabela que cria o `id_agrupado` e define a `descricao_padrao` usada nas etapas seguintes.

## Origem e Dependencias

- Origem principal: `produtos.parquet`
- Dependencias do pipeline: `produtos`
- Usa edicoes manuais em `storage/CNPJ/{cnpj}/edicoes/agregacao.json`

## Regras Principais

- aplica agregacao manual primeiro, quando existir override do auditor
- agrupa automaticamente o restante por:
  - GTIN compartilhado; ou
  - descricao normalizada com o mesmo NCM
- gera `id_agrupado` sequencial no formato `AGR_XXXXX`
- escolhe `descricao_padrao`, `ncm_padrao`, `cest_padrao` e unidades predominantes do grupo
- registra `origem` como `manual` ou `automatico`

## Campos Criticos

- `id_agrupado`: chave mestra do produto consolidado
- `descricao_padrao`: descricao canonica do grupo
- `ids_membros`: lista JSON dos `id_produto` membros
- `qtd_membros`, `qtd_total_nfe`, `valor_total`: escala do grupo
- `origem`, `criado_em`, `editado_em`, `status`: rastreabilidade da agregacao

## Uso Operacional

- alimenta `id_agrupados`, `fatores_conversao` e `produtos_final`
- suporta a tela de agregacao e o reprocessamento em cascata
- define a passagem do fio de ouro de `id_produto` para `id_agrupado`

## Limitacoes e Observacoes

- o agrupamento automatico e intencionalmente conservador
- a edicao manual tem prioridade sobre a heuristica automatica
- pode sair vazia legitimamente se `produtos.parquet` estiver vazio

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/produtos_agrupados.parquet
```
