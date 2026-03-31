# Mapeamento de IDs Agrupados

## Papel da Tabela

A `id_agrupados` e a tabela ponte entre o produto individual e o grupo mestre. Ela e a principal peca de navegacao entre `id_produto` e `id_agrupado`.

## Origem e Dependencias

- Origem principal: `produtos_agrupados.parquet`
- Dependencias do pipeline: `produtos_agrupados`
- Reflete indiretamente as edicoes manuais de agregacao

## Regras Principais

- expande `ids_membros` do grupo mestre em linhas individuais
- cria uma linha por `id_produto`
- preserva `descricao_original` e `descricao_padrao`
- nao cria nova heuristica; apenas materializa a ponte ja decidida em `produtos_agrupados`

## Campos Criticos

- `id_produto`: chave publica do produto individual
- `id_agrupado`: chave publica do grupo mestre
- `descricao_original`: identificacao do item de origem
- `descricao_padrao`: identificacao canonica do grupo

## Uso Operacional

- e usada em joins do backend para mapear movimentos documentais ao grupo mestre
- alimenta `nfe_entrada`, `mov_estoque` e `st_itens`
- serve de apoio para auditoria de agregacao e desagregacao

## Limitacoes e Observacoes

- so existe depois que `produtos_agrupados` estiver materializada
- nao substitui o catalogo mestre; apenas liga item e grupo

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/id_agrupados.parquet
```
