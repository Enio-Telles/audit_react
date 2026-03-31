# NFe de Entrada

## Papel da Tabela

A `nfe_entrada` materializa as entradas documentais ja convertidas para a unidade de referencia do grupo. Ela e a base de entrada do estoque no pipeline gold.

## Origem e Dependencias

- Origem principal: `silver/fontes_produtos.parquet`
- Fontes auxiliares:
  - `produtos.parquet`
  - `id_agrupados.parquet`
  - `produtos_agrupados.parquet`
  - `produtos_final.parquet`
- Dependencias do pipeline:
  - `produtos_final`
  - `id_agrupados`

## Regras Principais

- filtra somente linhas de `tipo_movimento = entrada`
- mapeia a linha documental para `id_agrupado`
- converte `quantidade` para `qtd_ref` conforme:
  - mesma `unid_ref`
  - ou `unid_compra`
  - ou `unid_venda`
- preserva chave documental, data, CFOP e emitente

## Campos Criticos

- `chave_nfe`, `data_emissao`, `cfop`
- `id_agrupado`
- `quantidade`, `unidade`, `qtd_ref`
- `valor_unitario`, `valor_total`
- `cnpj_emitente`

## Uso Operacional

- alimenta diretamente `mov_estoque`
- e usada para reconciliar entradas documentais na unidade de referencia
- pode ser consultada diretamente pela API e exportada

## Limitacoes e Observacoes

- pode sair vazia legitimamente se nao houver entradas mapeaveis para grupo
- depende da qualidade do mapeamento entre documento e catalogo de produtos

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/nfe_entrada.parquet
```
