# ST por Item

## Papel da Tabela

A `st_itens` e a tabela gold complementar da fase ST para conciliacao tributaria por documento e item. Ela junta a trilha `c176_xml` com a base `nfe_dados_st` e tenta reconciliar o item ao `id_agrupado` do catalogo mestre, sem alterar o contrato das 11 gold do core.

## Origem e Dependencias

- Origem principal:
  - `silver/c176_xml.parquet`
  - `silver/nfe_dados_st.parquet`
- Fontes auxiliares:
  - `produtos.parquet`
  - `id_agrupados.parquet`
  - `produtos_final.parquet`
- Dependencias do pipeline:
  - `produtos`
  - `id_agrupados`
  - `produtos_final`
- Nao altera o contrato das 11 gold do core

## Regras Principais

- padroniza separadamente a trilha do `c176` e a trilha XML de ST/FCP
- consolida por `id_linha_origem`, `chave_documento` e `item_documento`
- preserva a melhor informacao tributaria de cada lado
- continua usando o mesmo contrato gold publico mesmo apos o enriquecimento aditivo da `silver/c176_xml`
- classifica a linha como:
  - `conciliado`
  - `somente_c176`
  - `somente_xml`
- registra `origem_st` como `c176`, `xml` ou `c176+xml`
- tenta mapear a linha ao produto mestre via a mesma heuristica do core

## Campos Criticos

- `id_linha_origem`, `chave_documento`, `item_documento`
- `id_agrupado`, `descricao_padrao`
- `codigo_fonte`, `codigo_produto`, `descricao`
- `ncm`, `cest`, `cfop`, `cst`, `csosn`
- `bc_st_xml`, `vl_st_xml`, `vl_icms_substituto`, `vl_st_retido`
- `bc_fcp_st`, `p_fcp_st`, `vl_fcp_st`
- `vl_ressarc_credito_proprio`, `vl_ressarc_st_retido`, `vl_total_ressarcimento`
- `origem_st`, `status_conciliacao`

## Uso Operacional

- expoe a trilha ST complementar na API e nas exportacoes
- suporta homologacao por paridade com `C:\funcoes - Copia`
- consome uma `silver/c176_xml` mais rica em contexto documental e em dados diretos do XML de entrada, sem precisar mudar o schema gold
- reaproveita o catalogo de produtos do core sem reabrir os contratos existentes

## Limitacoes e Observacoes

- pode sair vazia legitimamente quando `c176` e `nfe_dados_st` ainda nao foram extraidas localmente
- a existencia da tabela nao significa que a extracao ST ja foi homologada para todos os CNPJs
- o comportamento atual e deliberadamente mais controlado que o desktop externo: ele prioriza rastreabilidade e estabilidade do pipeline
- no piloto `37671507000187`, a tabela ja foi materializada com dados reais, mantendo o mesmo schema publico e 1947 registros
- a paridade externa da trilha ST continua classificada como `divergente`, mas a divergencia estrutural ampla foi reduzida no comparador por meio de projecoes canonicas; o gap remanescente ficou concentrado em contagem de registros
- a logica de homologacao canonicamente enriquecida fica no comparador de paridade, nao dentro da gold `st_itens`

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/st_itens.parquet
```
