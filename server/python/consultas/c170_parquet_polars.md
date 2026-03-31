# C170 em Parquet + Polars

## Objetivo real da consulta

A SQL `c170.sql` atende uma demanda de auditoria e consolidacao por CNPJ: transformar itens do registro C170 em base reutilizavel para produto, estoque e reconciliacao fiscal sem repetir joins caros no Oracle.

## Leitura estrutural da SQL original

- `PARAMETROS`: recebe `CNPJ` e data limite de processamento.
- `ARQUIVOS_VALIDOS`: resolve a versao mais recente do `REG_0000` por `cnpj + dt_ini`.
- `DADOS_C100`: traz chaves e metadados do documento fiscal.
- `DADOS_C170`: traz os itens do documento e seus valores fiscais.
- `DADOS_0200`: traz o cadastro do item para descricao, NCM, CEST e tipo.
- `SELECT` final: materializa um C170 ja enriquecido.

## Leitura fiscal

Campos fiscais que interferem diretamente no uso analitico da consulta:

- `cnpj`: eixo de isolamento dos dados e da pasta do contribuinte.
- `dt_ini` e `data_entrega`: definem qual versao do arquivo EFD vale para cada periodo.
- `chv_nfe`, `num_doc`, `ser`: prova documental para reconciliacao.
- `ind_oper` e `ind_emit`: distinguem entrada, saida e autoria da emissao.
- `cfop`, `cst_icms`, `aliq_icms`, `aliq_st`: contexto tributario do item.
- `cod_item`, `descr_item`, `cod_ncm`, `cest`, `tipo_item`, `cod_barra`: identidade fiscal do produto.

## Gargalos da forma monolitica

- O join com `C100` e `0200` acontecia ainda no Oracle, antes de qualquer reutilizacao.
- A mesma regra de enriquecimento precisava ser recalculada toda vez que outro fluxo queria os itens do C170.
- O resultado final servia bem para leitura imediata, mas mal para rastreabilidade de camadas e reuso por tabela.
- O join com `0200` era de cardinalidade relativamente baixa, mas repetido sem necessidade se o cadastro ja estivesse persistido em Parquet.

## Decomposicao proposta

### Bronze

- `reg0000.parquet`
  - Origem: `SPED.REG_0000`
  - Chave: `reg_0000_id`
  - Papel: historico/versionamento do EFD por CNPJ e periodo
- `c100.parquet`
  - Origem: `SPED.REG_C100`
  - Chave: `reg_c100_id + reg_0000_id`
  - Papel: cabecalho fiscal do documento
- `c170.parquet`
  - Origem: `SPED.REG_C170`
  - Chave: `reg_c100_id + reg_0000_id + num_item`
  - Papel: item fiscal base com metricas e chaves de join
- `reg0200.parquet`
  - Origem: `SPED.REG_0200`
  - Chave: `reg_0200_id`, com join analitico por `reg_0000_id + cod_item`
  - Papel: cadastro de produto do EFD

### Silver

- `c170_enriquecido_lazy`
  - Recomposicao em Polars LazyFrame
  - Join `c170` -> `c100` por `reg_c100_id + reg_0000_id`
  - Join `c170` -> `reg0200` por `reg_0000_id + cod_item`
  - Materializa apenas no final

### Gold

- `produtos_unidades.parquet`
- futuros datasets de `nfe_entrada`, estoque e reconciliacao por item

## Contrato minimo dos Parquets relevantes

### c170.parquet

- `periodo_efd`
- `cnpj`
- `reg_0000_id`
- `reg_c100_id`
- `num_item`
- `cod_item`
- `descr_compl`
- `cfop`
- `cst_icms`
- `qtd`
- `unid`
- `vl_item`
- `vl_desc`
- `vl_icms`
- `vl_bc_icms`
- `aliq_icms`
- `vl_bc_icms_st`
- `vl_icms_st`
- `aliq_st`

### c100.parquet

- `reg_c100_id`
- `reg_0000_id`
- `chv_nfe`
- `cod_sit`
- `ind_emit`
- `ind_oper`
- `num_doc`
- `ser`
- `dt_doc`
- `dt_e_s`

### reg0200.parquet

- `reg_0200_id`
- `reg_0000_id`
- `cod_item`
- `cod_barra`
- `cod_ncm`
- `cest`
- `tipo_item`
- `descr_item`

## Plano de recomposicao em Polars

1. `scan_parquet("c170.parquet")` para itens base.
2. `scan_parquet("c100.parquet")` para cabecalho fiscal.
3. `scan_parquet("reg0200.parquet")` para cadastro do item.
4. Selecionar colunas minimas em cada scan para permitir pruning.
5. Fazer join interno com `c100` e join esquerdo com `reg0200`.
6. Derivar `codigo_fonte = cnpj + "|" + cod_item`.
7. Materializar apenas no consumidor final, como `produtos_unidades`.

## Decisoes de implementacao no projeto

- `server/python/consultas/c170.sql` passou a extrair somente a base do C170.
- `server/python/consultas/c100.sql` agora expone as chaves `reg_c100_id` e `reg_0000_id`.
- `server/python/consultas/reg0200.sql` agora expone `reg_0200_id` e `reg_0000_id`.
- `server/python/audit_engine/utils/recomposicao_c170.py` centraliza a recomposicao lazy.
- `produtos_unidades` passou a consumir essa recomposicao em vez de depender de um C170 ja enriquecido no Oracle.

## Hipoteses e pontos para validacao fiscal

- A regra de escolher apenas o arquivo mais recente por `cnpj + dt_ini` foi preservada.
- O join de cadastro por `reg_0000_id + cod_item` assume que o codigo do item e valido dentro do mesmo arquivo EFD.
- Interpretacoes finas sobre cancelamento, escrituracao extemporanea e uso de `cod_sit` em cenarios especificos ainda devem ser validadas com a area fiscal antes de virar regra de negocio mais restritiva.
