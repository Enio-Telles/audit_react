# C176 Xml

## Papel da Tabela na Camada Silver

A `c176_xml` e a trilha intermediaria de ressarcimento e ST por documento e item na camada `silver`. Ela traduz o `C176` extraido para um contrato operacional reutilizavel pela conciliacao gold de ST, preservando o shape necessario para `st_itens` e enriquecendo o documento com informacao direta do XML de entrada.

## Origem em `extraidos`

- `extraidos/c176.parquet`
- `extraidos/nfe.parquet`

## Regras Principais de Normalizacao

- padroniza o `C176` em um artefato por documento de saida e item declarado
- gera `codigo_fonte` e `id_linha_origem` para manter vinculo com produto e documento
- preserva chaves da ultima entrada, motivo de ressarcimento e valores unitarios de ST
- materializa quantidade, valor total e componentes de ressarcimento no mesmo schema silver
- replica no parquet silver os campos documentais mais relevantes do `C176`, como periodo, finalidade, item de saida e numero do item declarado da ultima entrada
- consulta `extraidos/nfe.parquet` apenas para entradas e enriquece a linha por `chave_nfe_ultima_entrada + num_item_ultima_entrada`, trazendo unidade, quantidade e valores comerciais do XML de entrada
- nao depende de tabelas gold para ser materializada; enriquecimentos com `id_agrupado`, fatores e unidade de referencia ficam restritos a projecoes canonicas de homologacao no comparador de paridade

## Campos Criticos

- `id_linha_origem`
- `fonte`, `chave_documento`, `item_documento`
- `periodo_efd`, `data_entrega_efd_periodo`, `cod_fin_efd`, `finalidade_efd`
- `chave_saida`, `num_nf_saida`, `dt_doc_saida`, `dt_e_s_saida`
- `cod_item_ref_saida`, `descricao_item`, `num_item_saida`, `cfop_saida`, `unid_saida`, `qtd_item_saida`
- `codigo_fonte`, `codigo_produto`, `descricao`
- `cfop`, `data_documento`, `cnpj_referencia`
- `quantidade`, `valor_total`
- `cod_mot_res`, `descricao_motivo_ressarcimento`
- `chave_nfe_ultima_entrada`, `num_item_ultima_entrada`, `c176_num_item_ult_e_declarado`, `dt_ultima_entrada`
- `prod_nitem`, `unid_entrada_xml`, `qtd_entrada_xml`, `vl_total_entrada_xml`, `vl_unitario_entrada_xml`
- `vl_unit_bc_st_entrada`, `vl_unit_icms_proprio_entrada`
- `vl_unit_ressarcimento_st`, `vl_ressarc_credito_proprio`, `vl_ressarc_st_retido`, `vr_total_ressarcimento`

## Tabelas Gold que Consomem a Tabela

- `st_itens`

## Limitacoes e Observacoes

- a trilha ST depende de extracao local de `c176`; por isso esta tabela ainda pode aparecer com schema valido e zero linhas em alguns CNPJs
- o enriquecimento de entrada XML depende de `extraidos/nfe.parquet`; se a chave ou o item nao forem encontrados, os campos de entrada permanecem nulos sem quebrar o schema
- isso nao significa paridade externa concluida; significa apenas que o contrato e a materializacao local ja existem
- ela nao altera o contrato das 11 gold do core homologado
- no piloto `37671507000187`, a tabela ja foi materializada com dados reais e passou a ter 43 colunas no parquet silver
- a divergencia principal deixou de ser de schema/colunas na visao canonica do comparador; no estado atual do piloto, a divergencia residual de `c176_xml` ficou concentrada em contagem de registros frente ao projeto externo
- a paridade externa da ST agora e avaliada em duas visoes no comparador:
  - `shape_bruto_local`, que preserva o contrato operacional do `audit_react`
  - `shape_canonico_local`, que aproxima o contrato externo para homologacao sem empobrecer o dado persistido

## Saida Gerada

```text
storage/CNPJ/{cnpj}/silver/c176_xml.parquet
```
