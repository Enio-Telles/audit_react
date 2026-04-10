# Catálogo de Datasets e Parquet

Objetivo: definir os datasets canônicos que servirão de contrato entre extração SQL, consolidação em Parquet, API e frontend.

## EFD
Datasets sugeridos:
- efd_arquivos_validos
- efd_resumo_bloco
- efd_c100
- efd_c170
- efd_c176
- efd_c197
- efd_e110
- efd_h005
- efd_h010
- efd_k200

## Documentos Fiscais
Datasets sugeridos:
- docs_nfe_itens
- docs_nfce_itens
- docs_cte
- docs_nfe_info_complementar
- docs_nfe_contatos
- docs_resumo

## Fiscalização
Datasets sugeridos:
- fiscalizacao_fronteira
- fiscalizacao_fisconforme_chaves
- fiscalizacao_malhas
- fiscalizacao_resolucoes
- fiscalizacao_resumo

## Análise
Datasets sugeridos:
- cross_efd_docs
- cross_efd_xml
- cross_efd_fronteira
- cross_sped_xml_sitafe
- verificacoes_agregacao
- verificacoes_conversao
- verificacoes_integridade
- produtos_catalogo_mestre
- produtos_classificacao_pendente
- produtos_conflitos

## Metadata obrigatória
- dataset_id
- dataset_granularidade
- cnpj_base
- periodo_inicio
- periodo_fim
- origem_dado
- sql_id_origem
- tabela_origem
- estrategia_execucao
- gerado_em
- cache_key
- qtd_linhas
