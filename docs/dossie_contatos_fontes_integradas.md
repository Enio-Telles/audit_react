# Dossie de Contatos Integrados

## Objetivo

Documentar as fontes de telefone, email e endereco usadas pela secao `contato` do Dossie, a regra de agregacao por entidade e os pontos ainda mapeados, mas nao materializados no fluxo atual.

## Visualizacao integrada materializada

A UI da secao `contato` passou a consolidar o parquet em tres tabelas auditaveis:

- `Agenda da empresa`
- `Agenda dos socios`
- `Agenda dos contadores`

Cada linha representa uma entidade consolidada e agrega:

- vinculos encontrados
- situacoes atuais ou historicas
- telefones por fonte
- emails por fonte
- enderecos por fonte
- fontes consolidadas
- tabelas Oracle de origem

## Fontes materializadas por grupo

| Grupo      | Camada                | SQL / dataset                       | Tabelas Oracle                                                                                                                       | Campos usados                                                                           |
| ---------- | --------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| Empresa    | principal             | `dados_cadastrais.sql`              | `BI.DM_PESSOA`, `BI.DM_LOCALIDADE`, `BI.DM_REGIME_PAGTO_DESCRICAO`, `SITAFE.SITAFE_HISTORICO_SITUACAO`                               | nome, endereco, situacao cadastral                                                      |
| Empresa    | complementar          | `dossie_historico_fac.sql`          | `SITAFE.SITAFE_HISTORICO_CONTRIBUINTE`, `SITAFE.SITAFE_PESSOA`, `SITAFE.SITAFE_TABELAS_CADASTRO`, `BI.DM_LOCALIDADE`                 | endereco FAC, telefone, email, endereco de correspondencia, telefone de correspondencia |
| Empresa    | filial raiz           | `dossie_filiais_raiz.sql`           | `BI.DM_PESSOA`, `BI.DM_SITUACAO_CONTRIBUINTE`                                                                                        | nome, endereco, situacao, indicador matriz/filial                                       |
| Empresa    | sinal documental      | `NFe.sql`, `NFCe.sql`               | `BI.FATO_NFE_DETALHE`, `BI.FATO_NFCE_DETALHE`                                                                                        | emails observados em documentos fiscais                                                 |
| Socios     | atual e antigo        | `dossie_historico_socios.sql`       | `SITAFE.SITAFE_HISTORICO_SOCIO`, `SITAFE.SITAFE_PESSOA`, `SITAFE.SITAFE_TABELAS_CADASTRO`, `BI.DM_LOCALIDADE`                        | situacao, cpf/cnpj, nome, endereco, telefone, email                                     |
| Contadores | historico de vinculo  | `dossie_contador.sql`               | `SITAFE.SITAFE_HISTORICO_CONTRIBUINTE`, `BI.DM_PESSOA`, `BI.DM_LOCALIDADE`                                                           | situacao atual/anterior, documento, nome, localidade                                    |
| Contadores | contato FAC           | `dossie_historico_fac.sql`          | `SITAFE.SITAFE_HISTORICO_CONTRIBUINTE`, `SITAFE.SITAFE_PESSOA`, `SITAFE.SITAFE_TABELAS_CADASTRO`, `BI.DM_PESSOA`, `BI.DM_LOCALIDADE` | endereco, telefone, email, endereco de correspondencia, telefone de correspondencia     |
| Contadores | fallback rascunho     | `dossie_rascunho_fac_contador.sql`  | `SITAFE.SITAFE_RASCUNHO_FAC`                                                                                                         | nome, crc, endereco, telefone, email                                                    |
| Contadores | fallback requerimento | `dossie_req_inscricao_contador.sql` | `SITAFE.SITAFE_REQ_INSCRICAO`, `BI.DM_LOCALIDADE`                                                                                    | nome, crc, telefone, municipio, uf                                                      |
| Contadores | sinal reconciliado    | `NFe.sql`, `NFCe.sql`               | `BI.FATO_NFE_DETALHE`, `BI.FATO_NFCE_DETALHE`                                                                                        | telefone observado por reconciliacao exata do CPF/CNPJ do contador                      |

## Regras de agregacao

### Empresa

- agrega `EMPRESA_PRINCIPAL`, `EMPRESA_FAC_ATUAL`, `MATRIZ_RAIZ`, `FILIAL_RAIZ` e `EMAIL_NFE`
- consolida por `cpf_cnpj_referencia`
- preserva cada evidencia com `fonte`, `origem_dado` e `tabela_origem`

### Socios

- preserva `SOCIO_ATUAL` e `SOCIO_ANTIGO`
- usa `situacao` do historico como status funcional
- nao descarta socio antigo mesmo sem telefone, email ou endereco

### Contadores

- consolida identidades por documento + nome
- mantem `dossie_historico_fac.sql` como referencia principal quando houver dados mais completos
- preserva a lista de situacoes vindas de `dossie_contador.sql` para indicar atual e anterior
- anexa telefone de `NFe/NFCe` apenas com reconciliacao exata de CPF/CNPJ

## Regras visuais da agenda

- status `confirmado`: mesma entidade com confirmacao em mais de uma fonte ou cobertura completa de telefone, email e endereco
- status `parcial`: entidade com algum contato, mas sem confirmacao forte
- status `divergente`: mais de um valor distinto para telefone, email ou endereco
- status `sem contato`: nenhuma evidencia de telefone, email ou endereco

## Regra de convergencia tecnica

- a comparacao entre `composicao_polars` e `sql_consolidado` da secao `contato` passou a considerar a entidade consolidada, e nao apenas a linha bruta retornada por cada estrategia
- a chave funcional agora agrupa por `tipo_vinculo`, documento de referencia, nome e `crc_contador`, consolidando telefone, telefone de `NFe/NFCe`, email, endereco e `situacao_cadastral`
- esse criterio reduz falso negativo quando a mesma evidencia aparece distribuida em mais de uma fonte Oracle na composicao Polars
- o resumo da comparacao e o relatorio tecnico agora expõem tambem a estrategia de referencia, a SQL principal de referencia e a ultima contagem de chaves faltantes e extras
- o relatorio tecnico passou a listar amostras de chaves faltantes e extras, alem do delta de preenchimento dos campos criticos entre a extracao atual e a estrategia de referencia
- o relatorio mestre de convergencia passou a reaproveitar esses sinais para classificar prioridade operacional da secao `contato` por CNPJ
- foi criado um script dedicado para gerar um painel markdown de prioridades da secao `contato` a partir do JSON de comparacao estrutural e dos relatorios tecnicos por CNPJ

## Fontes mapeadas que seguem como candidatas

O levantamento em `dados/referencias/referencias/mapeamento` mostrou fontes adicionais que ainda nao entram no builder atual:

| Tabela Oracle                 | Evidencia mapeada                                                                                                                 | Observacao                                                  |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| `SITAFE.SITAFE_RASCUNHO_FAC`  | campos da empresa: `IT_NO_LOGRADOURO`, `IT_NO_BAIRRO`, `IT_NU_DDD`, `IT_NU_TELEFONE`, `IT_CO_CORREIO_ELETRONICO`, correspondencia | hoje o fluxo usa apenas os campos do contador               |
| `SITAFE.SITAFE_REQ_INSCRICAO` | campos de telefone, municipio, identificacao e contador                                                                           | hoje o fluxo materializa apenas o bloco do contador         |
| `SITAFE.SITAFE_PESSOA`        | endereco, telefone, email, endereco de correspondencia, CRC                                                                       | hoje aproveitado para FAC, socios e parte do contador       |
| `SITAFE.SIT_CONTADOR_CRC01`   | identificacao e CRC do contador                                                                                                   | mapeado, mas ainda nao usado como fallback adicional        |
| `BI.DM_PESSOA`                | `CO_CNPJ_CPF_CONTADOR`, endereco, bairro, cep                                                                                     | hoje usado no cadastro principal e no historico de contador |

## Riscos e pontos de atencao

- o modo `sql_consolidado`, acionado por `dossie_contato.sql`, foi alinhado parcialmente ao contrato novo:
  - passou a incluir `EMPRESA_FAC_ATUAL`
  - passou a materializar `SOCIO_ANTIGO`
  - passou a propagar `situacoes_vinculo` do contador para `situacao_cadastral`
- ainda assim, a validacao funcional completa desse caminho depende de execucao real contra Oracle e nova rodada de comparacao historica
- as fontes candidatas de empresa em `SITAFE_RASCUNHO_FAC` e `SITAFE_REQ_INSCRICAO` ja estao mapeadas, mas ainda precisam de SQLs dedicados para entrar sem ambiguidade no contrato da secao
