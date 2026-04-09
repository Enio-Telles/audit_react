# Bloco D - Documentos Fiscais II (Serviços - ICMS)

## Registro D100 - Nota Fiscal de Serviço de Transporte (Código 07) e CT-e (Código 57)

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| IND_OPER | VARCHAR2 (4) | 02 | IND_OPER | Indicador do tipo de operação: | 0 - Aquisição; |
| IND_EMIT | VARCHAR2 (4) | 03 | IND_EMIT | Indicador do emitente do documento fiscal: | 0 - Emissão própria; |
| COD_PART | VARCHAR2 (240) | Preenchido se ind_prop = 1 ou 2 |
| COD_MOD | VARCHAR2 (8) | 05 | COD_MOD | Código do modelo do documento fiscal, | conforme a Tabela 4.1.3 |
| COD_SIT | VARCHAR2 (8) | Escrituração extemporânea de documentos – Os documentos que deveriam ter sido escriturados em períodos anteriores devem | ser registrados na EFD-ICMS/IPI com COD_SIT igual a 1, 3 ou 7. Nestes casos, a data de emissão e a data de entrada ou saída | não devem pertencer ao período da escrituração informado no registro 0000. Observe-se que, quando se tratar de documento | fiscal de saída de produtos ou prestação de serviços, os valores de impostos não serão totalizados no período da EFD-ICMS/IPI, |
| SER | VARCHAR2 (16) | Este documento não pretende contemplar todas as orientações técnicas sobre a elaboração do arquivo digital, cuja | orientação integral sobre sua estrutura e apresentação deve ser buscada no Manual de Orientação, estabelecido pela Nota | Técnica EFD ICMS IPI, conforme Ato COTEPE/ICMS nº 44/18 e alterações, bem como na legislação de cada uma das unidades | federadas e da Receita Federal do Brasil. |
| SUB | VARCHAR2 (12) | 07 | SUB | Subsérie do documento fiscal | N |
| NUM_DOC | VARCHAR2 (36) | mesma combinação de valores dos campos formadores da chave do registro. | A chave do registro B020 é: IND_OPER, COD_PART, COD_MOD, SER, NUM_DOC e DT_DOC | Nº | Campo |
| CHV_CTE | VARCHAR2 (176) | documentos especificados. | O campo CHV_CTE passa a ser de preenchimento obrigatório a partir de abril de 2012 em todas as situações, exceto para COD_SIT | = 5 (numeração inutilizada). | A partir da vigência do Ajuste SINIEF 28/2021 e 39/2021 (01/12/2021) deixa de ser obrigatória a informação referente |
| DT_DOC | VARCHAR2 (32) | mesma combinação de valores dos campos formadores da chave do registro. | A chave do registro B020 é: IND_OPER, COD_PART, COD_MOD, SER, NUM_DOC e DT_DOC | Nº | Campo |
| DT_A_P | VARCHAR2 (32) | O | 12 DT_A_P | Data da aquisição ou da prestação do serviço | N |
| TP_CT_E | VARCHAR2 (4) | Sem descrição encontrada no PDF. |
| CHV_CTE_REF | VARCHAR2 (176) | OC | 14 CHV_CTE_REF | Chave do Documento Eletrônico Substituído | N |
| VL_DOC | NUMBER (22,38) | 12 | VL_DOC | Valor total do documento fiscal | N |
| VL_DESC | NUMBER (22,38) | 14 | VL_DESC | Valor total do desconto | N |
| IND_FRT | VARCHAR2 (4) | 17 | IND_FRT | Indicador do tipo do frete: | 0 - Por conta de terceiros; |
| VL_SERV | NUMBER (22,38) | 9 - Sem cobrança de frete. | 18 VL_SERV | Valor total da prestação de serviço | N |
| VL_BC_ICMS | NUMBER (22,38) | 21 | VL_BC_ICMS | Valor da base de cálculo do ICMS | N |
| VL_ICMS | NUMBER (22,38) | 22 | VL_ICMS | Valor do ICMS | N |
| VL_NT | NUMBER (22,38) | 14 | VL_NT | Valor das saídas sob não-incidência ou não- | tributadas pelo ICMS |
| COD_INF | VARCHAR2 (24) | O | 02 COD_INF Código da informação complementar do documento fiscal. | C | 006 |
| COD_CTA | VARCHAR2 (1020) | 33 | COD_CTA | G130 | 09 |
| COD_MUN_OR | VARCHAR2 (28) | Sem descrição encontrada no PDF. |
| COD_MUN_DEST | VARCHAR2 (28) | 25 | COD_MUN_DEST | REGISTROS INCLUÍDOS NO LEIAUTE A PARTIR DO PERÍODO DE APURAÇÃO DE JANEIRO DE 2018: | Bloco Descrição |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---

## Registro D190 - Registro Analítico dos Documentos (Código 07, 08, 09, 10, 11, 26, 27, 57 e 67)

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_D100_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| CST_ICMS | VARCHAR2 (12) | 10 | CST_ICMS | Código da Situação Tributária referente ao | ICMS, conforme a Tabela indicada no item 4.3.1 |
| CFOP | VARCHAR2 (16) | existentes) e deverão ser preenchidos, quando houver informação a ser prestada. Exemplos: a) Nota fiscal emitida em | substituição ao cupom fiscal – CFOP igual a 5.929 ou 6.929 – (lançamento efetuado em decorrência de emissão de documento | fiscal relativo à operação ou à prestação também registrada em equipamento Emissor de Cupom Fiscal – ECF, exceto para o | contribuinte do Estado do Paraná, que deve efetuar a escrituração de acordo com a regra estabelecida na tabela de código de |
| ALIQ_ICMS | NUMBER (22,17) | 12 | ALIQ_ICMS | Alíquota de ICMS aplicável ao item nas | operações internas |
| VL_OPR | NUMBER (22,17) | Campo 12 (VL_DOC) – Preenchimento: o valor informado neste campo deve corresponder ao valor total da nota fiscal. | Quando houver CBS, IBS ou IS incidentes na operação, o valor deste campo não corresponderá à soma do campo VL_OPR | dos registros C190 (“filhos” deste registro C100), exceto para o exercício 2026, quando não devem ser considerados os valores | de CBS, IBS e IS no cômputo do valor total do documento fiscal. |
| VL_BC_ICMS | NUMBER (22,17) | 21 | VL_BC_ICMS | Valor da base de cálculo do ICMS | N |
| VL_ICMS | NUMBER (22,17) | 22 | VL_ICMS | Valor do ICMS | N |
| VL_RED_BC | NUMBER (22,17) | 10 | VL_RED_BC | Valor não tributado em função da redução da base | de cálculo do ICMS, referente à combinação de |
| COD_OBS | VARCHAR2 (24) | 12 | COD_OBS | C500 | 26 |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---
