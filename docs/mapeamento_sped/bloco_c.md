# Bloco C - Documentos Fiscais I (Mercadorias - ICMS/IPI)

## Registro C100 - Nota Fiscal (Código 01), Nota Fiscal Avulsa (Código 1B), Nota Fiscal de Produtor (Código 04), NF-e (Código 55) e NFC-e (Código 65)

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
| SER | VARCHAR2 (12) | Este documento não pretende contemplar todas as orientações técnicas sobre a elaboração do arquivo digital, cuja | orientação integral sobre sua estrutura e apresentação deve ser buscada no Manual de Orientação, estabelecido pela Nota | Técnica EFD ICMS IPI, conforme Ato COTEPE/ICMS nº 44/18 e alterações, bem como na legislação de cada uma das unidades | federadas e da Receita Federal do Brasil. |
| NUM_DOC | VARCHAR2 (36) | mesma combinação de valores dos campos formadores da chave do registro. | A chave do registro B020 é: IND_OPER, COD_PART, COD_MOD, SER, NUM_DOC e DT_DOC | Nº | Campo |
| CHV_NFE | VARCHAR2 (176) | 09 | CHV_NFE | Chave da Nota Fiscal Eletrônica | N |
| DT_DOC | VARCHAR2 (32) | mesma combinação de valores dos campos formadores da chave do registro. | A chave do registro B020 é: IND_OPER, COD_PART, COD_MOD, SER, NUM_DOC e DT_DOC | Nº | Campo |
| DT_E_S | VARCHAR2 (1020) | 11 | DT_E_S | Data da entrada ou da saída | N |
| VL_DOC | NUMBER (22,38) | 12 | VL_DOC | Valor total do documento fiscal | N |
| IND_PGTO | VARCHAR2 (4) | 13 | IND_PGTO | Indicador do tipo de pagamento: | 0 - À vista; |
| VL_DESC | NUMBER (22,38) | 14 | VL_DESC | Valor total do desconto | N |
| VL_ABAT_NT | NUMBER (22,38) | 38 | VL_ABAT_NT | SUBSEÇÃO 9 – ALTERAÇÕES NO LEIAUTE 2020 | No leiaute estabelecido na Nota Técnica, conforme Ato COTEPE/ICMS nº 44/2018 e alterações, foram inseridos os seguintes |
| VL_MERC | NUMBER (22,38) | 16 | VL_MERC | Valor total das mercadorias e serviços | N |
| IND_FRT | VARCHAR2 (4) | 17 | IND_FRT | Indicador do tipo do frete: | 0 - Por conta de terceiros; |
| VL_FRT | NUMBER (22,38) | 18 | VL_FRT | Valor do frete indicado no documento fiscal | N |
| VL_SEG | NUMBER (22,38) | 19 | VL_SEG | Valor do seguro indicado no documento fiscal | N |
| VL_OUT_DA | NUMBER (22,38) | 20 | VL_OUT_DA | Valor de outras despesas acessórias | N |
| VL_BC_ICMS | NUMBER (22,38) | 21 | VL_BC_ICMS | Valor da base de cálculo do ICMS | N |
| VL_ICMS | NUMBER (22,38) | 22 | VL_ICMS | Valor do ICMS | N |
| VL_BC_ICMS_ST | NUMBER (22,38) | 23 | VL_BC_ICMS_ST Valor da base de cálculo do ICMS substituição | tributária | N |
| VL_ICMS_ST | NUMBER (22,38) | 24 | VL_ICMS_ST | Valor do ICMS retido por substituição | tributária |
| VL_IPI | NUMBER (22,38) | 25 | VL_IPI | Valor total do IPI | N |
| VL_PIS | NUMBER (22,38) | 26 | VL_PIS | Valor total do PIS | N |
| VL_COFINS | NUMBER (22,38) | 27 | VL_COFINS | Valor total da COFINS | N |
| VL_PIS_ST | NUMBER (22,38) | C197. No registro C100, não devem ser informados os campos COD_PART, VL_BC_ICMS_ST, VL_ICMS_ST, VL_IPI, | VL_PIS, VL_COFINS, VL_PIS_ST e VL_COFINS_ST. Os demais campos seguirão a obrigatoriedade definida pelo registro. | As NFC-e não devem ser escrituradas nas entradas. A partir de janeiro de 2020, também poderá ser informado o Registro | C185, a critério de cada UF. |
| VL_COFINS_ST | NUMBER (22,38) | 29 | VL_COFINS_ST | Valor total da COFINS retido por substituição | tributária |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---

## Registro C113 - Documento Fiscal Referenciado

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_C100_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_C110_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| IND_OPER | VARCHAR2 (4) | 02 | IND_OPER | Indicador do tipo de operação: | 0 - Aquisição; |
| IND_EMIT | VARCHAR2 (4) | 03 | IND_EMIT | Indicador do emitente do documento fiscal: | 0 - Emissão própria; |
| COD_PART | VARCHAR2 (240) | Preenchido se ind_prop = 1 ou 2 |
| COD_MOD | VARCHAR2 (8) | 05 | COD_MOD | Código do modelo do documento fiscal, | conforme a Tabela 4.1.3 |
| SER | VARCHAR2 (16) | Este documento não pretende contemplar todas as orientações técnicas sobre a elaboração do arquivo digital, cuja | orientação integral sobre sua estrutura e apresentação deve ser buscada no Manual de Orientação, estabelecido pela Nota | Técnica EFD ICMS IPI, conforme Ato COTEPE/ICMS nº 44/18 e alterações, bem como na legislação de cada uma das unidades | federadas e da Receita Federal do Brasil. |
| SUB | VARCHAR2 (12) | 07 | SUB | Subsérie do documento fiscal | N |
| NUM_DOC | VARCHAR2 (36) | mesma combinação de valores dos campos formadores da chave do registro. | A chave do registro B020 é: IND_OPER, COD_PART, COD_MOD, SER, NUM_DOC e DT_DOC | Nº | Campo |
| DT_DOC | VARCHAR2 (32) | mesma combinação de valores dos campos formadores da chave do registro. | A chave do registro B020 é: IND_OPER, COD_PART, COD_MOD, SER, NUM_DOC e DT_DOC | Nº | Campo |
| CHV_DOC_E | VARCHAR2 (176) | Sem descrição encontrada no PDF. |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---

## Registro C170 - Itens do Documento (Código 01, 1B, 04 e 55)

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_C100_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| NUM_ITEM | VARCHAR2 (12) | obrigatório e deve ser preenchido. Os demais campos e registros filhos do registro C100 serão informados, quando houver | informação a ser prestada. Se for informado o registro C170 o campo NUM_ITEM deve ser preenchido. | Exceção 4: Notas Fiscais emitidas por regime especial ou norma específica (campo COD_SIT igual a “08”). Para documentos | fiscais emitidos com base em regime especial ou norma específica, deverão ser apresentados os registros C100 e C190, |
| COD_ITEM | VARCHAR2 (240) | Nova chave de produto fonte |
| DESCR_COMPL | VARCHAR2 (1020) | 04 | DESCR_COMPL | Descrição complementar do item como adotado | no documento fiscal |
| QTD | NUMBER (22,17) | 05 | QTD | Quantidade do item | N |
| UNID | VARCHAR2 (24) | 05 | UNID | G140 | 06 |
| VL_ITEM | NUMBER (22,17) | Campo 16 (VL_MERC) - Validação: se o campo COD_MOD for diferente de “55”, campo IND_EMIT for diferente de “0” | e o campo COD_SIT for igual a “00” ou “01”, o valor informado no campo deve ser igual à soma do campo VL_ITEM dos | registros C170 (“filhos” deste registro C100). | Campo 17 (IND_FRT) - Valores válidos: [0, 1, 2, 9] |
| VL_DESC | NUMBER (22,17) | 14 | VL_DESC | Valor total do desconto | N |
| IND_MOV | VARCHAR2 (4) | 02 | IND_MOV | Indicador de movimento: | 0- Bloco com dados informados; |
| CST_ICMS | VARCHAR2 (12) | 10 | CST_ICMS | Código da Situação Tributária referente ao | ICMS, conforme a Tabela indicada no item 4.3.1 |
| CFOP | VARCHAR2 (16) | existentes) e deverão ser preenchidos, quando houver informação a ser prestada. Exemplos: a) Nota fiscal emitida em | substituição ao cupom fiscal – CFOP igual a 5.929 ou 6.929 – (lançamento efetuado em decorrência de emissão de documento | fiscal relativo à operação ou à prestação também registrada em equipamento Emissor de Cupom Fiscal – ECF, exceto para o | contribuinte do Estado do Paraná, que deve efetuar a escrituração de acordo com a regra estabelecida na tabela de código de |
| COD_NAT | VARCHAR2 (40) | 02 | COD_NAT | Código da natureza da operação/prestação | C |
| VL_BC_ICMS | NUMBER (22,17) | 21 | VL_BC_ICMS | Valor da base de cálculo do ICMS | N |
| ALIQ_ICMS | NUMBER (22,17) | 12 | ALIQ_ICMS | Alíquota de ICMS aplicável ao item nas | operações internas |
| VL_ICMS | NUMBER (22,17) | 22 | VL_ICMS | Valor do ICMS | N |
| VL_BC_ICMS_ST | NUMBER (22,17) | 23 | VL_BC_ICMS_ST Valor da base de cálculo do ICMS substituição | tributária | N |
| ALIQ_ST | NUMBER (22,17) | 17 | ALIQ_ST | Alíquota do ICMS da substituição tributária na | unidade da federação de destino |
| VL_ICMS_ST | NUMBER (22,17) | 24 | VL_ICMS_ST | Valor do ICMS retido por substituição | tributária |
| IND_APUR | VARCHAR2 (4) | 19 | IND_APUR | Indicador de período de apuração do IPI: | 0 - Mensal; |
| CST_IPI | VARCHAR2 (8) | 20 | CST_IPI | Código da Situação Tributária referente ao IPI, | conforme a Tabela indicada no item 4.3.2. |
| COD_ENQ | VARCHAR2 (12) | 21 | COD_ENQ | Código de enquadramento legal do IPI, | conforme tabela indicada no item 4.5.3. |
| VL_BC_IPI | NUMBER (22,17) | 22 | VL_BC_IPI | Valor da base de cálculo do IPI | N |
| ALIQ_IPI | NUMBER (22,17) | 23 | ALIQ_IPI | Alíquota do IPI | N |
| VL_IPI | NUMBER (22,17) | 25 | VL_IPI | Valor total do IPI | N |
| CST_PIS | VARCHAR2 (8) | 25 | CST_PIS | Código da Situação Tributária referente ao PIS. | N |
| VL_BC_PIS | NUMBER (22,17) | 26 | VL_BC_PIS | Valor da base de cálculo do PIS | N |
| ALIQ_PIS_P | NUMBER (22,17) | Sem descrição encontrada no PDF. |
| QUANT_BC_PIS | NUMBER (22,17) | 28 | QUANT_BC_PIS | Quantidade – Base de cálculo PIS | N |
| ALIQ_PIS | NUMBER (22,17) | 27 | ALIQ_PIS | Alíquota do PIS (em percentual) | N |
| VL_PIS | NUMBER (22,17) | 26 | VL_PIS | Valor total do PIS | N |
| CST_COFINS | VARCHAR2 (8) | 31 | CST_COFINS | Código da Situação Tributária referente ao | COFINS. |
| VL_BC_COFINS | NUMBER (22,17) | 32 | VL_BC_COFINS | Valor da base de cálculo da COFINS | N |
| ALIQ_COFINS_P | NUMBER (22,17) | Sem descrição encontrada no PDF. |
| QUANT_BC_COFINS | NUMBER (22,17) | Sem descrição encontrada no PDF. |
| ALIQ_COFINS | NUMBER (22,17) | 33 | ALIQ_COFINS | Alíquota do COFINS (em percentual) | N |
| VL_COFINS | NUMBER (22,17) | 27 | VL_COFINS | Valor total da COFINS | N |
| COD_CTA | VARCHAR2 (1020) | 33 | COD_CTA | G130 | 09 |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| VL_ABAT_NT | NUMBER (22,17) | 38 | VL_ABAT_NT | SUBSEÇÃO 9 – ALTERAÇÕES NO LEIAUTE 2020 | No leiaute estabelecido na Nota Técnica, conforme Ato COTEPE/ICMS nº 44/2018 e alterações, foram inseridos os seguintes |

---

## Registro C176 - Ressarcimento de ICMS e Fundo de Combate à Pobreza (FCP) em Operações com Substituição Tributária (Código 01, 55)

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_C100_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_C170_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| COD_MOD_ULT_E | VARCHAR2 (8) | O | 02 COD_MOD_ULT_E | Código do modelo do documento fiscal | relativa a última entrada |
| NUM_DOC_ULT_E | VARCHAR2 (36) | O | 03 NUM_DOC_ULT_E | Número do documento fiscal relativa a | última entrada |
| SER_ULT_E | VARCHAR2 (12) | O | 04 SER_ULT_E | Série do documento fiscal relativa a última | entrada |
| DT_ULT_E | VARCHAR2 (32) | OC | 05 DT_ULT_E | Data relativa a última entrada da mercadoria | N |
| COD_PART_ULT_E | VARCHAR2 (240) | O | 06 COD_PART_ULT_E | Código do participante (do emitente do | documento relativa a última entrada) |
| QUANT_ULT_E | NUMBER (22,17) | O | 07 QUANT_ULT_E | Quantidade do item relativa a última entrada | N |
| VL_UNIT_ULT_E | NUMBER (22,17) | O | 08 VL_UNIT_ULT_E | Valor unitário da mercadoria constante na | NF relativa a última entrada inclusive |
| VL_UNIT_BC_ST | NUMBER (22,17) | O | 09 VL_UNIT_BC_ST | Valor unitário da base de cálculo do imposto | pago por substituição. |
| CHAVE_NFE_ULT | VARCHAR2 (176) | Sem descrição encontrada no PDF. |
| NUM_ITEM_ULT_E | VARCHAR2 (12) | 11 | NUM_ITEM_ULT_E | C176 | 12 |
| VL_UNIT_BC_ICMS_ULT_E | NUMBER (22,17) | 12 | VL_UNIT_BC_ICMS_ULT_E | C176 | 13 |
| ALIQ_ICMS_ULT_E | NUMBER (22,17) | 13 | ALIQ_ICMS_ULT_E | C176 | 14 |
| VL_UNIT_LIMITE_BC_ICMS_ULT_E | NUMBER (22,17) | 14 | VL_UNIT_LIMITE_BC_ICMS_ULT_E | C176 | 15 |
| VL_UNIT_ICMS_ULT_E | NUMBER (22,17) | 15 | VL_UNIT_ICMS_ULT_E | C176 | 16 |
| ALIQ_ST_ULT_E | NUMBER (22,38) | 16 | ALIQ_ST_ULT_E | C176 | 17 |
| COD_RESP_RET | VARCHAR2 (4) | 18 | COD_RESP_RET | C176 | 19 |
| COD_MOT_RES | VARCHAR2 (4) | 19 | COD_MOT_RES | C176 | 20 |
| CHAVE_NFE_RET | VARCHAR2 (176) | 20 | CHAVE_NFE_RET | C176 | 21 |
| COD_PART_NFE_R | VARCHAR2 (240) | OC | 21 COD_PART_NFE_R | ET | Código do participante do emitente da NF-e |
| SER_NFE_RET | VARCHAR2 (12) | 22 | SER_NFE_RET | C176 | 23 |
| NUM_NFE_RET | VARCHAR2 (36) | 23 | NUM_NFE_RET | C176 | 24 |
| ITEM_NFE_RET | VARCHAR2 (12) | 24 | ITEM_NFE_RET | C176 | 25 |
| COD_DA | VARCHAR2 (4) | 25 | COD_DA | C176 | 26 |
| NUM_DA | VARCHAR2 (1020) | 26 | NUM_DA | 0200 | 13 |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| VL_UNIT_RES_FCP_ST | NUMBER (22,17) | Sem descrição encontrada no PDF. |
| VL_UNIT_RES | NUMBER (22,17) | 17 | VL_UNIT_RES | C176 | 18 |

---

## Registro C190 - Registro Analítico do Documento (Código 01, 1B, 04, 55 e 65)

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_C100_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| CST_ICMS | VARCHAR2 (12) | 10 | CST_ICMS | Código da Situação Tributária referente ao | ICMS, conforme a Tabela indicada no item 4.3.1 |
| CFOP | VARCHAR2 (16) | existentes) e deverão ser preenchidos, quando houver informação a ser prestada. Exemplos: a) Nota fiscal emitida em | substituição ao cupom fiscal – CFOP igual a 5.929 ou 6.929 – (lançamento efetuado em decorrência de emissão de documento | fiscal relativo à operação ou à prestação também registrada em equipamento Emissor de Cupom Fiscal – ECF, exceto para o | contribuinte do Estado do Paraná, que deve efetuar a escrituração de acordo com a regra estabelecida na tabela de código de |
| ALIQ_ICMS | NUMBER (22,17) | 12 | ALIQ_ICMS | Alíquota de ICMS aplicável ao item nas | operações internas |
| VL_OPR | NUMBER (22,17) | Campo 12 (VL_DOC) – Preenchimento: o valor informado neste campo deve corresponder ao valor total da nota fiscal. | Quando houver CBS, IBS ou IS incidentes na operação, o valor deste campo não corresponderá à soma do campo VL_OPR | dos registros C190 (“filhos” deste registro C100), exceto para o exercício 2026, quando não devem ser considerados os valores | de CBS, IBS e IS no cômputo do valor total do documento fiscal. |
| VL_BC_ICMS | NUMBER (22,17) | 21 | VL_BC_ICMS | Valor da base de cálculo do ICMS | N |
| VL_ICMS | NUMBER (22,17) | 22 | VL_ICMS | Valor do ICMS | N |
| VL_BC_ICMS_ST | NUMBER (22,17) | 23 | VL_BC_ICMS_ST Valor da base de cálculo do ICMS substituição | tributária | N |
| VL_ICMS_ST | NUMBER (22,17) | 24 | VL_ICMS_ST | Valor do ICMS retido por substituição | tributária |
| VL_RED_BC | NUMBER (22,17) | 10 | VL_RED_BC | Valor não tributado em função da redução da base | de cálculo do ICMS, referente à combinação de |
| VL_IPI | NUMBER (22,17) | 25 | VL_IPI | Valor total do IPI | N |
| COD_OBS | VARCHAR2 (24) | 12 | COD_OBS | C500 | 26 |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---
