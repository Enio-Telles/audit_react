# Bloco K - Controle da Produção e do Estoque

## Registro K100 - Período de Apuração do ICMS/IPI

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| DT_INI | VARCHAR2 (32) | O | 04 DT_INI | Data inicial das informações contidas no arquivo. | N |
| DT_FIN | VARCHAR2 (32) | O | 05 DT_FIN | Data final das informações contidas no arquivo. | N |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---

## Registro K200 - Estoque Escriturado

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_K100_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| DT_EST | VARCHAR2 (32) | 02 | DT_EST | Data do estoque final | N |
| COD_ITEM | VARCHAR2 (240) | Nova chave de produto fonte |
| QTD | NUMBER (22,17) | 05 | QTD | Quantidade do item | N |
| IND_EST | VARCHAR2 (4) | no campo 06 do registro 0200 –UNID_INV. | A chave deste registro são os campos: DT_EST, COD_ITEM, IND_EST e COD_PART (este, quando houver). | O estoque escriturado informado no Registro K200 deve refletir a quantidade existente na data final do período de | apuração informado no Registro K100, estoque este derivado dos apontamentos de estoque inicial / entrada / produção |
| COD_PART | VARCHAR2 (240) | Preenchido se ind_prop = 1 ou 2 |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---

## Registro K230 - Itens Produzidos

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_K100_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| DT_INI_OP | VARCHAR2 (32) | O | 02 DT_INI_OP | Data de início da ordem de produção | N |
| DT_FIN_OP | VARCHAR2 (32) | OC | 03 DT_FIN_OP | Data de conclusão da ordem de produção | N |
| COD_DOC_OP | VARCHAR2 (120) | Validação do Registro: Quando houver identificação da ordem de produção, a chave deste registro são os campos: | COD_DOC_OP e COD_ITEM. Nos casos em que a ordem de produção não for identificada, o campo chave passa a ser | COD_ITEM. | Nº Campo |
| COD_ITEM | VARCHAR2 (240) | Nova chave de produto fonte |
| QTD_ENC | NUMBER (22,17) | Página 272 de 360 | 06 QTD_ENC | Quantidade de produção acabada | N |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---
