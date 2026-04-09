# Bloco E - Apuração do ICMS e do IPI

## Registro E110 - Apuração do ICMS - Operações Próprias

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_E100_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| VL_TOT_DEBITOS | NUMBER (22,17) | O | 02 VL_TOT_DEBITOS | Valor total dos débitos por "Saídas e prestações com | débito do imposto" |
| VL_AJ_DEBITOS | NUMBER (22,17) | Os valores de ICMS ou ICMS ST (campo 07-VL_ICMS) serão somados diretamente na apuração, no registro E110 – | Apuração do ICMS – Operações Próprias, campo VL_AJ_DEBITOS ou campo VL_AJ_CREDITOS, e no registro E210 – | Apuração do ICMS – Substituição Tributária, campo VL_AJ_CREDITOS_ST e campo VL_AJ_DEBITOS_ST, de acordo com | a especificação do TERCEIRO CARACTERE do Código do Ajuste (Tabela 5.3 -Tabela de Ajustes e Valores provenientes do |
| VL_TOT_AJ_DEBITOS | NUMBER (22,17) | O | 04 VL_TOT_AJ_DEBITOS | Valor total de "Ajustes a débito" | N |
| VL_ESTORNOS_CRED | NUMBER (22,17) | O | 05 VL_ESTORNOS_CRED | Valor total de Ajustes “Estornos de créditos” | N |
| VL_TOT_CREDITOS | NUMBER (22,17) | O | 06 VL_TOT_CREDITOS | Valor total dos créditos por "Entradas e aquisições com | crédito do imposto" |
| VL_AJ_CREDITOS | NUMBER (22,17) | Os valores de ICMS ou ICMS ST (campo 07-VL_ICMS) serão somados diretamente na apuração, no registro E110, | campo VL_AJ_DEBITOS, campo VL_AJ_CREDITOS ou no campo DEB_ESP e no registro E210, campo | VL_AJ_CREDITOS_ST, campo VL_AJ_DEBITOS_ST ou no campo DEB_ESP_ST, de acordo com a especificação do | TERCEIRO CARACTERE do Código do Ajuste (Tabela 5.3 da Nota Técnica, instituída pelo Ato COTEPE/ICMS nº 44/2018 |
| VL_TOT_AJ_CREDITOS | NUMBER (22,17) | O | 08 VL_TOT_AJ_CREDITOS | Valor total de "Ajustes a crédito" | N |
| VL_ESTORNOS_DEB | NUMBER (22,17) | O | 09 VL_ESTORNOS_DEB | Valor total de Ajustes “Estornos de Débitos” | N |
| VL_SLD_CREDOR_ANT | NUMBER (22,17) | O | 10 VL_SLD_CREDOR_ANT | Valor total de "Saldo credor do período anterior" | N |
| VL_SLD_APURADO | NUMBER (22,17) | O | 11 VL_SLD_APURADO | Valor do saldo devedor apurado | N |
| VL_TOT_DED | NUMBER (22,17) | O | 12 VL_TOT_DED | Valor total de "Deduções" | N |
| VL_ICMS_RECOLHER | NUMBER (22,17) | O | 13 VL_ICMS_RECOLHER | Valor total de "ICMS a recolher (11-12) | N |
| VL_SLD_CREDOR_TRANSPORTAR | NUMBER (22,17) | Sem descrição encontrada no PDF. |
| DEB_ESP | NUMBER (22,17) | Os valores de ICMS ou ICMS ST (campo 07-VL_ICMS) serão somados diretamente na apuração, no registro E110, | campo VL_AJ_DEBITOS, campo VL_AJ_CREDITOS ou no campo DEB_ESP e no registro E210, campo | VL_AJ_CREDITOS_ST, campo VL_AJ_DEBITOS_ST ou no campo DEB_ESP_ST, de acordo com a especificação do | TERCEIRO CARACTERE do Código do Ajuste (Tabela 5.3 da Nota Técnica, instituída pelo Ato COTEPE/ICMS nº 44/2018 |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---

## Registro E210 - Apuração do ICMS - Substituição Tributária

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_E200_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| IND_MOV_ST | VARCHAR2 (4) | O | 02 IND_MOV_ST | Indicador de movimento: | 0 – Sem operações com ST |
| VL_SLD_CRED_ANT_ST | NUMBER (22,17) | O | 03 VL_SLD_CRED_ANT_ST | Valor do "Saldo credor de período anterior – | Substituição Tributária" |
| VL_DEVOL_ST | NUMBER (22,17) | O | 04 VL_DEVOL_ST | Valor total do ICMS ST de devolução de mercadorias | N |
| VL_RESSARC_ST | NUMBER (22,17) | O | 05 VL_RESSARC_ST | Valor total do ICMS ST de ressarcimentos | N |
| VL_OUT_CRED_ST | NUMBER (22,17) | O | 06 VL_OUT_CRED_ST | Valor total de Ajustes "Outros créditos ST" e | “Estorno de débitos ST” |
| VL_AJ_CREDITOS_ST | NUMBER (22,17) | Apuração do ICMS – Operações Próprias, campo VL_AJ_DEBITOS ou campo VL_AJ_CREDITOS, e no registro E210 – | Apuração do ICMS – Substituição Tributária, campo VL_AJ_CREDITOS_ST e campo VL_AJ_DEBITOS_ST, de acordo com | a especificação do TERCEIRO CARACTERE do Código do Ajuste (Tabela 5.3 -Tabela de Ajustes e Valores provenientes do | Documento Fiscal). |
| VL_RETENCAO_ST | NUMBER (22,17) | Sem descrição encontrada no PDF. |
| VL_OUT_DEB_ST | NUMBER (22,17) | O | 09 VL_OUT_DEB_ST | Valor Total dos ajustes "Outros débitos ST" " e | “Estorno de créditos ST” |
| VL_AJ_DEBITOS_ST | NUMBER (22,17) | campo VL_AJ_DEBITOS, campo VL_AJ_CREDITOS ou no campo DEB_ESP e no registro E210, campo | VL_AJ_CREDITOS_ST, campo VL_AJ_DEBITOS_ST ou no campo DEB_ESP_ST, de acordo com a especificação do | TERCEIRO CARACTERE do Código do Ajuste (Tabela 5.3 da Nota Técnica, instituída pelo Ato COTEPE/ICMS nº 44/2018 | e alterações). |
| VL_SLD_DEV_ANT_ST | NUMBER (22,17) | O | 11 VL_SLD_DEV_ANT_ST | Valor total de Saldo devedor antes das deduções | N |
| VL_DEDUCOES_ST | NUMBER (22,17) | Sem descrição encontrada no PDF. |
| VL_ICMS_RECOL_ST | NUMBER (22,17) | Página 231 de 360 | 13 VL_ICMS_RECOL_ST | Imposto a recolher ST (11-12) | N |
| VL_SLD_CRED_ST_TRANSPORTAR | NUMBER (22,17) | Sem descrição encontrada no PDF. |
| DEB_ESP_ST | NUMBER (22,17) | O | 15 DEB_ESP_ST | Valores recolhidos ou a recolher, extra-apuração. | N |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---
