# Bloco H - Inventário Físico

## Registro H010 - Inventário

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_H005_ID | VARCHAR2 (1020) | Chave Estrangeira para o Cabeçalho H005 |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| COD_ITEM | VARCHAR2 (240) | Nova chave de produto fonte |
| UNID | VARCHAR2 (24) | 05 | UNID | G140 | 06 |
| QTD | NUMBER (22,38) | 05 | QTD | Quantidade do item | N |
| VL_UNIT | NUMBER (22,38) | O | 05 VL_UNIT | Valor unitário do item | N |
| VL_ITEM | NUMBER (22,38) | Campo 16 (VL_MERC) - Validação: se o campo COD_MOD for diferente de “55”, campo IND_EMIT for diferente de “0” | e o campo COD_SIT for igual a “00” ou “01”, o valor informado no campo deve ser igual à soma do campo VL_ITEM dos | registros C170 (“filhos” deste registro C100). | Campo 17 (IND_FRT) - Valores válidos: [0, 1, 2, 9] |
| IND_PROP | VARCHAR2 (4) | 0=Próprio em poder, 1=Próprio em terceiros, 2=Terceiros em poder |
| COD_PART | VARCHAR2 (240) | Preenchido se ind_prop = 1 ou 2 |
| TXT_COMPL | VARCHAR2 (1020) | Descrição complementar (se houver) |
| COD_CTA | VARCHAR2 (1020) | 33 | COD_CTA | G130 | 09 |
| VL_ITEM_IR | NUMBER (22,38) | 11 | VL_ITEM_IR | SUBSEÇÃO 5 – ALTERAÇÕES NO LEIAUTE 2016 | REGISTROS INCLUÍDOS NO LEIAUTE A PARTIR DO PERÍODO DE APURAÇÃO DE JANEIRO DE 2016. |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---
