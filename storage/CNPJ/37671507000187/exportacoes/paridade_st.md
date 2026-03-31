# Relatorio de Paridade ST - 37671507000187

## Resumo

- status da paridade externa: `divergente`
- artefatos equivalentes: `0/4`
- divergencias: `4`
- pendencias locais: `0`
- pendencias externas: `0`
- cadeia local completa: `True`
- artefatos locais presentes: `8/8`
- artefatos locais vazios: `0`

## Comparacoes Externas

| Artefato | Camada | Registros local | Registros externo | Bruto schema | Bruto colunas | Canonico schema | Canonico colunas | Diff canonico | Divergencia residual |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| extraidos/c176 | bronze | 286 | 270 | nao | nao | sim | sim | 16 | extraidos/c176: contagem divergente apos projecao canonica (+16) |
| extraidos/nfe_dados_st | bronze | 1947 | 1928 | nao | nao | sim | sim | 19 | extraidos/nfe_dados_st: contagem divergente apos projecao canonica (+19) |
| extraidos/e111 | bronze | 78 | 76 | nao | nao | sim | sim | 2 | extraidos/e111: contagem divergente apos projecao canonica (+2) |
| silver/c176_xml | silver | 286 | 270 | nao | nao | sim | sim | 16 | silver/c176_xml: contagem divergente apos projecao canonica (+16) |

## Resumo por Camada

| Camada | Artefatos | Equivalentes | Divergentes | Pendentes locais | Pendentes externos |
| --- | --- | --- | --- | --- | --- |
| bronze | 3 | 0 | 3 | 0 | 0 |
| silver | 1 | 0 | 1 | 0 | 0 |

## Cadeia Local

| Artefato | Existe | Registros | Colunas |
| --- | --- | --- | --- |
| extraidos/c176 | sim | 286 | 27 |
| extraidos/nfe_dados_st | sim | 1947 | 23 |
| extraidos/e111 | sim | 78 | 8 |
| silver/c176_xml | sim | 286 | 43 |
| silver/nfe_dados_st | sim | 1947 | 24 |
| silver/e111_ajustes | sim | 78 | 8 |
| parquets/ajustes_e111 | sim | 78 | 9 |
| parquets/st_itens | sim | 1947 | 29 |
