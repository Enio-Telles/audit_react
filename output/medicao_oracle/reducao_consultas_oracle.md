# Medicao de Reducao de Consultas Oracle

- Gerado em: `2026-04-09T10:11:29.993330`
- Metodo: comparacao entre baseline sem reuso (`total_sql_ids`) e execucao real no estado atual (`sql_ids_executados`).
- Escopo: secoes do Dossie com SQL mapeada; secoes `cache_catalog` ficam fora do baseline por nao demandarem Oracle nesse fluxo.

## CNPJ 37671507000187

- Secoes medidas: `11`
- Baseline de SQLs Oracle: `20`
- SQLs realmente executadas: `4`
- SQLs reutilizadas: `16`
- Reducao efetiva de consultas Oracle: `80.0%`

| Secao | Baseline SQLs | Executadas | Reutilizadas | Reducao | Estrategia |
|---|---:|---:|---:|---:|---|
| `cadastro` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `documentos_fiscais` | 2 | 2 | 0 | 0.0% | `composicao_polars` |
| `enderecos` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `historico_situacao` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `regime_pagamento` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `atividades` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `contador` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `historico_fac` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `vistorias` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `socios` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `contato` | 9 | 2 | 7 | 77.78% | `composicao_polars` |

## CNPJ 84654326000394

- Secoes medidas: `11`
- Baseline de SQLs Oracle: `20`
- SQLs realmente executadas: `4`
- SQLs reutilizadas: `16`
- Reducao efetiva de consultas Oracle: `80.0%`

| Secao | Baseline SQLs | Executadas | Reutilizadas | Reducao | Estrategia |
|---|---:|---:|---:|---:|---|
| `cadastro` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `documentos_fiscais` | 2 | 2 | 0 | 0.0% | `composicao_polars` |
| `enderecos` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `historico_situacao` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `regime_pagamento` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `atividades` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `contador` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `historico_fac` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `vistorias` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `socios` | 1 | 0 | 1 | 100.0% | `sql_direto` |
| `contato` | 9 | 2 | 7 | 77.78% | `composicao_polars` |

