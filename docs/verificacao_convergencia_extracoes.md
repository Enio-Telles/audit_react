# Verificacao de Convergencia de Extracoes por CNPJ

Este documento define o procedimento para verificar convergencia total entre o estado atual da extracao e o estado posterior as mudancas do plano.

## Objetivo

Garantir que, para o mesmo CNPJ e o mesmo conjunto de entrada, o resultado "depois" preserve integralmente o inventario esperado do "antes", salvo mudanca explicitamente aprovada.

## Regra de comparacao

- O "antes" e a extracao atual existente no workspace.
- O "depois" sera a extracao gerada apos as mudancas implementadas no plano.
- A convergencia deve ser total para os mesmos CNPJs verificados.

## CNPJs de referencia

- `37671507000187`
- `84654326000394`

## Criterios minimos de convergencia

- Mesmo conjunto de arquivos Parquet esperados por caminho relativo, salvo migracao explicitamente aprovada.
- Mesma quantidade de linhas por arquivo equivalente.
- Mesmo conjunto de colunas e mesmo schema por arquivo equivalente.
- Nenhuma perda silenciosa de tabela gerada.

## Ferramenta de verificacao

Script:
- [verificar_convergencia_extracoes.py](C:/Sistema_react/scripts/verificar_convergencia_extracoes.py)

Exemplo para salvar o baseline atual:

```powershell
python scripts/verificar_convergencia_extracoes.py 37671507000187 84654326000394 --salvar-baseline output/verificacao_convergencia/baseline_extracao_atual.json
```

Exemplo para comparar o "depois" contra o baseline:

```powershell
python scripts/verificar_convergencia_extracoes.py 37671507000187 84654326000394 --comparar-com output/verificacao_convergencia/baseline_extracao_atual.json --salvar-baseline output/verificacao_convergencia/baseline_extracao_depois.json
```

Exemplo para comparar o "depois" e gerar automaticamente o relatorio mestre na mesma execucao:

```powershell
python scripts/verificar_convergencia_extracoes.py 37671507000187 84654326000394 --comparar-com output/verificacao_convergencia/baseline_extracao_atual.json --salvar-baseline output/verificacao_convergencia/baseline_extracao_depois.json --gerar-relatorio-mestre
```

Exemplo para gerar o relatorio mestre consolidado, cruzando a comparacao estrutural com os relatorios da secao `contato` por CNPJ:

```powershell
python scripts/gerar_relatorio_mestre_convergencia.py --comparacao-json output/verificacao_convergencia/baseline_extracao_depois_comparacao.json --saida output/verificacao_convergencia/relatorio_mestre_convergencia.md
```

Exemplo para gerar o painel operacional de prioridades da secao `contato`, ordenando os CNPJs com maior divergencia funcional:

```powershell
python scripts/gerar_painel_prioridades_contato.py --comparacao-json output/verificacao_convergencia/baseline_extracao_depois_comparacao.json --saida output/verificacao_convergencia/painel_prioridades_contato.md
```

## Observacao importante

Se houver reorganizacao fisica de pastas, mas sem alteracao funcional, a migracao precisa ser documentada antes. Sem isso, a verificacao deve tratar diferenca de caminho como divergencia.

## Resultado Executado em 2026-04-08

Arquivos gerados:

- `output/verificacao_convergencia/baseline_extracao_depois.json`
- `output/verificacao_convergencia/baseline_extracao_depois_comparacao.json`
- `output/verificacao_convergencia/relatorio_mestre_convergencia.md`

Resultado observado:

- `37671507000187`: convergencia total
- `84654326000394`: convergencia total
- nenhum arquivo apenas no "antes"
- nenhum arquivo apenas no "depois"
- nenhum arquivo com divergencia de linhas, colunas ou schema

Observacao:

- a partir da materializacao real da secao `contato`, os dois CNPJs passaram a ter relatorio tecnico proprio do Dossie.
- a convergencia estrutural permaneceu total.
- apos a materializacao real de `dossie_filiais_raiz.sql` no caminho Polars e a promocao de `NFe/NFCe` para `shared_sql` atual, a comparacao funcional entre `composicao_polars` e `sql_consolidado` tambem passou a registrar convergencia total nos dois CNPJs.
- os relatorios tecnicos da secao `contato` e o relatorio mestre consolidado ja passaram a registrar `convergencia_funcional` como ultimo status nos CNPJs de referencia.
