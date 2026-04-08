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

## Observacao importante

Se houver reorganizacao fisica de pastas, mas sem alteracao funcional, a migracao precisa ser documentada antes. Sem isso, a verificacao deve tratar diferenca de caminho como divergencia.
