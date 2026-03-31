# Ajustes E111

## Papel da Tabela

A `ajustes_e111` e a tabela gold complementar da fase ST para trilha de apuracao. Ela materializa os ajustes E111 por competencia, preservando codigo, descricao e valor original.

## Origem e Dependencias

- Origem principal: `silver/e111_ajustes.parquet`
- Dependencias do pipeline: nenhuma gold anterior obrigatoria
- Nao altera o contrato das 11 gold do core

## Regras Principais

- carrega a trilha silver de ajustes E111
- deriva o `ano` a partir de `periodo_efd`
- preserva a granularidade por competencia e codigo de ajuste
- nao tenta misturar ajuste de apuracao com regra de estoque

## Campos Criticos

- `periodo_efd`, `ano`
- `cnpj_referencia`
- `codigo_ajuste`
- `descricao_codigo_ajuste`
- `descricao_complementar`
- `valor_ajuste`
- `data_entrega_efd_periodo`, `cod_fin_efd`

## Uso Operacional

- expõe a trilha de apuracao da fase ST para consulta e exportacao
- serve de apoio para comparacao com o projeto externo e futuras visoes tributarias

## Limitacoes e Observacoes

- pode sair vazia legitimamente quando `e111` ainda nao foi extraida localmente
- a tabela existe sem quebrar o core, mas isso nao significa que a paridade externa ja foi concluida
- no piloto `37671507000187`, a trilha local ja foi materializada com dados reais; a divergencia externa atual permanece no bronze de `e111`

## Saida Gerada

```text
storage/CNPJ/{cnpj}/parquets/ajustes_e111.parquet
```
