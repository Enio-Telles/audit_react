# E111 Ajustes

## Papel da Tabela na Camada Silver

A `e111_ajustes` e a trilha intermediaria de apuracao para os ajustes declarados no `E111`. Ela preserva a competencia, o codigo de ajuste e o valor original antes da consolidacao gold.

## Origem em `extraidos`

- `extraidos/e111.parquet`

## Regras Principais de Normalizacao

- padroniza os ajustes do `E111` em uma linha por competencia e codigo de ajuste
- preserva descricao oficial, descricao complementar e data de entrega da EFD
- converte tipos para o contrato silver usado no pipeline
- mantem `periodo_efd` e `cod_fin_efd` para rastreabilidade da apuracao

## Campos Criticos

- `periodo_efd`
- `cnpj_referencia`
- `codigo_ajuste`
- `descricao_codigo_ajuste`
- `descricao_complementar`
- `valor_ajuste`
- `data_entrega_efd_periodo`
- `cod_fin_efd`

## Tabelas Gold que Consomem a Tabela

- `ajustes_e111`

## Limitacoes e Observacoes

- pode sair vazia legitimamente quando a extracao `e111` nao existir para o CNPJ
- alimenta diretamente a gold `ajustes_e111`, mas nao altera o contrato das 11 gold do core
- a trilha de apuracao ainda depende de homologacao cruzada com o projeto externo quando houver base local comparavel

## Saida Gerada

```text
storage/CNPJ/{cnpj}/silver/e111_ajustes.parquet
```
