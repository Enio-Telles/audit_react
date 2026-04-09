# Plano de Otimizacao das Extracoes SQL

> **Objetivo**: eliminar sobreposicao de consultas Oracle, reduzir extracoes redundantes e centralizar a materializacao de Parquets em datasets compartilhados reutilizaveis.
>
> **Principio central**: cada tabela Oracle deve ser consultada no maximo 1 vez por CNPJ em cada ciclo de extracao. Toda composicao posterior deve ocorrer em Polars sobre Parquets ja materializados.

---

## Decisoes Confirmadas

| Decisao | Resposta |
|---------|----------|
| TTL dos datasets | Permanente, com refresh apenas quando o usuario solicitar |
| Migracao | Gradual com fallback, preservando caminhos legados |
| `dossie_contato.sql` | Mantida como alternativa ao modo Polars |
| Verificacao | Obrigatoria, com comparacao entre fluxo otimizado e legado |

---

## Fase 1 - Registry Centralizado

> **Status: CONCLUIDA** - 2026-04-09

### Entregaveis

| Artefato | Arquivo | Status |
|----------|---------|--------|
| Dataset Registry | [dataset_registry.py](/c:/Sistema_react/src/utilitarios/dataset_registry.py) | concluido |
| Testes do Registry | [test_dataset_registry.py](/c:/Sistema_react/tests/test_dataset_registry.py) | 50/50 aprovados |
| Script de convergencia | [verificar_convergencia_otimizacao.py](/c:/Sistema_react/scripts/verificar_convergencia_otimizacao.py) | pronto |

### O que foi implementado

- Catalogo inicial com datasets por CNPJ e dimensoes globais.
- Resolucao canonica em `shared_sql/` com fallback para caminhos legados.
- API de localizacao, carregamento lazy, registro e diagnostico.
- Metadata sidecar `.metadata.json` para auditoria.
- Script de verificacao de convergencia entre caminho canonico e legado.

---

## Fase 2 - Unificacao de Caminhos

> **Status: CONCLUIDA** - 2026-04-09

### Objetivo

Migrar os motores de extracao para gravar em `shared_sql/` via registry, preservando fallback legado.

### Tarefas concluídas

- `extracao_oracle_eficiente.py` passou a registrar datasets no registry apos cada extracao.
- `dossie_dataset_reuse.py` passou a consultar o registry como prioridade 1.
- Fisconforme passou a reaproveitar dimensoes fixas e dados cadastrais do registry quando disponiveis.
- Suite de integracao inicial aprovada.

### Arquivos principais

- [extracao_oracle_eficiente.py](/c:/Sistema_react/src/extracao/extracao_oracle_eficiente.py)
- [dossie_dataset_reuse.py](/c:/Sistema_react/src/interface_grafica/services/dossie_dataset_reuse.py)
- [extrator_oracle.py](/c:/Sistema_react/src/interface_grafica/fisconforme/extrator_oracle.py)

---

## Fase 3 - Composicao Polars

> **Status: EM ANDAMENTO** - 2026-04-09

### Objetivo

Substituir consultas Oracle redundantes por composicoes Polars sobre datasets ja materializados.

### Tarefas

- [x] `[3a]` `dif_ICMS_NFe_EFD.sql` -> composicao Polars
  Observacoes:
  Replica filtros temporais, status autorizados, uso de `seq_nitem = 1` e comparacao com saidas da EFD.
  Materializa `dif_icms_nfe_efd` no registry com metadata de datasets e tabelas de origem.
  Coberta por teste unitario.

- [x] `[3b]` `fronteira.sql` -> composicao Polars sobre `nfe_base` + `sitafe_calculo_item`
  Observacoes:
  Replica o `CASE` de `tipo_operacao` e o corte por `data_limite_processamento`.
  Materializa `composicao_fronteira` no registry com metadata auditavel.
  Coberta por teste unitario.

- [x] `[3c]` Avaliar `dossie_enderecos.sql` como composicao de `nfe_base`
  Observacoes:
  A parte historica de NF-e e reproduzivel em Polars.
  A linha oficial de `DM_PESSOA` ainda nao pode ser recomposta com convergencia total a partir do dataset `cadastral` atual, porque o parquet compartilhado nao expõe `desc_endereco`, `bairro` e `nu_cep` de forma separada.
  Decisao conservadora: manter fallback Oracle enquanto a granularidade cadastral nao for ampliada.
  Coberta por teste unitario que garante a nao ativacao da composicao sem esses campos.

### Situacao atual

- `dif_ICMS_NFe_EFD.sql` ja pode ser atendida por composicao local no registry.
- `fronteira.sql` ja pode ser atendida por composicao local no registry.
- `dossie_enderecos.sql` segue em fallback Oracle por restricao de granularidade do dataset cadastral compartilhado.
- A dependencia `sitafe_calculo_item` passou a ser materializavel de forma pontual pelo SQL `shared_sql/sitafe_nfe_calculo_item.sql`.
- O script [materializar_fase3_datasets.py](/c:/Sistema_react/scripts/materializar_fase3_datasets.py) consolida a materializacao operacional da fase.

### Arquivos alterados na fase

- [composicao_dif_icms.py](/c:/Sistema_react/src/transformacao/composicao_dif_icms.py)
- [composicao_fronteira.py](/c:/Sistema_react/src/transformacao/composicao_fronteira.py)
- [composicao_enderecos.py](/c:/Sistema_react/src/transformacao/composicao_enderecos.py)
- [test_composicao_polars_extracoes.py](/c:/Sistema_react/tests/test_composicao_polars_extracoes.py)
- [test_dossie_dataset_reuse.py](/c:/Sistema_react/tests/test_dossie_dataset_reuse.py)

---

## Fase 4 - Verificacao de Convergencia e Governanca

> **Status: CONCLUIDA** - 2026-04-09

### Objetivo

Validar que as otimizacoes produzem resultados equivalentes aos fluxos legados.

### Tarefas

- [x] Executar `verificar_convergencia_otimizacao.py` para CNPJ `37671507000187`.
- [x] Executar `verificar_convergencia_otimizacao.py` para CNPJ `84654326000394`.
- [x] Medir reducao efetiva de consultas Oracle antes x depois.
- [x] Atualizar este documento com os resultados consolidados da convergencia.

### Resultado observado em 2026-04-09

#### CNPJ `37671507000187`

- Resultado final apos rematerializacao com teto padrao na data atual: `16 CONVERGE`, `3 APENAS_CANONICO`, `0 AUSENTE`, `0 DIVERGE`.
- `APENAS_CANONICO`: `dif_icms_nfe_efd`, `composicao_enderecos` e `composicao_fronteira`.

#### CNPJ `84654326000394`

- Resultado final apos rematerializacao com teto padrao na data atual: `16 CONVERGE`, `3 APENAS_CANONICO`, `0 AUSENTE`, `0 DIVERGE`.
- `APENAS_CANONICO`: `dif_icms_nfe_efd`, `composicao_enderecos` e `composicao_fronteira`.

### Leitura tecnica

- As verificacoes iniciais estavam bloqueadas principalmente porque os Parquets ainda existiam apenas em caminhos legados.
- Foi executado backfill seguro para `shared_sql/` usando os Parquets legados ja existentes, sem reconsulta Oracle.
- A convergencia observada ate aqui indica ausencia de regressao nos datasets efetivamente comparados.
- Os status `AUSENTE` refletem datasets ainda nao materializados no workspace atual, e nao divergencia entre legado e canonico.
- `composicao_fronteira` deixou de estar bloqueada depois da materializacao de `sitafe_calculo_item` para os CNPJs validados.
- `composicao_enderecos` deixou de depender do fallback Oracle para os CNPJs homologados depois da ampliacao do dataset `cadastral` com os campos detalhados de endereco.
- O teto temporal padrao das extracoes/composicoes passou a ser a data atual, inclusive na UI desktop, evitando que o campo `Data limite EFD` herde datas futuras da ultima EFD encontrada.
- Foi necessario corrigir um desvio operacional: a extracao Oracle sem `pasta_saida_base` estava gravando no diretorio do CNPJ, e nao em `arquivos_parquet/`.
- Tambem foi corrigida a prioridade do fallback legado de `nfe_base`/`nfce_base`, para comparar primeiro o bruto (`NFe_`/`NFCe_`) antes dos agregados (`nfe_agr_`/`nfce_agr_`).
- O dataset `efd_c190` foi finalmente materializado para `37671507000187`, eliminando o ultimo `AUSENTE` do conjunto homologado.
- Depois desses ajustes e da rematerializacao dos legados no mesmo recorte temporal, a convergencia voltou a `100%` para os pares canonico x legado efetivamente comparaveis.
- A medicao operacional do Dossie com cache aquecido ficou acima do criterio minimo de aceite:
  - `37671507000187`: baseline de `20` SQLs contra `4` execucoes Oracle reais (`80.0%` de reducao).
  - `84654326000394`: baseline de `20` SQLs contra `4` execucoes Oracle reais (`80.0%` de reducao).
- O relatorio detalhado foi salvo em [reducao_consultas_oracle.md](/c:/Sistema_react/output/medicao_oracle/reducao_consultas_oracle.md).
- A secao `vistorias` voltou a participar da medicao depois da normalizacao do contrato de `dossie_vistorias.sql`, que passou a expor apenas o indicador textual `RELATORIO_ASSINADO` em vez do binario bruto de assinatura.

### Criterios de aceite

- 100% de convergencia entre extracao otimizada e legada para os cenarios homologados.
- Caminhos legados continuam funcionando como fallback.
- `shared_sql/` permanece como caminho canonico prioritario.
- Reducao de consultas Oracle de pelo menos 40%.

---

## Sobreposicoes Identificadas

| # | Sobreposicao | Status |
|---|-------------|--------|
| S1 | `BI.FATO_NFE_DETALHE` consultada por Pipeline ETL + Dossie + Fronteira | Fase 2/3 |
| S2 | `BI.FATO_NFCE_DETALHE` consultada por Pipeline ETL + Dossie | Fase 2 |
| S3 | `BI.DM_PESSOA` integral no Fisconforme vs filtrada no ETL | Fase 2 |
| S4 | CTE `ARQUIVOS_VALIDOS` duplicada em varias SQLs EFD | Aceito pelo design atual |
| S5 | Join C100 -> C170 repetido em `c170.sql`, `c176.sql` e `c176_v2.sql` | Aceito pelo design atual |
| S6 | `dif_ICMS_NFe_EFD.sql` consulta NFe + NFCe + C100 ja extraidos | Fase 3 concluida |
| S7 | Dimensoes fixas reextraidas a cada Fisconforme | Fase 2 concluida |

---

## Catalogo de Datasets

| Dataset ID | SQL de origem | Tipo | Tabelas Oracle |
|-----------|---------------|------|----------------|
| `nfe_base` | `NFe.sql` | por_cnpj | `BI.FATO_NFE_DETALHE` |
| `nfce_base` | `NFCe.sql` | por_cnpj | `BI.FATO_NFCE_DETALHE` |
| `efd_c100` | `c100.sql` | por_cnpj | `SPED.REG_C100`, `SPED.REG_0000` |
| `efd_c170` | `c170.sql` | por_cnpj | `SPED.REG_C170`, `SPED.REG_C100`, `SPED.REG_0200` |
| `efd_c176` | `c176.sql` | por_cnpj | `SPED.REG_C176`, `SPED.REG_C100`, `SPED.REG_C170` |
| `efd_c176_v2` | `c176_v2.sql` | por_cnpj | `SPED.REG_C176`, `SPED.REG_C100`, `SPED.REG_C170`, `SPED.REG_0200` |
| `efd_c176_mensal` | `c176_mensal.sql` | por_cnpj | `SPED.REG_C176`, `SPED.REG_C100`, `SPED.REG_C170` |
| `efd_c190` | `c190.sql` | por_cnpj | `SPED.REG_C190` |
| `efd_0200` | `reg_0200.sql` | por_cnpj | `SPED.REG_0200` |
| `efd_0190` | `reg_0190.sql` | por_cnpj | `SPED.REG_0190` |
| `efd_0000` | `reg_0000.sql` | por_cnpj | `SPED.REG_0000` |
| `efd_reg_0005` | `reg_0005.sql` | por_cnpj | `SPED.REG_0005` |
| `efd_bloco_h` | `bloco_h.sql` | por_cnpj | `SPED.REG_H010`, `SPED.REG_H005` |
| `efd_e111` | `E111.sql` | por_cnpj | `SPED.REG_E111` |
| `cadastral` | `dados_cadastrais.sql` | por_cnpj | `BI.DM_PESSOA`, `BI.DM_LOCALIDADE`, `BI.DM_REGIME_PAGTO_DESCRICAO` |
| `sitafe_calculo_item` | `shared_sql/sitafe_nfe_calculo_item.sql` | por_cnpj | `SITAFE.SITAFE_NFE_CALCULO_ITEM` |
| `dim_localidade` | integral | dimensao_global | `BI.DM_LOCALIDADE` |
| `dim_regime` | integral | dimensao_global | `BI.DM_REGIME_PAGTO_DESCRICAO` |
| `dim_situacao` | integral | dimensao_global | `BI.DM_SITUACAO_CONTRIBUINTE` |
| `dif_icms_nfe_efd` | `dif_ICMS_NFe_EFD.sql` | por_cnpj | `BI.FATO_NFE_DETALHE`, `BI.FATO_NFCE_DETALHE`, `SPED.REG_C100` |
| `composicao_fronteira` | `fronteira.sql` | por_cnpj | `BI.FATO_NFE_DETALHE`, `SITAFE.SITAFE_NFE_CALCULO_ITEM` |
| `composicao_enderecos` | `dossie_enderecos.sql` | por_cnpj | `BI.DM_PESSOA`, `BI.DM_LOCALIDADE`, `BI.FATO_NFE_DETALHE` |

---

## Historico de Atualizacoes

| Data | Fase | Alteracao |
|------|------|-----------|
| 2026-04-09 | Fase 1 | Criado `dataset_registry.py`, adicionados testes e script de convergencia |
| 2026-04-09 | Fase 2 | `extracao_oracle_eficiente.py` passou a registrar datasets em `shared_sql/`; `dossie_dataset_reuse.py` delegou ao registry; Fisconforme passou a reaproveitar dimensoes e cadastral |
| 2026-04-09 | Fase 3 | Implementadas composicoes Polars para `dif_ICMS_NFe_EFD.sql` e `fronteira.sql`; `dossie_enderecos.sql` mantido em fallback Oracle por falta de campos detalhados no dataset cadastral compartilhado |
| 2026-04-09 | Fase 4 | Executado backfill dos Parquets legados para `shared_sql/` e validada convergencia sem divergencias para os datasets comparaveis dos CNPJs `37671507000187` e `84654326000394` |
| 2026-04-09 | Fase 4 | Materializado `sitafe_calculo_item` via SQL dedicada, destravando `composicao_fronteira` para os CNPJs homologados e elevando a cobertura de convergencia sem introduzir divergencias |
| 2026-04-09 | Fase 4 | Padronizado o limite superior temporal para a data atual nas SQLs/documentos e na UI da extracao, removendo o preenchimento automatico do campo `Data limite EFD` com a ultima entrega da EFD |
| 2026-04-09 | Fase 4 | Rematerializados `nfe_base`, `nfce_base`, `efd_c100`, `sitafe_calculo_item`, `dif_icms_nfe_efd`, `composicao_fronteira` e `composicao_enderecos` para `37671507000187` e `84654326000394`; `composicao_enderecos` passou a existir canonicamente para ambos |
| 2026-04-09 | Fase 4 | Corrigido o destino padrao da extracao Oracle para `arquivos_parquet/` e ajustada a prioridade de fallback legado de `nfe_base`/`nfce_base`, restaurando `100%` de convergencia nos datasets comparaveis |
| 2026-04-09 | Fase 4 | Incluida a materializacao de `c190.sql` no fluxo operacional, fechando o ultimo `AUSENTE` homologado (`efd_c190` para `37671507000187`) |
| 2026-04-09 | Fase 4 | Medida a reducao efetiva de consultas Oracle no Dossie com cache aquecido: `80.0%` para `37671507000187` e `80.0%` para `84654326000394`, com `vistorias` reintegrada na medicao apos a normalizacao do contrato SQL |
