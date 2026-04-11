# Leitura adaptativa de Parquet e Delta no backend

## Objetivo

Este passo fecha a primeira parte operacional da transicao para Delta Lake: nao basta gravar em Delta, o backend precisa conseguir ler automaticamente datasets materializados como diretorio Delta ou arquivo Parquet.

---

## O que foi implementado

### Helper compartilhado

Arquivo:

- `backend/routers/fiscal_storage.py`

Funcoes principais:

- `resolve_materialized_path()`
- `read_materialized_frame()`
- `scan_materialized_frame()`
- `probe_materialized()`

Com isso, quando o backend recebe um caminho como `tb_documentos_123.parquet`, ele tenta:

1. usar o arquivo Parquet se existir;
2. se nao existir, procurar o diretorio Delta `tb_documentos_123`;
3. ler o dataset encontrado no formato correto.

---

## Onde a leitura adaptativa entrou

### 1. `fiscal_summary.py`

- os probes agora consideram Parquet ou Delta;
- o `stage_label()` passa a informar tambem o formato materializado.

### 2. `fiscal_documentos.py`

- as rotas do dominio documental passam a ler artefatos materializados no formato existente;
- a busca de dataset aceita tambem diretorios Delta sem sufixo `.parquet`.

### 3. `fiscal_analise_v2.py`

- as visoes de estoque, verificacoes e produtos passam a ler Parquet ou Delta automaticamente.

### 4. `backend/routers/parquet.py`

- o endpoint generico `/api/parquet/query` agora resolve o caminho materializado antes de abrir o dataset.

### 5. `src/interface_grafica/services/parquet_service.py`

- o service agora reconhece diretorios Delta;
- lista datasets Delta nos diretorios monitorados;
- faz `scan_delta()` quando o caminho eh um diretorio;
- pode salvar dataset usando Delta conforme configuracao ativa.

---

## Primeira onda beneficiada

Este passo atende principalmente a primeira onda sugerida de ativacao Delta:

- `tb_documentos`
- `movimentacao_estoque`
- `calculos_mensais` / `aba_mensal`

---

## Proximo passo recomendado

Agora o caminho mais valioso e executar uma rodada real de pipeline com:

```bash
export DATA_LAKE_FORMAT=delta
export DELTA_ENABLED_TABLES=tb_documentos,mov_estoque,aba_mensal
export DELTA_WRITE_MODE=overwrite
```

Depois validar:

- leitura no backend fiscal novo;
- consulta via `/api/parquet/query`;
- schema registry;
- eventos no Marquez;
- metricas no Grafana/Prometheus.
