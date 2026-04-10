# Tauri v2, Delta Lake, OpenLineage e Stack de Observabilidade

## Objetivo

Este documento registra a implementacao inicial da trilha pedida para:

- migracao para Tauri v2;
- adocao direta de Delta Lake;
- integracao de OpenLineage;
- dashboard Grafana/Prometheus de ponta a ponta;
- preparacao para mobile.

---

## 1. Tauri v2 e mobile

Arquivos criados:

- `frontend/src-tauri/Cargo.toml`
- `frontend/src-tauri/build.rs`
- `frontend/src-tauri/src/main.rs`
- `frontend/src-tauri/tauri.conf.json`
- `frontend/src-tauri/capabilities/default.json`
- `frontend/package.json` (scripts e dependencias Tauri)

### O que entrou

- shell desktop pronta para o frontend React/Vite;
- scripts `tauri:dev`, `tauri:build`, `tauri:android:dev`, `tauri:ios:dev`;
- configuracao compatível com Tauri v2;
- base unica para desktop e preparacao mobile.

### Limite atual

O repositorio ficou **preparado** para Tauri/mobile, mas o build real de Android/iOS ainda depende do ambiente nativo local:

- Rust toolchain
- Android Studio / SDK
- Xcode (macOS)

---

## 2. Delta Lake

Arquivos criados/alterados:

- `src/utilitarios/delta_lake.py`
- `src/utilitarios/salvar_para_parquet.py`
- `requirements.txt`

### O que entrou

- helper para `write_delta_table`, `read_delta_table` e `scan_delta_table`;
- escolha de formato por argumento ou por `DATA_LAKE_FORMAT`;
- suporte a `DELTA_WRITE_MODE`;
- registro do schema escrito no `SchemaRegistry`.

### Como usar

#### Manter Parquet (padrao)

```bash
export DATA_LAKE_FORMAT=parquet
```

#### Passar a escrever Delta

```bash
export DATA_LAKE_FORMAT=delta
export DELTA_WRITE_MODE=overwrite
```

A partir disso, chamadas existentes a `salvar_para_parquet()` podem passar a materializar tabelas Delta sem reescrever todo o pipeline de uma vez.

---

## 3. OpenLineage

Arquivos criados/alterados:

- `src/observabilidade/openlineage.py`
- `src/orquestrador_pipeline.py`

### O que entrou

- emissor OpenLineage por HTTP;
- inicio/fim de run do pipeline;
- emissao por etapa de extracao e tabela de negocio;
- status/falha por etapa;
- integracao opcional via variaveis de ambiente.

### Variaveis

```bash
export OPENLINEAGE_URL=http://localhost:5000/api/v1/lineage
export OPENLINEAGE_NAMESPACE=audit_react
```

---

## 4. Grafana/Prometheus e backend

Arquivos criados/alterados:

- `backend/main.py`
- `backend/routers/observabilidade.py`
- `docker-compose.observability.yml`
- `infra/observability/**`

### O que entrou

- middleware HTTP com metricas de latencia e volume;
- endpoint `/metrics` no backend;
- endpoint `/api/observabilidade/status`;
- compose com Prometheus, Grafana, Marquez e Postgres;
- dashboard inicial provisionado automaticamente no Grafana.

---

## 5. Proximo passo recomendado

Agora que o repositorio tem a base das cinco frentes, o proximo salto de valor e:

1. executar o frontend dentro do shell Tauri em ambiente local;
2. ligar datasets selecionados do pipeline em Delta por dominio;
3. subir a stack observability e validar o scrape de `/metrics`;
4. rodar o pipeline com `OPENLINEAGE_URL` configurado e confirmar os eventos no Marquez;
5. depois partir para responsividade e ajustes mobile reais.
