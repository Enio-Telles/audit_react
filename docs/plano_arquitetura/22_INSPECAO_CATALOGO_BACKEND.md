# Inspeção do catálogo no backend

## Objetivo

Este passo cria uma camada simples e central de inspeção do catálogo, para que o backend consiga responder perguntas operacionais sem espalhar regras de path manual.

---

## O que foi implementado

### Helper de inspeção

Arquivo:

- `backend/routers/fiscal_catalog_inspector.py`

Funções principais:

- `catalog_status()`
- `availability_for_cnpj(cnpj)`
- `inspect_dataset(cnpj, dataset_id, limit)`

O helper usa:

- `catalogo_resumido()`
- `diagnosticar_disponibilidade()`
- `encontrar_dataset()`
- `probe_parquet()`
- leitura materializada para prévia de linhas

### Endpoints adicionados

Arquivo:

- `backend/routers/observabilidade.py`

Novos endpoints:

- `GET /dataset-catalog`
- `GET /dataset-catalog/{cnpj}`
- `GET /dataset-catalog/{cnpj}/{dataset_id}`

---

## Ganho prático

Agora o backend consegue:

- listar disponibilidade de datasets por CNPJ;
- inspecionar um dataset específico;
- devolver uma prévia operacional sem depender de path hardcoded na camada de API.

Isso ajuda tanto debugging quanto validação operacional da migração Parquet/Delta.

---

## Teste adicionado

Arquivo:

- `tests/test_fiscal_catalog_inspector.py`

Cobertura:

- disponibilidade por CNPJ;
- inspeção de dataset com prévia de linhas.

---

## Próximo passo recomendado

Agora o próximo passo mais valioso é começar a usar esse inspector também em telas ou fluxos administrativos do frontend/Tauri, para apoiar validação operacional da materialização canônica.
