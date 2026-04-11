# Tela operacional de catálogo no frontend

## Objetivo

Este passo leva o inspector de catálogo do backend para a interface, permitindo validação operacional sem depender apenas de chamadas brutas aos endpoints.

---

## O que foi implementado

### Nova tab no frontend

Arquivos:

- `frontend/src/features/fiscal/catalogo/CatalogoDatasetsTab.tsx`
- `frontend/src/App.tsx`

A aplicação ganhou uma nova aba:

- `Catálogo Datasets`

### Integração com API

Arquivo:

- `frontend/src/features/fiscal/api.ts`

Métodos adicionados:

- `getDatasetCatalogSummary()`
- `getDatasetCatalogForCnpj(cnpj)`
- `inspectDatasetCatalog(cnpj, datasetId, limit)`

### Tipos adicionados

Arquivo:

- `frontend/src/features/fiscal/types.ts`

Novos tipos:

- `DatasetCatalogSummary`
- `DatasetCatalogAvailability`
- `DatasetAvailabilityItem`
- `DatasetInspection`

---

## O que a nova tela permite

- consultar o catálogo global;
- listar datasets por CNPJ;
- ver disponibilidade, formato e reaproveitamento;
- inspecionar um dataset específico;
- visualizar aliases, metadata, probe e prévia de linhas.

---

## Ganho prático

Isso transforma o inspector do backend em ferramenta de operação real da migração, útil para:

- validar materialização canônica;
- conferir Parquet vs Delta;
- diagnosticar datasets ausentes;
- revisar rapidamente o conteúdo inicial de um dataset sem sair da UI.

---

## Próximo passo recomendado

Agora o próximo passo mais valioso é usar essa tela como base para ações operacionais guiadas, como:

- abrir o domínio relacionado a partir do dataset selecionado;
- destacar datasets críticos ausentes;
- oferecer atalhos para reprocessamento/materialização.
