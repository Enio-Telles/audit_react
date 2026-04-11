# contrato_dados_api_primeira_leva_042026.md

## Objetivo

Definir o contrato mínimo de dados e API da primeira leva do frontend fiscal, garantindo que a migração preserve o comportamento funcional da UI atual e viabilize destaque de tabelas, filtros, ordenação, colunas e exportação.

Este documento complementa:

- `docs/primeira_leva_frontend_042026.md`
- `docs/contrato_componentes_frontend_042026.md`
- `docs/inventario_abas_mainwindow_042026.md`

---

## Escopo da primeira leva

Este contrato cobre:

- `mov_estoque`
- `tabela_mensal`
- `tabela_anual`
- `nfe_entrada`
- destaque de tabelas em nova aba/janela

---

## Princípios obrigatórios

1. Oracle deve continuar sendo usado de forma atômica e estável.
2. Joins, agregações, consolidações e regras analíticas devem ficar preferencialmente fora do Oracle, em Polars + Parquet ou camada equivalente.
3. O frontend não deve montar SQL.
4. O frontend deve consumir datasets e metadados já preparados para uso fiscal.
5. O contrato de resposta deve servir tanto para a tela principal quanto para visões destacadas.

---

## Arquitetura lógica de dados

### Camada 1 - origem atômica

Responsável por:

- consultas Oracle básicas;
- leitura de artefatos Parquet existentes;
- acesso a datasets já materializados.

### Camada 2 - preparação analítica

Responsável por:

- filtros iniciais por CNPJ e período;
- projeção de colunas;
- ordenação controlada;
- paginação ou janela de dados quando aplicável;
- composição de metadados de tabela.

### Camada 3 - contrato de entrega ao frontend

Responsável por devolver:

- linhas visíveis;
- colunas disponíveis;
- filtros suportados;
- ordenação atual;
- contexto serializável para destaque;
- totalizadores e metadados quando aplicável.

---

## Recursos de API da primeira leva

A primeira leva deve operar com recursos separados por dataset.

### 1. `mov_estoque`

**Recurso lógico**
- `/api/frontend/analise-fiscal/mov-estoque`

**Finalidade**
- entregar a grade operacional de movimentos de estoque para o bloco **Análise Fiscal**.

**Parâmetros mínimos de entrada**
- `cnpj`
- `page`
- `page_size`
- `sort_by`
- `sort_direction`
- `filters.id_agrupado`
- `filters.descricao`
- `filters.ncm`
- `filters.tipo_item`
- `filters.data_inicial`
- `filters.data_final`
- `filters.valor_min`
- `filters.valor_max`
- `visible_columns[]`

### 2. `tabela_mensal`

**Recurso lógico**
- `/api/frontend/analise-fiscal/tabela-mensal`

**Finalidade**
- entregar a consolidação mensal para o bloco **Análise Fiscal**.

**Parâmetros mínimos de entrada**
- `cnpj`
- `page`
- `page_size`
- `sort_by`
- `sort_direction`
- `filters.id_agrupado`
- `filters.descricao`
- `filters.ano`
- `filters.mes`
- `filters.valor_min`
- `filters.valor_max`
- `visible_columns[]`

### 3. `tabela_anual`

**Recurso lógico**
- `/api/frontend/analise-fiscal/tabela-anual`

**Finalidade**
- entregar a consolidação anual para o bloco **Análise Fiscal**.

**Parâmetros mínimos de entrada**
- `cnpj`
- `page`
- `page_size`
- `sort_by`
- `sort_direction`
- `filters.id_agrupado`
- `filters.descricao`
- `filters.ano`
- `filters.valor_min`
- `filters.valor_max`
- `filters.selection_anchor`
- `visible_columns[]`

### 4. `nfe_entrada`

**Recurso lógico**
- `/api/frontend/documentos-fiscais/nfe-entrada`

**Finalidade**
- entregar a grade de documentos fiscais de entrada para o bloco **Documentos Fiscais**.

**Parâmetros mínimos de entrada**
- `cnpj`
- `page`
- `page_size`
- `sort_by`
- `sort_direction`
- `filters.id_agrupado`
- `filters.descricao`
- `filters.ncm`
- `filters.co_sefin`
- `filters.data_inicial`
- `filters.data_final`
- `visible_columns[]`

---

## Contrato base de resposta

Toda API da primeira leva deve responder com a mesma estrutura-base:

```json
{
  "datasetId": "mov_estoque",
  "blocoFiscal": "analise_fiscal",
  "cnpj": "00000000000000",
  "title": "Movimentação de estoque",
  "columns": [
    {
      "key": "id_agrupado",
      "label": "ID Agrupado",
      "type": "string",
      "visible": true,
      "sortable": true,
      "filterable": true
    }
  ],
  "rows": [
    {
      "id_agrupado": "123",
      "descricao": "PRODUTO X"
    }
  ],
  "filters": {
    "applied": {},
    "supported": {}
  },
  "sorting": {
    "sort_by": "descricao",
    "sort_direction": "asc"
  },
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total_rows": 0,
    "total_pages": 0
  },
  "export": {
    "enabled": true,
    "formatos": ["xlsx", "csv", "parquet"]
  },
  "detach": {
    "enabled": true,
    "view_state": {}
  },
  "meta": {
    "source": "parquet",
    "generated_at": "2026-04-11T00:00:00Z"
  }
}
```

---

## Contrato de colunas

Cada coluna entregue ao frontend deve trazer ao menos:

- `key`
- `label`
- `type`
- `visible`
- `sortable`
- `filterable`

Opcionalmente:

- `width`
- `align`
- `format`
- `description`
- `group`

Isso permite que a camada visual opere sem hardcode excessivo por tabela.

---

## Contrato de filtros

O bloco `filters.supported` deve descrever explicitamente os filtros possíveis por dataset.

Exemplo lógico:

```json
{
  "descricao": {"type": "text"},
  "ncm": {"type": "text"},
  "data_inicial": {"type": "date"},
  "data_final": {"type": "date"},
  "valor_min": {"type": "number"},
  "valor_max": {"type": "number"}
}
```

Isso evita lógica implícita demais no frontend.

---

## Contrato de destaque de tabela

A abertura de nova aba/janela destacada deve operar a partir de um `view_state` serializável.

### Estrutura mínima do `view_state`

```json
{
  "datasetId": "mov_estoque",
  "blocoFiscal": "analise_fiscal",
  "cnpj": "00000000000000",
  "title": "Movimentação de estoque",
  "filters": {},
  "sorting": {
    "sort_by": "descricao",
    "sort_direction": "asc"
  },
  "visible_columns": ["id_agrupado", "descricao"],
  "page": 1,
  "page_size": 100,
  "perfil": "default"
}
```

### Regras obrigatórias

1. O destaque não deve abrir uma visão diferente da tela original.
2. O `view_state` deve ser suficiente para restaurar o mesmo recorte.
3. O shell Tauri deve receber esse estado como contrato de abertura da nova janela/aba.
4. A visão destacada deve poder exportar o mesmo recorte que está exibindo.

---

## Contrato de exportação

A exportação não deve disparar uma consulta solta diferente do estado visível.

Ela deve usar:

- o mesmo `datasetId`;
- os mesmos filtros aplicados;
- as mesmas colunas visíveis quando aplicável;
- o mesmo contexto de CNPJ.

**Recurso lógico sugerido**
- `/api/frontend/export`

**Payload mínimo**

```json
{
  "datasetId": "mov_estoque",
  "cnpj": "00000000000000",
  "filters": {},
  "sorting": {},
  "visible_columns": ["id_agrupado", "descricao"],
  "format": "xlsx"
}
```

---

## Contrato de performance

As APIs da primeira leva devem estar preparadas para:

- paginação server-side ou janela equivalente;
- ordenação controlada no backend/camada analítica quando necessário;
- evitar retorno de colunas desnecessárias;
- permitir projeção de colunas;
- manter tempos previsíveis para interação do analista.

---

## Contrato de separação de domínio

### Pode estar nas APIs desta primeira leva

- dados fiscais tabulares;
- metadados de filtros;
- metadados de colunas;
- contexto fiscal do CNPJ.

### Não deve vazar para a área principal do usuário

- SQL bruto;
- logs internos;
- credenciais;
- detalhes operacionais de conexão Oracle;
- estrutura técnica irrelevante para a leitura fiscal.

---

## Critério de aceitação

Este contrato será considerado atendido quando:

- cada dataset da primeira leva puder ser consumido por um componente tabular único;
- o frontend puder restaurar integralmente uma visão destacada via `view_state`;
- filtros, colunas, ordenação e exportação estiverem amarrados ao mesmo contrato de dados;
- a separação entre **Análise Fiscal** e **Documentos Fiscais** estiver refletida também nas rotas e payloads.

---

## Consequência prática

Com este contrato, o projeto passa a ter uma especificação objetiva para iniciar a implementação real da primeira leva no frontend e na camada de integração, sem reabrir discussão sobre escopo ou comportamento básico já decidido.
