# roteiro_implementacao_primeira_leva_042026.md

## Objetivo

Transformar a primeira leva do frontend em uma sequência objetiva de implementação, separando claramente:

- backend e integração de dados;
- frontend e shell Tauri;
- critérios de conclusão por etapa;
- ordem de entrega sem abrir escopo novo.

Este roteiro parte de documentos já definidos:

- `docs/primeira_leva_frontend_042026.md`
- `docs/contrato_componentes_frontend_042026.md`
- `docs/contrato_dados_api_primeira_leva_042026.md`
- `docs/inventario_abas_mainwindow_042026.md`

---

## Escopo fechado desta primeira leva

Implementar somente:

- mecanismo de destaque de tabelas;
- `mov_estoque`;
- `tabela_mensal`;
- `tabela_anual`;
- `nfe_entrada`.

Ficam fora desta etapa:

- EFD completo;
- CT-e;
- fronteira;
- fisconforme completo;
- agregação;
- conversão;
- SQL direto;
- logs;
- configurações.

---

## Ordem macro de implementação

### Etapa A - backend e contrato de dados

Objetivo:
criar a base de dados e APIs que sustentam a primeira leva sem depender ainda da UI final.

#### A1. Padronizar camada de entrega tabular

Implementar um contrato interno único para respostas tabulares contendo:

- `datasetId`
- `blocoFiscal`
- `cnpj`
- `title`
- `columns`
- `rows`
- `filters`
- `sorting`
- `pagination`
- `export`
- `detach`
- `meta`

**Critério de conclusão**
- os quatro datasets da primeira leva conseguem responder no mesmo formato-base.

#### A2. Implementar endpoints da primeira leva

Implementar recursos lógicos equivalentes a:

- `/api/frontend/analise-fiscal/mov-estoque`
- `/api/frontend/analise-fiscal/tabela-mensal`
- `/api/frontend/analise-fiscal/tabela-anual`
- `/api/frontend/documentos-fiscais/nfe-entrada`

**Critério de conclusão**
- os endpoints aceitam filtros mínimos, ordenação, projeção de colunas e paginação.

#### A3. Implementar serialização do `view_state`

Criar a estrutura de estado serializável para permitir:

- reabertura de visão;
- destaque em nova aba/janela;
- exportação coerente com a visão atual.

**Critério de conclusão**
- qualquer resposta de tabela já retorna `detach.view_state` suficiente para recriar a visão.

#### A4. Implementar exportação vinculada ao estado visível

Criar a rota ou serviço de exportação baseado no mesmo contrato da visão atual.

**Critério de conclusão**
- exportação usa o mesmo dataset, filtros, colunas e ordenação do recorte exibido.

---

### Etapa B - shell fiscal e navegação mínima

Objetivo:
subir a moldura inicial do frontend-alvo sem misturar área fiscal e técnica.

#### B1. Criar shell fiscal principal

O shell deve conter apenas, nesta primeira leva:

- seleção/contexto de CNPJ;
- navegação para **Análise Fiscal**;
- navegação para **Documentos Fiscais**;
- suporte à abertura de visão destacada.

**Critério de conclusão**
- o frontend já separa visualmente Análise Fiscal e Documentos Fiscais.

#### B2. Não expor área técnica nesta etapa

Garantir que não apareçam no fluxo principal:

- SQL;
- logs;
- configuração Oracle;
- diagnósticos internos.

**Critério de conclusão**
- a navegação da primeira leva é limpa para o analista fiscal.

---

### Etapa C - componente-base tabular

Objetivo:
criar a peça central reutilizável da primeira leva.

#### C1. Implementar `FiscalTableContainer`

Responsável por:

- toolbar de ações;
- barra de filtros;
- grade principal;
- controle de colunas;
- exportação;
- destaque.

**Critério de conclusão**
- um único componente-base consegue renderizar os quatro datasets da primeira leva.

#### C2. Implementar painel de filtros

Deve suportar conforme o dataset:

- texto;
- descrição;
- código;
- NCM;
- datas;
- faixas numéricas.

**Critério de conclusão**
- filtros funcionam de forma uniforme entre os datasets.

#### C3. Implementar grade com foco em escala

Deve suportar:

- ordenação;
- visibilidade de colunas;
- persistência de preferências;
- renderização eficiente para grande volume.

**Critério de conclusão**
- a experiência tabular é utilizável sem regressão grave frente à `MainWindow` atual.

---

### Etapa D - destaque em nova aba/janela

Objetivo:
validar o comportamento diferencial exigido para o frontend-alvo.

#### D1. Implementar abertura por `view_state`

A nova aba/janela deve nascer do `view_state` serializado.

**Critério de conclusão**
- a visão destacada abre com o mesmo recorte da visão original.

#### D2. Garantir paridade mínima de comportamento

A visão destacada deve manter:

- filtros;
- colunas;
- ordenação;
- exportação;
- contexto do CNPJ.

**Critério de conclusão**
- destacar não reduz funcionalidade da análise.

---

### Etapa E - telas da primeira leva

Objetivo:
ligar o componente-base aos datasets definidos.

#### E1. `mov_estoque`

Bloco:
- **Análise Fiscal**

**Critério de conclusão**
- filtros por id, descrição, NCM, tipo, datas e valores funcionando.

#### E2. `tabela_mensal`

Bloco:
- **Análise Fiscal**

**Critério de conclusão**
- filtros por id, descrição, ano, mês e valores funcionando.

#### E3. `tabela_anual`

Bloco:
- **Análise Fiscal**

**Critério de conclusão**
- filtros por id, descrição, ano e valores funcionando, com suporte ao filtro cruzado necessário.

#### E4. `nfe_entrada`

Bloco:
- **Documentos Fiscais**

**Critério de conclusão**
- filtros por id, descrição, NCM, co_sefin e datas funcionando.

---

## Ordem recomendada de execução real

1. Padronizar resposta tabular no backend.
2. Implementar endpoints dos quatro datasets.
3. Implementar `view_state` e exportação.
4. Subir shell fiscal mínimo.
5. Subir componente tabular base.
6. Validar destaque em nova aba/janela.
7. Ligar `mov_estoque`.
8. Ligar `tabela_mensal`.
9. Ligar `tabela_anual`.
10. Ligar `nfe_entrada`.
11. Validar paridade mínima contra a `MainWindow` atual.

---

## Critério de aceite da primeira leva

A primeira leva será considerada pronta somente quando:

- backend e frontend estiverem fechados para os quatro datasets;
- a separação entre **Análise Fiscal** e **Documentos Fiscais** estiver visível;
- o destaque em nova aba/janela estiver funcional;
- filtros, colunas, ordenação e exportação estiverem operando no mesmo contrato;
- não houver dependência do fluxo técnico para a experiência principal do analista fiscal.

---

## Regra de bloqueio

Enquanto esta primeira leva não estiver concluída em backend e frontend, não devem ser abertas novas frentes funcionais no frontend principal.

---

## Resultado esperado

Com este roteiro, o projeto passa a ter uma sequência objetiva de implementação para sair da fase apenas documental e entrar na execução real da primeira leva, preservando reaproveitamento, clareza de domínio e foco no analista fiscal.
