# boas_praticas_042026.md

## Objetivo

Registrar a trilha incremental de boas praticas a ser incorporada ao projeto sem romper a regra principal do plano:

- nao abrir reescritas paralelas;
- aplicar melhorias junto do escopo ativo;
- priorizar o que aumenta manutencao, previsibilidade e clareza do frontend fiscal.

Este documento complementa o `docs/plano_042026.md`.

---

## Princípio de aplicação

Boas praticas neste projeto nao devem virar uma frente abstrata separada da entrega.

A regra operacional e:

1. primeiro fechar o escopo ativo;
2. dentro dele, aplicar padronizacao, coesao, tipagem e clareza;
3. evitar refatoracao ampla sem ganho direto para a entrega atual.

---

## Boas praticas priorizadas

### 1. Backend

#### 1.1 Contratos padronizados

Prioridade alta:

- padronizar payloads tabulares em um formato unico;
- evitar mistura de retornos heterogeneos entre rotas legadas e novas;
- garantir `view_state` serializavel para destaque e restauracao de contexto.

Estado atual:

- parcialmente implementado na primeira leva via:
  - `backend/routers/frontend_table_contract.py`
  - `backend/routers/frontend_primeira_leva.py`

#### 1.2 Coesao por dominio

Prioridade alta:

- manter routers, helpers e servicos separados por dominio fiscal;
- evitar arquivos excessivamente grandes ou com responsabilidades acumuladas;
- reduzir acoplamento desnecessario entre infraestrutura, dados e resposta HTTP.

#### 1.3 Infraestrutura de dados

Prioridade alta:

- manter Oracle em consultas simples e estaveis;
- concentrar transformacao e consolidacao em Polars + Parquet;
- evitar caminhos paralelos de conexao ou acesso a dados quando houver helper central reaproveitavel.

#### 1.4 Testabilidade

Prioridade alta:

- adicionar testes para helpers de contrato e para routers de migracao;
- preferir pontos de composicao simples que possam ser exercitados por monkeypatch ou fixtures leves.

Estado atual:

- parcialmente implementado com testes da primeira leva.

---

### 2. Frontend

#### 2.1 Shell e navegacao

Prioridade alta:

- extrair shell fiscal principal;
- separar explicitamente area fiscal e area tecnica;
- reduzir condicionais extensas na tela principal.

#### 2.2 Componentizacao

Prioridade alta:

- criar componente-base tabular reutilizavel;
- centralizar filtros, colunas, exportacao e destaque;
- evitar repeticao de implementacao entre tabelas da primeira leva.

#### 2.3 Estado e serializacao

Prioridade alta:

- manter estado minimo compartilhado e serializavel;
- usar o mesmo contrato para tela principal e visao destacada;
- reduzir divergencia entre o que a tela mostra e o que a exportacao entrega.

#### 2.4 Tipagem e duplicacao

Prioridade media:

- reforcar tipagem de payloads e props;
- eliminar duplicacao de definicao de abas, menus e estruturas legadas quando a primeira leva estabilizar.

---

### 3. Arquitetura e governanca

#### 3.1 Reaproveitamento antes de reescrita

Prioridade alta:

- usar a `MainWindow` atual como referencia minima de comportamento;
- usar Tauri/React como direcao de forma, shell e navegacao;
- nao jogar fora capacidade funcional madura para ganhar apenas nova embalagem visual.

#### 3.2 Clareza de dominio

Prioridade alta:

- EFD separada de cruzamentos;
- Documentos Fiscais separados de Analise Fiscal;
- backend e frontend refletindo a mesma taxonomia.

#### 3.3 Observabilidade e erro

Prioridade media:

- padronizar logging e metadados de origem dos dados;
- padronizar resposta de erro para APIs novas;
- manter rastreabilidade dos datasets e artefatos servidos ao frontend.

---

## Ordem recomendada de incorporacao

### Curto prazo

- ativar e validar a primeira leva no backend;
- consumir o contrato tabular no frontend;
- subir shell fiscal minimo;
- criar componente tabular base.

### Medio prazo

- extrair navegacao fiscal e area tecnica;
- remover duplicacoes de fluxo e menus legados;
- reforcar tipagem e contratos compartilhados frontend/backend.

### Longo prazo

- refinar modulos maiores e pontos de alto acoplamento conforme a migracao real avancar;
- tratar refatoracoes mais amplas apenas quando houver ganho claro de manutencao ou performance.

---

## Regra de decisao

Quando houver duvida entre:

- criar nova camada do zero; ou
- reaproveitar uma base funcional existente,

priorizar reaproveitamento com padronizacao progressiva.

---

## Resultado esperado

Ao aplicar esta trilha, o projeto melhora manutencao, previsibilidade e clareza sem perder o foco principal:

- entregar o frontend fiscal orientado ao analista;
- preservar a capacidade real ja existente;
- migrar com menos retrabalho.
