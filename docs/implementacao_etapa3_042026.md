# implementacao_etapa3_042026.md

## Objetivo

Registrar a terceira etapa efetiva de implementação do plano de abril/2026, com foco em transformar a primeira leva do frontend em sequência executável de backend + frontend.

---

## O que foi implementado nesta etapa

### 1. Contrato de componentes da primeira leva

Foi criado `docs/contrato_componentes_frontend_042026.md`, contendo:

- shell fiscal principal;
- container tabular fiscal;
- painel de filtros;
- grade principal;
- gestor de destaque;
- gestor de perfis e preferências;
- exportador.

Também foram definidos:

- contrato por tela para `mov_estoque`, `tabela_mensal`, `tabela_anual` e `NFe Entrada`;
- estado compartilhado mínimo para restauração e destaque.

### 2. Contrato de dados e API da primeira leva

Foi criado `docs/contrato_dados_api_primeira_leva_042026.md`, contendo:

- rotas lógicas por dataset;
- parâmetros mínimos de entrada;
- contrato-base de resposta;
- contrato de colunas;
- contrato de filtros;
- contrato de `view_state` serializável;
- contrato de exportação;
- contrato de separação de domínio.

### 3. Roteiro de implementação da primeira leva

Foi criado `docs/roteiro_implementacao_primeira_leva_042026.md`, contendo:

- ordem macro de implementação;
- etapas A a E;
- dependências entre backend, shell, componente tabular, destaque e telas;
- critério de aceite da primeira leva;
- regra de bloqueio de escopo até conclusão de backend + frontend.

---

## O que ficou resolvido nesta etapa

Com esta etapa, a primeira leva do frontend passou a ter:

- escopo fechado;
- contrato de componentes;
- contrato de dados/API;
- ordem de implementação;
- critérios de conclusão.

Ou seja:

**a primeira leva já não depende mais de definição conceitual adicional para começar execução real.**

---

## O que ainda não foi implementado no código

Ainda não foi implementado no runtime do sistema:

- endpoints reais da primeira leva;
- serialização operacional de `view_state` no backend;
- shell fiscal mínimo no frontend;
- componente tabular base;
- destaque em nova aba/janela funcionando no alvo Tauri;
- telas reais de `mov_estoque`, `tabela_mensal`, `tabela_anual` e `NFe Entrada` no frontend-alvo.

---

## Próximo passo recomendado

O próximo passo técnico deve ser iniciar a execução real da **Etapa A** do roteiro:

1. padronizar resposta tabular no backend;
2. implementar endpoints da primeira leva;
3. devolver `detach.view_state` no contrato de resposta;
4. amarrar exportação ao mesmo estado visível.

---

## Resultado da etapa

A etapa 3 conclui a passagem do projeto de:

- diagnóstico e governança documental

para:

- especificação objetiva da primeira leva de implementação do frontend fiscal.
