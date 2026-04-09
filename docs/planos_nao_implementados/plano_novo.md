# Plano Estruturado — Evolução Visual e UX do Dossiê no `audit_react`

## 1. Objetivo

Estruturar a evolução visual e de experiência do usuário do Dossiê no repositório `Enio-Telles/audit_react`, aproveitando a arquitetura já existente e evitando redesign desnecessário.

Este plano parte de dois insumos:

- o plano de melhorias visuais e UX do dossiê, com foco em blocos, resumo executivo, contatos agregados, padrões visuais, tabelas, drill-down, evidências e presets;
- a implementação atual do `audit_react`, que já possui Dossiê por CNPJ, sincronização por seção, leitura materializada, detalhe especializado para contato, tabela reutilizável e contratos auditáveis.

## 2. Base de análise

### 2.1. O que o plano visual propõe

O plano visual prioriza:

- reorganização do dossiê em blocos principais;
- modo **Resumo Executivo** e **Auditoria**;
- cabeçalho executivo com cards-resumo;
- contatos agregados por papel;
- padrão visual de risco, divergência e confiança;
- tabelas mais limpas;
- drill-down;
- painel lateral de detalhe;
- seção de evidências;
- gráficos e presets por perfil.

### 2.2. O que o repositório já oferece

O `audit_react` já possui:

- frontend com **React + Vite + Tailwind + React Query + Zustand + TanStack Table**;
- Dossiê integrado ao fluxo principal do app;
- `DossieTab` com cards por seção, status, sync, atualização, estratégia e SQL principal;
- `DossieSectionDetail` para leitura do cache materializado;
- `DossieContatoDetalhe` com agrupamento por vínculo, alertas e origem;
- `DataTable` reutilizável com ordenação, filtros, ocultação, reordenação e redimensionamento;
- APIs do Dossiê para seções, dados materializados, sync e comparações da seção `contato`.

## 3. Diretriz principal

**Não redesenhar o Dossiê do zero.**

A estratégia correta é evoluir a interface em camadas, preservando:

- o contrato auditável por seção;
- o foco em CNPJ como ponto de entrada;
- a persistência e o reuso de dados;
- a rastreabilidade por origem, SQL e metadata;
- os componentes e capacidades já implementados.

## 4. Diagnóstico no contexto do `audit_react`

## 4.1. Pontos fortes já existentes

- A base técnica do frontend já suporta bem uma evolução de UX sem troca de stack.
- O Dossiê já é tratado como fluxo principal por CNPJ.
- A seção `contato` já está mais madura do que a UI atual deixa parecer.
- A camada de tabela já é forte e não precisa ser substituída.
- Há testes iniciais cobrindo `DossieTab` e `DossieContatoDetalhe`.

## 4.2. Principais fragilidades atuais

- O topo do Dossiê ainda não entrega leitura executiva imediata.
- A grade de seções tem pouco contraste de prioridade.
- Falta hierarquia visual entre resumo, ação, detalhe e evidência.
- O detalhe ainda aparece como continuação de tela, e não como navegação progressiva.
- A UX de contato é funcional, mas ainda pouco “produto” e muito “estrutura técnica”.
- O arquivo `DossieTab.tsx` já concentra responsabilidades demais.

## 5. Mapeamento: plano visual x aderência real ao repositório

## 5.1. Itens com alta aderência e baixo atrito

### A. Cabeçalho executivo + cards-resumo

**Aderência:** alta
**Dependência nova:** baixa

Pode ser construído usando os dados já retornados por `getSecoes`, por exemplo:

- total de seções;
- seções atualizadas;
- seções pendentes;
- total de linhas materializadas;
- seções com divergência/comparação;
- última atualização relevante.

### B. Modo “Resumo Executivo” e “Auditoria”

**Aderência:** alta
**Dependência nova:** baixa

Pode ser implementado como estado local no Dossiê ou preferência no Zustand.

### C. Melhoria visual da seção de contatos

**Aderência:** muito alta
**Dependência nova:** baixa

O componente especializado já existe. O ganho vem de melhorar hierarquia, densidade, destaque e leitura consolidada.

### D. Tabela com perfil compacto e analítico

**Aderência:** alta
**Dependência nova:** baixa

A infraestrutura já existe no `DataTable`. O trabalho é de configuração e integração, não de reconstrução.

## 5.2. Itens com aderência média e refatoração necessária

### E. Drill-down por linha e painel lateral

**Aderência:** média
**Dependência nova:** média

O Dossiê já tem detalhe por seção, mas não tem fluxo completo de inspeção por linha com painel lateral contextual.

### F. Presets por perfil

**Aderência:** média
**Dependência nova:** média

Viável com Zustand e persistência local, mas exige definição clara de perfis e escopo de configuração.

## 5.3. Itens com aderência parcial e dependência de backend ou nova infraestrutura

### G. Evidências e documentos relacionados por item

**Aderência:** parcial
**Dependência nova:** alta

Hoje há rastreabilidade e metadata, mas não um fluxo pronto de “abrir documento relacionado” por linha.

### H. Comparação lado a lado

**Aderência:** parcial
**Dependência nova:** alta

A comparação atual está concentrada na seção `contato` e no eixo de convergência entre estratégias.

### I. Gráficos

**Aderência:** parcial
**Dependência nova:** média/alta

O frontend não traz hoje uma camada específica de charting. Dá para fazer, mas não é P0.

## 6. Estratégia de implementação recomendada

## 6.1. Princípios

- preservar o contrato atual do Dossiê;
- evoluir a UI por componentes menores;
- priorizar ganho de leitura antes de novas funcionalidades;
- usar primeiro dados já expostos pela API;
- adiar dependências de backend para quando houver necessidade real;
- manter o Dossiê como fluxo principal do app.

## 6.2. Ordem recomendada

### Fase 1 — Leitura executiva imediata

Objetivo: melhorar o primeiro impacto visual do Dossiê sem mexer no backend.

Entregas:

- cabeçalho executivo;
- KPI cards;
- reorganização do topo;
- melhoria visual dos cards de seção;
- destaque mais claro de status, atualização e estratégia.

### Fase 2 — Organizar leitura por camadas

Objetivo: separar o que é resumo do que é análise detalhada.

Entregas:

- toggle entre modo Executivo e Auditoria;
- blocos visuais com hierarquia clara;
- seção de contatos com apresentação mais nobre;
- padrão visual de divergência, convergência e origem.

### Fase 3 — Melhorar a inspeção

Objetivo: reduzir atrito ao navegar do resumo para o detalhe.

Entregas:

- perfil compacto e analítico para tabela;
- abertura contextual de detalhe;
- base para painel lateral;
- refinamento do drill-down.

### Fase 4 — Elevar a camada analítica

Objetivo: adicionar recursos que exigem mais contrato ou mais infraestrutura.

Entregas:

- evidências por item;
- comparação lado a lado;
- gráficos simples;
- presets persistidos por perfil.

## 7. Backlog técnico priorizado

## P0 — Fazer agora

### UX e layout

- [x] Criar `DossieHeader` com CNPJ, razão social, última atualização, estratégia e ações principais.
- [x] Criar `DossieKpis` usando agregações derivadas de `sections`.
- [x] Reorganizar `DossieTab` em blocos visuais mais claros.
- [x] Melhorar os cards das seções com peso visual, estados e prioridade mais legíveis.
- [x] Criar toggle de visualização `Executivo | Auditoria`.
- [x] Refinar visualmente `DossieContatoDetalhe` para leitura mais executiva.
- [x] Criar perfil `compacto` e `analítico` para o detalhe tabular.

### Refatoração mínima

- [x] Extrair componentes de `DossieTab.tsx` para reduzir acoplamento.
- [x] Centralizar helpers de status, estratégia e atualização em utilitários próprios.

### Testes

- [x] Atualizar testes de `DossieTab` para cobrir cabeçalho e cards-resumo.
- [x] Manter cobertura do fluxo de sync após refatoração.
- [x] Garantir que a seção `contato` continue agrupada e legível após redesign.

## P1 — Fazer na sequência

### Navegação e detalhe

- [x] Implementar `DossieDetailPanel` ou painel lateral.
- [x] Adicionar navegação progressiva do card para a análise filtrada.
- [x] Melhorar a apresentação do `DossieSectionDetail`.
- [x] Salvar preferência de modo e layout por usuário localmente.

### Consistência visual

- [x] Definir sistema de chips para fonte/origem/estratégia.
- [x] Padronizar badges de convergência, divergência, atenção e pendência.
- [x] Refinar legibilidade de metadata técnica no modo Auditoria.

## P2 — Fazer depois

### Recursos avançados

- [ ] Criar visão de evidências por item.
- [ ] Criar comparação lado a lado por período, fonte ou estratégia.
- [ ] Introduzir gráficos simples e acionáveis.
- [ ] Criar presets completos por perfil de uso.

## 8. Arquivos com maior chance de impacto

### Frontend

- `frontend/src/features/dossie/components/DossieTab.tsx`
- `frontend/src/features/dossie/components/DossieSectionDetail.tsx`
- `frontend/src/features/dossie/components/DossieContatoDetalhe.tsx`
- `frontend/src/features/dossie/types.ts`
- `frontend/src/components/table/DataTable.tsx`
- `frontend/src/store/appStore.ts`

### Novos componentes sugeridos

- `frontend/src/features/dossie/components/DossieHeader.tsx`
- `frontend/src/features/dossie/components/DossieKpis.tsx`
- `frontend/src/features/dossie/components/DossieSectionGrid.tsx`
- `frontend/src/features/dossie/components/DossieViewModeToggle.tsx`
- `frontend/src/features/dossie/components/DossieDetailPanel.tsx`

## 9. Proposta de arquitetura de componentes

```text
DossieTab
├── DossieHeader
├── DossieKpis
├── DossieViewModeToggle
├── DossieSectionGrid
│   └── DossieSectionCard
├── DossieSectionDetail
│   ├── DossieContatoDetalhe
│   └── DataTable
└── DossieDetailPanel (fase posterior)
```

## 10. Dependências por frente

## 10.1. O que pode ser feito só no frontend

- cabeçalho executivo;
- KPI cards;
- reorganização em blocos;
- modo Executivo/Auditoria;
- refinamento da leitura de contatos;
- perfis de exibição na tabela;
- badges e chips de status/origem.

## 10.2. O que precisa de alinhamento com backend

- evidências navegáveis por item;
- abertura de documento relacionado;
- comparação visual lado a lado fora do escopo atual de `contato`;
- agregados prontos para gráficos mais úteis e consistentes.

## 11. Critérios de aceite

## 11.1. Critérios funcionais

- O Dossiê continua operando no fluxo principal por CNPJ.
- O sync por seção continua funcionando sem regressão.
- A leitura detalhada continua baseada em cache materializado.
- A rastreabilidade por origem, estratégia e SQL permanece visível.
- A seção `contato` continua preservando agrupamento e alertas.

## 11.2. Critérios de UX

- O usuário entende o estado geral do Dossiê em poucos segundos.
- As seções críticas ficam visualmente mais óbvias.
- A leitura de contatos exige menos esforço.
- O modo executivo reduz ruído sem esconder o caminho para auditoria.
- O detalhe técnico fica mais acessível sem poluir o topo da tela.

## 12. Riscos

### Risco 1 — Melhorar visual e perder rastreabilidade

**Mitigação:** manter metadata e origem visíveis no modo Auditoria.

### Risco 2 — `DossieTab` crescer demais

**Mitigação:** extrair componentes logo na Fase 1.

### Risco 3 — Criar layout bonito, mas sem ganho real

**Mitigação:** priorizar leitura do topo, status e contatos antes de gráficos.

### Risco 4 — Introduzir recursos que dependem de backend sem contrato claro

**Mitigação:** manter evidências, comparação avançada e gráficos como P1/P2.

## 13. Recomendação final

A melhor decisão para o `audit_react` é:

1. **não recomeçar o Dossiê**;
2. **tratar a melhoria visual como evolução incremental**;
3. **atacar primeiro leitura executiva, hierarquia e contatos**;
4. **deixar recursos avançados para depois**;
5. **aproveitar a infraestrutura já pronta de tabela, detalhe e contrato auditável**.

## 14. Resumo executivo

### O que já sustenta o plano

- stack moderna e compatível;
- Dossiê como fluxo principal;
- contato já especializado;
- tabela forte;
- APIs suficientes para a primeira fase;
- testes básicos existentes.

### O que entra primeiro

- cabeçalho executivo;
- KPI cards;
- reorganização visual;
- modo Executivo/Auditoria;
- melhoria visual da seção `contato`;
- tabela com perfil compacto/analítico.

## 15. Estado atual no código

### Entregas P0 já materializadas

- `DossieTab` já foi quebrado em `DossieHeader`, `DossieKpis`, `DossieSectionGrid`, `DossieViewModeToggle` e `DossieSectionDetail`.
- O cabeçalho já mostra CNPJ, razão social, última atualização, contadores executivos e ações principais.
- O topo já permite abrir a seção prioritária e sincronizar seções pendentes em lote, preservando o sync por seção existente.
- O modo `Executivo | Auditoria` já é persistido em Zustand.
- `DossieContatoDetalhe` já possui leitura executiva com grupos por vínculo, alertas, fontes consolidadas e distinção do modo auditoria.
- O detalhe tabular já suporta perfil `compacto | analitico`, além de ordenação e filtros persistidos por seção.
- O detalhe agora usa painel lateral dedicado, chips padronizados e separa sinais operacionais de metadata técnica.

### Próxima frente recomendada

- Iniciar a frente de evidências navegáveis por item, aproveitando a metadata já exposta e o novo painel lateral.

### O que fica para depois

- painel lateral;
- evidências navegáveis;
- comparação lado a lado;
- gráficos;
- presets avançados.
