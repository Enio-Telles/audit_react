# Plano Completo de Implementação

## Objetivo
Implementar a nova estrutura fiscal do sistema com base em quatro domínios:
1. EFD
2. Documentos Fiscais
3. Fiscalização
4. Cruzamentos / Verificações / Classificação dos Produtos

A cadeia operacional do produto é:
SQL e banco -> extração eficiente -> consolidação em Parquet -> API -> visualização e análise.

## Macroetapas

### Etapa 0 — Preparação e inventário
- levantar queries existentes
- levantar tabs atuais
- mapear dependências entre SQL, parquet, endpoints e telas
- marcar duplicações

### Etapa 1 — Contratos de domínio e taxonomia
- congelar nomenclatura dos quatro domínios
- congelar nomenclatura de datasets
- definir metadata obrigatória
- definir contratos de schema e API

### Etapa 2 — Camada de extração SQL
- reorganizar catálogo SQL por domínio
- modularizar parâmetros, cortes temporais e bases comuns
- reduzir duplicação estrutural

### Etapa 3 — Consolidação em Parquet
- criar materializadores por domínio
- definir schemas canônicos
- persistir metadata e sidecar

### Etapa 4 — Backend FastAPI
- criar routers do módulo fiscal
- expor endpoints de leitura e materialização
- padronizar paginação, filtros e metadata

### Etapa 5 — Frontend React
- criar estrutura `features/fiscal`
- implementar navegação e telas por domínio
- exibir detalhe, filtros, metadata e linhagem

### Etapa 6 — Migração das abas atuais
- mover Estoque para Cruzamentos
- mover Agregação para Verificações
- mover Conversão para Verificações

### Etapa 7 — Qualidade e observabilidade
- testes de materialização
- testes de schema
- testes de API
- testes de frontend
- benchmark de performance

### Etapa 8 — Rollout e endurecimento
- habilitar por fases
- recolher feedback
- estabilizar UX e contratos
