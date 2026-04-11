# implementacao_etapa1_042026.md

## Objetivo

Registrar a primeira etapa efetiva de implementacao do plano de abril/2026, limitada a alinhamento estrutural, governanca e direcao de frontend.

Esta etapa nao abre novas funcionalidades.
Ela consolida as bases para a reorganizacao segura do frontend e da documentacao.

---

## O que foi implementado nesta etapa

### 1. Plano unificado criado

Foi criado o plano unificado de abril/2026 em `docs/plano_042026.md`, contendo:

- estado real confirmado da `main`;
- backlog unificado;
- riscos de retrabalho;
- classificacao proposta x implementacao real;
- passos executados.

### 2. Frontend de referencia oficializado

Foi criado `docs/frontend_referencia_042026.md` para oficializar o mock React/TypeScript como referencia de:

- forma visual;
- navegacao;
- separacao entre area fiscal e area tecnica;
- direcao de uso com Tauri.

### 3. Matriz inicial de migracao criada

Foi criado `docs/matriz_migracao_frontend_042026.md` para mapear:

- UI atual em PySide6;
- bloco alvo no frontend fiscal;
- status de enquadramento;
- regras de preservacao de comportamento.

### 4. Enquadramento do Dossie redefinido

Foi criado `docs/dossie_enquadramento_042026.md` para compatibilizar:

- backend e cache do Dossie ja existentes;
- nova arquitetura de navegacao em tres blocos fiscais.

O Dossie passa a ser tratado como recurso contextual, e nao mais como eixo unico da navegacao principal.

---

## O que foi confirmado como base funcional existente

A interface atual em PySide6 ja possui comportamento importante que precisa ser preservado:

- filtros textuais;
- filtros por datas;
- filtros numericos;
- ordenacao;
- selecao de colunas;
- perfis;
- exportacao;
- destaque de tabelas;
- modulos concretos para agregacao, estoque, NFe Entrada, fisconforme e SQL.

---

## O que ainda nao foi implementado nesta etapa

- reorganizacao real da UI em tres blocos fiscais;
- area tecnica isolada no frontend real;
- runtime Tauri ativo como principal;
- modulo EFD completo com blocos oficiais;
- consolidacao final de CT-e e fronteira na navegacao.

---

## Resultado da etapa

A etapa 1 fecha a base documental e decisoria para que a migracao futura:

- parta do estado real do codigo;
- respeite a funcionalidade madura do desktop atual;
- use o mock como referencia visual sem tratA-lo como pronto;
- reduza risco de retrabalho.

---

## Proximo passo recomendado

O proximo passo tecnico deve ser:

1. inventariar as abas e tabelas da `MainWindow` com nivel de paridade exigido;
2. marcar quais entram em EFD, Documentos Fiscais, Analise Fiscal e Manutencao/T.I.;
3. so depois iniciar mudancas de frontend e shell.
