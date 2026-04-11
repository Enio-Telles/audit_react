# frontend_referencia_042026.md

## Objetivo

Registrar o mock React/TypeScript adotado em abril/2026 como referencia oficial de direcao visual e estrutural do frontend fiscal.

Este documento nao substitui o plano unificado.
Ele existe apenas para fixar o que pode ser reaproveitado do mock sem tratar como implementado o que ainda e apenas simulacao.

---

## Referencia adotada

Foi adotado como referencia de frontend o mock React/TypeScript enviado em abril/2026, com as seguintes direcoes principais:

- navegacao principal do usuario fiscal em tres blocos:
  - EFD
  - Documentos Fiscais
  - Analise Fiscal
- separacao clara entre area fiscal e area tecnica/manutencao;
- area principal centrada em tabela, filtros e leitura fiscal;
- uso de destaque de tabela como gesto central do workbench analitico;
- intencao de uso do shell Tauri para suportar novas janelas/abas destacadas.

---

## O que vale como referencia oficial

### 1. Hierarquia de navegacao

A navegacao principal deve partir de:

- EFD
- Documentos Fiscais
- Analise Fiscal

### 2. Separacao de dominio

A area tecnica nao deve poluir o fluxo principal do analista fiscal.
Recursos de manutencao, SQL direto, logs e diagnostico devem ficar em area propria.

### 3. Direcao de UX

A tela principal deve priorizar:

- tabelas
- filtros por datas e codigos
- leitura clara
- baixa poluicao visual
- destaque de recortes analiticos em nova aba/janela

### 4. Uso como referencia Tauri

A ideia de destaque de tabela por nova janela pode ser reaproveitada como direcao de integracao com Tauri.

---

## O que nao pode ser tratado como implementado

O mock nao comprova implementacao real de:

- destaque real via Tauri;
- filtros avancados completos;
- ordenacao real;
- selecao de colunas;
- agregacao funcional;
- virtualizacao/paginacao real;
- integracao backend/frontend;
- cobertura completa da EFD conforme todos os blocos oficiais do Guia Pratico.

---

## Regra de uso desta referencia

1. O mock vale como referencia visual e estrutural.
2. A `MainWindow` atual em PySide6 continua sendo a referencia minima de paridade funcional.
3. Nenhuma migracao de frontend deve perder funcionalidades ja existentes no desktop atual.
4. Antes de implementar a navegacao definitiva de EFD, o mock deve ser expandido para contemplar a taxonomia oficial completa.

---

## Consequencia pratica

A partir de abril/2026:

- o mock React/TypeScript passa a orientar a forma do frontend;
- a UI PySide6 continua orientando o contrato minimo de comportamento;
- Tauri permanece como alvo arquitetural, sem apagar a necessidade de reaproveitar o que ja esta funcional.
