# README alinhado - abril/2026

## Objetivo

Registrar a versão alinhada do conteúdo de entrada do projeto enquanto o `README.md` principal ainda não pode ser substituído diretamente pelo conector atual.

Este documento deve ser tratado como referência de alinhamento até a substituição física do arquivo principal.

---

## Estado arquitetural atual

O projeto está em fase de reorganização arquitetural com duas referências que precisam ser entendidas sem ambiguidade:

- **runtime principal atualmente confirmado:** PySide6, iniciado por `python app.py`;
- **arquitetura-alvo mantida para evolução do frontend:** Tauri, com frontend orientado ao analista fiscal.

Isso significa que:

- a interface hoje em produção e manutenção ainda é a `MainWindow` em PySide6;
- a base atual em PySide6 continua sendo a referência mínima de comportamento funcional;
- a migração futura de frontend não deve perder filtros, colunas, exportação, destaque de tabelas e demais capacidades já existentes.

---

## Diretriz atual de frontend

A evolução do frontend deve seguir estas regras:

- a navegação principal do usuário fiscal deve ser organizada em três blocos:
  - **EFD**
  - **Documentos Fiscais**
  - **Análise Fiscal**
- a área técnica/manutenção não deve poluir o fluxo principal do analista fiscal;
- a área de EFD deve conter apenas visualizações e relações estritamente da EFD;
- cruzamentos entre EFD e documentos fiscais devem ficar em módulos próprios de **Análise Fiscal**;
- a prioridade inicial da interface é tabela, filtros, datas, códigos, colunas, ordenação, exportação e destaque de tabelas;
- toda migração deve reaproveitar o comportamento já maduro da UI atual.

---

## Referências operacionais principais

A referência operacional atual deve partir de:

- `docs/plano_042026.md`
- `docs/frontend_referencia_042026.md`
- `docs/matriz_migracao_frontend_042026.md`
- `docs/inventario_abas_mainwindow_042026.md`
- `docs/dossie_enquadramento_042026.md`

---

## Regra de uso

Enquanto o `README.md` principal não for fisicamente substituído:

1. este documento deve ser usado como referência de alinhamento de arquitetura;
2. o `README.md` antigo deve ser lido com a ressalva de que ele ainda reflete a linha PySide6 sem explicitar corretamente o alvo Tauri;
3. a evolução do projeto deve seguir o plano unificado e não a formulação antiga isolada.
