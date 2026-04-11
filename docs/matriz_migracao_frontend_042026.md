# matriz_migracao_frontend_042026.md

## Objetivo

Registrar a matriz inicial de migracao entre a interface atual em PySide6 e o frontend alvo orientado ao analista fiscal com shell Tauri.

Este documento existe para evitar reescrita cega.
A UI atual deve ser tratada como referencia minima de comportamento.
O frontend de referencia em React/TypeScript deve ser tratado como referencia de forma, navegacao e separacao de dominio.

---

## Premissas obrigatorias

1. Runtime atual confirmado: PySide6.
2. Alvo arquitetural mantido: Tauri.
3. O frontend principal deve ser orientado ao analista fiscal.
4. A area tecnica deve ficar separada do fluxo principal.
5. Nenhuma migracao pode perder:
   - filtros
   - ordenacao
   - selecao de colunas
   - perfis
   - exportacao
   - destaque de tabela
6. Nenhuma frente nova deve ser aberta antes de fechar backend + frontend do escopo atual.

---

## Referencias de base

### Referencia de forma e navegacao

- mock React/TypeScript adotado em abril/2026;
- tres blocos fiscais principais:
  - EFD
  - Documentos Fiscais
  - Analise Fiscal
- area tecnica separada.

### Referencia minima de comportamento

- `MainWindow` atual em PySide6;
- suporte ja existente a filtros, filtros por data, filtros numericos, perfis, selecao de colunas, exportacao, tabelas destacadas, agregacao, estoque, NFe Entrada e SQL.

---

## Matriz inicial proposta

| Origem atual em PySide6 | Evidencia funcional atual | Bloco alvo no frontend | Status de enquadramento | Regra de migracao |
|---|---|---|---|---|
| Configuracoes | conexoes Oracle, cache, tema, status tecnico | Manutencao / T.I. | fora do fluxo principal do usuario | nao migrar para a area fiscal principal |
| Consulta SQL | console SQL, parametros, execucao Oracle, exportacao | Manutencao / T.I. | fora do fluxo principal do usuario | manter isolado do analista fiscal |
| Logs | leitura operacional e diagnostico | Manutencao / T.I. | fora do fluxo principal do usuario | nao poluir navegacao fiscal |
| Consulta | tabela generica com filtros, colunas e exportacao | ponte temporaria | parcial | reclassificar por dominio antes da migracao final |
| Agregacao | agrupamento, filtros relacionais, lote agregado, reversao | Analise Fiscal | aderente | migrar preservando filtros, colunas e destaque |
| Conversao | fatores, unid_ref, importacao/exportacao, recalculo | Analise Fiscal | aderente | manter como modulo de suporte analitico |
| Estoque > mov_estoque | filtros avancados, datas, numericos, exportacao, destaque | Analise Fiscal | aderente | alta prioridade de paridade |
| Estoque > Tabela mensal | filtros por ano/mes e numericos, exportacao, destaque | Analise Fiscal | aderente | alta prioridade de paridade |
| Estoque > Tabela anual | filtros por ano e numericos, exportacao, destaque | Analise Fiscal | aderente | alta prioridade de paridade |
| Estoque > Resumo global | consolidacao mensal/anual | Analise Fiscal | aderente | manter como resumo derivado |
| Estoque > Produtos selecionados | consolidacao por id_agregado | Analise Fiscal | aderente | manter exportacao multipla |
| Estoque > id_agrupados | consolidacao de grupos | Analise Fiscal | aderente | manter navegacao analitica |
| NFe Entrada | filtros, datas, colunas, perfis, exportacao, destaque | Documentos Fiscais | aderente | migrar como tabela de consulta principal |
| Analise Lote CNPJ / Fisconforme | painel funcional especializado | Documentos Fiscais | aderente | posicionar sob fisconforme |
| CPF/CNPJ + arvore de arquivos | selecao operacional de contexto e artefatos | contexto global / manutencao assistida | parcial | separar contexto fiscal do explorador tecnico de arquivos |

---

## Reclassificacao funcional alvo

### 1. EFD

Deve receber apenas visualizacao e navegacao estritamente de escritutacao.
A implementacao futura deve ser organizada por blocos do Guia Pratico.

Estado atual:
- nao confirmado como bloco autonomo na UI principal.
- ainda precisa nascer como modulo proprio.

### 2. Documentos Fiscais

Deve concentrar:
- NF-e
- CT-e
- fisconforme
- fronteira

Estado atual:
- NFe Entrada ja existe como base concreta.
- Fisconforme ja existe como painel dedicado.
- CT-e e fronteira ainda precisam de consolidacao formal na navegacao.

### 3. Analise Fiscal

Deve concentrar:
- cruzamentos EFD x documentos;
- estoque;
- agregacao;
- conversao;
- ressarcimento ST;
- modulos de inconsistencia e conciliacao.

Estado atual:
- estoque, agregacao e conversao ja existem como base concreta.
- precisam ser reorganizados sob a taxonomia-alvo.

### 4. Manutencao / T.I.

Deve concentrar:
- status Oracle e runtime;
- logs;
- SQL direto;
- configuracoes de conexao;
- operacao e diagnostico.

Estado atual:
- ja existe na pratica, mas ainda misturado com a area principal.

---

## Contrato minimo de paridade funcional

Toda tabela migrada para o frontend alvo deve manter, quando aplicavel:

- filtro textual;
- filtro por datas;
- filtro por codigos;
- filtro numerico;
- ordenacao;
- selecao de colunas;
- perfis salvos;
- exportacao;
- destaque em nova aba ou janela;
- preservacao de contexto do CNPJ e do recorte atual.

---

## Prioridade tecnica de migracao

### P0

- mov_estoque
- tabela mensal
- tabela anual
- NFe Entrada
- destaque de tabelas

### P1

- agregacao superior/inferior
- produtos selecionados
- id_agrupados
- fisconforme

### P2

- conversao
- resumo global
- dossie contextual

### P3

- SQL direto
- configuracoes
- logs

---

## Observacao final

A migracao nao deve partir de nomes de abas atuais apenas.
Ela deve partir da combinacao entre:

- valor fiscal para o usuario;
- maturidade funcional ja existente;
- facilidade de separar manutencao de analise;
- compatibilidade com a navegacao alvo baseada em EFD, Documentos Fiscais e Analise Fiscal.
