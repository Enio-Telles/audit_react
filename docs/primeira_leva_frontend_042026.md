# primeira_leva_frontend_042026.md

## Objetivo

Definir a primeira leva concreta de migração do frontend, priorizando superfícies com:

- alto valor para o analista fiscal;
- funcionalidade já madura na UI atual em PySide6;
- baixo risco de ambiguidade de domínio;
- forte aderência ao frontend-alvo em Tauri/React.

Esta etapa não abre novos módulos.
Ela seleciona o que deve ser preservado e migrado primeiro.

---

## Escopo da primeira leva

A primeira leva do frontend deve priorizar:

1. mecanismo de destaque de tabelas;
2. tabela `mov_estoque`;
3. tabela mensal;
4. tabela anual;
5. tabela `NFe Entrada`.

Esses itens foram escolhidos porque:

- já existem na `MainWindow` atual com comportamento maduro;
- cobrem o núcleo da experiência tabular e analítica;
- validam a separação entre **Análise Fiscal** e **Documentos Fiscais**;
- permitem testar o shell Tauri sem depender ainda da implementação completa de EFD.

---

## Enquadramento por bloco fiscal

### Análise Fiscal

Devem entrar na primeira leva:

- `mov_estoque`
- `tabela mensal`
- `tabela anual`

### Documentos Fiscais

Deve entrar na primeira leva:

- `NFe Entrada`

### Recurso transversal obrigatório

Deve entrar junto:

- destaque de tabela em nova aba/janela, preservando contexto e comportamento.

---

## Contrato mínimo da primeira leva

### 1. Destaque de tabela

Toda tabela da primeira leva deve poder ser destacada em nova aba ou nova janela, preservando:

- contexto do CNPJ;
- filtros ativos;
- colunas visíveis;
- ordenação;
- posição analítica atual;
- capacidade de exportação.

### 2. Filtros

A primeira leva deve preservar, conforme aplicável:

- filtro textual;
- filtro por descrição;
- filtro por código;
- filtro por NCM;
- filtro por datas;
- filtro numérico por faixas;
- limpeza rápida de filtros.

### 3. Colunas e perfis

Deve preservar:

- seleção de colunas;
- reordenação quando aplicável;
- perfis salvos;
- persistência de preferências.

### 4. Exportação

Deve preservar:

- exportação dos dados filtrados;
- exportação coerente com o recorte visível ao usuário.

---

## Requisitos por superfície

### `mov_estoque`

**Bloco alvo**
- Análise Fiscal

**Requisitos mínimos**
- filtros por id, descrição, NCM e tipo;
- filtros por data;
- filtros numéricos;
- colunas configuráveis;
- perfis;
- exportação;
- destaque;
- preservação da trilha de auditoria já visível na UI atual.

### `tabela mensal`

**Bloco alvo**
- Análise Fiscal

**Requisitos mínimos**
- filtros por id, descrição, ano e mês;
- filtros numéricos;
- colunas configuráveis;
- perfis;
- exportação;
- destaque.

### `tabela anual`

**Bloco alvo**
- Análise Fiscal

**Requisitos mínimos**
- filtros por id, descrição e ano;
- filtros numéricos;
- colunas configuráveis;
- perfis;
- exportação;
- destaque;
- suporte ao filtro cruzado por seleção quando aplicável.

### `NFe Entrada`

**Bloco alvo**
- Documentos Fiscais

**Requisitos mínimos**
- filtros por id, descrição, NCM, co_sefin e datas;
- colunas configuráveis;
- perfis;
- exportação;
- destaque.

---

## O que fica fora desta primeira leva

Ainda não entram nesta etapa:

- bloco EFD completo;
- CT-e e fronteira;
- fisconforme como painel completo;
- agregação;
- conversão;
- consulta SQL;
- logs;
- configurações.

Esses itens continuam importantes, mas não devem atrasar a validação inicial do frontend-alvo.

---

## Critério de conclusão da primeira leva

A primeira leva será considerada concluída quando houver, no frontend-alvo:

- separação visível entre **Análise Fiscal** e **Documentos Fiscais**;
- renderização utilizável das quatro tabelas prioritárias;
- destaque funcional em nova aba/janela;
- manutenção dos filtros essenciais;
- manutenção de colunas, perfis e exportação;
- paridade mínima validada contra a `MainWindow` atual.

---

## Resultado esperado

Ao concluir esta primeira leva, o projeto passa a ter uma base concreta para:

1. validar o frontend-alvo sem perder o valor fiscal já entregue;
2. comprovar a separação entre área fiscal e área técnica;
3. estabelecer o padrão que será replicado depois em EFD, fisconforme, agregação e demais módulos.
