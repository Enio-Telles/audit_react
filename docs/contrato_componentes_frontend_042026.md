# contrato_componentes_frontend_042026.md

## Objetivo

Definir o contrato mínimo de componentes do frontend para a primeira leva de migração, garantindo que a implementação em Tauri/React preserve o comportamento fiscal já maduro da UI atual.

Este documento traduz o plano e a primeira leva em unidades implementáveis de frontend.

---

## Escopo coberto por este contrato

Este contrato cobre a primeira leva definida em `docs/primeira_leva_frontend_042026.md`:

- mecanismo de destaque de tabelas;
- tabela `mov_estoque`;
- tabela mensal;
- tabela anual;
- tabela `NFe Entrada`.

---

## Estrutura mínima de componentes

A primeira leva do frontend deve nascer com estes componentes lógicos:

### 1. Shell fiscal principal

Responsável por:

- contexto do CNPJ selecionado;
- navegação principal por blocos fiscais;
- roteamento entre **Análise Fiscal** e **Documentos Fiscais**;
- abertura de abas ou janelas destacadas.

### 2. Container de tabela fiscal

Componente-base reutilizável para todas as tabelas da primeira leva.

Deve concentrar:

- barra de filtros;
- grade principal;
- toolbar de colunas;
- toolbar de exportação;
- ação de destaque;
- persistência de preferências.

### 3. Painel de filtros

Deve suportar, conforme o dataset:

- filtro textual simples;
- filtro por descrição;
- filtro por código;
- filtro por NCM;
- filtro por datas inicial/final;
- filtro numérico por faixas;
- limpeza total de filtros;
- indicação visual dos filtros ativos.

### 4. Grade tabular principal

Deve suportar:

- ordenação por coluna;
- controle de visibilidade de colunas;
- reordenação quando aplicável;
- seleção de linhas quando aplicável;
- renderização eficiente de volumes grandes;
- preservação do recorte atual quando destacada.

### 5. Gestor de destaque

Responsável por abrir a mesma tabela em nova aba/janela, mantendo:

- dataset alvo;
- filtros ativos;
- colunas visíveis;
- ordenação atual;
- contexto do CNPJ;
- título funcional da visão destacada.

### 6. Gestor de perfis e preferências

Deve preservar:

- colunas visíveis;
- ordem das colunas quando aplicável;
- filtros salvos quando essa funcionalidade existir;
- preferência por tabela;
- contexto do usuário local.

### 7. Exportador

Deve exportar o recorte efetivamente visível ao usuário, e não uma consulta paralela diferente do que está na tela.

---

## Contrato por tela da primeira leva

### `mov_estoque`

**Bloco fiscal**
- Análise Fiscal

**Dataset esperado**
- movimentos de estoque em granularidade operacional.

**Comportamentos obrigatórios**
- filtros por id, descrição, NCM e tipo;
- filtros por data;
- filtros numéricos;
- ordenação;
- colunas configuráveis;
- exportação;
- destaque;
- preservação do contexto analítico.

**Observação**
- deve ser tratada como uma das superfícies de maior prioridade do sistema.

### `tabela mensal`

**Bloco fiscal**
- Análise Fiscal

**Dataset esperado**
- consolidação mensal derivada.

**Comportamentos obrigatórios**
- filtros por id, descrição, ano e mês;
- filtros numéricos;
- ordenação;
- colunas configuráveis;
- exportação;
- destaque.

### `tabela anual`

**Bloco fiscal**
- Análise Fiscal

**Dataset esperado**
- consolidação anual derivada.

**Comportamentos obrigatórios**
- filtros por id, descrição e ano;
- filtros numéricos;
- ordenação;
- colunas configuráveis;
- exportação;
- destaque;
- filtro cruzado por seleção quando necessário.

### `NFe Entrada`

**Bloco fiscal**
- Documentos Fiscais

**Dataset esperado**
- documentos NF-e/NFC-e de entrada consolidados para consulta fiscal.

**Comportamentos obrigatórios**
- filtros por id, descrição, NCM, co_sefin e datas;
- ordenação;
- colunas configuráveis;
- exportação;
- destaque.

---

## Contrato de estado compartilhado

O frontend precisa manter um estado mínimo compartilhado entre telas e destaques:

- `cnpjAtual`
- `blocoFiscalAtual`
- `datasetAtual`
- `filtrosAtivos`
- `colunasVisiveis`
- `ordenacaoAtual`
- `perfilAtual`
- `tituloDaVisao`

Esse estado deve poder ser serializado para permitir:

- reabertura da visão;
- destaque em nova aba/janela;
- persistência local;
- depuração funcional da navegação.

---

## Contrato de separação de domínio

A implementação desta primeira leva deve respeitar estritamente:

### Área fiscal do usuário

Pode exibir:

- tabelas fiscais;
- filtros;
- agregados visíveis ao usuário;
- exportação;
- navegação por blocos fiscais.

### Área técnica/manutenção

Não deve aparecer nesta primeira leva principal:

- SQL direto;
- logs;
- status técnico detalhado;
- configuração de conexão Oracle;
- elementos de depuração de runtime.

---

## Contrato de performance

A primeira leva deve nascer com preocupação de escala.

No mínimo, o frontend-alvo deve considerar:

- virtualização ou estratégia equivalente para tabelas grandes;
- atualização controlada de filtros;
- separação entre estado visual e estado de dados;
- evitar recomputações desnecessárias ao abrir destaques.

---

## Critério de aceitação

Este contrato será considerado atendido quando a primeira leva do frontend:

- renderizar `mov_estoque`, mensal, anual e `NFe Entrada` em blocos corretos;
- permitir filtros e ordenação utilizáveis;
- permitir destaque preservando contexto;
- preservar colunas, perfis e exportação;
- manter separação clara entre área fiscal e área técnica.

---

## Consequência prática

Com este contrato, a migração da primeira leva deixa de ser apenas uma decisão de escopo e passa a ter um alvo objetivo de implementação para frontend e integração com o shell Tauri.
