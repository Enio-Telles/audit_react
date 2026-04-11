# inventario_abas_mainwindow_042026.md

## Objetivo

Inventariar as abas reais da `MainWindow` atual em PySide6 para:

- separar o que pertence ao analista fiscal do que pertence a manutencao/T.I.;
- definir o destino funcional de cada area na arquitetura alvo;
- registrar a paridade minima que a migracao deve preservar.

Este documento parte do codigo real da `main`, nao de mock ou plano antigo.

---

## Estrutura principal confirmada na UI atual

A `MainWindow` atual contem as seguintes abas principais:

- Configuracoes
- Consulta
- Consulta SQL
- Agregacao
- Conversao
- Estoque
- NFe Entrada
- Analise Lote CNPJ
- Logs

A aba `Estoque` contem subtabs confirmadas:

- Tabela mov_estoque
- Tabela mensal
- Tabela anual
- Resumo Global
- Produtos selecionados
- id_agrupados

---

## Reclassificacao por bloco futuro

### Manutencao / T.I.

#### 1. Configuracoes

**Funcao atual**
- conexoes Oracle;
- status de conectividade;
- cache;
- tema;
- persistencia em `.env`.

**Destino futuro**
- Manutencao / T.I.

**Justificativa**
- e uma area operacional e tecnica;
- nao deve poluir o fluxo principal do analista fiscal.

**Paridade minima**
- testes de conexao;
- persistencia de configuracao;
- leitura de status.

#### 2. Consulta SQL

**Funcao atual**
- leitura de SQL do catalogo;
- formulario dinamico de parametros;
- execucao Oracle;
- exportacao;
- destaque da tabela de resultados.

**Destino futuro**
- Manutencao / T.I.

**Justificativa**
- e ferramenta de apoio tecnico e validacao;
- nao deve ser navegado como modulo fiscal principal.

**Paridade minima**
- selecao de SQL;
- parametros;
- execucao;
- filtro textual em resultado;
- exportacao;
- destaque.

#### 3. Logs

**Funcao atual**
- leitura de logs internos e historico operacional.

**Destino futuro**
- Manutencao / T.I.

**Justificativa**
- e painel de diagnostico e observabilidade.

**Paridade minima**
- leitura de logs;
- atualizacao simples.

---

### Analise Fiscal

#### 4. Agregacao

**Funcao atual**
- tabela superior filtravel;
- tabela inferior com historico/agregados;
- filtros relacionais por NCM/CEST/GTIN;
- perfis;
- colunas;
- destaque;
- reprocessamento;
- reversao de agrupamento.

**Destino futuro**
- Analise Fiscal

**Justificativa**
- e um modulo analitico de consolidacao e governanca de produto;
- nao e EFD pura nem documento fiscal puro.

**Paridade minima**
- duas grades;
- filtros relacionais;
- selecao e reversao;
- perfis;
- colunas;
- destaque;
- reprocessamento.

#### 5. Conversao

**Funcao atual**
- tabela de fatores de conversao;
- filtros por id_agrupado e descricao;
- ajuste de unidade de referencia;
- importacao/exportacao Excel;
- recalculo de derivados.

**Destino futuro**
- Analise Fiscal

**Justificativa**
- e modulo analitico de normalizacao e consistencia de unidades.

**Paridade minima**
- filtros;
- edicao controlada;
- importacao/exportacao;
- destaque;
- recalculo posterior.

#### 6. Estoque > Tabela mov_estoque

**Funcao atual**
- tabela operacional de movimentos;
- filtros por id, descricao, NCM, tipo;
- filtros por data;
- filtros numericos;
- perfis;
- colunas;
- exportacao;
- destaque;
- menu contextual e trilha de auditoria.

**Destino futuro**
- Analise Fiscal

**Justificativa**
- e uma das principais superfícies analiticas do sistema.

**Paridade minima**
- todas as capacidades atuais devem ser preservadas.

#### 7. Estoque > Tabela mensal

**Funcao atual**
- visao mensal consolidada;
- filtros por id, descricao, ano, mes e faixa numerica;
- perfis;
- colunas;
- exportacao;
- destaque.

**Destino futuro**
- Analise Fiscal

**Paridade minima**
- filtros temporais;
- filtros numericos;
- exportacao;
- destaque.

#### 8. Estoque > Tabela anual

**Funcao atual**
- visao anual consolidada;
- filtros por id, descricao, ano e faixa numerica;
- perfis;
- colunas;
- exportacao;
- destaque;
- filtro cruzado com selecao.

**Destino futuro**
- Analise Fiscal

**Paridade minima**
- filtros;
- cruzamento por selecao;
- exportacao;
- destaque.

#### 9. Estoque > Resumo Global

**Funcao atual**
- consolidacao derivada da mensal e anual.

**Destino futuro**
- Analise Fiscal

**Paridade minima**
- calculo consolidado;
- exportacao.

#### 10. Estoque > Produtos selecionados

**Funcao atual**
- consolidacao por id_agregado;
- filtros por id, descricao, ano e data;
- exportacao multipla de mov_estoque, mensal, anual e ICMS devido;
- destaque.

**Destino futuro**
- Analise Fiscal

**Paridade minima**
- consolidacao multipla;
- filtros temporais;
- exportacao multipla;
- destaque.

#### 11. Estoque > id_agrupados

**Funcao atual**
- tabela de grupos consolidados;
- filtros;
- colunas;
- exportacao;
- destaque.

**Destino futuro**
- Analise Fiscal

**Paridade minima**
- filtros;
- exportacao;
- destaque.

#### 12. Consulta

**Funcao atual**
- grade generica de parquet com filtros, filtros rapidos, colunas, perfis, exportacao e destaque.

**Destino futuro**
- suporte transversal temporario

**Justificativa**
- nao e modulo fiscal final;
- hoje funciona como workbench geral de inspeção.

**Paridade minima**
- manter enquanto a navegacao final por blocos nao estiver pronta.

---

### Documentos Fiscais

#### 13. NFe Entrada

**Funcao atual**
- visao de NF-e/NFC-e de entrada;
- filtros por id, descricao, NCM, co_sefin e datas;
- perfis;
- colunas;
- exportacao;
- destaque.

**Destino futuro**
- Documentos Fiscais

**Justificativa**
- e tabela de documento fiscal consolidado;
- deve virar um dos pilares desse bloco.

**Paridade minima**
- filtros por data e codigo;
- exportacao;
- destaque;
- perfis;
- colunas.

#### 14. Analise Lote CNPJ / Fisconforme

**Funcao atual**
- painel funcional dedicado a Fisconforme nao atendido.

**Destino futuro**
- Documentos Fiscais

**Justificativa**
- esta ligado ao universo documental/fiscal externo ao bloco puro de EFD.

**Paridade minima**
- manter painel dedicado.

---

### EFD

#### 15. EFD

**Estado atual**
- nao foi confirmada uma aba principal autonoma de EFD na `MainWindow` atual.

**Destino futuro**
- bloco proprio do frontend

**Lacuna**
- ainda precisa nascer como navegacao dedicada organizada por blocos do Guia Pratico.

**Regra obrigatoria**
- nao misturar cruzamentos externos dentro da area pura de EFD.

---

## Capacidades transversais confirmadas no codigo atual

As seguintes capacidades aparecem repetidamente e formam o contrato minimo da migracao:

- filtros textuais;
- filtros por datas;
- filtros numericos;
- ordenacao;
- selecao e reordenacao de colunas;
- perfis salvos;
- exportacao;
- destaque de tabela em janela separada;
- preservacao de contexto do CNPJ;
- persistencia de preferencias por tabela.

---

## Prioridade de migracao recomendada

### P0

- mov_estoque
- tabela mensal
- tabela anual
- NFe Entrada
- mecanismo de destaque de tabelas

### P1

- produtos selecionados
- id_agrupados
- agregacao
- fisconforme

### P2

- conversao
- resumo global
- consulta generica como workbench temporario

### P3

- configuracoes
- consulta SQL
- logs

---

## Resultado operacional deste inventario

Com este inventario, o projeto passa a ter base concreta para:

1. separar area fiscal e area tecnica;
2. definir a primeira leva de migracao para Tauri/React;
3. evitar perda de comportamento ja maduro na UI atual;
4. preparar a criacao futura do bloco EFD sem misturar dominios.
