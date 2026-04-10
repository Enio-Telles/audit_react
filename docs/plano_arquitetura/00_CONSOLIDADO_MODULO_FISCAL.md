# Consolidado do Módulo Fiscal

## 1. Objetivo

Este documento concatena a visão arquitetural, os contratos operacionais, a migração das abas antigas e o estado atual de implementação do novo módulo fiscal.

A lógica oficial do produto permanece:

1. extrair dados dos bancos da forma mais rápida e eficiente possível;
2. unir, consolidar e materializar as informações em Parquet;
3. exibir os dados para visualização, auditoria e análise.

---

## 2. Navegação fiscal oficial

A navegação principal do domínio Fiscal possui 4 itens:

1. EFD
2. Documentos Fiscais
3. Fiscalização
4. Cruzamentos / Verificações / Classificação dos Produtos

### Reposicionamento das abas antigas

- Estoque -> Cruzamentos
- Agregação -> Verificações
- Conversão -> Verificações

---

## 3. Arquitetura de dados

O modelo adotado no projeto é:

**banco de dados -> extração otimizada -> parquet canônico ou ponte legada -> API -> UI**

### Regra atual de transição

Enquanto os datasets canônicos finais não substituem toda a camada antiga, o módulo fiscal novo pode operar sobre duas bases:

- **datasets canônicos novos**, quando já existirem;
- **pontes legadas reais**, quando a informação ainda estiver materializada apenas pelos fluxos antigos.

Isso permite evolução incremental sem quebrar operação já existente.

---

## 4. Estado atual de implementação

### 4.1 EFD

Já implementado no módulo novo:

- resumo real por domínio;
- localização real de artefatos EFD legados;
- tabela operacional de `C170`;
- tabela operacional de `Bloco H`;
- filtro textual global;
- ordenação por coluna;
- detalhe do registro selecionado;
- paginação.

Artefatos hoje observados pela ponte:

- `c170_xml_{cnpj}.parquet`
- `c176_xml_{cnpj}.parquet`
- `bloco_h_{cnpj}.parquet`
- `c197_{cnpj}.parquet` ou `c197_agrupado_{cnpj}.parquet`
- `k200_{cnpj}.parquet`

### 4.2 Documentos Fiscais

Já implementado no módulo novo:

- resumo real por domínio;
- tabelas operacionais para `NF-e`, `NFC-e`, `CT-e`, `informações complementares` e `contatos`;
- filtro textual global;
- ordenação por coluna;
- detalhe do registro selecionado;
- paginação.

A localização dos artefatos usa nomes preferenciais e fallback por padrões em subpastas do CNPJ.

### 4.3 Fiscalização

Já implementado no módulo novo:

- resumo real por domínio;
- leitura do cache `dados_cadastrais.parquet` do Fisconforme;
- tabela operacional de `malhas.parquet`;
- listagem de DSFs relacionadas ao CNPJ;
- filtro textual global nas malhas;
- ordenação nas malhas;
- detalhe da malha selecionada;
- paginação de malhas.

### 4.4 Análise Fiscal

Já implementado no módulo novo:

- resumo real por domínio;
- tabelas operacionais para:
  - `mov_estoque`
  - `aba_mensal`
  - `aba_anual`
  - `produtos_agrupados`
  - `fatores_conversao`
  - `produtos_final`
- filtro textual global;
- ordenação por coluna;
- detalhe do registro selecionado;
- paginação.

---

## 5. Contratos de endpoints do módulo novo

### EFD
- `GET /api/fiscal/efd/resumo`
- `GET /api/fiscal/efd/datasets`
- `GET /api/fiscal/efd/c170`
- `GET /api/fiscal/efd/bloco-h`

### Documentos Fiscais
- `GET /api/fiscal/documentos/resumo`
- `GET /api/fiscal/documentos/datasets`
- `GET /api/fiscal/documentos/nfe`
- `GET /api/fiscal/documentos/nfce`
- `GET /api/fiscal/documentos/cte`
- `GET /api/fiscal/documentos/info-complementar`
- `GET /api/fiscal/documentos/contatos`

### Fiscalização
- `GET /api/fiscal/fiscalizacao/resumo`
- `GET /api/fiscal/fiscalizacao/datasets`
- `GET /api/fiscal/fiscalizacao/cadastro`
- `GET /api/fiscal/fiscalizacao/malhas`
- `GET /api/fiscal/fiscalizacao/dsfs`

### Análise Fiscal
- `GET /api/fiscal/analise/resumo`
- `GET /api/fiscal/analise/datasets`
- `GET /api/fiscal/analise/estoque-mov`
- `GET /api/fiscal/analise/estoque-mensal`
- `GET /api/fiscal/analise/estoque-anual`
- `GET /api/fiscal/analise/agregacao`
- `GET /api/fiscal/analise/conversao`
- `GET /api/fiscal/analise/produtos-base`

---

## 6. Situação da migração

### Já migrado para a nova UI

- leitura e inspeção operacional de EFD;
- leitura e inspeção operacional de documentos fiscais;
- leitura e inspeção operacional de fiscalização;
- leitura e inspeção operacional de cruzamentos e verificações.

### Ainda convivendo com legado

As abas antigas continuam existindo por compatibilidade e comparação funcional:

- Estoque
- Agregação
- Conversão

No desenho novo, elas já têm equivalentes ou pontes diretas no módulo fiscal.

---

## 7. Próximos passos recomendados

### Prioridade alta

- adicionar filtros por coluna no backend novo;
- destacar campos-chave por domínio no painel de detalhe;
- reduzir a dependência visual das abas legadas.

### Prioridade estrutural

- substituir pontes legadas por datasets canônicos finais;
- consolidar catálogos por domínio com metadata completa;
- revisar continuamente duplicações entre consultas SQL e datasets já materializados.

### Prioridade documental

- manter este consolidado como visão principal do estado atual;
- sincronizar `AGENTS_NOVO.md` e `AGENTS_SQL_NOVO.md` com o que já estiver implementado;
- mover documentos históricos antigos para referência secundária quando necessário.

---

## 8. Conclusão

O módulo fiscal novo já deixou de ser apenas um plano. Ele já opera sobre dados reais do projeto nos 4 domínios principais.

O foco daqui em diante não é mais provar a arquitetura. O foco passa a ser:

- usabilidade operacional;
- detalhamento e filtros;
- substituição progressiva das pontes legadas por contratos canônicos definitivos.
