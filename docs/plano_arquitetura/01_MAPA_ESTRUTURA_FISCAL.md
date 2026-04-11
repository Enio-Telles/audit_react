# Mapa da Estrutura Fiscal

## Princípio operacional

A lógica oficial do produto é:

1. extrair dados dos bancos de dados da forma mais rápida e eficiente possível;
2. unir, consolidar e materializar as informações em Parquet;
3. exibir os dados para visualização, auditoria e análise.

Isso implica uma separação obrigatória entre:
- camada de extração;
- camada de consolidação em Parquet;
- camada de apresentação.

A UI não deve depender de consultas SQL isoladas. Ela deve depender de datasets canônicos em Parquet.

---

## Navegação fiscal oficial

### 1. EFD
Objetivo: exibir a EFD de forma amigável, didática e auditável.

Subvisões:
- resumo EFD
- blocos da EFD
- registros da EFD
- árvore pai-filho
- dicionário

### 2. Documentos Fiscais
Objetivo: exibir NF-e, NFC-e e CT-e com filtros por papel do contribuinte, operação, período, item e status.

Subvisões:
- NF-e
- NFC-e
- CT-e
- informações complementares
- contatos extraídos do documento
- detalhe do documento

### 3. Fiscalização
Objetivo: consolidar fronteira, Fisconforme, malhas, chaves e resoluções.

Subvisões:
- fronteira
- fisconforme
- malhas e pendências
- chaves
- resoluções

### 4. Cruzamentos / Verificações / Classificação dos Produtos
Objetivo: reunir o núcleo analítico do sistema.

Subvisões:
- cruzamentos
- verificações
- classificação dos produtos

---

## Migração das abas atuais

- Estoque -> Cruzamentos
- Agregação -> Verificações
- Conversão -> Verificações
