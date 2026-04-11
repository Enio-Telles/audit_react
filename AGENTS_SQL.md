# AGENTS_SQL.md — Guia canônico de SQL, Oracle e processamento analítico

Este documento define as regras obrigatórias para agentes que atuam na camada de extração Oracle, materialização Parquet e composição analítica em Polars no projeto `audit_react`.

Objetivo central:
extrair dados fiscais do Oracle de forma atômica e rápida, processar de forma otimizada em Polars/Parquet e alimentar um frontend tabular simples, guiado por contexto.

## 1. Princípio fundamental

O Oracle não é o lugar da experiência analítica final.

O Oracle deve fornecer dados brutos e filtrados pelo contexto.
A aplicação deve compor, cruzar, calcular e materializar os resultados reutilizáveis fora do banco.

## 2. Regra do contexto

Toda extração e toda composição devem respeitar o contexto de execução.

Contextos suportados:

- contexto por CNPJ
- contexto de lote, quando aplicável ao fluxo específico

Regras obrigatórias:

1. Não executar extrações abertas sem filtro de contexto quando o domínio exigir CNPJ ou lote.
2. Não criar SQL que dependa de a UI pedir novamente um CNPJ fora do contexto global.
3. Toda query recorrente deve estar preparada para receber bind variables de contexto.

## 3. Papel do Oracle

O Oracle deve concentrar apenas:

- extração atômica por entidade
- filtro por CNPJ, período e versão válida
- recorte mínimo necessário
- preservação de chaves técnicas e naturais
- leitura estável e rastreável

O Oracle não deve concentrar:

- cruzamentos pesados entre domínios
- consolidações anuais pesadas
- cálculo de estoque final consolidado
- cálculo de ICMS devido por competência
- score, ranking e heurísticas analíticas
- tabela final pronta para UX
- descrições cosméticas de tela

## 4. Regras obrigatórias de SQL

### 4.1 Localização

1. Todo SQL deve morar em arquivo estático `.sql` dentro de `sql/`.
2. Não escrever SQL inline em Python quando puder viver no catálogo.
3. O catálogo SQL continua sendo a única fonte de verdade das consultas.
4. Queries novas devem ser orientadas à entidade e à granularidade, não ao nome da tela.

### 4.2 Bind variables

O acesso Oracle deve usar bind variables.

Proibido:

- interpolação por f-string
- `.format()` para parâmetros
- concatenação manual de valores na query

Obrigatório:

```sql
SELECT *
FROM efd_bloco_c
WHERE cnpj_raiz = :CNPJ
  AND periodo >= :PERIODO_INICIAL
  AND periodo <= :PERIODO_FINAL
```

### 4.3 Projeção mínima

1. Evitar `SELECT *` em rotas recorrentes.
2. Projetar apenas colunas necessárias para composição posterior.
3. Sempre preservar as chaves necessárias para drill-down e rastreabilidade.

### 4.4 Janelas temporais

1. Toda consulta recorrente deve ter filtro temporal explícito quando o domínio permitir.
2. Evitar extrair movimentos, notas e estoque sem janela temporal quando isso gerar volume desnecessário.
3. A data de corte da EFD deve ser respeitada no recorte de extração quando aplicável.

## 5. Paradigma de extração simplificada

A abordagem correta do projeto é:

1. extrair a base bruta do contexto;
2. materializar em Parquet ou dataset reutilizável;
3. compor em Polars;
4. expor ao frontend por contratos tabulares estáveis.

### Exemplo correto

Bom:

```sql
SELECT
  cnpj_raiz,
  periodo,
  chv_nfe,
  cfop,
  vl_opr,
  vl_icms
FROM efd_bloco_c
WHERE cnpj_raiz = :CNPJ
  AND periodo BETWEEN :PERIODO_INICIAL AND :PERIODO_FINAL
```

Depois, em Polars:

- cruzar com documentos fiscais;
- calcular agregações;
- identificar inconsistências;
- materializar dataset para tabela analítica.

### Exemplo incorreto

Ruim:

```sql
SELECT cnpj, ano, SUM(estoque_inicial + compras - vendas) AS estoque_final
FROM efd_gigante a
JOIN nfe_gigante b ON ...
GROUP BY cnpj, ano
```

## 6. Papel do Polars e dos datasets materializados

O Polars deve concentrar:

- joins entre EFD e documentos
- reconstituição de estoque mensal e anual
- cálculo de ICMS devido por competência
- identificação de inconsistências
- ressarcimento ST
- deduplicação por regra de negócio
- tabelas finais de trabalho reutilizáveis
- composição de datasets para o frontend tabular

Regras obrigatórias:

1. Reaproveitar datasets materializados antes de reconsultar Oracle.
2. Favorecer cache-first.
3. Evitar loops Python que materializam listas gigantes de dicionários.
4. Preferir operações vetorizadas e composição em Polars.
5. Preservar proveniência do dado materializado.

## 7. Contratos para o frontend tabular

O frontend alvo é um workbench orientado a tabelas.

Portanto, toda saída relevante deve preservar, quando aplicável:

- `dataset_id`
- `bloco_fiscal`
- `origem_dado`
- `sql_id_origem`
- `tabela_origem`
- chaves técnicas e naturais
- paginação
- filtros aplicados
- ordenação
- colunas estáveis
- metadados de proveniência

Regras obrigatórias:

1. Não embutir cosmética de tela no SQL.
2. Não devolver payload opaco sem estrutura tabular reaproveitável.
3. Quando houver conflito entre query bonita para a tela e dataset reutilizável, escolher o dataset reutilizável.
4. O backend deve montar contratos estáveis para que a mesma tabela possa abrir em nova janela mantendo o contexto.

## 8. Taxonomia funcional obrigatória

As extrações e composições devem respeitar a taxonomia oficial do frontend.

### EFD

Primeira onda:

- Bloco 0
- Bloco C
- Bloco H

Regra obrigatória:

- não implementar Bloco K nesta primeira onda.

### Documentos Fiscais

Primeira onda:

- NF-e Emissão Própria
- CT-e Transportes
- Fisconforme
- Sitafe / Fronteira

### Análise Fiscal

Primeira onda:

- Cruzamento NF-e x EFD
- Estoque Mensal
- Estoque Anual
- ICMS devido por competência
- Produtos com inconsistências
- Ressarcimento ST

## 9. Regras de bloqueio de escopo

Enquanto o plano canônico estiver ativo:

- não criar novos domínios SQL fora da taxonomia oficial;
- não mover cálculo analítico pesado para o Oracle;
- não criar consultas orientadas à UI técnica antiga;
- não abrir novas frentes de negócio;
- não quebrar o reaproveitamento do modo lote/Fisconforme;
- não substituir serviços maduros sem necessidade comprovada.

## 10. Regras de rastreabilidade

Toda transformação relevante deve manter rastreabilidade suficiente para auditoria.

Sempre que fizer sentido, preservar:

- chave do contribuinte
- chave do documento
- período
- dataset materializado de origem
- SQL de origem ou alias do catálogo
- layer materializada
- caminho do dataset quando aplicável

## 11. Regra final

O objetivo deste projeto não é produzir SQL impressionante.
O objetivo é produzir extrações simples, rápidas e rastreáveis no Oracle e entregar, por Polars/Parquet, datasets corretos e reutilizáveis para um frontend fiscal tabular, simples e orientado por contexto.
