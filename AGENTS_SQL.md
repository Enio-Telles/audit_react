# AGENTS_SQL.md — Guia canônico de SQL, Oracle, extração e contratos analíticos

Este arquivo está alinhado ao plano canônico `docs/plano_042026.md`.
Se houver conflito entre práticas antigas, SQL histórica, documentos intermediários e este arquivo, prevalece o plano canônico atual.

## 1. Princípio central da camada de dados

No `audit_react`, o Oracle não é o lugar da experiência analítica final.

O Oracle serve para:

- extrair dados brutos com estabilidade;
- recortar os dados corretos do contexto em foco;
- devolver bases atômicas e rastreáveis;
- minimizar custo e risco operacional na origem.

A camada analítica do projeto vive principalmente em:

- Polars;
- Parquet;
- contratos tabulares do backend.

## 2. Regra do contexto global na extração

Toda extração deve respeitar o contexto oficial do sistema.

### 2.1 Contexto unitário

No modo por CNPJ, as consultas devem ser recortadas pelo contribuinte em foco e, quando aplicável, pelo período e pela data de corte.

### 2.2 Contexto de lote

No modo lote/Fisconforme, o backend pode trabalhar com múltiplos CNPJs, mas sem perder rastreabilidade por item.

Quando o lote for tratado na camada SQL, preferir:

- execução iterada por CNPJ quando isso simplifica;
- materialização controlada por item de lote;
- tabelas auxiliares ou mecanismos controlados de lote quando realmente necessários.

Evitar:

- listas literais gigantes em `IN (...)` montadas por string;
- consultas abertas sem recorte do contexto;
- lotes opacos sem rastreabilidade por CNPJ.

## 3. Regras obrigatórias para SQL Oracle

### 3.1 SQL estático em arquivo

Toda consulta deve ficar em arquivo `.sql` dentro de `sql/`.

Proibido:

- SQL montado por concatenação de string;
- SQL de negócio disperso em Python;
- f-strings com parâmetros injetados na query.

Obrigatório:

- arquivo SQL estático;
- leitura por catálogo ou helper existente;
- binds Oracle.

### 3.2 Bind variables

Obrigatório usar bind variables Oracle.

Exemplo correto:

```sql
SELECT col1, col2
FROM bi.alguma_tabela
WHERE cnpj_raiz = :CNPJ
  AND periodo >= :PERIODO_INICIAL
  AND periodo <= :PERIODO_FINAL
```

Proibido:

- `.format()`;
- interpolação com f-string;
- literais de CNPJ inseridos direto no texto SQL.

### 3.3 Extração atômica por entidade

A consulta ideal do projeto é simples, focada e previsível.

Exemplos esperados:

- registro EFD cru por bloco;
- documento fiscal cru;
- item fiscal cru;
- malha fiscal crua;
- tabela cadastral crua;
- movimentos de estoque em granularidade base.

### 3.4 Evitar `SELECT *`

Selecionar apenas colunas necessárias para:

- rastreabilidade;
- composição analítica posterior;
- renderização tabular;
- drill-down.

## 4. O que deve ficar no Oracle

Pode e deve ficar no Oracle:

- filtro por CNPJ/contexto;
- filtro por período;
- filtro por data de corte quando aplicável;
- leitura de tabelas e views de origem;
- preservação de chaves técnicas e naturais;
- recortes estáveis e reprodutíveis;
- extrações base por domínio fiscal.

## 5. O que não deve ficar no Oracle

Não colocar no Oracle, salvo exceção muito bem justificada:

- joins analíticos pesados entre domínios;
- reconstituição de estoque anual;
- cálculo de ICMS devido por competência;
- score, ranking ou heurística analítica;
- consolidação final para UX;
- cruzamento EFD x documentos fiscais como produto final;
- detecção final de inconsistências de negócio;
- agregações pesadas que o Polars resolve melhor;
- apresentações prontas de tela;
- lógica visual.

## 6. Regra operacional: Oracle simples, Polars forte

Quando houver dúvida entre resolver em Oracle ou em Polars, a preferência padrão é:

1. extrair a base atômica do Oracle;
2. materializar ou reutilizar dataset intermediário;
3. compor em Polars;
4. expor como contrato tabular no backend.

Exceções só devem existir quando houver evidência concreta de necessidade operacional.

## 7. Organização e nomenclatura de SQL

### 7.1 Fonte de verdade

A única fonte de verdade das consultas do projeto é `sql/`.

### 7.2 Nomenclatura

Usar nomes por entidade e granularidade, e não por tela ornamental.

Exemplos bons:

- `reg_0000_raw.sql`
- `reg_c100_raw.sql`
- `reg_c170_raw.sql`
- `cte_documentos_raw.sql`
- `nfe_itens_raw.sql`
- `malhas_fisconforme_raw.sql`
- `sitafe_fronteira_raw.sql`

Evitar nomes vagos como:

- `consulta_final.sql`
- `relatorio_tela_x.sql`
- `query_nova_ok.sql`

### 7.3 Estrutura recomendada

Manter a organização dentro de `sql/` por domínio fiscal já existente no projeto, reforçando a separação por contexto e granularidade.

## 8. Contratos tabulares para o frontend

Toda saída candidata a alimentar o frontend deve ser pensada para tabela.

### 8.1 A saída deve preservar

- colunas atômicas;
- nomes estáveis de coluna;
- chaves naturais ou técnicas suficientes para drill-down;
- origem do dado;
- `dataset_id` quando aplicável;
- `layer` quando aplicável;
- rastreabilidade da consulta ou dataset base.

### 8.2 A saída não deve embutir

- cosmetização de tela;
- textos consolidados apenas para exibição;
- rótulos prontos de card;
- agrupamentos artificiais pensados só para UI;
- semântica visual dentro do SQL.

## 9. Cache-first e reutilização

Antes de reexecutar Oracle, verificar:

- datasets já materializados por CNPJ;
- datasets compartilhados reaproveitáveis;
- parquets canônicos;
- resultados operacionais que já atendem o contrato da visão;
- cache já utilizado por Dossiê, Fisconforme ou catálogos.

A regra padrão é:

- reutilizar primeiro;
- consultar Oracle depois.

## 10. Regras por bloco fiscal

### 10.1 EFD

As consultas devem servir visualizações puras de escrituração.

Primeira onda:

- Bloco 0;
- Bloco C;
- Bloco H.

Bloco K fica fora desta etapa e não deve ser introduzido por expansão oportunista.

### 10.2 Documentos Fiscais

As consultas devem servir leitura documental por contexto, conforme a visualização aprovada:

- NF-e Emissão Própria;
- CT-e Transportes;
- Fisconforme;
- Sitafe / Fronteira.

### 10.3 Análise Fiscal

As consultas base devem alimentar composições em Polars para gerar visões como:

- Cruzamento NF-e x EFD;
- Estoque Mensal;
- Estoque Anual;
- ICMS devido por competência;
- Produtos com inconsistências;
- Ressarcimento ST.

## 11. Regras de performance e manutenção

### 11.1 Performance

- evitar múltiplas passagens redundantes quando Polars puder fazer em uma só;
- evitar reexecução de extrações já materializadas;
- evitar trazer colunas desnecessárias;
- evitar datasets finais gigantes quando visões particionadas resolvem melhor;
- priorizar filtros no ponto mais barato da arquitetura.

### 11.2 Manutenção

- SQL pequena é melhor que SQL brilhante porém opaca;
- nome claro é melhor que nome inventivo;
- uma consulta atômica reutilizável é melhor que uma query final montada para uma única tela;
- a regra de negócio deve morar no código analítico, não no SQL bruto.

## 12. Regras de bloqueio desta fase

Nesta fase, é proibido:

- empurrar a simplificação visual do sistema para dentro do Oracle;
- criar queries gigantes para substituir composição em Polars;
- abrir novas frentes de negócio via SQL;
- usar SQL para resolver layout de frontend;
- introduzir Bloco K na primeira onda;
- criar novas consultas sem antes verificar o catálogo SQL existente.

## 13. Verificação antes de propor mudanças

Antes de sugerir nova query, sempre verificar:

- se a consulta já existe no catálogo atual;
- se um dataset materializado já resolve;
- se a composição pode acontecer em Polars;
- se o frontend realmente precisa de nova saída;
- se a proposta respeita o contexto global e a taxonomia oficial.

## 14. Regra final

O objetivo não é escrever SQL impressionante.
O objetivo é produzir extrações atômicas, corretas, rastreáveis e rápidas, que permitam ao backend compor workbenches tabulares claros para o auditor fiscal, preservando o modo lote/Fisconforme e a área de Configuração & Acervo.
