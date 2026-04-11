# AGENTS_SQL.md - Guia de padroes SQL, Oracle e contratos analiticos

## 1. Identidade e proposito

Este documento define as regras de arquitetura, escrita e consumo de SQL no projeto fiscal.
Ele vale para extracoes Oracle, consultas de apoio ao Dossie, contratos reutilizaveis para Polars/Parquet e dados expostos ao frontend analitico.

### Prioridades

1. Corretude fiscal e rastreabilidade acima de conveniencia.
2. SQL minima, atomica e reutilizavel.
3. Cache-first e reaproveitamento de datasets antes de nova consulta ao Oracle.
4. Joins, consolidacoes e regras analiticas preferencialmente fora do Oracle.
5. Contratos estaveis para FastAPI, React e shell desktop.

## 2. Regras estruturais para SQL

### 2.1 Localizacao e organizacao

1. Todo SQL deve morar em arquivo estatico `.sql` dentro de `sql/`.
2. Nao escrever SQL inline em Python quando a consulta puder viver no catalogo.
3. Toda query nova deve ter nome curto, claro e orientado a entidade, nao a tela.
4. SQL legada oriunda de XML, PL/SQL Developer ou export antigo deve ser destrinchada e salva como arquivo `.sql` independente.

### 2.2 Nomenclatura

1. Preferir nomes por entidade e granularidade.
2. Evitar nomes que descrevam apenas relatorio final ou tela final.
3. No Dossie, respeitar os aliases do catalogo e da resolucao nominal.

Exemplos validos:

- `dossie_dados_cadastrais.sql`
- `dossie_enderecos.sql`
- `dossie_historico_socios.sql`
- `reg_c170_raw.sql`
- `reg_c190_raw.sql`

## 3. Bind variables e seguranca

O acesso Oracle deve usar bind variables.

### Proibido

- `query = "SELECT * FROM bi.dm_pessoa WHERE cnpj = {}".format(cnpj)`
- `query = f"SELECT * FROM tb WHERE foo = '{x}'"`

### Obrigatorio

```sql
SELECT a.cpf, b.empresa
FROM bi.dm_pessoa a
WHERE a.co_cnpj_cpf = :CNPJ
```

```python
params = {
    "CNPJ": cnpj_pesquisado,
    "ANO_MES_INICIO": "202301",
    "ANO_MES_FIM": "202312",
}
cursor.execute(sql_puro, params)
```

## 4. Papel do Oracle vs Polars

### O que deve ficar no Oracle

1. Extracao base por entidade.
2. Filtro por CNPJ, periodo e versao valida.
3. Preservacao de chaves tecnicas e naturais.
4. Recortes minimos para reduzir custo de leitura.

### O que deve sair do Oracle

1. Join pesado entre dominios.
2. Agregacao mensal ou anual.
3. Score, ranking ou heuristica analitica.
4. Deduplicacao por regra de negocio.
5. Descricoes e campos montados apenas para UX.
6. Tabela final de trabalho pronta para tela.

### Regra operacional

Quando houver duvida entre resolver no Oracle ou no lake, preferir extrair a base granular e recompor em Polars.

## 5. Integracao com Polars e datasets compartilhados

1. Evitar materializar listas gigantes de dicionarios em loops.
2. Preferir leitura orientada a dataset reutilizavel.
3. Antes de reexecutar Oracle, verificar cache compartilhado e parquets canonicos.
4. Queries base devem suportar composicao posterior por Polars sem perder rastreabilidade.

## 6. Limites e janelas temporais

1. Toda consulta recorrente deve ter filtro temporal explicito quando o dominio permitir.
2. Evitar consulta aberta sem janela em movimentos, notas ou estoque.
3. Preferir projetar apenas colunas necessarias.
4. Evitar `SELECT *`.

## 7. Contratos para frontend analitico

O frontend alvo deve evoluir para uma interface minimalista no chrome, mas com o maximo possivel de recursos na visualizacao de tabelas.

### Regras

1. SQL e datasets nao devem embutir cosmetica de tela.
2. O enriquecimento para UX deve acontecer no backend e no frontend, sem distorcer a base fiscal.
3. Toda consulta candidata a alimentar grade analitica deve preservar colunas atomicas, e nao blocos textuais consolidados apenas para exibicao.
4. Sempre que fizer sentido, manter nos contratos de saida:
   - `origem_dado`
   - `sql_id_origem`
   - `tabela_origem`
   - chaves tecnicas e naturais suficientes para drill-down
5. Evitar nomes de colunas opacos ou descartaveis. A grade analitica depende de ordenacao, filtro, comparacao e exportacao sobre nomes estaveis.
6. Nao mover para o Oracle logica de destaque visual, agrupamento de interface ou descricao pronta para tela.
7. Quando uma consulta for base de workbench, priorizar colunas que suportem filtro avancado, ordenacao, comparacao, exportacao e rastreabilidade.
8. Quando houver conflito entre SQL mais bonita para exibir e dataset mais reutilizavel, escolher o dataset mais reutilizavel.

## 8. Regra final

O objetivo nao e produzir SQL impressionante.
O objetivo e produzir datasets corretos, rastreaveis, reutilizaveis e aptos a sustentar workbenches analiticos ricos com o menor custo possivel no Oracle.
