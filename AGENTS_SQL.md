# Agent_SQL.md — Guia de Padrões e Boas Práticas de Banco de Dados

## 1. Identidade e Propósito

Este documento dita as normas e restrições fundamentais no armazenamento, arquitetura, design e consumo de dados baseados em SQL (Oracle) neste ecossistema PySide/React/Polars.
Seja executando queries para tabelas isoladas ou consultas compostas do fluxo de **Dossiê**, siga *rigorosamente* as determinações deste guia.

### Prioridades Nucleares:
1. **Abstração Limpa:** Todo SQL deve morar em um arquivo estático e unitário com extensão `.sql` na pasta `/sql`.
2. **Sem concatenação string em Python:** Uso integral de *bind variables* seguras (`:CNPJ`, `:NOME`, `:IE`, `:DT_INICIO`).
3. **Reuso de Catálogo:** Regra do cache-first antes de reescrever queries idênticas.

---

## 2. Tratamento e Isolamento (Pasta `/sql`)

### 2.1 Regra do XML "Legacy"
Não parsear logicamente os legados da SEFIN exportados do Oracle (como o antigo `dossie_nif.xml`). 
**Diretriz de Conversão:**
- Todo relatório ou *display block* legados de XML, PL/SQL Developer ou BUs deverão ser explicitamente destrinchados, copiando seu core puro e salvo em `/sql/nome_curto.sql`.
- Substituições dos parâmetros legados via sintaxe de dois pontos (ex: substitua `?` ou tags XML para os binds universais deste sistema).

### 2.2 Estrutura e Nomenclatura no Dossiê
O `dossie_catalog.py` atua via resolução nominal, para isso deve-se usar os aliases definidos sem estressar IDs muito complicados.
* Exemplo de queries para o fluxo Dossiê:
  * `dossie_dados_cadastrais.sql`
  * `dossie_enderecos.sql`
  * `dossie_historico_socios.sql`

---

## 3. Especificações e Sintaxe de Bind Variables em Python

O motor adotado (geralmente `cx_Oracle` ou `oracledb`) funciona com bind dinâmico via dicionário em tempo de driver, o que protege contra injeção e possibilita "pre-compilação" (caching) de planos de execução do lado do Oracle.

* **❌ PROIBIDO:** `query = "SELECT * FROM bi.dm_pessoa WHERE cnpj = {}".format(cnpj)`
* **❌ PROIBIDO:** `query = f"SELECT * FROM tb WHERE foo = '{x}'"`
* **✅ OBRIGATÓRIO:** 
   ```sql
   SELECT a.cpf, b.empresa 
   FROM bi.dm_pessoa a 
   WHERE a.co_cnpj_cpf = :CNPJ
   ```

No backend (e.g. `ler_sql.py` e extrações com oracledb/cx_Oracle), você repassa em Python usando dicts. O driver mapeia automaticamente para as variáveis declaradas (evitando falhas por aspas vazias ou caracters estranhos):
```python
# Correto tratamento de Binds no Python
params = {
    "CNPJ": cnpj_pesquisado,
    "ANO_MES_INICIO": "202301",
    "ANO_MES_FIM": "202312"
}
cursor.execute(sql_puro, params)
```

---

## 4. Integração Polars vs Oracle

Ao interagir entre banco relacional e in-memory dataset, a obtenção de dados maciços **não deve** materializar listas gigantes de dicionários em loops `for`.
* Utilize `pl.read_database(query, connection, execute_options={"parameters": params})` como ponte nativa (por debaixo dos panos converte o iterador fetchmany via Arrow para alocações contíguas).
* Queries do catálogo `Dossiê` usualmente resultam em pequenos retornos em linhas visíveis, porém o processamento do lado do SGBD pode ser custoso (joins imensos). Verifique sempre o uso de CTEs (Common Table Expressions) lógicas contendo cláusulas com `WHERE a.co_cnpj_cpf = :CNPJ` ou subqueries para isolar partições antes do Join.

---

## 5. Regra de Limites e Janelas Temporais

Sempre que a lógica não impor explicitamente um intervalo, assuma que listagens contínuas (notas emitidas, estoque) dependem de uma trava para não causar *Timeouts* e saturamento de RAM.
* O baseline de consultas da receita (onde não tipificado) usualmente não deve se alongar de forma aberta desde a década de noventa. Se houver filtro "por competência/referência", certifique-se da presença de `:ANO_MES_INICIO` até `:ANO_MES_FIM`.
* Relatórios gerenciais cadastrais (`/dossie`) muitas vezes operam sem limite de tempo pois a agregação usualmente foca nas linhas mais recentes por CNPJ (ex: last status, historico situacao order by desc limit X ou `FETCH FIRST 100 ROWS ONLY`).
* Prefira limitar as projeções (SELECT) diretamente; evite usar `SELECT *`. Sempre enumere as colunas explícitas que o modelo Python do Polars irá exigir no schema de Output.
