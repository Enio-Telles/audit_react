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

No backend (e.g. `ler_sql.py` e extração de serviços), você repassa em Python:
```python
params = {"CNPJ": cnpj_pesquisado}
cursor.execute(sql_puro, params)
# Ou via o driver/serviço equivalente
```

---

## 4. Polars vs Oracledb

Ao interagir entre banco relacional e in-memory dataset, a obtenção de dados maciços **não deve** materializar listas gigantes de `dict`s em loops "for".
* Utilize mecanismos que importem batch ou result-sets direto ao DataFrame local (e.g. `pl.read_database` ou `pl.from_arrow`/`fetchall() -> DataFrame`).
* Queries do catálogo `Dossiê` usualmente resultam em pequenos retornos em linhas visíveis, o custo costuma estar nos joins imensos; garanta índices (ou use os indexes lógicos do data warehouse da SEFIN disponíveis como partições por tempo ou PK `co_cnpj_cpf`).

---

## 5. Regra de Limites e Histórico Temporal

Sempre que a lógica não impor explicitamente um intervalo, assuma que listagens contínuas (notas emitidas, estoque) dependem de uma trava para não causar *Timeouts*. 
* O baseline de consultas da receita (onde não tipificado) usualmente não deve se alongar de forma aberta desde a década de noventa, se houver filtro "por competência/referência", certifique-se da presença de `:ANO_MES_INICIO` até `:ANO_MES_FIM`.
* Dossiê de cadastro e metadados (`/dossie`) pode operar sem limite de tempo pois a agregação usualmente foca nas linhas mais recentes por CNPJ (ex: last status, historico situacao order by desc limit X ou fetch last 100).
