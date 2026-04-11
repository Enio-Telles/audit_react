# SQL Cache, Observabilidade e Versionamento de Schemas

## Objetivo

Este documento registra a implementação pragmática feita no repositório para três frentes propostas durante a análise técnica:

1. cache do catálogo SQL;
2. observabilidade básica do pipeline;
3. versionamento leve de schemas.

---

## 1. Cache do catálogo SQL

### Situação anterior

- `ler_sql.py` fazia leitura direta do disco a cada chamada;
- `sql_catalog.py` reindexava o catálogo repetidamente;
- consultas repetidas pagavam custo de `rglob()` e `read_text()` desnecessariamente.

### Implementação realizada

Arquivos envolvidos:

- `src/utilitarios/sql_cache.py`
- `src/utilitarios/ler_sql.py`
- `src/utilitarios/sql_catalog.py`

### O que entrou

- cache L1 em memória com política LRU;
- cache L2 persistido em `workspace/app_state/sql_cache/`;
- checksum por arquivo SQL para invalidar cache quando o conteúdo muda;
- estatísticas básicas de hit/miss;
- índice do catálogo em memória com `lru_cache`;
- função explícita de invalidação do catálogo.

### Ganho esperado

- menos chamadas redundantes de `rglob()`;
- menos I/O de leitura SQL repetida;
- menor latência na resolução de SQLs já usadas na sessão.

---

## 2. Observabilidade básica

### Situação anterior

- logging concentrado em erro em arquivo simples;
- sem métricas estruturadas reutilizáveis;
- sem camada simples para instrumentar etapas do pipeline.

### Implementação realizada

Arquivo envolvido:

- `src/observabilidade/__init__.py`

### O que entrou

- `configure_structured_logging()` com saída JSON;
- `observe_step(step_name)` para instrumentar duração e erro de etapas;
- `observe_records(step_name, records_processed)` para registrar volume processado;
- integração opcional com `prometheus-client`, sem tornar o código dependente rígido da lib.

### Uso sugerido

```python
from observabilidade import observe_records, observe_step

@observe_step("tb_documentos")
def processar_documentos(df):
    resultado = ...
    observe_records("tb_documentos", len(resultado))
    return resultado
```

---

## 3. Versionamento leve de schemas

### Situação anterior

- o projeto validava e consumia schemas, mas sem histórico simples persistido por dataset;
- não havia diff fácil do schema mais recente versus o schema atual de uma etapa.

### Implementação realizada

Arquivo envolvido:

- `src/utilitarios/schema_registry.py`

### O que entrou

- registro de snapshots de schema por tabela;
- hash de schema para evitar criar versão repetida sem mudança real;
- histórico salvo em `workspace/app_state/schema_registry.json`;
- diff entre schema atual e último schema conhecido;
- validação simples de colunas esperadas.

### Observação importante

Isto **não substitui Delta Lake**. É uma etapa leve de governança para entrar já, sem forçar mudança de formato físico do acervo. A evolução natural, se fizer sentido depois, é:

- Parquet + schema registry atual
- depois Delta Lake/Iceberg em datasets que realmente precisem de time-travel, ACID e evolução nativa

---

## 4. Testes adicionados

Arquivo:

- `tests/test_sql_cache.py`

Cobertura incluída:

- hit de cache após primeira materialização;
- limpeza básica do SQL em `ler_sql()`;
- registro e diff de versões de schema.

---

## 5. Próximos passos recomendados

### Prioridade alta

- instrumentar explicitamente o orquestrador e as etapas mais pesadas com `@observe_step`;
- expor estatísticas do cache SQL em endpoint ou painel de diagnóstico;
- registrar schemas dos datasets críticos logo após sua materialização.

### Prioridade média

- invalidar cache SQL quando arquivos forem alterados em desenvolvimento;
- separar métricas de leitura SQL, pipeline e API em namespaces próprios;
- destacar no frontend ou nas rotas administrativas quando houve mudança de schema entre versões.

### Prioridade estrutural

- avaliar Delta Lake ou Iceberg apenas para domínios em que time-travel e schema evolution tragam retorno real.
