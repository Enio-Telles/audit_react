# Validacao local de Delta, OpenLineage e Observabilidade

## Objetivo

Este documento registra o passo seguinte da trilha ja implantada:

- chaveamento seletivo de Delta por tabela;
- validacao local da stack Prometheus/Grafana/Marquez;
- emissao sintetica de lineage;
- endpoints de diagnostico.

---

## 1. Delta seletivo por tabela

O projeto agora suporta ativacao global de Delta e restricao por tabelas especificas.

### Variaveis de ambiente

```bash
export DATA_LAKE_FORMAT=delta
export DELTA_ENABLED_TABLES=tb_documentos,movimentacao_estoque,calculos_mensais
export DELTA_WRITE_MODE=overwrite
```

### Regra

- se `DATA_LAKE_FORMAT=parquet`, tudo continua em Parquet;
- se `DATA_LAKE_FORMAT=delta` e `DELTA_ENABLED_TABLES` estiver vazio, tudo passa a Delta;
- se `DATA_LAKE_FORMAT=delta` e `DELTA_ENABLED_TABLES` tiver itens, apenas essas tabelas usam Delta.

---

## 2. Endpoints de diagnostico

### `GET /api/observabilidade/status`

Retorna:

- estatisticas do cache SQL;
- status/configuracao de OpenLineage;
- configuracao ativa do Delta.

### `GET /api/observabilidade/stack-smoke`

Verifica conectividade basica por socket para:

- API local
- Prometheus
- Grafana
- Marquez API
- Marquez UI

### `POST /api/observabilidade/openlineage/test`

Emite um evento sintetico de lineage para testar a integracao com Marquez.

Payload exemplo:

```json
{
  "cnpj": "00000000000000",
  "job_name": "audit_react.smoke"
}
```

---

## 3. Script de validacao

Arquivo:

- `scripts/validate_local_stack.py`

### Uso

```bash
python scripts/validate_local_stack.py
```

O script chama:

- `/api/health`
- `/api/observabilidade/status`
- `/api/observabilidade/stack-smoke`
- `/api/observabilidade/openlineage/test`

---

## 4. Sequencia sugerida de teste local

1. subir a API em `localhost:8000`;
2. configurar `OPENLINEAGE_URL`;
3. subir `docker compose -f docker-compose.observability.yml up -d`;
4. rodar `python scripts/validate_local_stack.py`;
5. depois executar pipeline real com algumas tabelas em Delta seletivo.

---

## 5. Tabelas sugeridas para primeira ativacao Delta

Para reduzir risco de migracao brusca, a primeira onda recomendada e:

- `tb_documentos`
- `movimentacao_estoque`
- `calculos_mensais`

Essas tabelas tendem a ser boas candidatas porque concentram bastante valor analitico e ajudam a validar:

- materializacao Delta
- registro de schema
- leitura posterior do dataset
- visibilidade em observabilidade/lineage
