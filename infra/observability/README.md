# Observability Stack

Este diretório contém uma stack mínima de observabilidade pronta para subir localmente com:

- Prometheus
- Grafana
- Marquez (OpenLineage)
- PostgreSQL para o Marquez

## Como subir

1. suba a API localmente em `http://localhost:8000`
2. exporte o endpoint de lineage:

```bash
export OPENLINEAGE_URL=http://localhost:5000/api/v1/lineage
export OPENLINEAGE_NAMESPACE=audit_react
```

3. suba a stack:

```bash
docker compose -f docker-compose.observability.yml up -d
```

## Enderecos

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001` (`admin` / `admin`)
- Marquez API: `http://localhost:5000`
- Marquez UI: `http://localhost:3002`

## Observacao

A stack foi preparada para desenvolvimento local. Em producao, ajuste:

- retention de Prometheus
- seguranca do Grafana
- volumes persistentes
- autenticacao da API OpenLineage
