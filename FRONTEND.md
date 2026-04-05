# Fiscal Parquet Analyzer — Web Frontend

Interface web completa espelhando a aplicação PySide6 existente, com backend FastAPI e frontend React 19.

## Arquitetura

```
backend/        FastAPI (Python) — expõe serviços existentes via REST
  main.py
  routers/
    cnpj.py           GET/POST/DELETE /api/cnpj, /files, /schema
    parquet.py        POST /api/parquet/query
    pipeline.py       POST /api/pipeline/run, GET /status
    estoque.py        GET /api/estoque/{cnpj}/mov_estoque|mensal|anual|...
    aggregation.py    GET /api/aggregation/{cnpj}/tabela_agrupada
    sql_query.py      GET/POST /api/sql/...

frontend/       React 19 + Vite + TypeScript + Tailwind CSS
  src/
    api/          axios client + tipos TypeScript
    store/        Zustand (estado global: CNPJ, arquivo, filtros, tabs)
    components/
      layout/     LeftPanel (lista CNPJs, arquivos, pipeline)
      table/      DataTable (TanStack Table) + FilterBar
      tabs/       ConsultaTab, ConsultaSqlTab, AgregacaoTab,
                  ConversaoTab, EstoqueTab, LogsTab
```

## Como rodar

### 1. Backend (porta 8000)

```powershell
conda activate audit
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Ou execute `backend/start.ps1`.

### 2. Frontend (porta 5173)

```powershell
cd frontend
npm run dev
```

Ou execute `frontend/start.ps1`.

Acesse **http://localhost:5173** — o Vite proxeia `/api` para o backend automaticamente.

## Funcionalidades implementadas

| Aba          | Funcionalidade                                              |
|--------------|-------------------------------------------------------------|
| Consulta     | Filtros dinâmicos, paginação, seleção de colunas, export CSV |
| Consulta SQL | Carregar arquivos .sql, editar e executar contra Oracle     |
| Agregação    | Tabela agrupada com busca por desc/NCM/CEST                 |
| Conversão    | Fatores de conversão de unidades com filtro                 |
| Estoque      | Subtabs: mov_estoque, tabela mensal, tabela anual, id_agrupados |
| Logs         | Status e progresso do pipeline em tempo real                |

## Tema visual

Dark navy matching a UI PySide6 original (`#0a1628` base, `#0f1b33` cards, azul accent).
