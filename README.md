# Audit React

**Sistema de Auditoria e Análise Fiscal** — SEFIN

Aplicação web que combina a interface React do `sefin_audit_5` com a estrutura modular de análise de dados do `audit_pyside`, criando um pipeline completo de auditoria fiscal.

---

## Arquitetura

```
audit_react/
├── client/                          # Frontend React + TypeScript + Tailwind
│   ├── src/
│   │   ├── pages/                   # Páginas da aplicação
│   │   │   ├── Dashboard.tsx        # Visão geral, KPIs, atalhos
│   │   │   ├── Extracao.tsx         # Seleção CNPJ, consultas SQL, pipeline
│   │   │   ├── Consulta.tsx         # Visualização de tabelas Parquet
│   │   │   ├── Agregacao.tsx        # Agrupamento de produtos (De/Para)
│   │   │   ├── Conversao.tsx        # Fatores de conversão e unidades
│   │   │   ├── Estoque.tsx          # Movimentação, mensal, anual
│   │   │   └── Configuracoes.tsx    # Preferências e conexão Oracle
│   │   ├── components/
│   │   │   └── layout/
│   │   │       └── DashboardLayout.tsx  # Sidebar + header contextual
│   │   ├── hooks/
│   │   │   └── useAuditApi.ts       # Hooks para API do backend
│   │   ├── types/
│   │   │   └── audit.ts            # Tipos compartilhados
│   │   ├── App.tsx                  # Rotas e layout principal
│   │   └── index.css               # Tema Swiss Design Fiscal
│   └── index.html
│
├── server/
│   ├── python/
│   │   ├── api.py                   # API FastAPI (endpoints REST)
│   │   ├── requirements.txt         # Dependências Python
│   │   └── audit_engine/            # Motor de auditoria
│   │       ├── __init__.py
│   │       ├── contratos/
│   │       │   └── tabelas.py       # Schemas e dependências de tabelas
│   │       ├── modulos/
│   │       │   ├── produtos.py      # Geração de tabelas de produtos
│   │       │   ├── agregacao.py     # Agrupamento De/Para
│   │       │   ├── conversao.py     # Fatores e unidades
│   │       │   └── estoque.py       # Movimentação e saldos
│   │       ├── pipeline/
│   │       │   └── orquestrador.py  # Orquestração do pipeline
│   │       └── utils/
│   │           └── parquet_io.py    # I/O de arquivos Parquet
│   └── index.ts                     # Servidor Express (gateway)
│
└── shared/
    └── const.ts                     # Constantes compartilhadas
```

## Pipeline de Tabelas

O sistema gera 11 tabelas analíticas em ordem de dependência:

```
produtos_unidades  ──►  produtos  ──►  produtos_agrupados  ──►  fatores_conversao
                                              │                        │
                                              ▼                        ▼
                                        id_agrupados           produtos_final
                                                                       │
                                                                       ▼
                                                               nfe_entrada
                                                                       │
                                                                       ▼
                                                               mov_estoque
                                                                       │
                                                                       ▼
                                                               aba_mensal
                                                                       │
                                                                       ▼
                                                               aba_anual

                                        produtos_selecionados (derivado de produtos_final)
```

## Módulos do Frontend

| Página            | Descrição               | Funcionalidades                                        |
| ----------------- | ----------------------- | ------------------------------------------------------ |
| **Dashboard**     | Visão geral do sistema  | KPIs, status do pipeline, atalhos rápidos              |
| **Extração**      | Entrada de dados        | Seleção de CNPJ, consultas SQL, execução do pipeline   |
| **Consulta**      | Visualização de dados   | Browser de tabelas Parquet com filtros e paginação     |
| **Agregação**     | Agrupamento de produtos | Seleção múltipla, merge, desfazer, sugestão automática |
| **Conversão**     | Fatores de conversão    | Edição inline, importar/exportar Excel, recalcular     |
| **Estoque**       | Análise de estoque      | Movimentação, consolidação mensal/anual, omissões      |
| **Configurações** | Preferências            | Conexão Oracle, caminhos, status do backend            |

## Contratos de Tabelas

Cada tabela é definida por um **contrato** (`ContratoTabela`) que especifica:

- **Schema**: colunas com tipos e descrições
- **Dependências**: tabelas que precisam existir antes
- **Módulo/Função**: gerador responsável pela criação
- **Saída**: nome do arquivo Parquet

O orquestrador resolve a ordem topológica e executa os geradores automaticamente.

## Tecnologias

### Frontend

- React 19 + TypeScript
- Tailwind CSS 4 + shadcn/ui
- Wouter (roteamento)
- Recharts (gráficos)
- Lucide React (ícones)

### Backend

- FastAPI (API REST)
- Polars (processamento de dados)
- Parquet (armazenamento)
- Oracle DB (extração via oracledb)

### Design

- **Tema**: "Institutional Precision" — Swiss Design Fiscal
- **Tipografia**: DM Sans + JetBrains Mono
- **Paleta**: Sidebar escura (#0f172a) + workspace off-white (#f8fafc)
- **Primária**: Azul institucional (#1e40af)

## Desenvolvimento

```bash
# Frontend
pnpm install
pnpm dev

# Backend Python
cd server/python
pip install -r requirements.txt
uvicorn api:app --reload --port 8000
```

## Origem

Este projeto é uma fusão de:

- **sefin_audit_5**: Interface web React com dashboard, tabelas e pipeline visual
- **audit_pyside**: Estrutura modular Python com orquestração de pipeline, contratos de tabelas e módulos de análise fiscal (produtos, agregação, conversão, estoque)

## Licença

MIT
