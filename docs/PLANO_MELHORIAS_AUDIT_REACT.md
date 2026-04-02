# Plano Estruturado de Melhorias — `audit_react`

> SEFIN/RO — Sistema de Auditoria Fiscal  
> Versão do plano: 1.0 | Abril/2026  
> Base analisada: branch `main` — 54 commits

---

## Sumário

1. [Contexto e Objetivos](#1-contexto-e-objetivos)
2. [Diagnóstico Geral](#2-diagnóstico-geral)
3. [Melhorias por Categoria](#3-melhorias-por-categoria)
   - [M1 — Limpeza do Repositório](#m1--limpeza-do-repositório)
   - [M2 — Segurança](#m2--segurança)
   - [M3 — Performance do Backend](#m3--performance-do-backend)
   - [M4 — Arquitetura — Eliminar Gateway Node.js](#m4--arquitetura--eliminar-gateway-nodejs)
   - [M5 — Pipeline — Execução Paralela](#m5--pipeline--execução-paralela)
   - [M6 — Frontend — Auditoria de Dependências](#m6--frontend--auditoria-de-dependências)
   - [M7 — Qualidade — Testes de Contrato](#m7--qualidade--testes-de-contrato)
   - [M8 — Organização de Documentação](#m8--organização-de-documentação)
4. [Matriz de Prioridade](#4-matriz-de-prioridade)
5. [Roadmap de Execução](#5-roadmap-de-execução)
6. [Critérios de Conclusão](#6-critérios-de-conclusão)

---

## 1. Contexto e Objetivos

O `audit_react` é um sistema fullstack de auditoria fiscal da SEFIN/RO resultante da fusão dos projetos `sefin_audit_5` (backend Python/Polars) e `audit_pyside` (interface desktop migrada para React). O sistema extrai dados do Oracle, processa via pipeline Polars em camadas Parquet e expõe uma UI para análise por CNPJ.

**Objetivos deste plano:**

- Reduzir débito técnico acumulado na fusão dos projetos
- Melhorar segurança dos endpoints
- Aumentar performance do pipeline e das consultas
- Simplificar a stack de runtime
- Estabelecer base de testes confiável para crescimento sustentável

---

## 2. Diagnóstico Geral

| Área | Status | Risco |
|------|--------|-------|
| Repositório Git | Artefatos temporários e de agentes IA commitados | Médio |
| Segurança | Path traversal possível via CNPJ não validado | Alto |
| Performance backend | Parquet relido do disco a cada requisição | Médio |
| Arquitetura | Gateway Node.js desnecessário entre browser e FastAPI | Médio |
| Pipeline | Execução 100% sequencial com etapas paralelizáveis | Baixo/Médio |
| Frontend deps | ~20 pacotes Radix UI instalados, uso real desconhecido | Baixo |
| Testes | Suite `test_api.py` existente, sem testes de contrato de schema | Médio |
| Documentação | Arquivos de planejamento dispersos na raiz | Baixo |

---

## 3. Melhorias por Categoria

---

### M1 — Limpeza do Repositório

**Problema:** Artefatos de agentes IA (Jules/Google, Qwen), `venv/` Python, PDFs temporários e scripts de migração avulsos estão versionados, inflando o repositório e poluindo o histórico.

**Arquivos/diretórios a remover do tracking:**

```bash
# Executar na raiz do repositório
git rm -r --cached venv/
git rm -r --cached .Jules/ .jules/ .qwen/ .tmp-decompiled/
git rm --cached .tmp-*.pdf
git rm --cached tmp_extract.py tmp_schema.txt
git rm --cached plan.txt plano_novo.md ideas.md
git rm --cached python_migrate_contracts.py
git rm --cached python_migrate_generators.py
git rm --cached python_migrate_generators_2.py
git rm --cached fix_relatorio_fiscal.py_patch
git commit -m "chore: remover artefatos temporários e de agentes IA"
```

**Adições ao `.gitignore`:**

```gitignore
# Ambientes Python
venv/
.venv/
__pycache__/
*.pyc

# Artefatos de agentes IA
.Jules/
.jules/
.qwen/
.tmp-decompiled/

# Temporários de desenvolvimento
.tmp-*.pdf
*.py_patch
tmp_*.py
tmp_*.txt

# Arquivos de plano/rascunho (manter apenas em docs/)
plan.txt
plano_novo.md
ideas.md
```

**Resultado esperado:** redução estimada de 30–60 MB no tamanho do repositório; clone mais limpo para novos colaboradores.

---

### M2 — Segurança

#### M2.1 — Validação de CNPJ nos endpoints (Path Traversal)

**Problema:** Endpoints como `/api/tabelas/{cnpj}` e `/api/storage/{cnpj}/manifesto` recebem o CNPJ diretamente como path parameter e o usam para construir caminhos de arquivo (`storage/CNPJ/{cnpj}/...`). Sem validação, uma entrada maliciosa como `../../../etc` pode expor arquivos do sistema.

**Solução — adicionar dependência de validação em `api.py`:**

```python
import re
from fastapi import HTTPException, Depends

def validar_cnpj(cnpj: str) -> str:
    """Aceita apenas 14 dígitos numéricos — bloqueia path traversal."""
    if not re.match(r'^\d{14}$', cnpj):
        raise HTTPException(status_code=400, detail="CNPJ inválido: deve conter 14 dígitos numéricos")
    return cnpj

# Uso nos endpoints:
@app.get("/api/tabelas/{cnpj}")
def listar_tabelas(cnpj: str = Depends(validar_cnpj), camada: str = "parquets"):
    ...

@app.get("/api/storage/{cnpj}/manifesto")
def manifesto(cnpj: str = Depends(validar_cnpj)):
    ...
```

#### M2.2 — Verificar caminhos Windows hardcoded

**Problema:** O README menciona `C:\funcoes - Copia\sql` como referência de origem dos SQLs. Verificar se há caminhos Windows hardcoded em scripts ou SQLs.

```bash
# Auditoria de caminhos Windows nos fontes
grep -r "C:\\\\" server/python/ --include="*.py" --include="*.sql" --include="*.txt"
grep -r "funcoes - Copia" server/python/
```

Qualquer caminho encontrado deve ser substituído por variável de ambiente ou path relativo ao projeto.

#### M2.3 — Auditoria do `.env.example`

Verificar se todos os campos sensíveis estão documentados no `.env.example` e se nenhum valor real foi commitado por engano:

```bash
git log --all --full-history -- .env
git grep -i "password\|senha\|secret\|token\|dsn" -- "*.py" "*.ts" "*.json"
```

---

### M3 — Performance do Backend

#### M3.1 — Cache LRU de Parquet

**Problema:** Cada requisição à API relê o arquivo Parquet do disco, mesmo que o arquivo não tenha sido modificado. Em consultas repetidas na aba "Consulta", isso gera latência desnecessária.

**Solução — cache com invalidação por `mtime`:**

```python
from functools import lru_cache
import polars as pl
from pathlib import Path

@lru_cache(maxsize=64)
def _parquet_cached(caminho: str, mtime: float) -> pl.DataFrame:
    """Cache LRU invalidado automaticamente quando o arquivo muda."""
    return pl.read_parquet(caminho)

def ler_tabela(cnpj: str, nome: str, camada: str = "parquets") -> pl.DataFrame:
    path = Path(f"storage/CNPJ/{cnpj}/{camada}/{nome}.parquet")
    if not path.exists():
        raise FileNotFoundError(f"Tabela não encontrada: {path}")
    mtime = path.stat().st_mtime  # chave de invalidação
    return _parquet_cached(str(path), mtime)
```

**Resultado esperado:** leituras repetidas da mesma tabela: 10–50x mais rápido (memória vs. disco).

#### M3.2 — Streaming de tabelas grandes

Para tabelas com muitas linhas (ex: `nfe_entrada`, `mov_estoque`), implementar paginação no endpoint de leitura em vez de retornar o DataFrame inteiro:

```python
@app.get("/api/tabelas/{cnpj}/{nome}")
def ler_tabela_paginada(
    cnpj: str = Depends(validar_cnpj),
    nome: str,
    camada: str = "parquets",
    pagina: int = 0,
    tamanho: int = 500,
):
    df = ler_tabela(cnpj, nome, camada)
    total = len(df)
    slice_ = df.slice(pagina * tamanho, tamanho)
    return {
        "total": total,
        "pagina": pagina,
        "dados": slice_.to_dicts(),
    }
```

O frontend (AG Grid) já suporta paginação — basta conectar.

---

### M4 — Arquitetura — Eliminar Gateway Node.js

**Problema:** A stack atual adiciona uma camada intermediária desnecessária:

```
Browser → Node.js/Express (server/index.ts) → FastAPI Python (:8000)
```

O gateway Node faz apenas duas coisas: serve os estáticos do Vite e faz proxy de `/api`. Ambas podem ser feitas pelo FastAPI.

**Solução — FastAPI serve estáticos + proxy direto no dev:**

```python
# server/python/api.py — adicionar ao final, após todas as rotas
from fastapi.staticfiles import StaticFiles

# Em produção: serve o build do Vite
app.mount("/", StaticFiles(directory="../../dist", html=True), name="static")
```

```typescript
// vite.config.ts — proxy direto para FastAPI no dev
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
```

```json
// package.json — remover scripts que dependem de server/index.ts
// "start": "NODE_ENV=production node dist/index.js"  ← substituir por:
// "start": "uvicorn api:app --host 0.0.0.0 --port 8000"
```

**Resultado esperado:** eliminar Node.js como dependência de runtime; reduzir uma camada de latência; simplificar o deploy e o diagnóstico de problemas.

> **Nota:** Manter o `server/index.ts` no repositório comentado/arquivado durante a transição, removendo apenas após validar o novo fluxo.

---

### M5 — Pipeline — Execução Paralela

**Problema:** As 13 etapas do pipeline executam em sequência estrita, mas a topologia de dependências permite paralelismo em vários pontos.

**Análise de dependências:**

```
Nível 0 (sem deps):     produtos_unidades, produtos
Nível 1:                produtos_agrupados
Nível 2:                fatores_conversao
Nível 3:                produtos_final
Nível 4:                id_agrupados
Nível 5:                nfe_entrada
Nível 6:                mov_estoque
Nível 7 (paralelo):     aba_mensal ║ aba_anual
Nível 8:                produtos_selecionados
Nível 9 (paralelo):     ajustes_e111 ║ st_itens
```

**Solução — orquestrador assíncrono por nível:**

```python
import asyncio
from typing import Callable

async def executar_em_paralelo(*geradores: Callable, cnpj: str):
    """Executa múltiplos geradores do mesmo nível em paralelo."""
    loop = asyncio.get_event_loop()
    tarefas = [
        loop.run_in_executor(None, gerador, cnpj)
        for gerador in geradores
    ]
    await asyncio.gather(*tarefas)

async def executar_pipeline_async(cnpj: str):
    # Nível 0
    await executar_em_paralelo(gen_produtos_unidades, gen_produtos, cnpj=cnpj)
    # Níveis 1–6 (sequencial por dependência)
    for gen in [gen_produtos_agrupados, gen_fatores_conversao,
                gen_produtos_final, gen_id_agrupados,
                gen_nfe_entrada, gen_mov_estoque]:
        await asyncio.get_event_loop().run_in_executor(None, gen, cnpj)
    # Nível 7 — paralelo
    await executar_em_paralelo(gen_aba_mensal, gen_aba_anual, cnpj=cnpj)
    # Nível 8
    await asyncio.get_event_loop().run_in_executor(None, gen_produtos_selecionados, cnpj)
    # Nível 9 — paralelo
    await executar_em_paralelo(gen_ajustes_e111, gen_st_itens, cnpj=cnpj)
```

**Resultado esperado:** redução estimada de 20–40% no tempo total de execução do pipeline, especialmente nos níveis 0 e 9.

---

### M6 — Frontend — Auditoria de Dependências

**Problema:** O `package.json` lista todos os ~20 componentes Radix UI e várias outras bibliotecas que podem não estar em uso, inflando o bundle.

**Passo 1 — identificar o que é realmente importado:**

```bash
# Mapear imports reais no client/
grep -r "from '@radix-ui" client/src --include="*.tsx" --include="*.ts" | \
  grep -oP "@radix-ui/react-\w+" | sort -u

# Verificar outras libs suspeitas
grep -r "embla-carousel\|input-otp\|cmdk\|vaul\|framer-motion" \
  client/src --include="*.tsx" -l
```

**Candidatos a remoção (verificar antes de remover):**

| Pacote | Suspeita |
|--------|----------|
| `@radix-ui/react-aspect-ratio` | Raro em UIs de dados |
| `@radix-ui/react-hover-card` | Raro em UIs de dados |
| `@radix-ui/react-menubar` | Substituível por dropdown |
| `@radix-ui/react-navigation-menu` | Verificar uso |
| `embla-carousel-react` | Sistema fiscal não usa carrossel |
| `input-otp` | Sem autenticação OTP aparente |
| `@types/google.maps` | Sem uso de Maps identificado |
| `streamdown` | Verificar uso |
| `vaul` | Drawer component — verificar |

**Passo 2 — analisar bundle após remoção:**

```bash
pnpm build
# Verificar dist/assets/*.js — tamanho antes e depois
```

**Resultado esperado:** redução de 15–30% no tamanho do bundle JavaScript.

---

### M7 — Qualidade — Testes de Contrato de Schema

**Problema:** O pipeline evolui rapidamente (54 commits, 41 PRs) sem testes que garantam que os schemas das tabelas gold permanecem estáveis. O `comparar_paridade_externa.py` faz isso manualmente para a trilha ST, mas não há cobertura automática.

**Solução — adicionar `tests/test_contratos.py`:**

```python
# server/python/tests/test_contratos.py
import polars as pl
import pytest
from pathlib import Path

CNPJ_PILOTO = "37671507000187"
BASE = Path(f"storage/CNPJ/{CNPJ_PILOTO}")

def parquet(camada: str, nome: str) -> pl.DataFrame:
    path = BASE / camada / f"{nome}.parquet"
    if not path.exists():
        pytest.skip(f"Parquet não encontrado: {path}")
    return pl.read_parquet(path)

# ── Contratos da camada gold ──────────────────────────────────────────

def test_fatores_conversao_schema():
    df = parquet("parquets", "fatores_conversao")
    assert "descricao_padrao" in df.columns, "Campo canônico ausente"
    assert "fator" in df.columns
    assert df["fator"].dtype in (pl.Float64, pl.Float32)
    assert df.filter(pl.col("fator").is_null()).is_empty(), "Fator nulo encontrado"

def test_st_itens_schema():
    df = parquet("parquets", "st_itens")
    campos_obrigatorios = ["cnpj", "chave_nfe", "item", "vbc_st", "vicms_st"]
    for campo in campos_obrigatorios:
        assert campo in df.columns, f"Campo obrigatório ausente: {campo}"

def test_ajustes_e111_schema():
    df = parquet("parquets", "ajustes_e111")
    assert "cod_ajuste" in df.columns
    assert "valor_ajuste" in df.columns
    assert df["valor_ajuste"].dtype in (pl.Float64, pl.Float32, pl.Decimal)

def test_nfe_entrada_sem_duplicatas():
    df = parquet("parquets", "nfe_entrada")
    chaves = df.select(["chave_nfe", "item"]).filter(
        pl.col("chave_nfe").is_not_null()
    )
    assert chaves.is_duplicated().sum() == 0, "Duplicatas em nfe_entrada"

def test_mov_estoque_saldo_nao_negativo():
    df = parquet("parquets", "mov_estoque")
    if "saldo" in df.columns:
        negativos = df.filter(pl.col("saldo") < 0)
        assert len(negativos) == 0, f"{len(negativos)} registros com saldo negativo"

# ── Contratos da camada silver ────────────────────────────────────────

def test_c176_xml_campos_enriquecidos():
    df = parquet("silver", "c176_xml")
    # Deve ter campos diretos do C176 + dados de entrada XML
    assert "chave" in df.columns
    assert "item" in df.columns

def test_nfe_dados_st_silver():
    df = parquet("silver", "nfe_dados_st")
    assert len(df) > 0, "silver/nfe_dados_st vazia"
```

**Integrar ao CI (quando houver):**

```bash
# Executar junto com os testes existentes
cd server/python
python -m pytest -q tests/test_api.py tests/test_contratos.py
```

---

### M8 — Organização de Documentação

**Problema:** Arquivos de planejamento, análises e rascunhos estão misturados na raiz com arquivos de configuração do projeto.

**Estrutura proposta:**

```
docs/
├── README.md                          # pipeline funcional (já existe)
├── arquitetura.md                     # ← novo: diagrama e decisões
├── pipeline.md                        # ← mover IMPLEMENTACAO_CONCLUIDA.md
├── referencias_sql.md                 # ← mover IMPLEMENTACAO_REFERENCIAS_RESUMO.md
├── importacao_referencias.md          # ← mover RESUMO_IMPORTACAO_REFERENCIAS.md
├── ux_analysis.md                     # ← mover UX_ANALYSIS.md
├── entrega_relatorios.md              # ← mover ENTREGA_RELATORIOS.md
├── plano_melhorias.md                 # ← este documento
└── historico/
    ├── plano_estruturado_original.md  # ← mover plano_estruturado_*.md
    └── plano_novo.md                  # ← mover plano_novo.md
```

**Comando de reorganização:**

```bash
mkdir -p docs/historico
git mv IMPLEMENTACAO_CONCLUIDA.md docs/pipeline.md
git mv IMPLEMENTACAO_REFERENCIAS_RESUMO.md docs/referencias_sql.md
git mv RESUMO_IMPORTACAO_REFERENCIAS.md docs/importacao_referencias.md
git mv UX_ANALYSIS.md docs/ux_analysis.md
git mv ENTREGA_RELATORIOS.md docs/entrega_relatorios.md
git mv plano_estruturado_concluir_implementacao_audit_react.md docs/historico/plano_estruturado_original.md
git mv plano_novo.md docs/historico/plano_novo.md
git mv AGENTS.md docs/agents.md
git commit -m "docs: reorganizar documentação em docs/"
```

---

## 4. Matriz de Prioridade

```
IMPACTO
  │
A │  M2.1 (segurança)    M3.1 (cache Parquet)
L │  M1 (limpeza git)    M7 (testes contrato)
T │
O ├──────────────────────────────────────────
  │  M4 (sem Node.js)    M5 (pipeline paralelo)
B │  M6 (deps frontend)
A │
I │  M2.2 (paths Win)    M8 (docs)
X │  M2.3 (env audit)
O │
  └──────────────────────────────────────────
         BAIXO ESFORÇO      ALTO ESFORÇO
```

| ID | Melhoria | Esforço | Impacto | Prioridade |
|----|----------|---------|---------|------------|
| M2.1 | Validação CNPJ (path traversal) | 🟢 Baixo | 🔴 Alto | **P0** |
| M1 | Limpeza .gitignore + git rm | 🟢 Baixo | 🟠 Médio-Alto | **P1** |
| M3.1 | Cache LRU Parquet | 🟢 Baixo | 🟠 Médio-Alto | **P1** |
| M7 | Testes de contrato de schema | 🟡 Médio | 🔴 Alto | **P1** |
| M4 | Eliminar gateway Node.js | 🟡 Médio | 🟠 Médio | **P2** |
| M3.2 | Paginação de tabelas grandes | 🟡 Médio | 🟠 Médio | **P2** |
| M5 | Pipeline paralelo | 🟡 Médio | 🟠 Médio | **P2** |
| M6 | Auditoria de deps frontend | 🟢 Baixo | 🟡 Baixo-Médio | **P3** |
| M2.2 | Verificar paths Windows | 🟢 Baixo | 🟡 Baixo-Médio | **P3** |
| M8 | Reorganização de docs | 🟢 Baixo | 🟡 Baixo | **P4** |

---

## 5. Roadmap de Execução

### Sprint 1 — Segurança e Higiene (1–2 dias)

- [ ] **M2.1** — Implementar `validar_cnpj()` como dependência FastAPI em todos os endpoints com `{cnpj}`
- [ ] **M1** — Executar limpeza do repositório e atualizar `.gitignore`
- [ ] **M2.2** — Auditar caminhos Windows hardcoded nos fontes Python
- [ ] **M2.3** — Revisar `.env.example` e fazer `git grep` por secrets

### Sprint 2 — Performance e Qualidade (3–5 dias)

- [ ] **M3.1** — Implementar cache LRU com invalidação por `mtime`
- [ ] **M7** — Criar `tests/test_contratos.py` com cobertura das tabelas gold
- [ ] **M3.2** — Adicionar paginação no endpoint de leitura de tabelas

### Sprint 3 — Arquitetura (1 semana)

- [ ] **M4** — Configurar FastAPI para servir estáticos; ajustar `vite.config.ts`; validar em dev e produção; remover `server/index.ts`
- [ ] **M5** — Refatorar orquestrador do pipeline para execução por nível com `asyncio.gather`

### Sprint 4 — Otimizações Menores (1–2 dias)

- [ ] **M6** — Auditar imports reais, remover deps não usadas, medir bundle
- [ ] **M8** — Reorganizar documentação em `docs/`

---

## 6. Critérios de Conclusão

Cada melhoria é considerada concluída quando:

| ID | Critério de Aceite |
|----|-------------------|
| M1 | `git ls-files venv/ .Jules/ .jules/ .qwen/` retorna vazio |
| M2.1 | Requisição `GET /api/tabelas/../../../etc` retorna HTTP 400 |
| M2.2 | `grep -r "C:\\\\"` retorna 0 resultados |
| M3.1 | Segunda requisição à mesma tabela responde em < 10ms |
| M3.2 | Endpoint aceita `?pagina=0&tamanho=500` e retorna `total` |
| M4 | `pnpm build && uvicorn api:app` serve frontend sem Node.js |
| M5 | Tempo de pipeline com CNPJ piloto reduz ≥ 15% |
| M6 | `pnpm build` — tamanho do bundle JS reduz ≥ 10% |
| M7 | `pytest tests/test_contratos.py` passa com CNPJ piloto |
| M8 | Raiz do repo contém apenas configs, `README.md` e código-fonte |

---

*Documento gerado em 02/04/2026 com base na análise do branch `main` do repositório `Enio-Telles/audit_react`.*
