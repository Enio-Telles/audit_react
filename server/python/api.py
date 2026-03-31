"""
API FastAPI — audit_react
Endpoints REST para comunicação entre frontend React e audit_engine Python.
"""
import json
import logging
import os
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from audit_engine.contratos.base import (
    CONTRATOS,
    listar_contratos,
    obter_contrato,
    ordem_topologica,
)
from audit_engine.pipeline.orquestrador import OrquestradorPipeline

# Configuração
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Audit React API",
    description="API do sistema de auditoria fiscal",
    version="1.0.0",
)

# Restrict CORS to specific origins instead of ["*"] for security
# This prevents Cross-Site Request Forgery (CSRF) and data theft from malicious domains
raw_origins = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5173,http://localhost:3000,http://localhost:8000"
).split(",")

# Strip whitespace and filter out the wildcard to prevent CSRF and startup errors
allowed_origins = [
    origin.strip() for origin in raw_origins
    if origin.strip() and origin.strip() != "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "Authorization", "Accept"],
)

# Diretório base para CNPJs
BASE_DIR = Path("/storage/CNPJ")


# === Modelos Pydantic ===

class ExecucaoRequest(BaseModel):
    cnpj: str
    consultas: List[str] = []
    data_limite: Optional[str] = None
    tabelas_alvo: Optional[List[str]] = None


class EdicaoFatorRequest(BaseModel):
    id_agrupado: str
    unid_ref: Optional[str] = None
    fator: Optional[float] = None


class AgregacaoRequest(BaseModel):
    ids_produtos: List[int]
    descricao_padrao: Optional[str] = None


class DesagregacaoRequest(BaseModel):
    id_grupo: str


# === Endpoints: Sistema ===

@app.get("/api/health")
async def health_check():
    return {"status": "ok", "version": "1.0.0"}


@app.get("/api/contratos")
async def listar_contratos_endpoint():
    """Lista todos os contratos de tabelas registrados."""
    contratos = listar_contratos()
    return [
        {
            "nome": c.nome,
            "descricao": c.descricao,
            "modulo": c.modulo,
            "funcao": c.funcao,
            "dependencias": c.dependencias,
            "saida": c.saida,
            "colunas": [
                {"nome": col.nome, "tipo": col.tipo.value, "descricao": col.descricao}
                for col in c.colunas
            ],
        }
        for c in contratos
    ]


@app.get("/api/contratos/ordem")
async def ordem_execucao():
    """Retorna a ordem topológica de execução das tabelas."""
    return {"ordem": ordem_topologica()}


# === Endpoints: Pipeline ===

@app.post("/api/pipeline/executar")
async def executar_pipeline(request: ExecucaoRequest):
    """Executa o pipeline completo ou parcial para um CNPJ."""
    cnpj_limpo = request.cnpj.replace(".", "").replace("/", "").replace("-", "")
    diretorio = BASE_DIR / cnpj_limpo

    if not diretorio.exists():
        diretorio.mkdir(parents=True, exist_ok=True)

    orquestrador = OrquestradorPipeline(diretorio, cnpj_limpo)
    resultado = orquestrador.executar_pipeline_completo(
        tabelas_alvo=request.tabelas_alvo
    )

    return {
        "cnpj": resultado.cnpj,
        "status": resultado.status,
        "duracao_ms": resultado.duracao_total_ms,
        "tabelas_geradas": resultado.tabelas_geradas,
        "erros": resultado.erros,
        "etapas": [
            {
                "tabela": e.tabela,
                "status": e.status.value,
                "mensagem": e.mensagem,
                "duracao_ms": e.duracao_ms,
                "registros": e.registros_gerados,
            }
            for e in resultado.etapas
        ],
    }


@app.post("/api/pipeline/reprocessar")
async def reprocessar_pipeline(cnpj: str, tabela_editada: str):
    """Reprocessa tabelas dependentes após edição."""
    cnpj_limpo = cnpj.replace(".", "").replace("/", "").replace("-", "")
    diretorio = BASE_DIR / cnpj_limpo

    if not diretorio.exists():
        raise HTTPException(status_code=404, detail="CNPJ não encontrado")

    orquestrador = OrquestradorPipeline(diretorio, cnpj_limpo)
    resultado = orquestrador.reprocessar_a_partir_de(tabela_editada)

    return {
        "cnpj": resultado.cnpj,
        "status": resultado.status,
        "tabelas_reprocessadas": resultado.tabelas_geradas,
    }


@app.get("/api/pipeline/status/{cnpj}")
async def status_pipeline(cnpj: str):
    """Verifica integridade dos parquets de um CNPJ."""
    cnpj_limpo = cnpj.replace(".", "").replace("/", "").replace("-", "")
    diretorio = BASE_DIR / cnpj_limpo

    if not diretorio.exists():
        raise HTTPException(status_code=404, detail="CNPJ não encontrado")

    orquestrador = OrquestradorPipeline(diretorio, cnpj_limpo)
    integridade = orquestrador.verificar_integridade()

    return {
        "cnpj": cnpj_limpo,
        "tabelas": integridade,
        "completo": all(integridade.values()),
    }


# === Endpoints: Tabelas / Parquet ===

@app.get("/api/tabelas/{cnpj}")
async def listar_tabelas(cnpj: str):
    """Lista tabelas Parquet disponíveis para um CNPJ."""
    cnpj_limpo = cnpj.replace(".", "").replace("/", "").replace("-", "")
    diretorio = BASE_DIR / cnpj_limpo / "parquets"

    if not diretorio.exists():
        return {"tabelas": []}

    tabelas = []
    for arquivo in diretorio.glob("*.parquet"):
        contrato = CONTRATOS.get(arquivo.stem)
        tabelas.append({
            "nome": arquivo.stem,
            "arquivo": str(arquivo),
            "tamanho_bytes": arquivo.stat().st_size,
            "descricao": contrato.descricao if contrato else "",
        })

    return {"tabelas": tabelas}


@app.get("/api/tabelas/{cnpj}/{nome_tabela}")
async def ler_tabela(
    cnpj: str,
    nome_tabela: str,
    pagina: int = Query(1, ge=1),
    por_pagina: int = Query(50, ge=1, le=1000),
    filtro_coluna: Optional[str] = None,
    filtro_valor: Optional[str] = None,
    ordenar_por: Optional[str] = None,
    ordem: str = Query("asc", pattern="^(asc|desc)$"),
):
    """Lê dados de uma tabela Parquet com paginação e filtros."""
    cnpj_limpo = cnpj.replace(".", "").replace("/", "").replace("-", "")
    diretorio_parquets = BASE_DIR / cnpj_limpo / "parquets"
    arquivo = diretorio_parquets / f"{nome_tabela}.parquet"

    # Prevenir path traversal vulnerabilidade (ex: ../../../etc/passwd)
    try:
        arquivo.resolve().relative_to(diretorio_parquets.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="Caminho de tabela inválido")

    if not arquivo.exists():
        raise HTTPException(status_code=404, detail=f"Tabela {nome_tabela} não encontrada")

    try:
        import polars as pl

        df = pl.read_parquet(arquivo)

        # Aplicar filtro
        if filtro_coluna and filtro_valor and filtro_coluna in df.columns:
            df = df.filter(
                pl.col(filtro_coluna).cast(pl.Utf8).str.contains(filtro_valor, literal=True)
            )

        # Ordenar
        if ordenar_por and ordenar_por in df.columns:
            df = df.sort(ordenar_por, descending=(ordem == "desc"))

        total = len(df)
        inicio = (pagina - 1) * por_pagina
        dados = df.slice(inicio, por_pagina)

        return {
            "colunas": df.columns,
            "dados": dados.to_dicts(),
            "total_registros": total,
            "pagina": pagina,
            "por_pagina": por_pagina,
            "total_paginas": (total + por_pagina - 1) // por_pagina,
            "schema": {col: str(df[col].dtype) for col in df.columns},
        }
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="Polars não instalado. Execute: pip install polars",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# === Endpoints: Agregação ===

@app.post("/api/agregacao/agregar")
async def agregar_produtos(cnpj: str, request: AgregacaoRequest):
    """Agrega produtos em um grupo."""
    # Implementação será conectada ao módulo de agregação
    return {
        "status": "pendente",
        "mensagem": "Módulo de agregação em desenvolvimento",
        "ids_produtos": request.ids_produtos,
    }


@app.post("/api/agregacao/desagregar")
async def desagregar_grupo(cnpj: str, request: DesagregacaoRequest):
    """Remove um grupo de agregação."""
    return {
        "status": "pendente",
        "mensagem": "Módulo de desagregação em desenvolvimento",
        "id_grupo": request.id_grupo,
    }


# === Endpoints: Conversão ===

@app.put("/api/conversao/fator")
async def editar_fator(cnpj: str, request: EdicaoFatorRequest):
    """Edita fator de conversão de um produto agrupado."""
    return {
        "status": "pendente",
        "mensagem": "Módulo de conversão em desenvolvimento",
        "id_agrupado": request.id_agrupado,
    }


@app.post("/api/conversao/recalcular")
async def recalcular_derivados(cnpj: str):
    """Recalcula tabelas derivadas após edição de fatores."""
    return {
        "status": "pendente",
        "mensagem": "Reprocessamento em desenvolvimento",
    }


# === Endpoints: Exportação ===

@app.get("/api/exportar/{cnpj}/{nome_tabela}")
async def exportar_tabela(
    cnpj: str,
    nome_tabela: str,
    formato: str = Query("xlsx", pattern="^(xlsx|csv|parquet)$"),
):
    """Exporta uma tabela em formato Excel, CSV ou Parquet."""
    return {
        "status": "pendente",
        "mensagem": f"Exportação {formato} em desenvolvimento",
    }


# === Startup ===

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
