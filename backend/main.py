from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure src/ is on sys.path so existing services can be imported
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dotenv import load_dotenv
from utilitarios.project_paths import ENV_PATH

load_dotenv(ENV_PATH)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import (
    aggregation,
    cnpj,
    dossie,
    estoque,
    fiscal_analise_v2 as fiscal_analise,
    fiscal_documentos,
    fiscal_efd,
    fiscal_fiscalizacao,
    fisconforme,
    oracle,
    parquet,
    pipeline,
    ressarcimento,
    sql_query,
)

app = FastAPI(
    title="Fiscal Parquet API",
    description="REST API para o Fiscal Parquet",
    version="1.0.0",
)

allowed_origins_str = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173,http://localhost:3000")
allowed_origins = [o.strip() for o in allowed_origins_str.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["Content-Type", "Authorization", "Accept", "Origin", "X-Requested-With"],
)


@app.middleware("http")
async def add_security_headers(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
    return response

app.include_router(cnpj.router, prefix="/api/cnpj", tags=["cnpj"])
app.include_router(parquet.router, prefix="/api/parquet", tags=["parquet"])
app.include_router(pipeline.router, prefix="/api/pipeline", tags=["pipeline"])
app.include_router(estoque.router, prefix="/api/estoque", tags=["estoque"])
app.include_router(ressarcimento.router, prefix="/api/ressarcimento", tags=["ressarcimento"])
app.include_router(aggregation.router, prefix="/api/aggregation", tags=["aggregation"])
app.include_router(sql_query.router, prefix="/api/sql", tags=["sql"])
app.include_router(fisconforme.router, prefix="/api/fisconforme", tags=["fisconforme"])
app.include_router(oracle.router, prefix="/api/oracle", tags=["oracle"])
app.include_router(dossie.router, prefix="/api/dossie", tags=["dossie"])
app.include_router(fiscal_efd.router, prefix="/api/fiscal/efd", tags=["fiscal-efd"])
app.include_router(
    fiscal_documentos.router,
    prefix="/api/fiscal/documentos",
    tags=["fiscal-documentos"],
)
app.include_router(
    fiscal_fiscalizacao.router,
    prefix="/api/fiscal/fiscalizacao",
    tags=["fiscal-fiscalizacao"],
)
app.include_router(
    fiscal_analise.router,
    prefix="/api/fiscal/analise",
    tags=["fiscal-analise"],
)


@app.get("/api/health")
def health():
    return {"status": "ok"}
