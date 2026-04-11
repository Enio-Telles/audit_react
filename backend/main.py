from __future__ import annotations

import os
import sys
import time
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dotenv import load_dotenv
from utilitarios.project_paths import ENV_PATH

load_dotenv(ENV_PATH)

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
except Exception:  # pragma: no cover
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4"
    Counter = None
    Histogram = None
    generate_latest = None

from routers import (
    aggregation,
    cnpj,
    dossie,
    estoque,
    fiscal_agregacao,
    fiscal_analise_v2 as fiscal_analise,
    fiscal_catalog_inspector,
    fiscal_conversao,
    fiscal_documentos,
    fiscal_efd,
    fiscal_estoque,
    fiscal_fiscalizacao,
    fiscal_produto,
    fiscal_ressarcimento,
    fiscal_summary,
    fisconforme,
    frontend_primeira_leva,
    observabilidade,
    oracle,
    parquet,
    pipeline,
    ressarcimento,
    sql_query,
)

app = FastAPI(
    title="Fiscal Parquet API",
    description="REST API para o Fiscal Parquet",
    version="1.1.0",
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

HTTP_REQUESTS = (
    Counter(
        "audit_react_http_requests_total",
        "Total de requisicoes HTTP",
        labelnames=("method", "path", "status"),
    )
    if Counter
    else None
)
HTTP_LATENCY = (
    Histogram(
        "audit_react_http_request_duration_seconds",
        "Duracao das requisicoes HTTP",
        labelnames=("method", "path"),
    )
    if Histogram
    else None
)


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    started_at = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - started_at
    if HTTP_REQUESTS:
        HTTP_REQUESTS.labels(request.method, request.url.path, str(response.status_code)).inc()
    if HTTP_LATENCY:
        HTTP_LATENCY.labels(request.method, request.url.path).observe(duration)
    return response


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
    fiscal_agregacao.router,
    prefix="/api/fiscal/agregacao",
    tags=["fiscal-agregacao"],
)
app.include_router(
    fiscal_produto.router,
    prefix="/api/fiscal/produto",
    tags=["fiscal-produto"],
)
app.include_router(
    fiscal_conversao.router,
    prefix="/api/fiscal/conversao",
    tags=["fiscal-conversao"],
)
app.include_router(
    fiscal_estoque.router,
    prefix="/api/fiscal/estoque",
    tags=["fiscal-estoque"],
)
app.include_router(
    fiscal_documentos.router,
    prefix="/api/fiscal/documentos",
    tags=["fiscal-documentos"],
)
app.include_router(
    frontend_primeira_leva.router,
    prefix="/api/frontend",
    tags=["frontend-primeira-leva"],
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
app.include_router(
    fiscal_ressarcimento.router,
    prefix="/api/fiscal/ressarcimento",
    tags=["fiscal-ressarcimento"],
)
app.include_router(observabilidade.router, prefix="/api/observabilidade", tags=["observabilidade"])


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def metrics_endpoint():
    if generate_latest is None:
        return PlainTextResponse("prometheus-client nao instalado", status_code=503)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
