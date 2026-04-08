from __future__ import annotations

from pathlib import Path
from typing import Any
import asyncio

import polars as pl
from fastapi import HTTPException

from interface_grafica.services.dossie_resolution import resolver_secao_dossie
from interface_grafica.services.sql_service import SqlService
from utilitarios.project_paths import CNPJ_ROOT
from utilitarios.salvar_para_parquet import salvar_para_parquet


def obter_caminho_cache_dossie(cnpj: str, cache_file_name: str) -> Path:
    """Retorna o caminho canônico do cache parquet do dossie."""
    return CNPJ_ROOT / str(cnpj).strip() / "arquivos_parquet" / "dossie" / cache_file_name


def executar_sync_secao_sync(cnpj: str, secao_id: str, parametros: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Sincroniza proativamente uma secao do dossie:
     1. Resolve o script SQL (aliases/catalog).
     2. Executa todas as queries ou a primeira iterável da seção.
     3. Salva nativamente no filesystem usando a cache_key.
    """
    resolucao = resolver_secao_dossie(cnpj=cnpj, secao_id=secao_id, parametros=parametros)
    
    if not resolucao.sql_ids:
        raise ValueError(f"Nenhum SQL mapeado para a seção {secao_id}")
        
    # Vamos executar apenas o SQL de maior prioridade caso haja varios ou compor:
    # A resolução ja entrega sql_ids mapeados, geralmente o index 0 é o mais importante.
    sql_id = resolucao.sql_ids[0]
    
    try:
        sql_texto = SqlService.read_sql(sql_id)
    except Exception as e:
        raise ValueError(f"Falha ao carregar SQL {sql_id}: {e}")
        
    params_reais = {"CNPJ": cnpj}
    if parametros:
        params_reais.update(parametros)
        
    try:
        linhas_dict = SqlService.executar_sql(sql_texto, params=params_reais, cnpj=cnpj)
    except Exception as e:
        raise ValueError(f"Erro na execução da consulta Oracle para {sql_id}: {e}")
    
    # Materializar local
    df = pl.DataFrame(linhas_dict) if linhas_dict else pl.DataFrame()
    caminho_arquivo = obter_caminho_cache_dossie(cnpj, resolucao.cache_file_name)
    
    # Salvar para parquet
    sucesso = salvar_para_parquet(df, caminho_saida=caminho_arquivo.parent, nome_arquivo=caminho_arquivo.name)
    if not sucesso:
        raise RuntimeError("Erro ao persistir o resultado no diretório cache.")
        
    return {
        "status": "success",
        "cnpj": cnpj,
        "secao_id": secao_id,
        "linhas_extraidas": len(df),
        "cache_file": str(caminho_arquivo),
        "updatedAt": caminho_arquivo.stat().st_mtime if caminho_arquivo.exists() else None
    }


async def executar_sync_secao(cnpj: str, secao_id: str, parametros: dict[str, Any] | None = None) -> dict[str, Any]:
    """Wrapper assincrono para não bloquear o Event Loop do FastAPI."""
    try:
        resultado = await asyncio.to_thread(executar_sync_secao_sync, cnpj, secao_id, parametros)
        return resultado
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
