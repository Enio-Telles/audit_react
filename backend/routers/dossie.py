from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl
from fastapi import APIRouter
from pydantic import BaseModel

from interface_grafica.services.dossie_catalog import listar_secoes_dossie
from interface_grafica.services.dossie_resolution import resolver_secao_dossie
from interface_grafica.services.dossie_cache_keys import gerar_chave_cache_dossie
from utilitarios.project_paths import CNPJ_ROOT

router = APIRouter()


class DossieSectionSummaryResponse(BaseModel):
    id: str
    title: str
    description: str
    sourceType: str
    status: str
    rowCount: Optional[int] = None
    updatedAt: Optional[str] = None


def normalizar_cnpj(cnpj: str) -> str:
    """Remove caracteres nao numericos para manter compatibilidade com a pasta do CNPJ."""

    return "".join(caractere for caractere in str(cnpj or "") if caractere.isdigit())


def contar_linhas_parquet(caminho_arquivo: Path) -> int:
    """Conta linhas via scan_parquet para evitar carga desnecessaria do arquivo inteiro."""

    return int(pl.scan_parquet(caminho_arquivo).select(pl.len()).collect().item())


def obter_data_atualizacao_arquivo(caminho_arquivo: Path) -> str | None:
    """Retorna a data de modificacao em ISO para facilitar rastreabilidade no frontend."""

    try:
        return datetime.fromtimestamp(caminho_arquivo.stat().st_mtime).isoformat()
    except OSError:
        return None


def escolher_primeiro_arquivo_existente(candidatos: list[Path]) -> Path | None:
    """Seleciona o primeiro arquivo existente respeitando a ordem de prioridade da secao."""

    for caminho_arquivo in candidatos:
        if caminho_arquivo.exists():
            return caminho_arquivo
    return None


def resumir_arquivos_parquet(caminhos_arquivos: list[Path]) -> tuple[int, str | None]:
    """Soma linhas e retorna a data mais recente entre os arquivos realmente utilizados."""

    total_linhas = 0
    datas_atualizacao: list[str] = []

    for caminho_arquivo in caminhos_arquivos:
        total_linhas += contar_linhas_parquet(caminho_arquivo)
        data_atualizacao = obter_data_atualizacao_arquivo(caminho_arquivo)
        if data_atualizacao:
            datas_atualizacao.append(data_atualizacao)

    return total_linhas, max(datas_atualizacao) if datas_atualizacao else None


def obter_arquivos_secao_cadastro(cnpj: str) -> list[Path]:
    """Mapeia a secao de cadastro para o parquet cadastral persistido do CNPJ."""

    return [
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"dados_cadastrais_{cnpj}.parquet",
    ]


def obter_arquivos_secao_documentos_fiscais(cnpj: str) -> list[Path]:
    """Seleciona uma fonte por tipo documental para evitar contagem dupla entre visoes equivalentes."""

    candidatos_nfe = [
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"nfe_agr_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"NFe_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / "fiscal" / "documentos" / f"NFe_{cnpj}.parquet",
    ]
    candidatos_nfce = [
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"nfce_agr_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"NFCe_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / "fiscal" / "documentos" / f"NFCe_{cnpj}.parquet",
    ]

    arquivos_encontrados = [
        escolher_primeiro_arquivo_existente(candidatos_nfe),
        escolher_primeiro_arquivo_existente(candidatos_nfce),
    ]
    return [caminho_arquivo for caminho_arquivo in arquivos_encontrados if caminho_arquivo is not None]


def obter_arquivos_secao_arrecadacao(cnpj: str) -> list[Path]:
    """Usa os artefatos fiscais ja persistidos ligados a arrecadacao e conta corrente."""

    candidatos = [
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"E111_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / "fiscal" / "fronteira" / f"fronteira_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / "fisconforme" / "malhas" / f"Fisconforme_malha_cnpj_{cnpj}.parquet",
    ]
    return [caminho_arquivo for caminho_arquivo in candidatos if caminho_arquivo.exists()]


def obter_arquivos_por_secao(secao_id: str, cnpj: str) -> list[Path]:
    """Centraliza o mapeamento das secoes do dossie para artefatos ja materializados."""

    candidatos: list[Path] = []

    # Busca primeiro o cache canonico para priorizar a nova persistencia do Dossie.
    try:
        from interface_grafica.services.dossie_extraction_service import obter_caminho_cache_dossie

        resolucao = resolver_secao_dossie(cnpj=cnpj, secao_id=secao_id)
        arquivo_canonico = obter_caminho_cache_dossie(cnpj, resolucao.cache_file_name)
        if arquivo_canonico.exists():
            candidatos.append(arquivo_canonico)
    except Exception:
        pass

    # Mantem as fontes legadas como fallback para nao quebrar a leitura do Dossie ja materializado.
    if secao_id == "cadastro":
        candidatos.extend(obter_arquivos_secao_cadastro(cnpj))
    elif secao_id == "documentos_fiscais":
        candidatos.extend(obter_arquivos_secao_documentos_fiscais(cnpj))
    elif secao_id == "arrecadacao":
        candidatos.extend(obter_arquivos_secao_arrecadacao(cnpj))

    return [caminho_arquivo for caminho_arquivo in list(dict.fromkeys(candidatos)) if caminho_arquivo.exists()]


def montar_resumo_secao(secao_id: str, cnpj: str) -> tuple[str, int | None, str | None]:
    """Resume o estado atual da secao sem disparar novas consultas nem alterar cache existente."""

    caminhos_arquivos = obter_arquivos_por_secao(secao_id, cnpj)
    if not caminhos_arquivos:
        return "idle", None, None

    try:
        total_linhas, data_atualizacao = resumir_arquivos_parquet(caminhos_arquivos)
        return "cached", total_linhas, data_atualizacao
    except Exception:
        return "error", None, None


class SyncDossieRequest(BaseModel):
    parametros: Optional[dict] = None


async def sincronizar_secao_dossie(
    cnpj: str,
    secao_id: str,
    payload: SyncDossieRequest | None = None,
):
    """Centraliza a sincronizacao do dossie para manter os contratos compativeis."""

    from interface_grafica.services.dossie_extraction_service import executar_sync_secao

    parametros = payload.parametros if payload else None
    cnpj_normalizado = normalizar_cnpj(cnpj)

    return await executar_sync_secao(cnpj=cnpj_normalizado, secao_id=secao_id, parametros=parametros)


@router.post("/{cnpj}/secoes/{secao_id}/sync")
async def post_sync_secao(cnpj: str, secao_id: str, payload: SyncDossieRequest | None = None):
    """Aciona a materializacao SQL no contrato canonico atual do backend."""

    return await sincronizar_secao_dossie(cnpj=cnpj, secao_id=secao_id, payload=payload)


@router.post("/{cnpj}/sync/{secao_id}")
async def post_sync_secao_legado(cnpj: str, secao_id: str, payload: SyncDossieRequest | None = None):
    """Mantem compatibilidade com o contrato originalmente descrito no plano."""

    return await sincronizar_secao_dossie(cnpj=cnpj, secao_id=secao_id, payload=payload)


@router.get("/{cnpj}/secoes", response_model=list[DossieSectionSummaryResponse])
def get_secoes(cnpj: str):
    """
    Lista as secoes do dossie com base nos artefatos ja persistidos do CNPJ.

    Regra conservadora:
    - nao dispara novas consultas Oracle;
    - nao altera arquivos existentes;
    - apenas resume o que ja foi materializado no workspace do projeto.
    """

    cnpj_normalizado = normalizar_cnpj(cnpj)
    secoes = listar_secoes_dossie()
    resultado: list[DossieSectionSummaryResponse] = []

    for secao in secoes:
        if secao.exige_cnpj and not cnpj_normalizado:
            continue

<<<<<<< HEAD        status_secao, quantidade_linhas, data_atualizacao = montar_resumo_secao(secao.id, cnpj_normalizado)
        resultado.append(
            DossieSectionSummaryResponse(
                id=secao.id,
                title=secao.titulo,
                description=secao.descricao,
                sourceType=secao.tipo_fonte,
                status=status_secao,
                rowCount=quantidade_linhas,
                updatedAt=data_atualizacao,
            )
        )

    return resultado
