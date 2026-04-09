from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
import json
import re

import polars as pl

from utilitarios.project_paths import CNPJ_ROOT
from utilitarios.salvar_para_parquet import salvar_para_parquet


@dataclass(frozen=True)
class DatasetCompartilhadoDossie:
    """Representa um dataset carregado por reuso ou por extracao Oracle."""

    sql_id: str
    dataframe: pl.DataFrame
    caminho_origem: Path | None
    reutilizado: bool
    metadata: dict[str, Any] | None = None


def normalizar_nome_sql_para_dataset(sql_id: str) -> str:
    """Gera um nome estavel para o cache compartilhado de uma SQL."""

    nome_base = Path(str(sql_id).strip()).stem.lower()
    nome_base = re.sub(r"[^a-z0-9]+", "_", nome_base).strip("_")
    return nome_base or "consulta"


def obter_pasta_datasets_compartilhados(cnpj: str) -> Path:
    """Centraliza a pasta de datasets SQL compartilhados por CNPJ."""

    return CNPJ_ROOT / str(cnpj).strip() / "arquivos_parquet" / "shared_sql"


def obter_caminho_dataset_compartilhado(cnpj: str, sql_id: str) -> Path:
    """Retorna o caminho canonico do dataset compartilhado de uma SQL."""

    nome_base = normalizar_nome_sql_para_dataset(sql_id)
    return obter_pasta_datasets_compartilhados(cnpj) / f"{nome_base}_{str(cnpj).strip()}.parquet"


def obter_caminho_metadata_dataset_compartilhado(cnpj: str, sql_id: str) -> Path:
    """Retorna o caminho do metadata sidecar do dataset compartilhado."""

    return obter_caminho_dataset_compartilhado(cnpj, sql_id).with_suffix(".metadata.json")


def _caminhos_canonicos_por_sql(cnpj: str, sql_id: str) -> list[Path]:
    """Mapeia SQLs conhecidas para artefatos reutilizaveis do workspace."""

    base = CNPJ_ROOT / str(cnpj).strip() / "arquivos_parquet"
    base_analises = CNPJ_ROOT / str(cnpj).strip() / "analises"
    base_produtos = base_analises / "produtos"
    base_ressarcimento = base_analises / "ressarcimento_st"
    sql_id_normalizado = str(sql_id).strip().lower()

    if sql_id_normalizado == "dados_cadastrais.sql":
        return [base / f"dados_cadastrais_{cnpj}.parquet"]
    if sql_id_normalizado == "nfe.sql":
        return [
            base / f"nfe_agr_{cnpj}.parquet",
            base / f"NFe_{cnpj}.parquet",
            base / f"nfe_{cnpj}.parquet",
            base / "fiscal" / "documentos" / f"NFe_{cnpj}.parquet",
        ]
    if sql_id_normalizado == "nfce.sql":
        return [
            base / f"nfce_agr_{cnpj}.parquet",
            base / f"NFCe_{cnpj}.parquet",
            base / f"nfce_{cnpj}.parquet",
            base / "fiscal" / "documentos" / f"NFCe_{cnpj}.parquet",
        ]
    if sql_id_normalizado in {"mov_estoque.sql", "mov_estoque", "estoque_movimentacao.sql"}:
        return [base_produtos / f"mov_estoque_{cnpj}.parquet"]
    if sql_id_normalizado in {"aba_mensal.sql", "aba_mensal", "calculos_mensais.sql"}:
        return [base_produtos / f"aba_mensal_{cnpj}.parquet"]
    if sql_id_normalizado in {"aba_anual.sql", "aba_anual", "calculos_anuais.sql"}:
        return [base_produtos / f"aba_anual_{cnpj}.parquet"]
    if sql_id_normalizado in {"ressarcimento_st_item.sql", "ressarcimento_st_item"}:
        return [base_ressarcimento / f"ressarcimento_st_item_{cnpj}.parquet"]
    if sql_id_normalizado in {"ressarcimento_st_mensal.sql", "ressarcimento_st_mensal"}:
        return [base_ressarcimento / f"ressarcimento_st_mensal_{cnpj}.parquet"]
    if sql_id_normalizado in {"ressarcimento_st_conciliacao.sql", "ressarcimento_st_conciliacao"}:
        return [base_ressarcimento / f"ressarcimento_st_conciliacao_{cnpj}.parquet"]
    if sql_id_normalizado in {"ressarcimento_st_validacoes.sql", "ressarcimento_st_validacoes"}:
        return [base_ressarcimento / f"ressarcimento_st_validacoes_{cnpj}.parquet"]

    return []


def listar_caminhos_reutilizaveis(cnpj: str, sql_id: str) -> list[Path]:
    """Lista caminhos candidatos para reuso antes de nova consulta Oracle."""

    candidatos = [obter_caminho_dataset_compartilhado(cnpj, sql_id)]
    candidatos.extend(_caminhos_canonicos_por_sql(cnpj, sql_id))

    vistos: set[Path] = set()
    ordenados: list[Path] = []
    for caminho in candidatos:
        if caminho in vistos:
            continue
        vistos.add(caminho)
        ordenados.append(caminho)
    return ordenados


def carregar_metadata_dataset_compartilhado(cnpj: str, sql_id: str) -> dict[str, Any] | None:
    """Carrega os metadados do dataset compartilhado quando existirem."""

    caminho_metadata = obter_caminho_metadata_dataset_compartilhado(cnpj, sql_id)
    if not caminho_metadata.exists():
        return None

    try:
        return json.loads(caminho_metadata.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def carregar_lazyframe_reutilizavel(cnpj: str, sql_id: str) -> tuple[pl.LazyFrame, Path] | None:
    """Retorna uma leitura lazy do primeiro dataset reutilizavel encontrado."""

    for caminho in listar_caminhos_reutilizaveis(cnpj, sql_id):
        if caminho.exists():
            return pl.scan_parquet(caminho), caminho
    return None


def carregar_dataset_reutilizavel(cnpj: str, sql_id: str) -> DatasetCompartilhadoDossie | None:
    """Carrega um dataset compartilhado quando ele ja existe materializado."""

    resultado_lazy = carregar_lazyframe_reutilizavel(cnpj, sql_id)
    if resultado_lazy is None:
        return None

    lazyframe, caminho = resultado_lazy
    return DatasetCompartilhadoDossie(
        sql_id=sql_id,
        dataframe=lazyframe.collect(),
        caminho_origem=caminho,
        reutilizado=True,
        metadata=carregar_metadata_dataset_compartilhado(cnpj, sql_id),
    )


def criar_metadata_dataset_compartilhado(
    *,
    cnpj: str,
    sql_id: str,
    parametros: dict[str, Any] | None = None,
    versao_consulta: str | None = None,
    dependencias: list[str] | None = None,
    entidade: str | None = None,
) -> dict[str, Any]:
    """Gera o payload auditavel de metadados do dataset compartilhado."""

    return {
        "cnpj": str(cnpj).strip(),
        "sql_id": sql_id,
        "entidade": entidade or "cnpj",
        "parametros": parametros or {},
        "versao_consulta": versao_consulta,
        "dependencias": dependencias or [],
        "extraido_em": datetime.now(UTC).isoformat(),
    }


def salvar_metadata_dataset_compartilhado(cnpj: str, sql_id: str, metadata: dict[str, Any]) -> Path | None:
    """Persiste o metadata sidecar para auditoria e rastreabilidade do dataset compartilhado."""

    caminho_metadata = obter_caminho_metadata_dataset_compartilhado(cnpj, sql_id)
    try:
        caminho_metadata.parent.mkdir(parents=True, exist_ok=True)
        caminho_metadata.write_text(json.dumps(metadata, ensure_ascii=True, indent=2), encoding="utf-8")
    except OSError:
        return None
    return caminho_metadata


def salvar_dataset_compartilhado(
    cnpj: str,
    sql_id: str,
    dataframe: pl.DataFrame,
    *,
    metadata: dict[str, Any] | None = None,
) -> Path | None:
    """Persiste o resultado bruto de uma SQL para reuso futuro entre secoes."""

    caminho_saida = obter_caminho_dataset_compartilhado(cnpj, sql_id)
    sucesso = salvar_para_parquet(
        dataframe,
        caminho_saida=caminho_saida.parent,
        nome_arquivo=caminho_saida.name,
    )
    if not sucesso:
        return None

    if metadata is not None:
        salvar_metadata_dataset_compartilhado(cnpj, sql_id, metadata)
    return caminho_saida
