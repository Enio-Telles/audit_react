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
from transformacao.composicao_dif_icms import atualizar_composicao_dif_icms
from transformacao.composicao_enderecos import atualizar_composicao_enderecos
from transformacao.composicao_fronteira import atualizar_composicao_fronteira

# Integração com o registry centralizado (lazy para evitar circular).
_registry_modulo = None


def _get_registry():
    global _registry_modulo
    if _registry_modulo is None:
        try:
            from utilitarios import dataset_registry
            _registry_modulo = dataset_registry
        except ImportError:
            pass
    return _registry_modulo


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


def obter_caminho_metadata_por_dataset(caminho_dataset: Path | None) -> Path | None:
    """Resolve o sidecar do caminho efetivamente reutilizado."""

    if caminho_dataset is None:
        return None
    return caminho_dataset.with_suffix(".metadata.json")


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
    """Lista caminhos candidatos para reuso antes de nova consulta Oracle.

    Ordem de prioridade:
    1. Caminhos do registry centralizado (shared_sql/ + legados mapeados)
    2. Caminho do dataset compartilhado do dossie (shared_sql/ por sql_id)
    3. Caminhos legados manuais deste módulo
    """

    vistos: set[str] = set()
    ordenados: list[Path] = []

    def _add(caminho: Path) -> None:
        chave = str(caminho).lower()
        if chave not in vistos:
            vistos.add(chave)
            ordenados.append(caminho)

    # Prioridade 1: registry centralizado (inclui shared_sql/ + legados)
    registry = _get_registry()
    if registry is not None:
        dataset_id = registry.resolver_dataset_por_sql_id(sql_id)
        if dataset_id is not None:
            for caminho in registry.listar_caminhos_com_fallback(cnpj, dataset_id):
                _add(caminho)

    # Prioridade 2: caminho do dossie dataset compartilhado
    _add(obter_caminho_dataset_compartilhado(cnpj, sql_id))

    # Prioridade 3: caminhos legados deste módulo
    for caminho in _caminhos_canonicos_por_sql(cnpj, sql_id):
        _add(caminho)

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


def carregar_metadata_dataset_por_caminho(caminho_dataset: Path | None) -> dict[str, Any] | None:
    """Carrega o metadata do arquivo realmente reutilizado."""

    caminho_metadata = obter_caminho_metadata_por_dataset(caminho_dataset)
    if caminho_metadata is None or not caminho_metadata.exists():
        return None

    try:
        return json.loads(caminho_metadata.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def carregar_lazyframe_reutilizavel(cnpj: str, sql_id: str) -> tuple[pl.LazyFrame, Path] | None:
    """Retorna uma leitura lazy do primeiro dataset reutilizavel encontrado."""

    for caminho in listar_caminhos_reutilizaveis(cnpj, sql_id):
        if not caminho.exists():
            continue
        if not _parquet_reutilizavel_esta_integro(caminho):
            continue
        return pl.scan_parquet(caminho), caminho
    return None


def _parquet_reutilizavel_esta_integro(caminho: Path) -> bool:
    """Descarta artefatos parciais para forcar rematerializacao segura."""

    try:
        if caminho.stat().st_size < 12:
            return False
        pl.scan_parquet(caminho).collect_schema()
        return True
    except Exception:
        return False



def _tentar_gerar_composicao(
    cnpj: str,
    sql_id: str,
    parametros: dict[str, Any] | None = None,
) -> bool:
    """Tenta gerar o dataset via composição Polars baseada em outros Parquets."""
    sid = sql_id.lower().strip()
    
    try:
        if sid == "dif_icms_nfe_efd.sql":
            return atualizar_composicao_dif_icms(cnpj) is not None
        if sid == "dossie_enderecos.sql":
            return atualizar_composicao_enderecos(cnpj) is not None
        if sid == "fronteira.sql":
            return atualizar_composicao_fronteira(
                cnpj,
                data_limite_processamento=(parametros or {}).get("data_limite_processamento"),
            ) is not None
    except Exception:
        return False
        
    return False


def carregar_dataset_reutilizavel(
    cnpj: str,
    sql_id: str,
    parametros: dict[str, Any] | None = None,
) -> DatasetCompartilhadoDossie | None:
    """Carrega um dataset compartilhado quando ele ja existe materializado.
    
    Se não existir, tenta compor via Polars antes de desistir.
    """

    # 1. Tentar carregar direto do filesystem (reuso simples)
    resultado_lazy = carregar_lazyframe_reutilizavel(cnpj, sql_id)
    
    # 2. Se não encontrou, tenta compor localmente
    if resultado_lazy is None:
        if _tentar_gerar_composicao(cnpj, sql_id, parametros=parametros):
            resultado_lazy = carregar_lazyframe_reutilizavel(cnpj, sql_id)

    if resultado_lazy is None:
        return None

    lazyframe, caminho = resultado_lazy
    metadata = carregar_metadata_dataset_por_caminho(caminho) or carregar_metadata_dataset_compartilhado(cnpj, sql_id)
    return DatasetCompartilhadoDossie(
        sql_id=sql_id,
        dataframe=lazyframe.collect(),
        caminho_origem=caminho,
        reutilizado=True,
        metadata=metadata,
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
