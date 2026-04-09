"""
Registro centralizado de datasets Oracle compartilhados.

Garante que cada tabela Oracle é consultada no máximo 1 vez por CNPJ
por ciclo de extração. Os módulos (Dossiê, Estoque, Ressarcimento,
Fisconforme, Pipeline ETL) consultam o registry antes de executar
qualquer consulta Oracle.

Arquitetura:
  1. Cada dataset tem um ID estável, uma SQL de origem e um caminho
     canônico em ``shared_sql/``.
  2. ``obter_caminho()`` resolve o caminho canônico prioritário
     e caminhos legados como fallback.
  3. ``encontrar_dataset()`` retorna o primeiro arquivo existente
     (prioridade: shared_sql > legado).
  4. ``registrar_dataset()`` grava o Parquet + metadata sidecar.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl

from utilitarios.project_paths import CNPJ_ROOT, DATA_ROOT

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tipos e constantes
# ---------------------------------------------------------------------------

TipoDataset = Literal["por_cnpj", "dimensao_global"]


@dataclass(frozen=True)
class DatasetDefinicao:
    """Definição de um dataset compartilhado no catálogo global."""

    dataset_id: str
    sql_id: str | None
    tipo: TipoDataset
    tabelas_oracle: tuple[str, ...]
    descricao: str = ""


@dataclass(frozen=True)
class DatasetLocalizado:
    """Resultado de uma busca por dataset no filesystem."""

    dataset_id: str
    caminho: Path
    reutilizado: bool
    metadata: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Caminhos canônicos
# ---------------------------------------------------------------------------

REFERENCIAS_ROOT = DATA_ROOT / "referencias" / "dimensoes"


def _pasta_shared_sql(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj.strip() / "arquivos_parquet" / "shared_sql"


def _caminho_canonico_por_cnpj(cnpj: str, dataset_id: str) -> Path:
    """Caminho canônico prioritário: ``shared_sql/<dataset_id>_<cnpj>.parquet``."""
    nome = re.sub(r"[^a-z0-9]+", "_", dataset_id.lower()).strip("_")
    return _pasta_shared_sql(cnpj) / f"{nome}_{cnpj.strip()}.parquet"


def _caminho_dimensao_global(dataset_id: str) -> Path:
    """Caminho para dimensões globais (sem CNPJ)."""
    nome = re.sub(r"[^a-z0-9]+", "_", dataset_id.lower()).strip("_")
    return REFERENCIAS_ROOT / f"{nome}.parquet"


def _caminho_metadata(caminho_parquet: Path) -> Path:
    return caminho_parquet.with_suffix(".metadata.json")


# ---------------------------------------------------------------------------
# Caminhos legados (fallback)
# ---------------------------------------------------------------------------

def _caminhos_legados(cnpj: str, dataset_id: str) -> list[Path]:
    """Retorna caminhos legados conhecidos para cada dataset_id."""

    base = CNPJ_ROOT / cnpj.strip() / "arquivos_parquet"
    base_analises = CNPJ_ROOT / cnpj.strip() / "analises"
    base_produtos = base_analises / "produtos"
    base_ressarcimento = base_analises / "ressarcimento_st"
    did = dataset_id.lower().strip()

    mapeamento: dict[str, list[Path]] = {
        "nfe_base": [
            base / f"NFe_{cnpj}.parquet",
            base / f"nfe_{cnpj}.parquet",
            base / "fiscal" / "documentos" / f"NFe_{cnpj}.parquet",
            base / f"nfe_agr_{cnpj}.parquet",
        ],
        "nfce_base": [
            base / f"NFCe_{cnpj}.parquet",
            base / f"nfce_{cnpj}.parquet",
            base / "fiscal" / "documentos" / f"NFCe_{cnpj}.parquet",
            base / f"nfce_agr_{cnpj}.parquet",
        ],
        "cadastral": [
            base / f"dados_cadastrais_{cnpj}.parquet",
        ],
        "efd_c100": [
            base / f"c100_{cnpj}.parquet",
        ],
        "efd_c170": [
            base / f"c170_{cnpj}.parquet",
        ],
        "efd_c176": [
            base / f"c176_{cnpj}.parquet",
        ],
        "efd_0200": [
            base / f"reg_0200_{cnpj}.parquet",
        ],
        "efd_0190": [
            base / f"reg_0190_{cnpj}.parquet",
        ],
        "efd_0000": [
            base / f"reg_0000_{cnpj}.parquet",
        ],
        "efd_bloco_h": [
            base_produtos / f"bloco_h_{cnpj}.parquet",
            base / f"bloco_h_{cnpj}.parquet",
            base / "fiscal" / "efd" / f"bloco_h_{cnpj}.parquet",
        ],
        "efd_e111": [
            base / f"E111_{cnpj}.parquet",
            base / f"e111_{cnpj}.parquet",
        ],
        "sitafe_calculo_item": [
            base / "shared_sql" / f"sitafe_nfe_calculo_item_{cnpj}.parquet",
            CNPJ_ROOT / cnpj.strip() / "shared_sql" / f"sitafe_nfe_calculo_item_{cnpj}.parquet",
        ],
        "efd_c176_v2": [
            base / f"c176_v2_{cnpj}.parquet",
        ],
        "efd_c176_mensal": [
            base / f"c176_mensal_{cnpj}.parquet",
        ],
        "efd_c190": [
            base / f"c190_{cnpj}.parquet",
        ],
        "efd_reg_0005": [
            base / f"reg_0005_{cnpj}.parquet",
        ],
        "mov_estoque": [
            base_produtos / f"mov_estoque_{cnpj}.parquet",
        ],
        "ressarcimento_st_item": [
            base_ressarcimento / f"ressarcimento_st_item_{cnpj}.parquet",
        ],
        "ressarcimento_st_mensal": [
            base_ressarcimento / f"ressarcimento_st_mensal_{cnpj}.parquet",
        ],
    }

    return mapeamento.get(did, [])


# ---------------------------------------------------------------------------
# Catálogo de datasets
# ---------------------------------------------------------------------------

CATALOGO: tuple[DatasetDefinicao, ...] = (
    # ── NFe / NFCe (BI) ──────────────────────────────────
    DatasetDefinicao(
        dataset_id="nfe_base",
        sql_id="NFe.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.FATO_NFE_DETALHE",),
        descricao="Extração completa de NFe por CNPJ.",
    ),
    DatasetDefinicao(
        dataset_id="nfce_base",
        sql_id="NFCe.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.FATO_NFCE_DETALHE",),
        descricao="Extração completa de NFCe por CNPJ.",
    ),
    # ── EFD / SPED ───────────────────────────────────────
    DatasetDefinicao(
        dataset_id="efd_c100",
        sql_id="c100.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C100", "SPED.REG_0000"),
        descricao="Documentos C100 (cabeçalho de documentos fiscais).",
    ),
    DatasetDefinicao(
        dataset_id="efd_c170",
        sql_id="c170.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C170", "SPED.REG_C100", "SPED.REG_0200"),
        descricao="Itens de documentos fiscais EFD (C170+C100+0200).",
    ),
    DatasetDefinicao(
        dataset_id="efd_c176",
        sql_id="c176.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C176", "SPED.REG_C100", "SPED.REG_C170"),
        descricao="Ressarcimento ST (C176+C100+C170).",
    ),
    DatasetDefinicao(
        dataset_id="efd_c176_v2",
        sql_id="c176_v2.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C176", "SPED.REG_C100", "SPED.REG_C170", "SPED.REG_0200"),
        descricao="Ressarcimento ST v2 com detalhamento estendido.",
    ),
    DatasetDefinicao(
        dataset_id="efd_c176_mensal",
        sql_id="c176_mensal.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C176", "SPED.REG_C100", "SPED.REG_C170"),
        descricao="Ressarcimento ST agrupado mensalmente.",
    ),
    DatasetDefinicao(
        dataset_id="efd_c190",
        sql_id="c190.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C190",),
        descricao="Registro analítico C190.",
    ),
    DatasetDefinicao(
        dataset_id="efd_0200",
        sql_id="reg_0200.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_0200",),
        descricao="Cadastro de itens do contribuinte.",
    ),
    DatasetDefinicao(
        dataset_id="efd_0190",
        sql_id="reg_0190.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_0190",),
        descricao="Unidades de medida.",
    ),
    DatasetDefinicao(
        dataset_id="efd_0000",
        sql_id="reg_0000.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_0000",),
        descricao="Abertura do arquivo digital (períodos EFD).",
    ),
    DatasetDefinicao(
        dataset_id="efd_reg_0005",
        sql_id="reg_0005.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_0005",),
        descricao="Dados complementares da entidade.",
    ),
    DatasetDefinicao(
        dataset_id="efd_bloco_h",
        sql_id="bloco_h.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_H010", "SPED.REG_H005"),
        descricao="Inventário físico (Bloco H).",
    ),
    DatasetDefinicao(
        dataset_id="efd_e111",
        sql_id="E111.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_E111",),
        descricao="Ajustes da apuração do ICMS.",
    ),
    # ── SITAFE / Fronteira ──────────────────────────────
    DatasetDefinicao(
        dataset_id="sitafe_calculo_item",
        sql_id="shared_sql/sitafe_nfe_calculo_item.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SITAFE.SITAFE_NFE_CALCULO_ITEM",),
        descricao="Itens de cálculo do SITAFE (Fronteira/ICMS ST).",
    ),
    # ── Cadastrais ────────────────────────────────────────
    DatasetDefinicao(
        dataset_id="cadastral",
        sql_id="dados_cadastrais.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.DM_PESSOA", "BI.DM_LOCALIDADE", "BI.DM_REGIME_PAGTO_DESCRICAO"),
        descricao="Dados cadastrais consolidados do contribuinte.",
    ),
    # ── Dimensões globais (extraídas 1 vez, sem CNPJ) ────
    DatasetDefinicao(
        dataset_id="dim_localidade",
        sql_id=None,
        tipo="dimensao_global",
        tabelas_oracle=("BI.DM_LOCALIDADE",),
        descricao="Dimensão de localidades (municípios/UF).",
    ),
    DatasetDefinicao(
        dataset_id="dim_regime",
        sql_id=None,
        tipo="dimensao_global",
        tabelas_oracle=("BI.DM_REGIME_PAGTO_DESCRICAO",),
        descricao="Dimensão de regimes de pagamento.",
    ),
    DatasetDefinicao(
        dataset_id="dim_situacao",
        sql_id=None,
        tipo="dimensao_global",
        tabelas_oracle=("BI.DM_SITUACAO_CONTRIBUINTE",),
        descricao="Dimensão de situações do contribuinte.",
    ),
    # ── Datasets Compostos (Fase 3 - Polars Composition) ──
    DatasetDefinicao(
        dataset_id="dif_icms_nfe_efd",
        sql_id="dif_ICMS_NFe_EFD.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.FATO_NFE_DETALHE", "BI.FATO_NFCE_DETALHE", "SPED.REG_C100"),
        descricao="Diferença ICMS NFe vs EFD (Composição Polars).",
    ),
    DatasetDefinicao(
        dataset_id="composicao_enderecos",
        sql_id="dossie_enderecos.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.DM_PESSOA", "BI.FATO_NFE_DETALHE"),
        descricao="Histórico de endereços consolidado (Composição Polars).",
    ),
    DatasetDefinicao(
        dataset_id="composicao_fronteira",
        sql_id="fronteira.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.FATO_NFE_DETALHE", "SITAFE.SITAFE_NFE_CALCULO_ITEM"),
        descricao="Relatório de Fronteira (Composição Polars).",
    ),
)

_INDICE_POR_ID: dict[str, DatasetDefinicao] = {d.dataset_id: d for d in CATALOGO}
_INDICE_POR_SQL: dict[str, DatasetDefinicao] = {
    d.sql_id.lower(): d for d in CATALOGO if d.sql_id
}


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def obter_definicao(dataset_id: str) -> DatasetDefinicao | None:
    """Localiza a definição de um dataset por ID."""
    return _INDICE_POR_ID.get(dataset_id.lower().strip())


def obter_definicao_por_sql(sql_id: str) -> DatasetDefinicao | None:
    """Localiza a definição de um dataset pelo sql_id correspondente."""
    return _INDICE_POR_SQL.get(sql_id.lower().strip())


def listar_datasets() -> list[DatasetDefinicao]:
    """Retorna todos os datasets do catálogo ordenados por ID."""
    return sorted(CATALOGO, key=lambda d: d.dataset_id)


def listar_datasets_por_tabela(tabela_oracle: str) -> list[DatasetDefinicao]:
    """Retorna todos os datasets que consultam uma determinada tabela Oracle."""
    tabela = tabela_oracle.upper().strip()
    return [d for d in CATALOGO if tabela in d.tabelas_oracle]


def obter_caminho(cnpj: str | None, dataset_id: str) -> Path:
    """Retorna o caminho canônico prioritário do dataset."""

    definicao = obter_definicao(dataset_id)
    if definicao is None:
        raise ValueError(f"Dataset desconhecido: {dataset_id}")

    if definicao.tipo == "dimensao_global":
        return _caminho_dimensao_global(dataset_id)

    if cnpj is None:
        raise ValueError(f"CNPJ obrigatório para dataset por_cnpj: {dataset_id}")
    return _caminho_canonico_por_cnpj(cnpj.strip(), dataset_id)


def listar_caminhos_com_fallback(cnpj: str, dataset_id: str) -> list[Path]:
    """Retorna o caminho canônico seguido dos caminhos legados como fallback.

    O primeiro caminho existente deve ser utilizado.
    """
    definicao = obter_definicao(dataset_id)
    if definicao is None:
        return []

    candidatos: list[Path] = []
    vistos: set[str] = set()

    def _add(caminho: Path) -> None:
        chave = str(caminho).lower()
        if chave not in vistos:
            vistos.add(chave)
            candidatos.append(caminho)

    # Prioridade 1: caminho canônico
    if definicao.tipo == "dimensao_global":
        _add(_caminho_dimensao_global(dataset_id))
    else:
        _add(_caminho_canonico_por_cnpj(cnpj.strip(), dataset_id))

    # Prioridade 2: caminhos legados
    for legado in _caminhos_legados(cnpj.strip(), dataset_id):
        _add(legado)

    return candidatos


def encontrar_dataset(cnpj: str, dataset_id: str) -> DatasetLocalizado | None:
    """Busca o dataset já materializado no filesystem.

    Prioriza ``shared_sql/`` e faz fallback para caminhos legados.
    Retorna ``None`` se nenhum Parquet for encontrado.
    """
    candidatos = listar_caminhos_com_fallback(cnpj, dataset_id)
    for caminho in candidatos:
        if caminho.exists():
            metadata = _ler_metadata(caminho)
            return DatasetLocalizado(
                dataset_id=dataset_id,
                caminho=caminho,
                reutilizado=True,
                metadata=metadata,
            )
    return None


def carregar_lazyframe(cnpj: str, dataset_id: str) -> tuple[pl.LazyFrame, Path] | None:
    """Carrega o primeiro Parquet encontrado como LazyFrame."""
    localizado = encontrar_dataset(cnpj, dataset_id)
    if localizado is None:
        return None
    return pl.scan_parquet(localizado.caminho), localizado.caminho


def carregar_dataframe(cnpj: str, dataset_id: str) -> DatasetLocalizado | None:
    """Carrega o dataset como DataFrame (collect imediato)."""
    resultado = carregar_lazyframe(cnpj, dataset_id)
    if resultado is None:
        return None
    lf, caminho = resultado
    metadata = _ler_metadata(caminho)
    return DatasetLocalizado(
        dataset_id=dataset_id,
        caminho=caminho,
        reutilizado=True,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Registro e persistência
# ---------------------------------------------------------------------------

def criar_metadata(
    *,
    cnpj: str | None,
    dataset_id: str,
    sql_id: str | None = None,
    parametros: dict[str, Any] | None = None,
    linhas: int | None = None,
) -> dict[str, Any]:
    """Gera o payload auditável de metadata para um dataset registrado."""
    return {
        "dataset_id": dataset_id,
        "cnpj": cnpj,
        "sql_id": sql_id,
        "parametros": parametros or {},
        "linhas": linhas,
        "extraido_em": datetime.now(UTC).isoformat(),
    }


def registrar_dataset(
    cnpj: str | None,
    dataset_id: str,
    dataframe: pl.DataFrame,
    *,
    metadata: dict[str, Any] | None = None,
) -> Path | None:
    """Grava o Parquet no caminho canônico + metadata sidecar."""

    caminho = obter_caminho(cnpj, dataset_id)
    caminho.parent.mkdir(parents=True, exist_ok=True)

    try:
        dataframe.write_parquet(caminho, compression="snappy")
    except Exception:
        logger.exception("Falha ao gravar dataset %s em %s", dataset_id, caminho)
        return None

    if metadata is not None:
        _gravar_metadata(caminho, metadata)

    logger.info(
        "Dataset %s registrado: %d linhas → %s",
        dataset_id,
        dataframe.height,
        caminho.name,
    )
    return caminho


# ---------------------------------------------------------------------------
# Metadata sidecar
# ---------------------------------------------------------------------------

def _ler_metadata(caminho_parquet: Path) -> dict[str, Any] | None:
    caminho_meta = _caminho_metadata(caminho_parquet)
    if not caminho_meta.exists():
        return None
    try:
        return json.loads(caminho_meta.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _gravar_metadata(caminho_parquet: Path, metadata: dict[str, Any]) -> None:
    caminho_meta = _caminho_metadata(caminho_parquet)
    try:
        caminho_meta.write_text(
            json.dumps(metadata, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
    except OSError:
        logger.warning("Falha ao gravar metadata sidecar: %s", caminho_meta)


# ---------------------------------------------------------------------------
# Diagnóstico
# ---------------------------------------------------------------------------

def diagnosticar_disponibilidade(cnpj: str) -> list[dict[str, Any]]:
    """Retorna o status de disponibilidade de cada dataset para um CNPJ."""
    resultado: list[dict[str, Any]] = []
    for definicao in CATALOGO:
        localizado = encontrar_dataset(cnpj, definicao.dataset_id)
        resultado.append({
            "dataset_id": definicao.dataset_id,
            "tipo": definicao.tipo,
            "sql_id": definicao.sql_id,
            "disponivel": localizado is not None,
            "caminho": str(localizado.caminho) if localizado else None,
            "reutilizado": localizado.reutilizado if localizado else False,
        })
    return resultado


def resolver_dataset_por_sql_id(sql_id: str) -> str | None:
    """Dado um sql_id, retorna o dataset_id correspondente (ou None)."""
    definicao = obter_definicao_por_sql(sql_id)
    if definicao is None:
        sql_basico = Path(str(sql_id).strip()).name.lower()
        for item in CATALOGO:
            if item.sql_id and Path(item.sql_id).name.lower() == sql_basico:
                definicao = item
                break
    return definicao.dataset_id if definicao else None
