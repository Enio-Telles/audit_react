"""
Catálogo centralizado de datasets compartilhados e materializados.

Objetivos:
  1. Manter um catálogo canônico de datasets por CNPJ e dimensões globais.
  2. Resolver aliases legados para nomes canônicos.
  3. Ler e registrar datasets materializados em Parquet ou Delta.
  4. Reduzir divergência entre nomes do pipeline, backend e camadas legadas.
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

from utilitarios.delta_lake import resolve_storage_format, scan_delta_table, write_delta_table
from utilitarios.project_paths import CNPJ_ROOT, DATA_ROOT

logger = logging.getLogger(__name__)

TipoDataset = Literal["por_cnpj", "dimensao_global"]


@dataclass(frozen=True)
class DatasetDefinicao:
    dataset_id: str
    sql_id: str | None
    tipo: TipoDataset
    tabelas_oracle: tuple[str, ...]
    descricao: str = ""
    relative_dir: tuple[str, ...] | None = None
    filename_template: str | None = None


@dataclass(frozen=True)
class DatasetLocalizado:
    dataset_id: str
    caminho: Path
    reutilizado: bool
    metadata: dict[str, Any] | None = None


REFERENCIAS_ROOT = DATA_ROOT / "referencias" / "dimensoes"


DATASET_ALIASES: dict[str, str] = {
    "cadastral": "dados_cadastrais",
    "dados-cadastrais": "dados_cadastrais",
    "cadastro_fisconforme": "dados_cadastrais",
    "malhas_fisconforme": "malhas",
    "efd_bloco_h": "bloco_h",
    "bloco_h_efd": "bloco_h",
    "movimentacao_estoque": "mov_estoque",
    "movimentacao-estoque": "mov_estoque",
    "tbdocumentos": "tb_documentos",
    "documentos_base": "tb_documentos",
    "cte": "cte_base",
    "documentos_cte": "cte_base",
    "info_complementar": "docs_info_complementar",
    "informacoes_complementares": "docs_info_complementar",
    "contatos_documentos": "docs_contatos",
    "email_nfe": "docs_contatos",
}


def _normalize_key(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")


CATALOGO: tuple[DatasetDefinicao, ...] = (
    DatasetDefinicao(
        dataset_id="tb_documentos",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Tabela documental consolidada do pipeline fiscal.",
        relative_dir=("analises", "produtos"),
    ),
    DatasetDefinicao(
        dataset_id="dados_cadastrais",
        sql_id="dados_cadastrais.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.DM_PESSOA", "BI.DM_LOCALIDADE", "BI.DM_REGIME_PAGTO_DESCRICAO"),
        descricao="Dados cadastrais consolidados do contribuinte.",
        relative_dir=("fisconforme",),
        filename_template="dados_cadastrais.parquet",
    ),
    DatasetDefinicao(
        dataset_id="malhas",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Malhas e pendências do Fisconforme por CNPJ.",
        relative_dir=("fisconforme",),
        filename_template="malhas.parquet",
    ),
    DatasetDefinicao(
        dataset_id="c170_xml",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C170", "SPED.REG_C100", "SPED.REG_0200"),
        descricao="Base materializada de C170 usada pelo domínio EFD e pela análise fiscal.",
        relative_dir=("analises", "produtos"),
    ),
    DatasetDefinicao(
        dataset_id="c176_xml",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C176", "SPED.REG_C100", "SPED.REG_C170"),
        descricao="Base materializada de C176 usada pelo domínio EFD e ressarcimento.",
        relative_dir=("analises", "produtos"),
    ),
    DatasetDefinicao(
        dataset_id="bloco_h",
        sql_id="bloco_h.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_H010", "SPED.REG_H005"),
        descricao="Inventário físico (Bloco H).",
        relative_dir=("analises", "produtos"),
    ),
    DatasetDefinicao(
        dataset_id="mov_estoque",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C170", "SPED.REG_C176", "SPED.REG_H010"),
        descricao="Movimentação de estoque consolidada.",
        relative_dir=("analises", "produtos"),
    ),
    DatasetDefinicao(
        dataset_id="aba_mensal",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Visão mensal consolidada da análise fiscal.",
        relative_dir=("analises", "produtos"),
    ),
    DatasetDefinicao(
        dataset_id="aba_anual",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Visão anual consolidada da análise fiscal.",
        relative_dir=("analises", "produtos"),
    ),
    DatasetDefinicao(
        dataset_id="fatores_conversao",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Fatores de conversão aplicados à malha de produtos.",
        relative_dir=("analises", "produtos"),
    ),
    DatasetDefinicao(
        dataset_id="produtos_agrupados",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Agrupamentos de produtos gerados pelo pipeline.",
        relative_dir=("analises", "produtos"),
    ),
    DatasetDefinicao(
        dataset_id="produtos_final",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Catálogo final de produtos do pipeline.",
        relative_dir=("analises", "produtos"),
    ),
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
    DatasetDefinicao(
        dataset_id="cte_base",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Extração documental de CT-e por CNPJ.",
    ),
    DatasetDefinicao(
        dataset_id="docs_info_complementar",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Informações complementares extraídas dos documentos fiscais.",
    ),
    DatasetDefinicao(
        dataset_id="docs_contatos",
        sql_id=None,
        tipo="por_cnpj",
        tabelas_oracle=(),
        descricao="Contatos extraídos dos documentos fiscais.",
    ),
    DatasetDefinicao(
        dataset_id="efd_c100",
        sql_id="c100.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C100", "SPED.REG_0000"),
        descricao="Documentos C100.",
    ),
    DatasetDefinicao(
        dataset_id="efd_c170",
        sql_id="c170.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C170", "SPED.REG_C100", "SPED.REG_0200"),
        descricao="Itens de documentos fiscais EFD.",
    ),
    DatasetDefinicao(
        dataset_id="efd_c176",
        sql_id="c176.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C176", "SPED.REG_C100", "SPED.REG_C170"),
        descricao="Ressarcimento ST EFD.",
    ),
    DatasetDefinicao(
        dataset_id="efd_c176_v2",
        sql_id="c176_v2.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C176", "SPED.REG_C100", "SPED.REG_C170", "SPED.REG_0200"),
        descricao="Ressarcimento ST v2.",
    ),
    DatasetDefinicao(
        dataset_id="efd_c176_mensal",
        sql_id="c176_mensal.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_C176", "SPED.REG_C100", "SPED.REG_C170"),
        descricao="Ressarcimento ST mensal.",
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
        descricao="Abertura do arquivo digital.",
    ),
    DatasetDefinicao(
        dataset_id="efd_reg_0005",
        sql_id="reg_0005.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_0005",),
        descricao="Dados complementares da entidade.",
    ),
    DatasetDefinicao(
        dataset_id="efd_e111",
        sql_id="E111.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SPED.REG_E111",),
        descricao="Ajustes da apuração do ICMS.",
    ),
    DatasetDefinicao(
        dataset_id="sitafe_calculo_item",
        sql_id="shared_sql/sitafe_nfe_calculo_item.sql",
        tipo="por_cnpj",
        tabelas_oracle=("SITAFE.SITAFE_NFE_CALCULO_ITEM",),
        descricao="Itens de cálculo do SITAFE.",
    ),
    DatasetDefinicao(
        dataset_id="dim_localidade",
        sql_id=None,
        tipo="dimensao_global",
        tabelas_oracle=("BI.DM_LOCALIDADE",),
        descricao="Dimensão de localidades.",
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
    DatasetDefinicao(
        dataset_id="dif_icms_nfe_efd",
        sql_id="dif_ICMS_NFe_EFD.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.FATO_NFE_DETALHE", "BI.FATO_NFCE_DETALHE", "SPED.REG_C100"),
        descricao="Diferença ICMS NFe vs EFD.",
    ),
    DatasetDefinicao(
        dataset_id="composicao_enderecos",
        sql_id="dossie_enderecos.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.DM_PESSOA", "BI.FATO_NFE_DETALHE"),
        descricao="Histórico de endereços consolidado.",
    ),
    DatasetDefinicao(
        dataset_id="composicao_fronteira",
        sql_id="fronteira.sql",
        tipo="por_cnpj",
        tabelas_oracle=("BI.FATO_NFE_DETALHE", "SITAFE.SITAFE_NFE_CALCULO_ITEM"),
        descricao="Relatório de Fronteira.",
    ),
)

_INDICE_POR_ID: dict[str, DatasetDefinicao] = {d.dataset_id: d for d in CATALOGO}
_INDICE_POR_SQL: dict[str, DatasetDefinicao] = {
    d.sql_id.lower(): d for d in CATALOGO if d.sql_id
}


def normalizar_dataset_id(dataset_id: str) -> str:
    normalized = _normalize_key(dataset_id)
    return DATASET_ALIASES.get(normalized, normalized)


def listar_aliases_dataset(dataset_id: str) -> list[str]:
    canonical = normalizar_dataset_id(dataset_id)
    aliases = [alias for alias, target in DATASET_ALIASES.items() if target == canonical]
    return sorted(set(aliases))


def catalogo_resumido() -> dict[str, Any]:
    materialized = [
        d.dataset_id
        for d in CATALOGO
        if d.tipo == "por_cnpj" and d.relative_dir is not None
    ]
    return {
        "total_datasets": len(CATALOGO),
        "total_aliases": len(DATASET_ALIASES),
        "materialized_datasets": sorted(materialized),
        "aliases": dict(sorted(DATASET_ALIASES.items())),
    }


def _pasta_shared_sql(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj.strip() / "arquivos_parquet" / "shared_sql"


def _build_filename(definicao: DatasetDefinicao, cnpj: str | None = None) -> str:
    if definicao.filename_template:
        return definicao.filename_template.format(dataset_id=definicao.dataset_id, cnpj=cnpj or "")
    if definicao.tipo == "dimensao_global":
        return f"{definicao.dataset_id}.parquet"
    return f"{definicao.dataset_id}_{cnpj}.parquet"


def _caminho_canonico_por_cnpj(cnpj: str, definicao: DatasetDefinicao) -> Path:
    if definicao.relative_dir:
        return CNPJ_ROOT / cnpj.strip() / Path(*definicao.relative_dir) / _build_filename(definicao, cnpj)
    return _pasta_shared_sql(cnpj) / _build_filename(definicao, cnpj)


def _caminho_dimensao_global(definicao: DatasetDefinicao) -> Path:
    return REFERENCIAS_ROOT / _build_filename(definicao)


def _expandir_caminho_materializado(caminho: Path) -> list[Path]:
    candidatos: list[Path] = [caminho]
    if caminho.suffix.lower() == ".parquet":
        candidatos.append(caminho.with_suffix(""))
    elif caminho.suffix == "":
        candidatos.append(caminho.with_suffix(".parquet"))
    return candidatos


def _resolver_caminho_materializado(caminho: Path) -> Path:
    for candidato in _expandir_caminho_materializado(caminho):
        if candidato.exists():
            return candidato
    return caminho


def _caminho_metadata(caminho_dataset: Path) -> Path:
    if caminho_dataset.suffix.lower() == ".parquet":
        return caminho_dataset.with_suffix(".metadata.json")
    return caminho_dataset / "_dataset.metadata.json"


def _caminhos_legados(cnpj: str, dataset_id: str) -> list[Path]:
    base = CNPJ_ROOT / cnpj.strip() / "arquivos_parquet"
    base_analises = CNPJ_ROOT / cnpj.strip() / "analises"
    base_produtos = base_analises / "produtos"
    base_ressarcimento = base_analises / "ressarcimento_st"
    base_fisconforme = CNPJ_ROOT / cnpj.strip() / "fisconforme"
    did = normalizar_dataset_id(dataset_id)

    mapeamento: dict[str, list[Path]] = {
        "tb_documentos": [
            base_produtos / f"tb_documentos_{cnpj}.parquet",
            base / f"tb_documentos_{cnpj}.parquet",
            CNPJ_ROOT / cnpj.strip() / f"tb_documentos_{cnpj}.parquet",
        ],
        "dados_cadastrais": [
            base_fisconforme / "dados_cadastrais.parquet",
            base / f"dados_cadastrais_{cnpj}.parquet",
        ],
        "malhas": [
            base_fisconforme / "malhas.parquet",
        ],
        "c170_xml": [
            base_produtos / f"c170_xml_{cnpj}.parquet",
            base / f"c170_xml_{cnpj}.parquet",
            base / "fiscal" / "efd" / f"c170_xml_{cnpj}.parquet",
        ],
        "c176_xml": [
            base_produtos / f"c176_xml_{cnpj}.parquet",
            base / f"c176_xml_{cnpj}.parquet",
            base / "fiscal" / "efd" / f"c176_xml_{cnpj}.parquet",
        ],
        "bloco_h": [
            base_produtos / f"bloco_h_{cnpj}.parquet",
            base / f"bloco_h_{cnpj}.parquet",
            base / "fiscal" / "efd" / f"bloco_h_{cnpj}.parquet",
        ],
        "mov_estoque": [
            base_produtos / f"mov_estoque_{cnpj}.parquet",
        ],
        "aba_mensal": [
            base_produtos / f"aba_mensal_{cnpj}.parquet",
            base_produtos / f"calculos_mensais_{cnpj}.parquet",
        ],
        "aba_anual": [
            base_produtos / f"aba_anual_{cnpj}.parquet",
            base_produtos / f"calculos_anuais_{cnpj}.parquet",
        ],
        "fatores_conversao": [
            base_produtos / f"fatores_conversao_{cnpj}.parquet",
        ],
        "produtos_agrupados": [
            base_produtos / f"produtos_agrupados_{cnpj}.parquet",
        ],
        "produtos_final": [
            base_produtos / f"produtos_final_{cnpj}.parquet",
        ],
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
        "cte_base": [
            base / f"CTe_{cnpj}.parquet",
            base / f"cte_{cnpj}.parquet",
            base / "fiscal" / "documentos" / f"CTe_{cnpj}.parquet",
            base / f"cte_xml_{cnpj}.parquet",
            base / f"CTe_xml_{cnpj}.parquet",
        ],
        "docs_info_complementar": [
            base / f"nfe_info_compl_{cnpj}.parquet",
            base / f"NFe_info_compl_{cnpj}.parquet",
            base / f"docs_nfe_info_complementar_{cnpj}.parquet",
        ],
        "docs_contatos": [
            base / f"email_nfe_{cnpj}.parquet",
            base / f"Email_NFe_{cnpj}.parquet",
            base / f"docs_nfe_contatos_{cnpj}.parquet",
        ],
        "efd_c100": [base / f"c100_{cnpj}.parquet"],
        "efd_c170": [base / f"c170_{cnpj}.parquet"],
        "efd_c176": [base / f"c176_{cnpj}.parquet"],
        "efd_0200": [base / f"reg_0200_{cnpj}.parquet"],
        "efd_0190": [base / f"reg_0190_{cnpj}.parquet"],
        "efd_0000": [base / f"reg_0000_{cnpj}.parquet"],
        "efd_e111": [base / f"E111_{cnpj}.parquet", base / f"e111_{cnpj}.parquet"],
        "sitafe_calculo_item": [
            base / "shared_sql" / f"sitafe_nfe_calculo_item_{cnpj}.parquet",
            CNPJ_ROOT / cnpj.strip() / "shared_sql" / f"sitafe_nfe_calculo_item_{cnpj}.parquet",
        ],
        "efd_c176_v2": [base / f"c176_v2_{cnpj}.parquet"],
        "efd_c176_mensal": [base / f"c176_mensal_{cnpj}.parquet"],
        "efd_c190": [base / f"c190_{cnpj}.parquet"],
        "efd_reg_0005": [base / f"reg_0005_{cnpj}.parquet"],
        "ressarcimento_st_item": [base_ressarcimento / f"ressarcimento_st_item_{cnpj}.parquet"],
        "ressarcimento_st_mensal": [base_ressarcimento / f"ressarcimento_st_mensal_{cnpj}.parquet"],
    }
    return mapeamento.get(did, [])


def obter_definicao(dataset_id: str) -> DatasetDefinicao | None:
    return _INDICE_POR_ID.get(normalizar_dataset_id(dataset_id))


def obter_definicao_por_sql(sql_id: str) -> DatasetDefinicao | None:
    return _INDICE_POR_SQL.get(sql_id.lower().strip())


def listar_datasets() -> list[DatasetDefinicao]:
    return sorted(CATALOGO, key=lambda d: d.dataset_id)


def listar_datasets_por_tabela(tabela_oracle: str) -> list[DatasetDefinicao]:
    tabela = tabela_oracle.upper().strip()
    return [d for d in CATALOGO if tabela in d.tabelas_oracle]


def obter_caminho(cnpj: str | None, dataset_id: str) -> Path:
    definicao = obter_definicao(dataset_id)
    if definicao is None:
        raise ValueError(f"Dataset desconhecido: {dataset_id}")
    if definicao.tipo == "dimensao_global":
        return _caminho_dimensao_global(definicao)
    if cnpj is None:
        raise ValueError(f"CNPJ obrigatório para dataset por_cnpj: {dataset_id}")
    return _caminho_canonico_por_cnpj(cnpj.strip(), definicao)


def listar_caminhos_com_fallback(cnpj: str, dataset_id: str) -> list[Path]:
    definicao = obter_definicao(dataset_id)
    if definicao is None:
        return []

    candidatos: list[Path] = []
    vistos: set[str] = set()

    def _add(caminho: Path) -> None:
        for item in _expandir_caminho_materializado(caminho):
            chave = str(item).lower()
            if chave not in vistos:
                vistos.add(chave)
                candidatos.append(item)

    if definicao.tipo == "dimensao_global":
        _add(_caminho_dimensao_global(definicao))
    else:
        _add(_caminho_canonico_por_cnpj(cnpj.strip(), definicao))

    for legado in _caminhos_legados(cnpj.strip(), definicao.dataset_id):
        _add(legado)

    return candidatos


def encontrar_dataset(cnpj: str, dataset_id: str) -> DatasetLocalizado | None:
    canonical = normalizar_dataset_id(dataset_id)
    candidatos = listar_caminhos_com_fallback(cnpj, canonical)
    for idx, caminho in enumerate(candidatos):
        resolved = _resolver_caminho_materializado(caminho)
        if resolved.exists():
            metadata = _ler_metadata(resolved)
            return DatasetLocalizado(
                dataset_id=canonical,
                caminho=resolved,
                reutilizado=idx > 0,
                metadata=metadata,
            )
    return None


def carregar_lazyframe(cnpj: str, dataset_id: str) -> tuple[pl.LazyFrame, Path] | None:
    localizado = encontrar_dataset(cnpj, dataset_id)
    if localizado is None:
        return None
    if localizado.caminho.is_dir():
        return scan_delta_table(localizado.caminho), localizado.caminho
    return pl.scan_parquet(localizado.caminho), localizado.caminho


def carregar_dataframe(cnpj: str, dataset_id: str) -> DatasetLocalizado | None:
    localizado = encontrar_dataset(cnpj, dataset_id)
    if localizado is None:
        return None
    return localizado


def criar_metadata(
    *,
    cnpj: str | None,
    dataset_id: str,
    sql_id: str | None = None,
    parametros: dict[str, Any] | None = None,
    linhas: int | None = None,
) -> dict[str, Any]:
    return {
        "dataset_id": normalizar_dataset_id(dataset_id),
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
    canonical = normalizar_dataset_id(dataset_id)
    caminho = obter_caminho(cnpj, canonical)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    formato = resolve_storage_format(caminho)

    try:
        if formato == "delta":
            destino = write_delta_table(dataframe, caminho, table_name=canonical)
        else:
            dataframe.write_parquet(caminho, compression="snappy")
            destino = caminho
    except Exception:
        logger.exception("Falha ao gravar dataset %s em %s", canonical, caminho)
        return None

    if metadata is not None:
        _gravar_metadata(destino, metadata)

    logger.info(
        "Dataset %s registrado: %d linhas -> %s",
        canonical,
        dataframe.height,
        destino,
    )
    return destino


def _ler_metadata(caminho_dataset: Path) -> dict[str, Any] | None:
    caminho_meta = _caminho_metadata(_resolver_caminho_materializado(caminho_dataset))
    if not caminho_meta.exists():
        return None
    try:
        return json.loads(caminho_meta.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _gravar_metadata(caminho_dataset: Path, metadata: dict[str, Any]) -> None:
    caminho_meta = _caminho_metadata(caminho_dataset)
    try:
        if caminho_dataset.is_dir():
            caminho_dataset.mkdir(parents=True, exist_ok=True)
        else:
            caminho_meta.parent.mkdir(parents=True, exist_ok=True)
        caminho_meta.write_text(
            json.dumps(metadata, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
    except OSError:
        logger.warning("Falha ao gravar metadata sidecar: %s", caminho_meta)


def diagnosticar_disponibilidade(cnpj: str) -> list[dict[str, Any]]:
    resultado: list[dict[str, Any]] = []
    for definicao in CATALOGO:
        localizado = encontrar_dataset(cnpj, definicao.dataset_id)
        resultado.append({
            "dataset_id": definicao.dataset_id,
            "aliases": listar_aliases_dataset(definicao.dataset_id),
            "tipo": definicao.tipo,
            "sql_id": definicao.sql_id,
            "disponivel": localizado is not None,
            "caminho": str(localizado.caminho) if localizado else None,
            "formato": "delta" if (localizado and localizado.caminho.is_dir()) else ("parquet" if localizado else None),
            "reutilizado": localizado.reutilizado if localizado else False,
        })
    return resultado


def resolver_dataset_por_sql_id(sql_id: str) -> str | None:
    definicao = obter_definicao_por_sql(sql_id)
    if definicao is None:
        sql_basico = Path(str(sql_id).strip()).name.lower()
        for item in CATALOGO:
            if item.sql_id and Path(item.sql_id).name.lower() == sql_basico:
                definicao = item
                break
    return definicao.dataset_id if definicao else None
