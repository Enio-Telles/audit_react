"""Materializacao canonica da camada base EFD."""
from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import polars as pl

from processing.efd import (
    build_base_arquivos_validos,
    build_base_bloco_h_tipado,
    build_base_reg_0190_tipado,
    build_base_reg_0200_tipado,
    build_base_reg_0220_tipado,
    build_base_reg_c100_tipado,
    build_base_reg_c170_tipado,
    build_base_reg_c176_tipado,
    build_base_reg_c190_tipado,
)
from utilitarios.dataset_registry import criar_metadata
from utilitarios.project_paths import CNPJ_ROOT


def _cnpj_dir(cnpj: str, pasta_cnpj: Path | None = None) -> Path:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    return pasta_cnpj or (CNPJ_ROOT / cnpj_limpo)


def _path_if_exists(path: Path) -> Path | None:
    return path if path.exists() else None


def _resolver_entrada(cnpj: str, pasta_cnpj: Path, chave: str) -> Path:
    arquivos_parquet = pasta_cnpj / "arquivos_parquet"
    atomizadas = arquivos_parquet / "atomizadas"
    raw_efd = pasta_cnpj / "raw" / "efd"

    candidatos: dict[str, list[Path]] = {
        "reg_0000": [
            raw_efd / "reg_0000",
            raw_efd / "reg_0000.parquet",
            arquivos_parquet / f"reg_0000_{cnpj}.parquet",
            atomizadas / "shared" / f"01_reg0000_historico_{cnpj}.parquet",
            atomizadas / "shared" / f"02_reg0000_versionado_{cnpj}.parquet",
            atomizadas / "shared" / f"03_reg0000_ultimo_periodo_{cnpj}.parquet",
        ],
        "reg_0190": [
            raw_efd / "reg_0190",
            raw_efd / "reg_0190.parquet",
            arquivos_parquet / f"reg_0190_{cnpj}.parquet",
        ],
        "reg_0200": [
            raw_efd / "reg_0200",
            raw_efd / "reg_0200.parquet",
            arquivos_parquet / f"reg_0200_{cnpj}.parquet",
            atomizadas / "dimensions" / f"50_reg0200_raw_{cnpj}.parquet",
        ],
        "reg_0220": [
            raw_efd / "reg_0220",
            raw_efd / "reg_0220.parquet",
            arquivos_parquet / f"reg_0220_{cnpj}.parquet",
        ],
        "c100": [
            raw_efd / "reg_c100",
            raw_efd / "reg_c100.parquet",
            arquivos_parquet / f"c100_{cnpj}.parquet",
        ],
        "c170": [
            raw_efd / "reg_c170",
            raw_efd / "reg_c170.parquet",
            arquivos_parquet / f"c170_{cnpj}.parquet",
        ],
        "c176": [
            raw_efd / "reg_c176",
            raw_efd / "reg_c176.parquet",
            arquivos_parquet / f"c176_{cnpj}.parquet",
        ],
        "c190": [
            raw_efd / "reg_c190",
            raw_efd / "reg_c190.parquet",
            arquivos_parquet / f"c190_{cnpj}.parquet",
        ],
        "h005": [
            raw_efd / "reg_h005",
            raw_efd / "reg_h005.parquet",
            arquivos_parquet / f"h005_{cnpj}.parquet",
            atomizadas / "bloco_h" / f"40_h005_raw_{cnpj}.parquet",
        ],
        "h010": [
            raw_efd / "reg_h010",
            raw_efd / "reg_h010.parquet",
            arquivos_parquet / f"h010_{cnpj}.parquet",
            atomizadas / "bloco_h" / f"41_h010_raw_{cnpj}.parquet",
        ],
        "h020": [
            raw_efd / "reg_h020",
            raw_efd / "reg_h020.parquet",
            arquivos_parquet / f"h020_{cnpj}.parquet",
            atomizadas / "bloco_h" / f"42_h020_raw_{cnpj}.parquet",
        ],
    }

    for caminho in candidatos.get(chave, []):
        existente = _path_if_exists(caminho)
        if existente is not None:
            return existente
    raise FileNotFoundError(f"Nenhuma fonte encontrada para {chave} do CNPJ {cnpj}.")


def _destino_base(pasta_cnpj: Path, nome: str) -> Path:
    return pasta_cnpj / "base" / "efd" / nome


def _limpar_destino(destino: Path) -> None:
    if destino.exists():
        shutil.rmtree(destino)


def _contar_linhas(destino: Path) -> int | None:
    parquet_files = sorted(destino.rglob("*.parquet")) if destino.is_dir() else [destino]
    if not parquet_files:
        return None
    return int(pl.scan_parquet([str(item) for item in parquet_files]).select(pl.len().alias("__count")).collect()["__count"][0])


def _gravar_metadata(destino: Path, metadata: dict[str, object]) -> None:
    destino.mkdir(parents=True, exist_ok=True)
    (destino / "_dataset.metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def _materializar(
    *,
    cnpj: str,
    pasta_cnpj: Path,
    nome_destino: str,
    dataset_id: str,
    builder,
    fontes: dict[str, str],
) -> bool:
    origem_resolvida = {
        nome: str(_resolver_entrada(cnpj, pasta_cnpj, chave))
        for nome, chave in fontes.items()
    }
    destino = _destino_base(pasta_cnpj, nome_destino)
    _limpar_destino(destino)
    builder(*origem_resolvida.values(), destino)
    metadata = criar_metadata(
        cnpj=cnpj,
        dataset_id=dataset_id,
        linhas=_contar_linhas(destino),
        parametros={"fontes": origem_resolvida, "camada": "base", "dominio": "efd"},
    )
    _gravar_metadata(destino, metadata)
    return True


def gerar_base_efd_arquivos_validos(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    pasta = _cnpj_dir(cnpj_limpo, pasta_cnpj)
    return _materializar(
        cnpj=cnpj_limpo,
        pasta_cnpj=pasta,
        nome_destino="arquivos_validos",
        dataset_id="base__efd__arquivos_validos",
        builder=build_base_arquivos_validos,
        fontes={"reg_0000_path": "reg_0000"},
    )


def gerar_base_efd_reg_0190_tipado(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    pasta = _cnpj_dir(cnpj_limpo, pasta_cnpj)
    return _materializar(
        cnpj=cnpj_limpo,
        pasta_cnpj=pasta,
        nome_destino="reg_0190_tipado",
        dataset_id="base__efd__reg_0190_tipado",
        builder=build_base_reg_0190_tipado,
        fontes={"reg_0190_path": "reg_0190"},
    )


def gerar_base_efd_reg_0200_tipado(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    pasta = _cnpj_dir(cnpj_limpo, pasta_cnpj)
    return _materializar(
        cnpj=cnpj_limpo,
        pasta_cnpj=pasta,
        nome_destino="reg_0200_tipado",
        dataset_id="base__efd__reg_0200_tipado",
        builder=build_base_reg_0200_tipado,
        fontes={"reg_0200_path": "reg_0200"},
    )


def gerar_base_efd_reg_0220_tipado(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    pasta = _cnpj_dir(cnpj_limpo, pasta_cnpj)
    return _materializar(
        cnpj=cnpj_limpo,
        pasta_cnpj=pasta,
        nome_destino="reg_0220_tipado",
        dataset_id="base__efd__reg_0220_tipado",
        builder=build_base_reg_0220_tipado,
        fontes={"reg_0220_path": "reg_0220"},
    )


def gerar_base_efd_reg_c100_tipado(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    pasta = _cnpj_dir(cnpj_limpo, pasta_cnpj)
    return _materializar(
        cnpj=cnpj_limpo,
        pasta_cnpj=pasta,
        nome_destino="reg_c100_tipado",
        dataset_id="base__efd__reg_c100_tipado",
        builder=build_base_reg_c100_tipado,
        fontes={
            "c100_path": "c100",
            "arquivos_validos_path": "reg_0000",
        },
    )


def gerar_base_efd_reg_c170_tipado(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    pasta = _cnpj_dir(cnpj_limpo, pasta_cnpj)
    return _materializar(
        cnpj=cnpj_limpo,
        pasta_cnpj=pasta,
        nome_destino="reg_c170_tipado",
        dataset_id="base__efd__reg_c170_tipado",
        builder=build_base_reg_c170_tipado,
        fontes={
            "c170_path": "c170",
            "c100_base_path": "c100",
        },
    )


def gerar_base_efd_reg_c190_tipado(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    pasta = _cnpj_dir(cnpj_limpo, pasta_cnpj)
    return _materializar(
        cnpj=cnpj_limpo,
        pasta_cnpj=pasta,
        nome_destino="reg_c190_tipado",
        dataset_id="base__efd__reg_c190_tipado",
        builder=build_base_reg_c190_tipado,
        fontes={
            "c190_path": "c190",
            "c100_base_path": "c100",
        },
    )


def gerar_base_efd_reg_c176_tipado(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    pasta = _cnpj_dir(cnpj_limpo, pasta_cnpj)
    return _materializar(
        cnpj=cnpj_limpo,
        pasta_cnpj=pasta,
        nome_destino="reg_c176_tipado",
        dataset_id="base__efd__reg_c176_tipado",
        builder=build_base_reg_c176_tipado,
        fontes={
            "c176_path": "c176",
            "c170_base_path": "c170",
        },
    )


def gerar_base_efd_bloco_h_tipado(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    pasta = _cnpj_dir(cnpj_limpo, pasta_cnpj)
    return _materializar(
        cnpj=cnpj_limpo,
        pasta_cnpj=pasta,
        nome_destino="bloco_h_tipado",
        dataset_id="base__efd__bloco_h_tipado",
        builder=build_base_bloco_h_tipado,
        fontes={
            "h005_path": "h005",
            "h010_path": "h010",
            "h020_path": "h020",
        },
    )


__all__ = [
    "gerar_base_efd_arquivos_validos",
    "gerar_base_efd_reg_0190_tipado",
    "gerar_base_efd_reg_0200_tipado",
    "gerar_base_efd_reg_0220_tipado",
    "gerar_base_efd_reg_c100_tipado",
    "gerar_base_efd_reg_c170_tipado",
    "gerar_base_efd_reg_c190_tipado",
    "gerar_base_efd_reg_c176_tipado",
    "gerar_base_efd_bloco_h_tipado",
]
