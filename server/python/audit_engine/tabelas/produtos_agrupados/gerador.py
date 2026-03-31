"""Gerador da tabela `produtos_agrupados`."""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.pipeline_fiscal import (
    carregar_json_se_existir,
    criar_dataframe_vazio_contrato,
    escrever_dataframe_ao_contrato,
    normalizar_descricao,
    normalizar_lista_ids,
)

logger = logging.getLogger(__name__)


def _montar_mapa_unidades(df_produtos_unidades: pl.DataFrame) -> dict[str, dict[str, str | None]]:
    """Monta lookup das unidades predominantes de cada produto publico."""
    mapa: dict[str, dict[str, str | None]] = {}
    for linha in df_produtos_unidades.to_dicts():
        mapa[str(linha["id_produto"])] = {
            "unid_compra": linha.get("unid_compra"),
            "unid_venda": linha.get("unid_venda"),
        }
    return mapa


def _escolher_texto_padrao(valores: list[str]) -> str:
    """Escolhe o texto mais frequente com desempate pelo maior comprimento."""
    textos_validos = [texto for texto in valores if texto]
    if not textos_validos:
        return ""
    contagem = Counter(textos_validos)
    return max(contagem, key=lambda texto: (contagem[texto], len(texto), texto))


def _agrupar_automaticamente(registros: list[dict[str, object]]) -> list[list[dict[str, object]]]:
    """Agrupa produtos restantes por GTIN compartilhado ou descricao+NCM."""
    grupos_por_gtin: dict[str, list[dict[str, object]]] = {}
    grupos_por_descricao_ncm: dict[tuple[str, str], list[dict[str, object]]] = {}

    for registro in registros:
        gtin = str(registro.get("gtin") or "").strip()
        descricao_normalizada = str(registro.get("descricao_normalizada") or "").strip()
        ncm = str(registro.get("ncm") or "").strip()

        if gtin:
            grupos_por_gtin.setdefault(gtin, []).append(registro)
            continue

        chave = (descricao_normalizada, ncm)
        grupos_por_descricao_ncm.setdefault(chave, []).append(registro)

    grupos = list(grupos_por_gtin.values()) + list(grupos_por_descricao_ncm.values())
    return [grupo for grupo in grupos if grupo]


def _criar_registro_grupo(
    id_agrupado: str,
    membros: list[dict[str, object]],
    mapa_unidades: dict[str, dict[str, str | None]],
    *,
    origem: str,
    descricao_padrao_manual: str | None = None,
) -> dict[str, object]:
    """Consolida um grupo de produtos em um unico registro publico."""
    ids_membros = sorted({str(membro["id_produto"]) for membro in membros})
    descricoes = [str(membro.get("descricao") or "").strip() for membro in membros]
    ncms = [str(membro.get("ncm") or "").strip() for membro in membros if str(membro.get("ncm") or "").strip()]
    cests = [str(membro.get("cest") or "").strip() for membro in membros if str(membro.get("cest") or "").strip()]
    unidades_compra = [
        str(mapa_unidades.get(id_membro, {}).get("unid_compra") or "").strip()
        for id_membro in ids_membros
        if str(mapa_unidades.get(id_membro, {}).get("unid_compra") or "").strip()
    ]
    unidades_venda = [
        str(mapa_unidades.get(id_membro, {}).get("unid_venda") or "").strip()
        for id_membro in ids_membros
        if str(mapa_unidades.get(id_membro, {}).get("unid_venda") or "").strip()
    ]
    agora = datetime.now(tz=timezone.utc).isoformat()

    return {
        "id_agrupado": id_agrupado,
        "descricao_padrao": descricao_padrao_manual or _escolher_texto_padrao(descricoes),
        "ncm_padrao": _escolher_texto_padrao(ncms),
        "cest_padrao": _escolher_texto_padrao(cests),
        "ids_membros": json.dumps(ids_membros, ensure_ascii=False),
        "qtd_membros": len(ids_membros),
        "qtd_total_nfe": int(sum(int(membro.get("qtd_total_nfe") or 0) for membro in membros)),
        "valor_total": float(sum(float(membro.get("valor_total") or 0.0) for membro in membros)),
        "unid_compra": _escolher_texto_padrao(unidades_compra),
        "unid_venda": _escolher_texto_padrao(unidades_venda),
        "origem": origem,
        "criado_em": agora,
        "editado_em": agora if origem == "manual" else None,
        "status": "ativo",
    }


@registrar_gerador("produtos_agrupados")
def gerar_produtos_agrupados(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Agrupa produtos automaticamente e aplica edicoes manuais do auditor."""
    arquivo_produtos = diretorio_parquets / "produtos.parquet"
    arquivo_produtos_unidades = diretorio_parquets / "produtos_unidades.parquet"
    arquivo_edicoes = diretorio_cnpj / "edicoes" / "agregacao.json"

    if not arquivo_produtos.exists():
        raise FileNotFoundError("produtos.parquet nao encontrado")

    df_produtos = pl.read_parquet(arquivo_produtos)
    if df_produtos.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_produtos_unidades = (
        pl.read_parquet(arquivo_produtos_unidades)
        if arquivo_produtos_unidades.exists()
        else pl.DataFrame(schema={"id_produto": pl.Int64, "unid_compra": pl.String, "unid_venda": pl.String})
    )
    mapa_unidades = _montar_mapa_unidades(df_produtos_unidades)
    edicoes_manuais = carregar_json_se_existir(arquivo_edicoes, {})

    registros_produtos = (
        df_produtos
        .with_columns(
            [
                pl.col("id_produto").cast(pl.Int64, strict=False),
                pl.col("descricao").cast(pl.Utf8, strict=False),
                pl.col("descricao").map_elements(normalizar_descricao, return_dtype=pl.String).alias("descricao_normalizada"),
                pl.col("ncm").cast(pl.Utf8, strict=False),
                pl.col("cest").cast(pl.Utf8, strict=False),
            ]
        )
        .sort("id_produto")
        .to_dicts()
    )

    ids_agrupados_manualmente: set[str] = set()
    grupos: list[dict[str, object]] = []
    sequencia = 1

    for descricao_manual, ids_manuais in edicoes_manuais.items():
        ids_normalizados = set(normalizar_lista_ids(ids_manuais))
        membros = [registro for registro in registros_produtos if str(registro["id_produto"]) in ids_normalizados]
        if not membros:
            continue

        grupos.append(
            _criar_registro_grupo(
                f"AGR_{sequencia:05d}",
                membros,
                mapa_unidades,
                origem="manual",
                descricao_padrao_manual=str(descricao_manual).strip() or None,
            )
        )
        ids_agrupados_manualmente.update(ids_normalizados)
        sequencia += 1

    registros_restantes = [
        registro
        for registro in registros_produtos
        if str(registro["id_produto"]) not in ids_agrupados_manualmente
    ]

    for membros in _agrupar_automaticamente(registros_restantes):
        grupos.append(_criar_registro_grupo(f"AGR_{sequencia:05d}", membros, mapa_unidades, origem="automatico"))
        sequencia += 1

    if not grupos:
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_grupos = pl.DataFrame(grupos)
    total_registros = escrever_dataframe_ao_contrato(df_grupos, arquivo_saida, contrato)
    logger.info("produtos_agrupados: %s grupos gerados", total_registros)
    return total_registros
