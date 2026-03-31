"""Compara a trilha ST local com artefatos equivalentes do projeto externo."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import polars as pl

DIRETORIO_PYTHON = Path(__file__).resolve().parents[1]
if str(DIRETORIO_PYTHON) not in sys.path:
    sys.path.insert(0, str(DIRETORIO_PYTHON))

from audit_engine.utils.pipeline_fiscal import normalizar_descricao


BASE_LOCAL = Path(r"c:\audit_react\storage\CNPJ")
BASE_EXTERNA = Path(r"C:\funcoes - Copia\dados\CNPJ")

ARTEFATOS_PARIDADE_ST: dict[str, dict[str, str | None]] = {
    "extraidos/c176": {
        "local": "extraidos/c176.parquet",
        "externo": "arquivos_parquet/c176_{cnpj}.parquet",
    },
    "extraidos/nfe_dados_st": {
        "local": "extraidos/nfe_dados_st.parquet",
        "externo": "arquivos_parquet/nfe_dados_st_{cnpj}.parquet",
    },
    "extraidos/e111": {
        "local": "extraidos/e111.parquet",
        "externo": "arquivos_parquet/e111_{cnpj}.parquet",
    },
    "silver/c176_xml": {
        "local": "silver/c176_xml.parquet",
        "externo": "analises/produtos/c176_xml_{cnpj}.parquet",
    },
    "silver/nfe_dados_st": {
        "local": "silver/nfe_dados_st.parquet",
        "externo": None,
    },
    "silver/e111_ajustes": {
        "local": "silver/e111_ajustes.parquet",
        "externo": None,
    },
    "parquets/ajustes_e111": {
        "local": "parquets/ajustes_e111.parquet",
        "externo": None,
    },
    "parquets/st_itens": {
        "local": "parquets/st_itens.parquet",
        "externo": None,
    },
}

SCHEMA_CANONICO_C176: dict[str, pl.DataType] = {
    "periodo_efd": pl.String,
    "data_entrega_efd_periodo": pl.String,
    "cod_fin_efd": pl.String,
    "finalidade_efd": pl.String,
    "chave_saida": pl.String,
    "num_nf_saida": pl.String,
    "dt_doc_saida": pl.String,
    "dt_e_s_saida": pl.String,
    "cod_item": pl.String,
    "descricao_item": pl.String,
    "num_item_saida": pl.String,
    "cfop_saida": pl.String,
    "unid_saida": pl.String,
    "qtd_item_saida": pl.Float64,
    "vl_total_item": pl.Float64,
    "cod_mot_res": pl.String,
    "descricao_motivo_ressarcimento": pl.String,
    "chave_nfe_ultima_entrada": pl.String,
    "c176_num_item_ult_e_declarado": pl.String,
    "dt_ultima_entrada": pl.String,
    "vl_unit_bc_st_entrada": pl.Float64,
    "vl_unit_icms_proprio_entrada": pl.Float64,
    "vl_unit_ressarcimento_st": pl.Float64,
    "vl_ressarc_credito_proprio": pl.Float64,
    "vl_ressarc_st_retido": pl.Float64,
    "vr_total_ressarcimento": pl.Float64,
}

SCHEMA_CANONICO_NFE_DADOS_ST: dict[str, pl.DataType] = {
    "chave_acesso": pl.String,
    "prod_nitem": pl.Int64,
    "prod_cprod": pl.String,
    "icms_vbcst": pl.Float64,
    "icms_vicmsst": pl.Float64,
    "icms_vicmssubstituto": pl.Float64,
    "icms_vicmsstret": pl.Float64,
    "icms_vbcfcpst": pl.Float64,
    "icms_pfcpst": pl.Float64,
    "icms_vfcpst": pl.Float64,
}

SCHEMA_CANONICO_E111: dict[str, pl.DataType] = {
    "periodo_efd": pl.String,
    "codigo_ajuste": pl.String,
    "descricao_codigo_ajuste": pl.String,
    "descr_compl": pl.String,
    "valor_ajuste": pl.Float64,
    "data_entrega_efd_periodo": pl.String,
    "cod_fin_efd": pl.String,
}

SCHEMA_CANONICO_C176_XML: dict[str, pl.DataType] = {
    "cnpj": pl.String,
    "periodo_efd": pl.String,
    "data_entrega_efd_periodo": pl.String,
    "cod_fin_efd": pl.String,
    "finalidade_efd": pl.String,
    "chave_saida": pl.String,
    "num_nf_saida": pl.String,
    "dt_doc_saida": pl.String,
    "dt_e_s_saida": pl.String,
    "cod_item_ref_saida": pl.String,
    "descricao_item": pl.String,
    "num_item_saida": pl.String,
    "cfop_saida": pl.String,
    "id_agrupado": pl.String,
    "descr_padrao": pl.String,
    "unid_saida": pl.String,
    "fator_saida": pl.Float64,
    "unid_ref": pl.String,
    "qtd_item_saida": pl.Float64,
    "qtd_saida_unid_ref": pl.Float64,
    "cod_mot_res": pl.String,
    "descricao_motivo_ressarcimento": pl.String,
    "chave_nfe_ultima_entrada": pl.String,
    "c176_num_item_ult_e_declarado": pl.String,
    "dt_ultima_entrada": pl.String,
    "prod_nitem": pl.Int64,
    "unid_entrada_xml": pl.String,
    "fator_entrada_xml": pl.Float64,
    "qtd_entrada_xml": pl.Float64,
    "qtd_entrada_xml_unid_ref": pl.Float64,
    "vl_total_entrada_xml": pl.Float64,
    "vl_unitario_entrada_xml": pl.Float64,
    "vl_unitario_entrada_xml_unid_ref": pl.Float64,
    "vl_unit_bc_st_entrada": pl.Float64,
    "vl_unit_bc_st_entrada_unid_ref": pl.Float64,
    "vl_unit_icms_proprio_entrada": pl.Float64,
    "vl_unit_icms_proprio_entrada_unid_ref": pl.Float64,
    "vl_unit_ressarcimento_st": pl.Float64,
    "vl_unit_ressarcimento_st_unid_ref": pl.Float64,
    "vl_ressarc_credito_proprio": pl.Float64,
    "vl_ressarc_st_retido": pl.Float64,
    "vr_total_ressarcimento": pl.Float64,
    "score_vinculo_entrada": pl.Int64,
    "diff_qtd_vinculo": pl.Float64,
    "regra_vinculo_entrada": pl.String,
    "match_saida_id_agrupado": pl.Boolean,
    "match_entrada_xml": pl.Boolean,
}


def _classificar_camada_artefato(nome_logico: str) -> str:
    """Classifica o artefato na camada operacional correspondente."""
    if nome_logico.startswith("extraidos/"):
        return "bronze"
    if nome_logico.startswith("silver/"):
        return "silver"
    if nome_logico.startswith("parquets/"):
        return "gold"
    return "desconhecida"


def _coletar_metadados_parquet(caminho: Path) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Coleta o dataframe e os metadados brutos de um parquet."""
    if not caminho.exists():
        return (
            pl.DataFrame(),
            {
                "existe": False,
                "caminho": str(caminho),
                "registros": 0,
                "colunas": [],
                "schema": {},
            },
        )

    dataframe = pl.read_parquet(caminho)
    return dataframe, _coletar_metadados_dataframe(dataframe, caminho, existe=True)


def _coletar_metadados_dataframe(
    dataframe: pl.DataFrame,
    caminho: Path,
    *,
    existe: bool = True,
) -> dict[str, Any]:
    """Resume um dataframe ja carregado para o formato do relatorio."""
    if not existe:
        return {
            "existe": False,
            "caminho": str(caminho),
            "registros": 0,
            "colunas": [],
            "schema": {},
        }

    return {
        "existe": True,
        "caminho": str(caminho),
        "registros": len(dataframe),
        "colunas": dataframe.columns,
        "schema": {coluna: str(tipo) for coluna, tipo in dataframe.schema.items()},
    }


def _selecionar_colunas_canonicas(
    dataframe: pl.DataFrame,
    schema_canonico: dict[str, pl.DataType],
    *,
    preencher_ausentes: bool = False,
) -> pl.DataFrame:
    """Seleciona apenas as colunas canonicas disponiveis com tipos normalizados."""
    if dataframe.is_empty() and not dataframe.columns:
        if preencher_ausentes:
            return pl.DataFrame(schema=schema_canonico)
        return pl.DataFrame()

    expressoes: list[pl.Expr] = []
    for coluna, tipo in schema_canonico.items():
        if coluna in dataframe.columns:
            expressoes.append(pl.col(coluna).cast(tipo, strict=False).alias(coluna))
        elif preencher_ausentes:
            expressoes.append(pl.lit(None, dtype=tipo).alias(coluna))

    if not expressoes:
        return pl.DataFrame()
    return dataframe.select(expressoes)


def _comparar_metadados(
    metadados_local: dict[str, Any],
    metadados_externo: dict[str, Any],
) -> dict[str, Any]:
    """Compara contagem, colunas e schema entre duas visoes do mesmo artefato."""
    colunas_local = set(metadados_local["colunas"])
    colunas_externo = set(metadados_externo["colunas"])

    return {
        "diff_registros": int(metadados_local["registros"]) - int(metadados_externo["registros"]),
        "colunas_apenas_local": sorted(colunas_local - colunas_externo),
        "colunas_apenas_externo": sorted(colunas_externo - colunas_local),
        "colunas_em_comum": sorted(colunas_local & colunas_externo),
        "registros_iguais": int(metadados_local["registros"]) == int(metadados_externo["registros"]),
        "schema_igual": metadados_local["schema"] == metadados_externo["schema"],
        "colunas_iguais": metadados_local["colunas"] == metadados_externo["colunas"],
    }


def _projetar_c176_local_canonico(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Reduz o bronze local do C176 ao contrato minimo de compatibilidade externa."""
    if dataframe.is_empty() and not dataframe.columns:
        return pl.DataFrame(schema=SCHEMA_CANONICO_C176)

    df_c176_canonico = dataframe.with_columns(
        [
            pl.col("num_item_ultima_entrada").cast(pl.String, strict=False).alias("c176_num_item_ult_e_declarado"),
            pl.col("periodo_efd").cast(pl.String, strict=False).alias("periodo_efd"),
            pl.col("data_entrega_efd_periodo").cast(pl.String, strict=False).alias("data_entrega_efd_periodo"),
            pl.col("cod_fin_efd").cast(pl.String, strict=False).alias("cod_fin_efd"),
            pl.col("finalidade_efd").cast(pl.String, strict=False).alias("finalidade_efd"),
            pl.col("chave_saida").cast(pl.String, strict=False).alias("chave_saida"),
            pl.col("num_nf_saida").cast(pl.String, strict=False).alias("num_nf_saida"),
            pl.col("dt_doc_saida").cast(pl.String, strict=False).alias("dt_doc_saida"),
            pl.col("dt_e_s_saida").cast(pl.String, strict=False).alias("dt_e_s_saida"),
            pl.col("cod_item").cast(pl.String, strict=False).alias("cod_item"),
            pl.col("descricao_item").cast(pl.String, strict=False).alias("descricao_item"),
            pl.col("num_item_saida").cast(pl.String, strict=False).alias("num_item_saida"),
            pl.col("cfop_saida").cast(pl.String, strict=False).alias("cfop_saida"),
            pl.col("unid_saida").cast(pl.String, strict=False).alias("unid_saida"),
            pl.col("qtd_item_saida").cast(pl.Float64, strict=False).alias("qtd_item_saida"),
            pl.col("vl_total_item").cast(pl.Float64, strict=False).alias("vl_total_item"),
            pl.col("cod_mot_res").cast(pl.String, strict=False).alias("cod_mot_res"),
            pl.col("descricao_motivo_ressarcimento").cast(pl.String, strict=False).alias("descricao_motivo_ressarcimento"),
            pl.col("chave_nfe_ultima_entrada").cast(pl.String, strict=False).alias("chave_nfe_ultima_entrada"),
            pl.col("dt_ultima_entrada").cast(pl.String, strict=False).alias("dt_ultima_entrada"),
            pl.col("vl_unit_bc_st_entrada").cast(pl.Float64, strict=False).alias("vl_unit_bc_st_entrada"),
            pl.col("vl_unit_icms_proprio_entrada").cast(pl.Float64, strict=False).alias("vl_unit_icms_proprio_entrada"),
            pl.col("vl_unit_ressarcimento_st").cast(pl.Float64, strict=False).alias("vl_unit_ressarcimento_st"),
            pl.col("vl_ressarc_credito_proprio").cast(pl.Float64, strict=False).alias("vl_ressarc_credito_proprio"),
            pl.col("vl_ressarc_st_retido").cast(pl.Float64, strict=False).alias("vl_ressarc_st_retido"),
            pl.col("vr_total_ressarcimento").cast(pl.Float64, strict=False).alias("vr_total_ressarcimento"),
        ]
    )

    return _selecionar_colunas_canonicas(df_c176_canonico, SCHEMA_CANONICO_C176)


def _projetar_c176_externo_canonico(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Normaliza o shape externo do C176 para o mesmo contrato canonico."""
    return _selecionar_colunas_canonicas(dataframe, SCHEMA_CANONICO_C176)


def _projetar_nfe_dados_st_canonico(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Normaliza a visao canonica minima do XML de ST para paridade estrutural."""
    return _selecionar_colunas_canonicas(dataframe, SCHEMA_CANONICO_NFE_DADOS_ST)


def _projetar_e111_local_canonico(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Reduz o bronze local do E111 ao contrato compartilhado com a base externa."""
    if dataframe.is_empty() and not dataframe.columns:
        return pl.DataFrame(schema=SCHEMA_CANONICO_E111)

    df_e111_canonico = dataframe.with_columns(
        [
            pl.col("periodo_efd").cast(pl.String, strict=False).alias("periodo_efd"),
            pl.col("codigo_ajuste").cast(pl.String, strict=False).alias("codigo_ajuste"),
            pl.col("descricao_codigo_ajuste").cast(pl.String, strict=False).alias("descricao_codigo_ajuste"),
            pl.col("descr_compl").cast(pl.String, strict=False).alias("descr_compl"),
            pl.col("valor_ajuste").cast(pl.Float64, strict=False).alias("valor_ajuste"),
            pl.col("data_entrega_efd_periodo").cast(pl.String, strict=False).alias("data_entrega_efd_periodo"),
            pl.col("cod_fin_efd").cast(pl.String, strict=False).alias("cod_fin_efd"),
        ]
    )

    return _selecionar_colunas_canonicas(df_e111_canonico, SCHEMA_CANONICO_E111)


def _projetar_e111_externo_canonico(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Normaliza o E111 externo para o mesmo contrato canonico."""
    return _selecionar_colunas_canonicas(dataframe, SCHEMA_CANONICO_E111)


def _carregar_lookup_id_agrupado_por_descricao(base_local_cnpj: Path) -> pl.DataFrame:
    """Monta lookup estavel de `descricao_original` para `id_agrupado`."""
    caminho_id_agrupados = base_local_cnpj / "parquets" / "id_agrupados.parquet"
    if not caminho_id_agrupados.exists():
        return pl.DataFrame(schema={"descricao_normalizada": pl.String, "id_agrupado": pl.String})

    df_id_agrupados = pl.read_parquet(caminho_id_agrupados)
    if df_id_agrupados.is_empty():
        return pl.DataFrame(schema={"descricao_normalizada": pl.String, "id_agrupado": pl.String})

    df_lookup = (
        df_id_agrupados
        .with_columns(
            pl.col("descricao_original")
            .cast(pl.String, strict=False)
            .map_elements(normalizar_descricao, return_dtype=pl.String)
            .alias("descricao_normalizada")
        )
        .group_by("descricao_normalizada")
        .agg(
            [
                pl.col("id_agrupado").n_unique().alias("__qtd_grupos__"),
                pl.col("id_agrupado").first().alias("id_agrupado"),
            ]
        )
        .filter(pl.col("__qtd_grupos__") == 1)
        .drop("__qtd_grupos__")
    )

    return df_lookup


def _carregar_produtos_final_canonico(base_local_cnpj: Path) -> pl.DataFrame:
    """Carrega a gold de produtos final para enriquecer a projecao canonica da ST."""
    caminho_produtos_final = base_local_cnpj / "parquets" / "produtos_final.parquet"
    schema_vazio = {
        "id_agrupado": pl.String,
        "descr_padrao": pl.String,
        "unid_ref": pl.String,
        "fator_entrada_xml": pl.Float64,
        "fator_saida": pl.Float64,
    }
    if not caminho_produtos_final.exists():
        return pl.DataFrame(schema=schema_vazio)

    df_produtos_final = pl.read_parquet(caminho_produtos_final)
    if df_produtos_final.is_empty():
        return pl.DataFrame(schema=schema_vazio)

    return df_produtos_final.select(
        [
            pl.col("id_agrupado").cast(pl.String, strict=False).alias("id_agrupado"),
            pl.col("descricao_padrao").cast(pl.String, strict=False).alias("descr_padrao"),
            pl.col("unid_ref").cast(pl.String, strict=False).alias("unid_ref"),
            pl.col("fator_compra_ref").cast(pl.Float64, strict=False).alias("fator_entrada_xml"),
            pl.col("fator_venda_ref").cast(pl.Float64, strict=False).alias("fator_saida"),
        ]
    )


def _projetar_c176_xml_local_canonico(dataframe: pl.DataFrame, base_local_cnpj: Path) -> pl.DataFrame:
    """Enriquece a `silver/c176_xml` com os campos canonicos de homologacao externa."""
    if dataframe.is_empty() and not dataframe.columns:
        return pl.DataFrame(schema={})

    df_lookup_id_agrupado = _carregar_lookup_id_agrupado_por_descricao(base_local_cnpj)
    df_produtos_final = _carregar_produtos_final_canonico(base_local_cnpj)

    df_c176_base = dataframe.with_columns(
        [
            # A descricao documental ancora o lookup para o grupo mestre apenas quando a descricao e univoca.
            pl.coalesce([pl.col("descricao_item"), pl.col("descricao")])
            .cast(pl.String, strict=False)
            .map_elements(normalizar_descricao, return_dtype=pl.String)
            .alias("__descricao_normalizada__"),
            pl.col("cnpj_referencia").cast(pl.String, strict=False).alias("cnpj"),
            pl.coalesce([pl.col("descricao_item"), pl.col("descricao")]).cast(pl.String, strict=False).alias("descricao_item"),
            pl.coalesce([pl.col("cod_item_ref_saida"), pl.col("codigo_produto")]).cast(pl.String, strict=False).alias("cod_item_ref_saida"),
            pl.coalesce([pl.col("chave_saida"), pl.col("chave_documento")]).cast(pl.String, strict=False).alias("chave_saida"),
            pl.col("num_nf_saida").cast(pl.String, strict=False).alias("num_nf_saida"),
            pl.coalesce([pl.col("dt_doc_saida"), pl.col("data_documento")]).cast(pl.String, strict=False).alias("dt_doc_saida"),
            pl.col("dt_e_s_saida").cast(pl.String, strict=False).alias("dt_e_s_saida"),
            pl.coalesce([pl.col("num_item_saida"), pl.col("item_documento")]).cast(pl.String, strict=False).alias("num_item_saida"),
            pl.coalesce([pl.col("cfop_saida"), pl.col("cfop")]).cast(pl.String, strict=False).alias("cfop_saida"),
            pl.col("periodo_efd").cast(pl.String, strict=False).alias("periodo_efd"),
            pl.col("data_entrega_efd_periodo").cast(pl.String, strict=False).alias("data_entrega_efd_periodo"),
            pl.col("cod_fin_efd").cast(pl.String, strict=False).alias("cod_fin_efd"),
            pl.col("finalidade_efd").cast(pl.String, strict=False).alias("finalidade_efd"),
            pl.col("unid_saida").cast(pl.String, strict=False).alias("unid_saida"),
            pl.col("qtd_item_saida").cast(pl.Float64, strict=False).alias("qtd_item_saida"),
            pl.col("cod_mot_res").cast(pl.String, strict=False).alias("cod_mot_res"),
            pl.col("descricao_motivo_ressarcimento").cast(pl.String, strict=False).alias("descricao_motivo_ressarcimento"),
            pl.col("chave_nfe_ultima_entrada").cast(pl.String, strict=False).alias("chave_nfe_ultima_entrada"),
            pl.col("c176_num_item_ult_e_declarado").cast(pl.String, strict=False).alias("c176_num_item_ult_e_declarado"),
            pl.col("dt_ultima_entrada").cast(pl.String, strict=False).alias("dt_ultima_entrada"),
            pl.col("prod_nitem").cast(pl.Int64, strict=False).alias("prod_nitem"),
            pl.col("unid_entrada_xml").cast(pl.String, strict=False).alias("unid_entrada_xml"),
            pl.col("qtd_entrada_xml").cast(pl.Float64, strict=False).alias("qtd_entrada_xml"),
            pl.col("vl_total_entrada_xml").cast(pl.Float64, strict=False).alias("vl_total_entrada_xml"),
            pl.col("vl_unitario_entrada_xml").cast(pl.Float64, strict=False).alias("vl_unitario_entrada_xml"),
            pl.col("vl_unit_bc_st_entrada").cast(pl.Float64, strict=False).alias("vl_unit_bc_st_entrada"),
            pl.col("vl_unit_icms_proprio_entrada").cast(pl.Float64, strict=False).alias("vl_unit_icms_proprio_entrada"),
            pl.col("vl_unit_ressarcimento_st").cast(pl.Float64, strict=False).alias("vl_unit_ressarcimento_st"),
            pl.col("vl_ressarc_credito_proprio").cast(pl.Float64, strict=False).alias("vl_ressarc_credito_proprio"),
            pl.col("vl_ressarc_st_retido").cast(pl.Float64, strict=False).alias("vl_ressarc_st_retido"),
            pl.col("vr_total_ressarcimento").cast(pl.Float64, strict=False).alias("vr_total_ressarcimento"),
        ]
    )

    df_c176_enriquecido = (
        df_c176_base
        .join(
            df_lookup_id_agrupado,
            left_on="__descricao_normalizada__",
            right_on="descricao_normalizada",
            how="left",
        )
        .join(
            df_produtos_final,
            on="id_agrupado",
            how="left",
        )
        .with_columns(
            [
                # O vinculo da saida ao grupo so e valido quando a descricao conseguiu resolver um grupo mestre.
                pl.col("id_agrupado").is_not_null().alias("match_saida_id_agrupado"),
                # A entrada so e considerada conciliada quando o match chave+item trouxe o item da NF-e de entrada.
                pl.col("prod_nitem").is_not_null().alias("match_entrada_xml"),
                # A saida e convertida para a unidade de referencia pelo fator de venda do grupo mestre.
                pl.when(pl.col("qtd_item_saida").is_not_null() & pl.col("fator_saida").is_not_null())
                .then(pl.col("qtd_item_saida") * pl.col("fator_saida"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("qtd_saida_unid_ref"),
                # A entrada usa o fator de compra do grupo mestre para a mesma unidade de referencia.
                pl.when(pl.col("qtd_entrada_xml").is_not_null() & pl.col("fator_entrada_xml").is_not_null())
                .then(pl.col("qtd_entrada_xml") * pl.col("fator_entrada_xml"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("qtd_entrada_xml_unid_ref"),
                # O valor unitario de entrada e dividido pelo fator de compra para manter comparabilidade na unidade de referencia.
                pl.when(
                    pl.col("vl_unitario_entrada_xml").is_not_null()
                    & pl.col("fator_entrada_xml").is_not_null()
                    & (pl.col("fator_entrada_xml") > 0)
                )
                .then(pl.col("vl_unitario_entrada_xml") / pl.col("fator_entrada_xml"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("vl_unitario_entrada_xml_unid_ref"),
                pl.when(
                    pl.col("vl_unit_bc_st_entrada").is_not_null()
                    & pl.col("fator_entrada_xml").is_not_null()
                    & (pl.col("fator_entrada_xml") > 0)
                )
                .then(pl.col("vl_unit_bc_st_entrada") / pl.col("fator_entrada_xml"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("vl_unit_bc_st_entrada_unid_ref"),
                pl.when(
                    pl.col("vl_unit_icms_proprio_entrada").is_not_null()
                    & pl.col("fator_entrada_xml").is_not_null()
                    & (pl.col("fator_entrada_xml") > 0)
                )
                .then(pl.col("vl_unit_icms_proprio_entrada") / pl.col("fator_entrada_xml"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("vl_unit_icms_proprio_entrada_unid_ref"),
                pl.when(
                    pl.col("vl_unit_ressarcimento_st").is_not_null()
                    & pl.col("fator_entrada_xml").is_not_null()
                    & (pl.col("fator_entrada_xml") > 0)
                )
                .then(pl.col("vl_unit_ressarcimento_st") / pl.col("fator_entrada_xml"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("vl_unit_ressarcimento_st_unid_ref"),
                # O score permanece binario nesta fase porque apenas o match exato por chave+item e aceito.
                pl.when(pl.col("prod_nitem").is_not_null()).then(pl.lit(100)).otherwise(pl.lit(0)).alias("score_vinculo_entrada"),
                pl.when(pl.col("prod_nitem").is_not_null()).then(pl.lit("chave+item")).otherwise(pl.lit(None, dtype=pl.String)).alias("regra_vinculo_entrada"),
            ]
        )
        .with_columns(
            [
                # A diferenca de quantidade so existe quando ambas as pontas ja foram convertidas para a unidade de referencia.
                pl.when(pl.col("qtd_saida_unid_ref").is_not_null() & pl.col("qtd_entrada_xml_unid_ref").is_not_null())
                .then((pl.col("qtd_saida_unid_ref") - pl.col("qtd_entrada_xml_unid_ref")).abs())
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("diff_qtd_vinculo")
            ]
        )
    )

    return _selecionar_colunas_canonicas(df_c176_enriquecido, SCHEMA_CANONICO_C176_XML)


def _projetar_c176_xml_externo_canonico(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Normaliza a tabela externa `c176_xml` para o contrato canonico de paridade."""
    return _selecionar_colunas_canonicas(dataframe, SCHEMA_CANONICO_C176_XML)


def _projetar_shape_canonico_local(
    nome_logico: str,
    dataframe: pl.DataFrame,
    base_local_cnpj: Path,
) -> pl.DataFrame:
    """Aplica a projecao canonica local conforme o artefato comparado."""
    if nome_logico == "extraidos/c176":
        return _projetar_c176_local_canonico(dataframe)
    if nome_logico == "extraidos/nfe_dados_st":
        return _projetar_nfe_dados_st_canonico(dataframe)
    if nome_logico == "extraidos/e111":
        return _projetar_e111_local_canonico(dataframe)
    if nome_logico == "silver/c176_xml":
        return _projetar_c176_xml_local_canonico(dataframe, base_local_cnpj)
    return dataframe


def _projetar_shape_canonico_externo(nome_logico: str, dataframe: pl.DataFrame) -> pl.DataFrame:
    """Aplica a projecao canonica externa para comparar contratos equivalentes."""
    if nome_logico == "extraidos/c176":
        return _projetar_c176_externo_canonico(dataframe)
    if nome_logico == "extraidos/nfe_dados_st":
        return _projetar_nfe_dados_st_canonico(dataframe)
    if nome_logico == "extraidos/e111":
        return _projetar_e111_externo_canonico(dataframe)
    if nome_logico == "silver/c176_xml":
        return _projetar_c176_xml_externo_canonico(dataframe)
    return dataframe


def _montar_justificativa_residual(
    nome_logico: str,
    comparacao_bruta: dict[str, Any],
    comparacao_canonica: dict[str, Any],
) -> str | None:
    """Classifica a divergencia residual remanescente apos a projecao canonica."""
    if comparacao_canonica["registros_iguais"] and comparacao_canonica["schema_igual"] and comparacao_canonica["colunas_iguais"]:
        if comparacao_bruta["registros_iguais"] and comparacao_bruta["schema_igual"] and comparacao_bruta["colunas_iguais"]:
            return None
        return "shape bruto local diferente do contrato minimo de paridade; a aceitacao principal deve considerar a visao canonica"

    motivos: list[str] = []
    if comparacao_canonica["diff_registros"] != 0:
        motivos.append(f"contagem divergente apos projecao canonica ({comparacao_canonica['diff_registros']:+d})")
    if comparacao_canonica["colunas_apenas_externo"]:
        motivos.append("campos externos ainda nao derivados localmente")
    if comparacao_canonica["colunas_apenas_local"]:
        motivos.append("campos canonicos locais adicionais fora do shape externo")
    if not comparacao_canonica["schema_igual"] and not comparacao_canonica["colunas_apenas_local"] and not comparacao_canonica["colunas_apenas_externo"]:
        motivos.append("tipos divergentes apos normalizacao canonica")
    if not motivos:
        motivos.append("divergencia residual nao classificada automaticamente")

    return f"{nome_logico}: " + "; ".join(motivos)


def _obter_comparacao_principal(comparacao: dict[str, Any]) -> dict[str, Any]:
    """Define a comparacao principal usada para o aceite da paridade."""
    return comparacao["paridade_shape_canonico"]


def _montar_status_comparacao_externa(comparacoes: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Resume o status de paridade externa para a trilha ST."""
    total = len(comparacoes)
    pendentes_local = 0
    pendentes_externo = 0
    divergencias = 0
    equivalentes = 0

    for comparacao in comparacoes.values():
        existe_local = bool(comparacao["shape_bruto_local"]["existe"])
        existe_externo = bool(comparacao["externo"]["existe"])
        comparacao_principal = _obter_comparacao_principal(comparacao)

        if not existe_local:
            pendentes_local += 1
            continue
        if not existe_externo:
            pendentes_externo += 1
            continue

        if (
            comparacao_principal["registros_iguais"]
            and comparacao_principal["schema_igual"]
            and comparacao_principal["colunas_iguais"]
        ):
            equivalentes += 1
        else:
            divergencias += 1

    if pendentes_local:
        status_geral = "pendente_extracao_local"
    elif pendentes_externo:
        status_geral = "pendente_base_externa"
    elif divergencias:
        status_geral = "divergente"
    elif equivalentes == total and total > 0:
        status_geral = "equivalente"
    else:
        status_geral = "parcial"

    return {
        "status_geral": status_geral,
        "total_artefatos": total,
        "artefatos_equivalentes": equivalentes,
        "artefatos_com_divergencia": divergencias,
        "artefatos_pendentes_local": pendentes_local,
        "artefatos_pendentes_externo": pendentes_externo,
    }


def _montar_resumo_por_camada(comparacoes: dict[str, dict[str, Any]]) -> dict[str, dict[str, int]]:
    """Agrupa equivalencias e divergencias por camada usando a visao canonica."""
    resumo: dict[str, dict[str, int]] = {}

    for comparacao in comparacoes.values():
        camada = comparacao["camada"]
        bucket = resumo.setdefault(
            camada,
            {
                "artefatos": 0,
                "equivalentes": 0,
                "divergentes": 0,
                "pendentes_local": 0,
                "pendentes_externo": 0,
            },
        )
        bucket["artefatos"] += 1

        existe_local = bool(comparacao["shape_bruto_local"]["existe"])
        existe_externo = bool(comparacao["externo"]["existe"])
        comparacao_principal = _obter_comparacao_principal(comparacao)

        if not existe_local:
            bucket["pendentes_local"] += 1
        elif not existe_externo:
            bucket["pendentes_externo"] += 1
        elif (
            comparacao_principal["registros_iguais"]
            and comparacao_principal["schema_igual"]
            and comparacao_principal["colunas_iguais"]
        ):
            bucket["equivalentes"] += 1
        else:
            bucket["divergentes"] += 1

    return resumo


def _montar_status_cadeia_local(artefatos_locais: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Resume a materializacao local completa da cadeia ST."""
    total = len(artefatos_locais)
    presentes = sum(1 for metadados in artefatos_locais.values() if metadados["existe"])
    vazios = sum(
        1 for metadados in artefatos_locais.values() if metadados["existe"] and int(metadados["registros"]) == 0
    )

    return {
        "total_artefatos": total,
        "artefatos_presentes": presentes,
        "artefatos_ausentes": total - presentes,
        "artefatos_vazios": vazios,
        "cadeia_local_completa": presentes == total,
    }


def comparar_paridade_st(
    cnpj: str,
    *,
    base_local: Path = BASE_LOCAL,
    base_externa: Path = BASE_EXTERNA,
) -> dict[str, Any]:
    """Compara a trilha ST local com os equivalentes externos disponiveis."""
    base_local_cnpj = base_local / cnpj
    base_externa_cnpj = base_externa / cnpj

    comparacoes_externas: dict[str, Any] = {}
    artefatos_locais: dict[str, Any] = {}

    for nome_logico, configuracao in ARTEFATOS_PARIDADE_ST.items():
        caminho_local = base_local_cnpj / str(configuracao["local"])
        dataframe_local, metadados_local = _coletar_metadados_parquet(caminho_local)
        artefatos_locais[nome_logico] = metadados_local

        caminho_externo_relativo = configuracao["externo"]
        if caminho_externo_relativo is None:
            continue

        caminho_externo = base_externa_cnpj / caminho_externo_relativo.format(cnpj=cnpj)
        dataframe_externo, metadados_externo = _coletar_metadados_parquet(caminho_externo)

        dataframe_local_canonico = _projetar_shape_canonico_local(nome_logico, dataframe_local, base_local_cnpj)
        dataframe_externo_canonico = _projetar_shape_canonico_externo(nome_logico, dataframe_externo)

        metadados_local_canonico = _coletar_metadados_dataframe(
            dataframe_local_canonico,
            caminho_local,
            existe=metadados_local["existe"],
        )
        metadados_externo_canonico = _coletar_metadados_dataframe(
            dataframe_externo_canonico,
            caminho_externo,
            existe=metadados_externo["existe"],
        )

        comparacao_bruta = _comparar_metadados(metadados_local, metadados_externo)
        comparacao_canonica = _comparar_metadados(metadados_local_canonico, metadados_externo_canonico)

        comparacoes_externas[nome_logico] = {
            "camada": _classificar_camada_artefato(nome_logico),
            "shape_bruto_local": metadados_local,
            "shape_canonico_local": metadados_local_canonico,
            "externo": metadados_externo,
            "shape_canonico_externo": metadados_externo_canonico,
            "paridade_shape_bruto": comparacao_bruta,
            "paridade_shape_canonico": comparacao_canonica,
            "divergencia_residual_justificada": _montar_justificativa_residual(
                nome_logico,
                comparacao_bruta,
                comparacao_canonica,
            ),
        }

    return {
        "cnpj": cnpj,
        "base_local": str(base_local_cnpj),
        "base_externa": str(base_externa_cnpj),
        "resumo_paridade_externa": _montar_status_comparacao_externa(comparacoes_externas),
        "resumo_por_camada": _montar_resumo_por_camada(comparacoes_externas),
        "resumo_cadeia_local": _montar_status_cadeia_local(artefatos_locais),
        "comparacoes_externas": comparacoes_externas,
        "cadeia_local": artefatos_locais,
    }


def renderizar_relatorio_markdown(relatorio: dict[str, Any]) -> str:
    """Renderiza um resumo Markdown da paridade ST."""
    resumo_externo = relatorio["resumo_paridade_externa"]
    resumo_local = relatorio["resumo_cadeia_local"]

    linhas = [
        f"# Relatorio de Paridade ST - {relatorio['cnpj']}",
        "",
        "## Resumo",
        "",
        f"- status da paridade externa: `{resumo_externo['status_geral']}`",
        f"- artefatos equivalentes: `{resumo_externo['artefatos_equivalentes']}/{resumo_externo['total_artefatos']}`",
        f"- divergencias: `{resumo_externo['artefatos_com_divergencia']}`",
        f"- pendencias locais: `{resumo_externo['artefatos_pendentes_local']}`",
        f"- pendencias externas: `{resumo_externo['artefatos_pendentes_externo']}`",
        f"- cadeia local completa: `{resumo_local['cadeia_local_completa']}`",
        f"- artefatos locais presentes: `{resumo_local['artefatos_presentes']}/{resumo_local['total_artefatos']}`",
        f"- artefatos locais vazios: `{resumo_local['artefatos_vazios']}`",
        "",
        "## Comparacoes Externas",
        "",
        "| Artefato | Camada | Registros local | Registros externo | Bruto schema | Bruto colunas | Canonico schema | Canonico colunas | Diff canonico | Divergencia residual |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]

    for nome_logico, comparacao in relatorio["comparacoes_externas"].items():
        comparacao_bruta = comparacao["paridade_shape_bruto"]
        comparacao_canonica = comparacao["paridade_shape_canonico"]
        linhas.append(
            "| {artefato} | {camada} | {reg_local} | {reg_externo} | {schema_bruto} | {colunas_bruto} | {schema_canonico} | {colunas_canonico} | {diff_canonico} | {justificativa} |".format(
                artefato=nome_logico,
                camada=comparacao["camada"],
                reg_local=comparacao["shape_bruto_local"]["registros"],
                reg_externo=comparacao["externo"]["registros"],
                schema_bruto="sim" if comparacao_bruta["schema_igual"] else "nao",
                colunas_bruto="sim" if comparacao_bruta["colunas_iguais"] else "nao",
                schema_canonico="sim" if comparacao_canonica["schema_igual"] else "nao",
                colunas_canonico="sim" if comparacao_canonica["colunas_iguais"] else "nao",
                diff_canonico=comparacao_canonica["diff_registros"],
                justificativa=comparacao["divergencia_residual_justificada"] or "-",
            )
        )

    linhas.extend(
        [
            "",
            "## Resumo por Camada",
            "",
            "| Camada | Artefatos | Equivalentes | Divergentes | Pendentes locais | Pendentes externos |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )

    for camada, resumo_camada in relatorio["resumo_por_camada"].items():
        linhas.append(
            "| {camada} | {artefatos} | {equivalentes} | {divergentes} | {pendentes_local} | {pendentes_externo} |".format(
                camada=camada,
                artefatos=resumo_camada["artefatos"],
                equivalentes=resumo_camada["equivalentes"],
                divergentes=resumo_camada["divergentes"],
                pendentes_local=resumo_camada["pendentes_local"],
                pendentes_externo=resumo_camada["pendentes_externo"],
            )
        )

    linhas.extend(
        [
            "",
            "## Cadeia Local",
            "",
            "| Artefato | Existe | Registros | Colunas |",
            "| --- | --- | --- | --- |",
        ]
    )

    for nome_logico, metadados in relatorio["cadeia_local"].items():
        linhas.append(
            "| {artefato} | {existe} | {registros} | {colunas} |".format(
                artefato=nome_logico,
                existe="sim" if metadados["existe"] else "nao",
                registros=metadados["registros"],
                colunas=len(metadados["colunas"]),
            )
        )

    return "\n".join(linhas) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Compara paridade ST entre audit_react e projeto externo")
    parser.add_argument("cnpj", help="CNPJ alvo")
    parser.add_argument("--saida", default="", help="Arquivo JSON opcional para salvar o relatorio")
    parser.add_argument("--saida-markdown", default="", help="Arquivo Markdown opcional para salvar o relatorio")
    args = parser.parse_args()

    relatorio = comparar_paridade_st(args.cnpj)
    payload_json = json.dumps(relatorio, indent=2, ensure_ascii=False)
    payload_markdown = renderizar_relatorio_markdown(relatorio)

    if args.saida:
        caminho_saida = Path(args.saida)
        caminho_saida.parent.mkdir(parents=True, exist_ok=True)
        caminho_saida.write_text(payload_json, encoding="utf-8")

    if args.saida_markdown:
        caminho_markdown = Path(args.saida_markdown)
        caminho_markdown.parent.mkdir(parents=True, exist_ok=True)
        caminho_markdown.write_text(payload_markdown, encoding="utf-8")

    print(payload_json)


if __name__ == "__main__":
    main()
