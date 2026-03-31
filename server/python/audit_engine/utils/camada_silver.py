"""Materializacao da camada silver por CNPJ."""

from __future__ import annotations

import logging
import unicodedata
from pathlib import Path
from typing import Callable

import polars as pl

from .camadas_cnpj import garantir_estrutura_camadas_cnpj

logger = logging.getLogger(__name__)


SCHEMAS_SILVER: dict[str, dict[str, pl.DataType]] = {
    "tb_documentos": {
        "fonte": pl.String,
        "tipo_documento": pl.String,
        "chave_documento": pl.String,
        "numero_documento": pl.String,
        "data_documento": pl.String,
        "tipo_movimento": pl.String,
        "cnpj_referencia": pl.String,
        "cnpj_emitente": pl.String,
        "cnpj_destinatario": pl.String,
        "valor_total": pl.Float64,
        "quantidade_itens": pl.Int64,
    },
    "item_unidades": {
        "codigo_fonte": pl.String,
        "codigo_produto": pl.String,
        "descricao": pl.String,
        "descricao_normalizada": pl.String,
        "ncm": pl.String,
        "cest": pl.String,
        "gtin": pl.String,
        "unidade": pl.String,
        "compras": pl.Float64,
        "vendas": pl.Float64,
        "qtd_compras": pl.Float64,
        "qtd_vendas": pl.Float64,
        "fontes": pl.String,
    },
    "itens": {
        "codigo_fonte": pl.String,
        "codigo_produto": pl.String,
        "descricao": pl.String,
        "descricao_normalizada": pl.String,
        "ncm": pl.String,
        "cest": pl.String,
        "gtin": pl.String,
        "lista_unidades": pl.String,
        "valor_total_compras": pl.Float64,
        "valor_total_vendas": pl.Float64,
        "qtd_total_compras": pl.Float64,
        "qtd_total_vendas": pl.Float64,
        "fontes": pl.String,
    },
    "descricao_produtos": {
        "codigo_fonte": pl.String,
        "codigo_produto": pl.String,
        "descricao": pl.String,
        "descricao_normalizada": pl.String,
        "ncm": pl.String,
        "cest": pl.String,
        "gtin": pl.String,
    },
    "fontes_produtos": {
        "id_linha_origem": pl.String,
        "fonte": pl.String,
        "tipo_movimento": pl.String,
        "chave_documento": pl.String,
        "item_documento": pl.String,
        "codigo_fonte": pl.String,
        "codigo_produto": pl.String,
        "descricao": pl.String,
        "descricao_normalizada": pl.String,
        "ncm": pl.String,
        "cest": pl.String,
        "gtin": pl.String,
        "unidade": pl.String,
        "quantidade": pl.Float64,
        "valor_unitario": pl.Float64,
        "valor_total": pl.Float64,
        "cfop": pl.String,
        "data_documento": pl.String,
        "cnpj_referencia": pl.String,
        "cnpj_emitente": pl.String,
        "cnpj_destinatario": pl.String,
    },
    "c170_xml": {
        "id_linha_origem": pl.String,
        "fonte": pl.String,
        "chave_documento": pl.String,
        "codigo_fonte": pl.String,
        "codigo_produto": pl.String,
        "descricao": pl.String,
        "ncm": pl.String,
        "cest": pl.String,
        "unidade": pl.String,
        "quantidade": pl.Float64,
        "valor_unitario": pl.Float64,
        "valor_total": pl.Float64,
        "cfop": pl.String,
        "data_documento": pl.String,
        "cnpj_referencia": pl.String,
    },
    "c176_xml": {
        "id_linha_origem": pl.String,
        "fonte": pl.String,
        "periodo_efd": pl.String,
        "data_entrega_efd_periodo": pl.String,
        "cod_fin_efd": pl.String,
        "finalidade_efd": pl.String,
        "chave_documento": pl.String,
        "chave_saida": pl.String,
        "num_nf_saida": pl.String,
        "item_documento": pl.String,
        "num_item_saida": pl.String,
        "codigo_fonte": pl.String,
        "codigo_produto": pl.String,
        "cod_item_ref_saida": pl.String,
        "descricao": pl.String,
        "descricao_item": pl.String,
        "cfop": pl.String,
        "cfop_saida": pl.String,
        "unid_saida": pl.String,
        "data_documento": pl.String,
        "dt_doc_saida": pl.String,
        "dt_e_s_saida": pl.String,
        "cnpj_referencia": pl.String,
        "quantidade": pl.Float64,
        "qtd_item_saida": pl.Float64,
        "valor_total": pl.Float64,
        "cod_mot_res": pl.String,
        "descricao_motivo_ressarcimento": pl.String,
        "chave_nfe_ultima_entrada": pl.String,
        "num_item_ultima_entrada": pl.String,
        "c176_num_item_ult_e_declarado": pl.String,
        "dt_ultima_entrada": pl.String,
        "prod_nitem": pl.Int64,
        "unid_entrada_xml": pl.String,
        "qtd_entrada_xml": pl.Float64,
        "vl_total_entrada_xml": pl.Float64,
        "vl_unitario_entrada_xml": pl.Float64,
        "vl_unit_bc_st_entrada": pl.Float64,
        "vl_unit_icms_proprio_entrada": pl.Float64,
        "vl_unit_ressarcimento_st": pl.Float64,
        "vl_ressarc_credito_proprio": pl.Float64,
        "vl_ressarc_st_retido": pl.Float64,
        "vr_total_ressarcimento": pl.Float64,
    },
    "nfe_dados_st": {
        "id_linha_origem": pl.String,
        "chave_documento": pl.String,
        "item_documento": pl.String,
        "codigo_fonte": pl.String,
        "codigo_produto": pl.String,
        "descricao": pl.String,
        "ncm": pl.String,
        "cest": pl.String,
        "cfop": pl.String,
        "cst": pl.String,
        "csosn": pl.String,
        "data_documento": pl.String,
        "cnpj_referencia": pl.String,
        "cnpj_emitente": pl.String,
        "cnpj_destinatario": pl.String,
        "quantidade": pl.Float64,
        "valor_total": pl.Float64,
        "bc_st": pl.Float64,
        "vl_st": pl.Float64,
        "vl_icms_substituto": pl.Float64,
        "vl_st_retido": pl.Float64,
        "bc_fcp_st": pl.Float64,
        "p_fcp_st": pl.Float64,
        "vl_fcp_st": pl.Float64,
    },
    "e111_ajustes": {
        "periodo_efd": pl.String,
        "cnpj_referencia": pl.String,
        "codigo_ajuste": pl.String,
        "descricao_codigo_ajuste": pl.String,
        "descricao_complementar": pl.String,
        "valor_ajuste": pl.Float64,
        "data_entrega_efd_periodo": pl.String,
        "cod_fin_efd": pl.String,
    },
}


def _criar_dataframe_vazio(nome_tabela: str) -> pl.DataFrame:
    """Cria dataframe vazio com o schema oficial da camada silver."""
    return pl.DataFrame(schema=SCHEMAS_SILVER[nome_tabela])


def _normalizar_texto(texto: str | None) -> str:
    """Normaliza descricoes para comparacao estavel no pipeline."""
    if texto is None:
        return ""

    texto_sem_acento = unicodedata.normalize("NFKD", str(texto))
    texto_ascii = "".join(caractere for caractere in texto_sem_acento if not unicodedata.combining(caractere))
    return " ".join(texto_ascii.upper().strip().split())


def _ler_parquet_se_existir(caminho: Path) -> pl.DataFrame:
    """Le parquet existente ou retorna dataframe vazio."""
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def _formatar_data_expr(coluna: str) -> pl.Expr:
    """Converte datas para string ISO para manter compatibilidade de leitura na API."""
    return (
        pl.when(pl.col(coluna).is_not_null())
        .then(pl.col(coluna).cast(pl.String, strict=False))
        .otherwise(pl.lit(None, dtype=pl.String))
        .alias(coluna)
    )


def _converter_periodo_efd_para_data_expr(coluna: str) -> pl.Expr:
    """Converte `YYYY/MM` em data ISO do primeiro dia do mes."""
    return (
        pl.when(pl.col(coluna).is_not_null())
        .then(
            pl.col(coluna)
            .cast(pl.String, strict=False)
            .str.replace("/", "-")
            + pl.lit("-01")
        )
        .otherwise(pl.lit(None, dtype=pl.String))
        .alias(coluna)
    )


def _padronizar_fonte_documental(
    dataframe: pl.DataFrame,
    *,
    fonte: str,
    tipo_movimento_expr: pl.Expr,
    chave_documento_expr: pl.Expr,
    item_documento_expr: pl.Expr,
    codigo_fonte_expr: pl.Expr,
    codigo_produto_expr: pl.Expr,
    descricao_expr: pl.Expr,
    ncm_expr: pl.Expr,
    cest_expr: pl.Expr,
    gtin_expr: pl.Expr,
    unidade_expr: pl.Expr,
    quantidade_expr: pl.Expr,
    valor_unitario_expr: pl.Expr,
    valor_total_expr: pl.Expr,
    cfop_expr: pl.Expr,
    data_documento_expr: pl.Expr,
    cnpj_referencia_expr: pl.Expr,
    cnpj_emitente_expr: pl.Expr,
    cnpj_destinatario_expr: pl.Expr,
) -> pl.DataFrame:
    """Padroniza uma fonte fiscal em schema unico de movimentos."""
    if dataframe.is_empty():
        return _criar_dataframe_vazio("fontes_produtos")

    df_padronizado = (
        dataframe
        .with_columns(
            [
                pl.lit(fonte).alias("fonte"),
                tipo_movimento_expr.alias("tipo_movimento"),
                chave_documento_expr.alias("chave_documento"),
                item_documento_expr.alias("item_documento"),
                codigo_fonte_expr.alias("codigo_fonte"),
                codigo_produto_expr.alias("codigo_produto"),
                descricao_expr.alias("descricao"),
                ncm_expr.alias("ncm"),
                cest_expr.alias("cest"),
                gtin_expr.alias("gtin"),
                unidade_expr.alias("unidade"),
                quantidade_expr.cast(pl.Float64, strict=False).fill_null(0.0).alias("quantidade"),
                valor_unitario_expr.cast(pl.Float64, strict=False).fill_null(0.0).alias("valor_unitario"),
                valor_total_expr.cast(pl.Float64, strict=False).fill_null(0.0).alias("valor_total"),
                cfop_expr.alias("cfop"),
                data_documento_expr.alias("data_documento"),
                cnpj_referencia_expr.alias("cnpj_referencia"),
                cnpj_emitente_expr.alias("cnpj_emitente"),
                cnpj_destinatario_expr.alias("cnpj_destinatario"),
            ]
        )
        .with_columns(
            [
                pl.col("descricao")
                .cast(pl.String, strict=False)
                .map_elements(_normalizar_texto, return_dtype=pl.String)
                .alias("descricao_normalizada"),
                (
                    pl.coalesce([pl.col("chave_documento"), pl.lit("")]).cast(pl.String, strict=False)
                    + pl.lit("|")
                    + pl.coalesce([pl.col("item_documento"), pl.lit("")]).cast(pl.String, strict=False)
                ).alias("id_linha_origem"),
            ]
        )
        .select(list(SCHEMAS_SILVER["fontes_produtos"].keys()))
    )

    return df_padronizado


def _consolidar_tabela_documentos(df_fontes: pl.DataFrame) -> pl.DataFrame:
    """Consolida metadados documentais distintos para consulta e auditoria."""
    if df_fontes.is_empty():
        return _criar_dataframe_vazio("tb_documentos")

    df_documentos = (
        df_fontes
        .group_by(["fonte", "chave_documento", "data_documento", "tipo_movimento", "cnpj_referencia", "cnpj_emitente", "cnpj_destinatario"])
        .agg(
            [
                pl.col("valor_total").sum().alias("valor_total"),
                pl.col("item_documento").n_unique().cast(pl.Int64).alias("quantidade_itens"),
                pl.col("chave_documento").first().alias("numero_documento"),
            ]
        )
        .with_columns(
            pl.when(pl.col("fonte").is_in(["nfe", "nfce"]))
            .then(pl.col("fonte").str.to_uppercase())
            .when(pl.col("fonte") == "c170")
            .then(pl.lit("EFD"))
            .otherwise(pl.lit("INVENTARIO"))
            .alias("tipo_documento")
        )
        .select(list(SCHEMAS_SILVER["tb_documentos"].keys()))
    )

    return df_documentos


def _consolidar_item_unidades(df_fontes: pl.DataFrame) -> pl.DataFrame:
    """Consolida movimentos por codigo_fonte e unidade para a camada silver."""
    if df_fontes.is_empty():
        return _criar_dataframe_vazio("item_unidades")

    df_item_unidades = (
        df_fontes
        .group_by(["codigo_fonte", "codigo_produto", "descricao", "descricao_normalizada", "ncm", "cest", "gtin", "unidade"])
        .agg(
            [
                pl.col("valor_total").filter(pl.col("tipo_movimento") == "entrada").sum().alias("compras"),
                pl.col("valor_total").filter(pl.col("tipo_movimento") == "saida").sum().alias("vendas"),
                pl.col("quantidade").filter(pl.col("tipo_movimento") == "entrada").sum().alias("qtd_compras"),
                pl.col("quantidade").filter(pl.col("tipo_movimento") == "saida").sum().alias("qtd_vendas"),
                pl.col("fonte").drop_nulls().unique().sort().str.concat("|").alias("fontes"),
            ]
        )
        .with_columns(
            [
                pl.col("compras").fill_null(0.0),
                pl.col("vendas").fill_null(0.0),
                pl.col("qtd_compras").fill_null(0.0),
                pl.col("qtd_vendas").fill_null(0.0),
            ]
        )
        .select(list(SCHEMAS_SILVER["item_unidades"].keys()))
    )

    return df_item_unidades


def _consolidar_itens(df_item_unidades: pl.DataFrame) -> pl.DataFrame:
    """Consolida informacoes por produto-origem."""
    if df_item_unidades.is_empty():
        return _criar_dataframe_vazio("itens")

    df_itens = (
        df_item_unidades
        .group_by(["codigo_fonte", "codigo_produto", "descricao", "descricao_normalizada", "ncm", "cest", "gtin"])
        .agg(
            [
                pl.col("unidade").drop_nulls().unique().sort().str.concat("|").alias("lista_unidades"),
                pl.col("compras").sum().alias("valor_total_compras"),
                pl.col("vendas").sum().alias("valor_total_vendas"),
                pl.col("qtd_compras").sum().alias("qtd_total_compras"),
                pl.col("qtd_vendas").sum().alias("qtd_total_vendas"),
                pl.col("fontes").drop_nulls().unique().sort().str.concat("|").alias("fontes"),
            ]
        )
        .select(list(SCHEMAS_SILVER["itens"].keys()))
    )

    return df_itens


def _consolidar_descricao_produtos(df_itens: pl.DataFrame) -> pl.DataFrame:
    """Extrai a dimensao de descricao padronizada de produtos."""
    if df_itens.is_empty():
        return _criar_dataframe_vazio("descricao_produtos")

    return df_itens.select(list(SCHEMAS_SILVER["descricao_produtos"].keys()))


def _montar_lookup_entrada_xml_por_chave_item(diretorio_extraidos: Path) -> pl.DataFrame:
    """Monta lookup da NF-e de entrada por chave e item para enriquecer o C176."""
    df_nfe = _ler_parquet_se_existir(diretorio_extraidos / "nfe.parquet")
    if df_nfe.is_empty():
        return pl.DataFrame(
            schema={
                "__chave_entrada_xml__": pl.String,
                "__item_entrada_xml__": pl.String,
                "prod_nitem": pl.Int64,
                "unid_entrada_xml": pl.String,
                "qtd_entrada_xml": pl.Float64,
                "vl_total_entrada_xml": pl.Float64,
                "vl_unitario_entrada_xml": pl.Float64,
            }
        )

    # O lookup usa apenas NF-e de entrada porque o C176 referencia a ultima entrada declarada do item.
    df_entradas = df_nfe.filter(
        pl.col("tipo_operacao").cast(pl.String, strict=False).str.contains("ENTRADA", literal=False)
    )
    if df_entradas.is_empty():
        return pl.DataFrame(
            schema={
                "__chave_entrada_xml__": pl.String,
                "__item_entrada_xml__": pl.String,
                "prod_nitem": pl.Int64,
                "unid_entrada_xml": pl.String,
                "qtd_entrada_xml": pl.Float64,
                "vl_total_entrada_xml": pl.Float64,
                "vl_unitario_entrada_xml": pl.Float64,
            }
        )

    df_lookup_entrada = (
        df_entradas
        .with_columns(
            [
                # Normaliza a chave de acesso para reuso no join com a chave de ultima entrada declarada no C176.
                pl.col("chave_acesso").cast(pl.String, strict=False).alias("__chave_entrada_xml__"),
                # Normaliza o numero do item como texto para compatibilizar com o item declarado no C176.
                pl.col("prod_nitem").cast(pl.String, strict=False).alias("__item_entrada_xml__"),
                # Preserva o numero original do item para a projecao canonica posterior.
                pl.col("prod_nitem").cast(pl.Int64, strict=False).alias("prod_nitem"),
                # Usa a unidade comercial do XML, que e a informacao documental mais direta do item de entrada.
                pl.col("prod_ucom").cast(pl.String, strict=False).alias("unid_entrada_xml"),
                # Mantem a quantidade comercial da entrada como base direta de conciliacao documental.
                pl.col("prod_qcom").cast(pl.Float64, strict=False).fill_null(0.0).alias("qtd_entrada_xml"),
                # Preserva o valor total do item na NF-e de entrada para reconciliacao da trilha ST.
                pl.col("prod_vprod").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_total_entrada_xml"),
                # Preserva o valor unitario comercial do item de entrada antes de qualquer fator de conversao.
                pl.col("prod_vuncom").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_unitario_entrada_xml"),
            ]
        )
        .select(
            [
                "__chave_entrada_xml__",
                "__item_entrada_xml__",
                "prod_nitem",
                "unid_entrada_xml",
                "qtd_entrada_xml",
                "vl_total_entrada_xml",
                "vl_unitario_entrada_xml",
            ]
        )
        .unique(subset=["__chave_entrada_xml__", "__item_entrada_xml__"], keep="first")
    )

    return df_lookup_entrada


def _montar_c176_xml(diretorio_extraidos: Path, cnpj: str) -> pl.DataFrame:
    """Padroniza o C176 extraido em um artefato silver rastreavel por documento e item."""
    df_c176 = _ler_parquet_se_existir(diretorio_extraidos / "c176.parquet")
    if df_c176.is_empty():
        return _criar_dataframe_vazio("c176_xml")

    df_lookup_entrada_xml = _montar_lookup_entrada_xml_por_chave_item(diretorio_extraidos)

    # O C176 representa a trilha fiscal de ressarcimento/ST declarada na EFD por item de saida.
    df_c176_xml = (
        df_c176
        .with_columns(
            [
                pl.lit("c176").alias("fonte"),
                pl.col("periodo_efd").cast(pl.String, strict=False).alias("periodo_efd"),
                _formatar_data_expr("data_entrega_efd_periodo"),
                pl.col("cod_fin_efd").cast(pl.String, strict=False).alias("cod_fin_efd"),
                pl.col("finalidade_efd").cast(pl.String, strict=False).alias("finalidade_efd"),
                pl.col("chave_saida").cast(pl.String, strict=False).alias("chave_documento"),
                pl.col("chave_saida").cast(pl.String, strict=False).alias("chave_saida"),
                pl.col("num_nf_saida").cast(pl.String, strict=False).alias("num_nf_saida"),
                pl.col("num_item_saida").cast(pl.String, strict=False).alias("item_documento"),
                pl.col("num_item_saida").cast(pl.String, strict=False).alias("num_item_saida"),
                (pl.lit(cnpj) + pl.lit("|") + pl.col("cod_item").cast(pl.String, strict=False)).alias("codigo_fonte"),
                pl.col("cod_item").cast(pl.String, strict=False).alias("codigo_produto"),
                pl.col("cod_item").cast(pl.String, strict=False).alias("cod_item_ref_saida"),
                pl.col("descricao_item").cast(pl.String, strict=False).alias("descricao"),
                pl.col("descricao_item").cast(pl.String, strict=False).alias("descricao_item"),
                pl.col("cfop_saida").cast(pl.String, strict=False).alias("cfop"),
                pl.col("cfop_saida").cast(pl.String, strict=False).alias("cfop_saida"),
                pl.col("unid_saida").cast(pl.String, strict=False).alias("unid_saida"),
                pl.col("dt_doc_saida").cast(pl.String, strict=False).alias("data_documento"),
                pl.col("dt_doc_saida").cast(pl.String, strict=False).alias("dt_doc_saida"),
                pl.col("dt_e_s_saida").cast(pl.String, strict=False).alias("dt_e_s_saida"),
                pl.col("cnpj_referencia").cast(pl.String, strict=False).alias("cnpj_referencia"),
                pl.col("qtd_item_saida").cast(pl.Float64, strict=False).fill_null(0.0).alias("quantidade"),
                pl.col("qtd_item_saida").cast(pl.Float64, strict=False).fill_null(0.0).alias("qtd_item_saida"),
                pl.col("vl_total_item").cast(pl.Float64, strict=False).fill_null(0.0).alias("valor_total"),
                pl.col("cod_mot_res").cast(pl.String, strict=False).alias("cod_mot_res"),
                pl.col("descricao_motivo_ressarcimento").cast(pl.String, strict=False).alias("descricao_motivo_ressarcimento"),
                pl.col("chave_nfe_ultima_entrada").cast(pl.String, strict=False).alias("chave_nfe_ultima_entrada"),
                pl.col("num_item_ultima_entrada").cast(pl.String, strict=False).alias("num_item_ultima_entrada"),
                pl.col("num_item_ultima_entrada").cast(pl.String, strict=False).alias("c176_num_item_ult_e_declarado"),
                _formatar_data_expr("dt_ultima_entrada"),
                pl.col("vl_unit_bc_st_entrada").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_unit_bc_st_entrada"),
                pl.col("vl_unit_icms_proprio_entrada").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_unit_icms_proprio_entrada"),
                pl.col("vl_unit_ressarcimento_st").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_unit_ressarcimento_st"),
                pl.col("vl_ressarc_credito_proprio").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_ressarc_credito_proprio"),
                pl.col("vl_ressarc_st_retido").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_ressarc_st_retido"),
                pl.col("vr_total_ressarcimento").cast(pl.Float64, strict=False).fill_null(0.0).alias("vr_total_ressarcimento"),
            ]
        )
        .join(
            # Cruza a ultima entrada declarada no C176 com a NF-e de entrada materializada localmente.
            df_lookup_entrada_xml,
            left_on=["chave_nfe_ultima_entrada", "num_item_ultima_entrada"],
            right_on=["__chave_entrada_xml__", "__item_entrada_xml__"],
            how="left",
        )
        .drop(["__chave_entrada_xml__", "__item_entrada_xml__"], strict=False)
        .with_columns(
            (
                pl.coalesce([pl.col("chave_documento"), pl.lit("")]).cast(pl.String, strict=False)
                + pl.lit("|")
                + pl.coalesce([pl.col("item_documento"), pl.lit("")]).cast(pl.String, strict=False)
            ).alias("id_linha_origem")
        )
        .select(list(SCHEMAS_SILVER["c176_xml"].keys()))
    )

    return df_c176_xml


def _montar_nfe_dados_st(diretorio_extraidos: Path) -> pl.DataFrame:
    """Padroniza a base XML de ST/FCP em um artefato silver por item de NF-e."""
    df_st = _ler_parquet_se_existir(diretorio_extraidos / "nfe_dados_st.parquet")
    if df_st.is_empty():
        return _criar_dataframe_vazio("nfe_dados_st")

    # A base de ST do XML preserva os campos tributarios do item de NF-e para conciliacao posterior.
    df_nfe_dados_st = (
        df_st
        .with_columns(
            [
                pl.col("chave_acesso").cast(pl.String, strict=False).alias("chave_documento"),
                pl.col("prod_nitem").cast(pl.String, strict=False).alias("item_documento"),
                pl.col("codigo_fonte").cast(pl.String, strict=False).alias("codigo_fonte"),
                pl.col("prod_cprod").cast(pl.String, strict=False).alias("codigo_produto"),
                pl.col("prod_xprod").cast(pl.String, strict=False).alias("descricao"),
                pl.col("prod_ncm").cast(pl.String, strict=False).alias("ncm"),
                pl.col("prod_cest").cast(pl.String, strict=False).alias("cest"),
                pl.col("co_cfop").cast(pl.String, strict=False).alias("cfop"),
                pl.col("icms_cst").cast(pl.String, strict=False).alias("cst"),
                pl.col("icms_csosn").cast(pl.String, strict=False).alias("csosn"),
                pl.col("cnpj_filtro").cast(pl.String, strict=False).alias("cnpj_referencia"),
                pl.col("co_emitente").cast(pl.String, strict=False).alias("cnpj_emitente"),
                pl.col("co_destinatario").cast(pl.String, strict=False).alias("cnpj_destinatario"),
                pl.col("dhemi").cast(pl.String, strict=False).alias("data_documento"),
                pl.col("prod_qcom").cast(pl.Float64, strict=False).fill_null(0.0).alias("quantidade"),
                pl.col("prod_vprod").cast(pl.Float64, strict=False).fill_null(0.0).alias("valor_total"),
                pl.col("icms_vbcst").cast(pl.Float64, strict=False).fill_null(0.0).alias("bc_st"),
                pl.col("icms_vicmsst").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_st"),
                pl.col("icms_vicmssubstituto").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_icms_substituto"),
                pl.col("icms_vicmsstret").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_st_retido"),
                pl.col("icms_vbcfcpst").cast(pl.Float64, strict=False).fill_null(0.0).alias("bc_fcp_st"),
                pl.col("icms_pfcpst").cast(pl.Float64, strict=False).fill_null(0.0).alias("p_fcp_st"),
                pl.col("icms_vfcpst").cast(pl.Float64, strict=False).fill_null(0.0).alias("vl_fcp_st"),
            ]
        )
        .with_columns(
            (
                pl.coalesce([pl.col("chave_documento"), pl.lit("")]).cast(pl.String, strict=False)
                + pl.lit("|")
                + pl.coalesce([pl.col("item_documento"), pl.lit("")]).cast(pl.String, strict=False)
            ).alias("id_linha_origem")
        )
        .select(list(SCHEMAS_SILVER["nfe_dados_st"].keys()))
    )

    return df_nfe_dados_st


def _montar_e111_ajustes(diretorio_extraidos: Path) -> pl.DataFrame:
    """Padroniza os ajustes E111 em uma trilha silver por competencia."""
    df_e111 = _ler_parquet_se_existir(diretorio_extraidos / "e111.parquet")
    if df_e111.is_empty():
        return _criar_dataframe_vazio("e111_ajustes")

    # O E111 materializa ajustes de apuracao e precisa preservar codigo, descricao e valor original.
    df_e111_ajustes = (
        df_e111
        .with_columns(
            [
                pl.col("periodo_efd").cast(pl.String, strict=False).alias("periodo_efd"),
                pl.col("cnpj_referencia").cast(pl.String, strict=False).alias("cnpj_referencia"),
                pl.col("codigo_ajuste").cast(pl.String, strict=False).alias("codigo_ajuste"),
                pl.col("descricao_codigo_ajuste").cast(pl.String, strict=False).alias("descricao_codigo_ajuste"),
                pl.col("descr_compl").cast(pl.String, strict=False).alias("descricao_complementar"),
                pl.col("valor_ajuste").cast(pl.Float64, strict=False).fill_null(0.0).alias("valor_ajuste"),
                _formatar_data_expr("data_entrega_efd_periodo"),
                pl.col("cod_fin_efd").cast(pl.String, strict=False).alias("cod_fin_efd"),
            ]
        )
        .select(list(SCHEMAS_SILVER["e111_ajustes"].keys()))
    )

    return df_e111_ajustes


def _montar_fontes_produtos(
    diretorio_extraidos: Path,
    cnpj: str,
) -> pl.DataFrame:
    """Monta a base unificada de movimentos a partir das extracoes brutas."""
    df_nfe = _ler_parquet_se_existir(diretorio_extraidos / "nfe.parquet")
    df_nfce = _ler_parquet_se_existir(diretorio_extraidos / "nfce.parquet")
    df_c170 = _ler_parquet_se_existir(diretorio_extraidos / "c170.parquet")
    df_bloco_h = _ler_parquet_se_existir(diretorio_extraidos / "bloco_h.parquet")

    partes: list[pl.DataFrame] = []

    if not df_nfe.is_empty():
        # Padroniza a NF-e preservando o sentido operacional do documento para o CNPJ analisado.
        partes.append(
            _padronizar_fonte_documental(
                df_nfe,
                fonte="nfe",
                tipo_movimento_expr=pl.when(pl.col("tipo_operacao").str.contains("ENTRADA"))
                .then(pl.lit("entrada"))
                .otherwise(pl.lit("saida")),
                chave_documento_expr=pl.col("chave_acesso").cast(pl.String, strict=False),
                item_documento_expr=pl.col("prod_nitem").cast(pl.String, strict=False),
                codigo_fonte_expr=pl.col("codigo_fonte").cast(pl.String, strict=False),
                codigo_produto_expr=pl.col("prod_cprod").cast(pl.String, strict=False),
                descricao_expr=pl.col("prod_xprod").cast(pl.String, strict=False),
                ncm_expr=pl.col("prod_ncm").cast(pl.String, strict=False),
                cest_expr=pl.col("prod_cest").cast(pl.String, strict=False),
                gtin_expr=pl.col("prod_cean").cast(pl.String, strict=False),
                unidade_expr=pl.col("prod_ucom").cast(pl.String, strict=False),
                quantidade_expr=pl.col("prod_qcom"),
                valor_unitario_expr=pl.col("prod_vuncom"),
                valor_total_expr=pl.col("prod_vprod"),
                cfop_expr=pl.col("co_cfop").cast(pl.String, strict=False),
                data_documento_expr=_formatar_data_expr("dhemi"),
                cnpj_referencia_expr=pl.col("cnpj_filtro").cast(pl.String, strict=False),
                cnpj_emitente_expr=pl.col("co_emitente").cast(pl.String, strict=False),
                cnpj_destinatario_expr=pl.col("co_destinatario").cast(pl.String, strict=False),
            )
        )

    if not df_nfce.is_empty():
        # Padroniza a NFC-e no mesmo contrato da NF-e para viabilizar recomposicao posterior.
        partes.append(
            _padronizar_fonte_documental(
                df_nfce,
                fonte="nfce",
                tipo_movimento_expr=pl.when(pl.col("tipo_operacao").str.contains("ENTRADA"))
                .then(pl.lit("entrada"))
                .otherwise(pl.lit("saida")),
                chave_documento_expr=pl.col("chave_acesso").cast(pl.String, strict=False),
                item_documento_expr=pl.col("prod_nitem").cast(pl.String, strict=False),
                codigo_fonte_expr=pl.col("codigo_fonte").cast(pl.String, strict=False),
                codigo_produto_expr=pl.col("prod_cprod").cast(pl.String, strict=False),
                descricao_expr=pl.col("prod_xprod").cast(pl.String, strict=False),
                ncm_expr=pl.col("prod_ncm").cast(pl.String, strict=False),
                cest_expr=pl.col("prod_cest").cast(pl.String, strict=False),
                gtin_expr=pl.col("prod_cean").cast(pl.String, strict=False),
                unidade_expr=pl.col("prod_ucom").cast(pl.String, strict=False),
                quantidade_expr=pl.col("prod_qcom"),
                valor_unitario_expr=pl.col("prod_vuncom"),
                valor_total_expr=pl.col("prod_vprod"),
                cfop_expr=pl.col("co_cfop").cast(pl.String, strict=False),
                data_documento_expr=_formatar_data_expr("dhemi"),
                cnpj_referencia_expr=pl.col("cnpj_filtro").cast(pl.String, strict=False),
                cnpj_emitente_expr=pl.col("co_emitente").cast(pl.String, strict=False),
                cnpj_destinatario_expr=pl.col("co_destinatario").cast(pl.String, strict=False),
            )
        )

    if not df_c170.is_empty():
        # O C170 representa a principal base de entradas escrituradas da EFD para o core de estoque.
        partes.append(
            _padronizar_fonte_documental(
                df_c170,
                fonte="c170",
                tipo_movimento_expr=pl.lit("entrada"),
                chave_documento_expr=pl.col("reg_c100_id").cast(pl.String, strict=False),
                item_documento_expr=pl.col("num_item").cast(pl.String, strict=False),
                codigo_fonte_expr=pl.lit(cnpj) + pl.lit("|") + pl.col("cod_item").cast(pl.String, strict=False),
                codigo_produto_expr=pl.col("cod_item").cast(pl.String, strict=False),
                descricao_expr=pl.coalesce([pl.col("descr_compl"), pl.col("cod_item").cast(pl.String, strict=False)]),
                ncm_expr=pl.lit(None, dtype=pl.String),
                cest_expr=pl.lit(None, dtype=pl.String),
                gtin_expr=pl.lit(None, dtype=pl.String),
                unidade_expr=pl.col("unid").cast(pl.String, strict=False),
                quantidade_expr=pl.col("qtd"),
                valor_unitario_expr=pl.when(pl.col("qtd").cast(pl.Float64, strict=False) > 0)
                .then(pl.col("vl_item").cast(pl.Float64, strict=False) / pl.col("qtd").cast(pl.Float64, strict=False))
                .otherwise(0.0),
                valor_total_expr=pl.col("vl_item"),
                cfop_expr=pl.col("cfop").cast(pl.String, strict=False),
                data_documento_expr=_converter_periodo_efd_para_data_expr("periodo_efd"),
                cnpj_referencia_expr=pl.col("cnpj").cast(pl.String, strict=False),
                cnpj_emitente_expr=pl.lit(cnpj),
                cnpj_destinatario_expr=pl.lit(cnpj),
            )
        )

    if not df_bloco_h.is_empty():
        # O Bloco H entra como inventario declarado e preserva a quantidade fisica para conciliacao.
        partes.append(
            _padronizar_fonte_documental(
                df_bloco_h,
                fonte="bloco_h",
                tipo_movimento_expr=pl.lit("inventario"),
                chave_documento_expr=pl.col("dt_inv").cast(pl.String, strict=False),
                item_documento_expr=pl.col("codigo_produto").cast(pl.String, strict=False),
                codigo_fonte_expr=pl.col("codigo_fonte").cast(pl.String, strict=False),
                codigo_produto_expr=pl.col("codigo_produto").cast(pl.String, strict=False),
                descricao_expr=pl.col("descricao_produto").cast(pl.String, strict=False),
                ncm_expr=pl.col("cod_ncm").cast(pl.String, strict=False),
                cest_expr=pl.col("cest").cast(pl.String, strict=False),
                gtin_expr=pl.col("cod_barra").cast(pl.String, strict=False),
                unidade_expr=pl.col("unidade_medida").cast(pl.String, strict=False),
                quantidade_expr=pl.col("quantidade"),
                valor_unitario_expr=pl.col("valor_unitario"),
                valor_total_expr=pl.col("valor_item"),
                cfop_expr=pl.lit(None, dtype=pl.String),
                data_documento_expr=_formatar_data_expr("dt_inv"),
                cnpj_referencia_expr=pl.col("cnpj").cast(pl.String, strict=False),
                cnpj_emitente_expr=pl.col("cnpj").cast(pl.String, strict=False),
                cnpj_destinatario_expr=pl.col("cnpj").cast(pl.String, strict=False),
            )
        )

    if not partes:
        return _criar_dataframe_vazio("fontes_produtos")

    return pl.concat(partes, how="vertical_relaxed")


def _escrever_tabela_silver(
    diretorio_silver: Path,
    nome_tabela: str,
    dataframe: pl.DataFrame,
) -> int:
    """Persiste uma tabela silver com compressao padrao."""
    caminho_saida = diretorio_silver / f"{nome_tabela}.parquet"
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    dataframe.write_parquet(caminho_saida, compression="zstd")
    logger.info("silver.%s: %s registros", nome_tabela, len(dataframe))
    return len(dataframe)


def materializar_camadas_silver(diretorio_cnpj: Path, cnpj: str) -> dict[str, int]:
    """Materializa a camada silver obrigatoria para o pipeline fiscal."""
    diretorios = garantir_estrutura_camadas_cnpj(diretorio_cnpj)
    diretorio_extraidos = diretorios["extraidos"]
    diretorio_silver = diretorios["silver"]

    df_fontes = _montar_fontes_produtos(diretorio_extraidos, cnpj)
    df_tb_documentos = _consolidar_tabela_documentos(df_fontes)
    df_item_unidades = _consolidar_item_unidades(df_fontes)
    df_itens = _consolidar_itens(df_item_unidades)
    df_descricao = _consolidar_descricao_produtos(df_itens)
    df_c170_xml = (
        df_fontes.filter(pl.col("fonte") == "c170").select(list(SCHEMAS_SILVER["c170_xml"].keys()))
        if not df_fontes.is_empty()
        else _criar_dataframe_vazio("c170_xml")
    )
    df_c176_xml = _montar_c176_xml(diretorio_extraidos, cnpj)
    df_nfe_dados_st = _montar_nfe_dados_st(diretorio_extraidos)
    df_e111_ajustes = _montar_e111_ajustes(diretorio_extraidos)

    resultados = {
        "tb_documentos": _escrever_tabela_silver(diretorio_silver, "tb_documentos", df_tb_documentos),
        "item_unidades": _escrever_tabela_silver(diretorio_silver, "item_unidades", df_item_unidades),
        "itens": _escrever_tabela_silver(diretorio_silver, "itens", df_itens),
        "descricao_produtos": _escrever_tabela_silver(diretorio_silver, "descricao_produtos", df_descricao),
        "fontes_produtos": _escrever_tabela_silver(diretorio_silver, "fontes_produtos", df_fontes),
        "c170_xml": _escrever_tabela_silver(diretorio_silver, "c170_xml", df_c170_xml),
        "c176_xml": _escrever_tabela_silver(diretorio_silver, "c176_xml", df_c176_xml),
        "nfe_dados_st": _escrever_tabela_silver(diretorio_silver, "nfe_dados_st", df_nfe_dados_st),
        "e111_ajustes": _escrever_tabela_silver(diretorio_silver, "e111_ajustes", df_e111_ajustes),
    }

    return resultados
