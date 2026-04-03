"""Gerador da tabela `fatores_conversao`."""

from __future__ import annotations

import logging
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
)

logger = logging.getLogger(__name__)


def calcular_metadados_unidades(df_item_unidades: pl.DataFrame) -> pl.DataFrame:
    """Calcula quantidades e precos medios de compras e vendas por descricao normalizada e unidade."""
    if df_item_unidades.is_empty():
        return pl.DataFrame(schema={"descricao_normalizada": pl.String, "unidade": pl.String, "preco_compra": pl.Float64, "preco_venda": pl.Float64, "qtd_mov_total": pl.Float64})
    
    # Adicionar descricao normalizada
    df_norm = df_item_unidades.with_columns(
        pl.col("descricao")
        .cast(pl.Utf8, strict=False)
        .map_elements(normalizar_descricao, return_dtype=pl.String)
        .alias("descricao_normalizada")
    )

    # Agrupar por descricao e unidade
    df_agg = df_norm.group_by(["descricao_normalizada", "unidade"]).agg(
        pl.col("compras").sum().alias("compras_total"),
        pl.col("qtd_compras").sum().alias("qtd_compras_total"),
        pl.col("vendas").sum().alias("vendas_total"),
        pl.col("qtd_vendas").sum().alias("qtd_vendas_total"),
    )

    # Calcular o preco medio
    return df_agg.with_columns(
        (pl.col("qtd_compras_total") + pl.col("qtd_vendas_total")).alias("qtd_mov_total"),
        (
            pl.when(pl.col("qtd_compras_total") > 0)
            .then(pl.col("compras_total") / pl.col("qtd_compras_total"))
            .otherwise(0.0)
        ).alias("preco_compra"),
        (
            pl.when(pl.col("qtd_vendas_total") > 0)
            .then(pl.col("vendas_total") / pl.col("qtd_vendas_total"))
            .otherwise(0.0)
        ).alias("preco_venda")
    )


def cruzar_agrupados_com_unidades(df_agrupados: pl.DataFrame, df_unidades: pl.DataFrame) -> pl.DataFrame:
    """Cruza a tabela mestre com as estatisticas de cada unidade conhecida para aquele produto."""
    # Como df_agrupados tem unid_compra e unid_venda, precisamos descobrir as propriedades de cada uma.
    # Primeiro normalizamos a desc_padrao para cruzar com as unidades
    df_base = df_agrupados.with_columns(
        pl.col("descricao_padrao")
        .cast(pl.Utf8, strict=False)
        .map_elements(normalizar_descricao, return_dtype=pl.String)
        .alias("descricao_normalizada")
    )
    
    return df_base.join(
        df_unidades,
        on="descricao_normalizada",
        how="left"
    )

@registrar_gerador("fatores_conversao")
def gerar_fatores_conversao(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Calcula fatores baseados no preco medio (COMPRA > VENDA), priorizando ajustes manuais."""
    arquivo_agrupados = diretorio_parquets / "produtos_agrupados.parquet"
    arquivo_item_unidades = diretorio_cnpj / "silver" / "item_unidades.parquet"
    arquivo_edicoes = diretorio_cnpj / "edicoes" / "fatores.json"

    if not arquivo_agrupados.exists():
        logger.warning("fatores_conversao: dependência produtos_agrupados ausente.")
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_agrupados = pl.read_parquet(arquivo_agrupados)
    df_item_unidades = pl.read_parquet(arquivo_item_unidades) if arquivo_item_unidades.exists() else pl.DataFrame()
    edicoes = carregar_json_se_existir(arquivo_edicoes, {})

    if df_agrupados.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    # 1. Obter precos e quantidades de cada unidade usada pelo produto
    df_unidades_calc = calcular_metadados_unidades(df_item_unidades)
    
    # 2. Iterar programaticamente sobre os agrupados aplicando regras finas (mantido loop pela facilidade manual)
    registros: list[dict[str, object]] = []
    
    # Pre-computar lookup dictionary para evitar chamadas de filter em loop O(N*M)
    lookup_unidades: dict[str, list[dict[str, object]]] = {}
    if not df_unidades_calc.is_empty():
        for row in df_unidades_calc.iter_rows(named=True):
            desc = str(row.get("descricao_normalizada") or "")
            if desc not in lookup_unidades:
                lookup_unidades[desc] = []
            lookup_unidades[desc].append(dict(row))

    for linha in df_agrupados.to_dicts():
        id_agrupado = str(linha["id_agrupado"])
        descricao_padrao = str(linha.get("descricao_padrao") or "")
        desc_norm = normalizar_descricao(descricao_padrao)
        unid_compra = str(linha.get("unid_compra") or "").strip()
        unid_venda = str(linha.get("unid_venda") or "").strip()
        
        # Filtra as unidades ativas daquele produto via dicionario
        unidades_prod = lookup_unidades.get(desc_norm, [])
        
        # Escolha da unidade referencia automatica:
        # unid_ref = fallback automatico (maior qtd_mov_total)
        unid_ref = unid_venda or unid_compra
        if unidades_prod:
            # Encontrar unidade com maior qtd_mov_total
            unid_ref_dict = max(unidades_prod, key=lambda u: float(u.get("qtd_mov_total") or 0.0))
            unid_ref = str(unid_ref_dict.get("unidade") or "")
            
        fator_compra_ref = 1.0
        fator_venda_ref = 1.0
        origem_fator = "SEM_PRECO"
        status = "ok" if not (unid_compra and unid_venda and unid_compra != unid_venda) else "pendente"
        editado_em = None
        
        # Aplicacao da edicao manual (Unidade de Ref assumida pela edição)
        edicao = edicoes.get(id_agrupado)
        editado_manualmente = False
        if edicao and edicao.get("unid_ref"):
            unid_ref = str(edicao["unid_ref"]).strip()
            editado_manualmente = True

        # Logica de fator baseada no preco medio:
        if unid_ref and unidades_prod:
            # Pegar precos da unidade referencia
            linha_ref = [u for u in unidades_prod if u.get("unidade") == unid_ref]
            preco_base = 0.0
            
            if linha_ref:
                u_ref = linha_ref[0]
                pm_compra = float(u_ref.get("preco_compra") or 0.0)
                pm_venda = float(u_ref.get("preco_venda") or 0.0)
                
                # Preco base prioriza COMPRA senao VENDA
                if pm_compra > 0:
                    preco_base = pm_compra
                    origem_fator = "COMPRA"
                elif pm_venda > 0:
                    preco_base = pm_venda
                    origem_fator = "VENDA"
            
            # Recalcular fatores perante o preco base
            if preco_base > 0:
                # Fator Compra
                linha_compra = [u for u in unidades_prod if u.get("unidade") == unid_compra]
                if linha_compra:
                    u_compra = linha_compra[0]
                    pm_compra_uc = float(u_compra.get("preco_compra") or 0.0)
                    pm_venda_uc = float(u_compra.get("preco_venda") or 0.0)
                    preco_base_uc = pm_compra_uc if pm_compra_uc > 0 else pm_venda_uc
                    if preco_base_uc > 0:
                        fator_compra_ref = preco_base_uc / preco_base
                        
                # Fator Venda
                linha_venda = [u for u in unidades_prod if u.get("unidade") == unid_venda]
                if linha_venda:
                    u_venda = linha_venda[0]
                    pm_compra_uv = float(u_venda.get("preco_compra") or 0.0)
                    pm_venda_uv = float(u_venda.get("preco_venda") or 0.0)
                    preco_base_uv = pm_compra_uv if pm_compra_uv > 0 else pm_venda_uv
                    if preco_base_uv > 0:
                        fator_venda_ref = preco_base_uv / preco_base

        # Se for manual sobrepoe tudo
        if editado_manualmente:
            if edicao.get("fator_compra_ref") is not None:
                fator_compra_ref = float(edicao["fator_compra_ref"])
            if edicao.get("fator_venda_ref") is not None:
                fator_venda_ref = float(edicao["fator_venda_ref"])
            origem_fator = "manual"
            status = "ok"
            editado_em = datetime.now(tz=timezone.utc).isoformat()

        registros.append({
            "id_agrupado": id_agrupado,
            "descricao_padrao": descricao_padrao,
            "unid_compra": unid_compra,
            "unid_venda": unid_venda,
            "unid_ref": unid_ref,
            "fator_compra_ref": fator_compra_ref,
            "fator_venda_ref": fator_venda_ref,
            "origem_fator": origem_fator,
            "status": status,
            "editado_em": editado_em,
        })

    df_fatores = pl.DataFrame(registros) if registros else criar_dataframe_vazio_contrato(contrato)
    total_registros = escrever_dataframe_ao_contrato(df_fatores, arquivo_saida, contrato)
    logger.info("fatores_conversao: %s registros gerados", total_registros)
    return total_registros
