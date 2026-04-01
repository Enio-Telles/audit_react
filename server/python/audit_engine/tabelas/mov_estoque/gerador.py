"""Gerador da tabela `mov_estoque`."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.pipeline_fiscal import (
    criar_dataframe_vazio_contrato,
    escrever_dataframe_ao_contrato,
    mapear_fontes_para_grupos,
)

logger = logging.getLogger(__name__)


def classificar_direcao_fiscal(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica o enumerador de ordem 0/1/2/3 para garantir a cronologia correta no mesmo dia."""
    return df.with_columns(
        pl.when(pl.col("fonte") == "c170")
        .then(pl.lit("1 - ENTRADA"))
        .when(pl.col("fonte").is_in(["nfe", "nfce"]))
        .then(pl.lit("2 - SAIDAS"))
        .when(pl.col("fonte") == "bloco_h")
        .then(pl.lit("3 - ESTOQUE FINAL"))
        .otherwise(pl.lit("1 - ENTRADA"))  # default entrada
        .alias("tipo")
    )

def _calcular_movimentacao_cronologica(df_movimentos: pl.DataFrame) -> pl.DataFrame:
    """Calcula saldo e custo medio intr-ano sequencialmente por `id_agrupado` e `ano`, acionando entradas desacobertadas."""
    if df_movimentos.is_empty():
        return df_movimentos

    # Ordenar por Produto -> Data -> Ordem Fato (0,1,2,3)
    df_movimentos = df_movimentos.with_columns(
        pl.col("data").cast(pl.Date, strict=False)
    ).with_columns(
        pl.col("data").dt.year().alias("ano"),
        pl.col("data").dt.month().alias("mes")
    )
    
    # Preencher exclusao default
    if "excluir_estoque" not in df_movimentos.columns:
        df_movimentos = df_movimentos.with_columns(pl.lit(False).alias("excluir_estoque"))
        
    df_sorted = df_movimentos.sort(["id_agrupado", "data", "tipo"])

    registros_saida: list[dict[str, object]] = []
    
    for (id_agrupado, ano), df_grupo in df_sorted.group_by(["id_agrupado", "ano"], maintain_order=True):
        saldo = 0.0
        saldo_financeiro = 0.0
        custo_medio = 0.0
        
        for linha in df_grupo.iter_rows(named=True):
            tipo = str(linha.get("tipo") or "")
            q_conv = float(linha.get("q_conv") or 0.0)
            valor_total = float(linha.get("valor_total") or 0.0)
            excluir_estoque = bool(linha.get("excluir_estoque") or False)
            entr_desac_anual = 0.0
            qtd_decl_final_audit = 0.0
            
            # Zerar efetividade da linha se neutralizada
            if excluir_estoque:
                q_conv = 0.0
                
            if tipo == "0 - ESTOQUE INICIAL":
                saldo += q_conv
                saldo_financeiro += valor_total
                custo_medio = saldo_financeiro / saldo if saldo > 0 else 0.0
            elif tipo == "1 - ENTRADA":
                saldo += q_conv
                saldo_financeiro += valor_total
                custo_medio = saldo_financeiro / saldo if saldo > 0 else 0.0
            elif tipo == "2 - SAIDAS":
                saldo -= q_conv
                if saldo < 0:
                    entr_desac_anual = abs(saldo)
                    saldo = 0.0  # omissao reequilibra o saldo fisico para zero
                saldo_financeiro -= q_conv * custo_medio
                saldo_financeiro = max(saldo_financeiro, 0.0)
            elif tipo == "3 - ESTOQUE FINAL":
                qtd_decl_final_audit = q_conv
                q_conv = 0.0  # nao altera o saldo, so registra para auditoria no final do ano
                
            linha_saida = dict(linha)
            linha_saida["q_conv"] = round(q_conv, 4)
            linha_saida["saldo_estoque_anual"] = round(saldo, 4)
            linha_saida["custo_medio_anual"] = round(custo_medio, 6)
            linha_saida["entr_desac_anual"] = round(entr_desac_anual, 4)
            linha_saida["__qtd_decl_final_audit__"] = round(qtd_decl_final_audit, 4)
            registros_saida.append(linha_saida)
            
    return pl.DataFrame(registros_saida)

@registrar_gerador("mov_estoque")
def gerar_mov_estoque(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Consolida entradas, saidas e inventario em uma trilha cronologica de estoque com deteccao de furos e neutralizacao."""
    arquivo_nfe_entrada = diretorio_parquets / "nfe_entrada.parquet"
    arquivo_produtos = diretorio_parquets / "produtos.parquet"
    arquivo_id_agrupados = diretorio_parquets / "id_agrupados.parquet"
    arquivo_produtos_final = diretorio_parquets / "produtos_final.parquet"
    arquivo_fatores = diretorio_parquets / "fatores_conversao.parquet"
    caminho_fontes = diretorio_cnpj / "silver" / "fontes_produtos.parquet"

    if not arquivo_nfe_entrada.exists():
        logger.warning("nfe_entrada ausente: gerando mov_estoque vazia.")
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_entradas = pl.read_parquet(arquivo_nfe_entrada)
    df_produtos = pl.read_parquet(arquivo_produtos)
    df_id_agrupados = pl.read_parquet(arquivo_id_agrupados)
    df_fatores = pl.read_parquet(arquivo_fatores) if arquivo_fatores.exists() else pl.DataFrame()

    # ⚡ BOLT: Otimizacao de performance. Usa lazy evaluation para filtrar direto na leitura do parquet (pushdown filter) reduzindo uso de memoria.
    df_saidas_inventario = pl.DataFrame()
    if caminho_fontes.exists():
        df_saidas_inventario = pl.scan_parquet(caminho_fontes).filter(pl.col("tipo_movimento").is_in(["saida", "inventario"])).collect()
        if not df_saidas_inventario.is_empty():
            df_saidas_inventario = mapear_fontes_para_grupos(df_saidas_inventario, df_produtos, df_id_agrupados).filter(pl.col("id_agrupado").is_not_null())

    if not df_saidas_inventario.is_empty() and not df_fatores.is_empty():
        # Juntar fatores as saidas
        df_saidas_inventario = (
            df_saidas_inventario
            .join(
                df_fatores.select(["id_agrupado", "unid_compra", "unid_venda", "unid_ref", "fator_compra_ref", "fator_venda_ref"]),
                on="id_agrupado",
                how="left",
            )
            .with_columns(
                pl.when(pl.col("unidade") == pl.col("unid_ref")).then(1.0)
                  .when(pl.col("unidade") == pl.col("unid_compra")).then(pl.col("fator_compra_ref"))
                  .when(pl.col("unidade") == pl.col("unid_venda")).then(pl.col("fator_venda_ref"))
                  .otherwise(1.0)
                  .alias("fator_aplicado")
            )
            .with_columns(
                (pl.col("quantidade").abs() * pl.col("fator_aplicado").abs()).alias("q_conv"),
                pl.lit(False).alias("excluir_estoque")
            )
        )
    else:
        # Fallback sem fatores
        df_saidas_inventario = df_saidas_inventario.with_columns(
            pl.col("quantidade").abs().alias("q_conv"),
            pl.lit(False).alias("excluir_estoque")
        )

    # Entradas da NFE
    if not df_entradas.is_empty():
        # A nfe_entrada ja traz os fatores na doc
        df_entradas = df_entradas.with_columns(
            pl.col("qtd_ref").cast(pl.Float64, strict=False).abs().alias("q_conv"),
            pl.lit(False).alias("excluir_estoque"),
            pl.lit("efd").alias("fonte"),
            pl.lit("nfe").alias("origem")
        )

    # Normalizar ambas as partes para um esquema comum obrigatorio
    colunas_comuns = ["id_agrupado", "descricao", "fonte", "origem", "q_conv", "valor_unitario", "valor_total", "cfop", "excluir_estoque"]
    
    def padronizar_tabela(df: pl.DataFrame, dt_col: str) -> pl.DataFrame:
        if df.is_empty():
            return pl.DataFrame(schema={c: pl.Utf8 if c in ("id_agrupado","descricao","fonte","origem","cfop") else (pl.Boolean if c == "excluir_estoque" else pl.Float64) for c in colunas_comuns + ["data"]})
        
        # garantindo que cfop string exista
        if "cfop" not in df.columns:
            df = df.with_columns(pl.lit("").alias("cfop"))
            
        df = df.with_columns(
            pl.col("cfop").cast(pl.Utf8, strict=False)
        )
        # Manter `excluir_estoque` booleano
        if "excluir_estoque" not in df.columns:
            df = df.with_columns(pl.lit(False).alias("excluir_estoque"))
            
        return df.select(
            pl.col(dt_col).alias("data"),
            *[pl.col(c) for c in colunas_comuns if c in df.columns]
        )

    df_entradas_clean = padronizar_tabela(df_entradas, "data_emissao") if not df_entradas.is_empty() else padronizar_tabela(pl.DataFrame(), "")
    df_saidas_clean = padronizar_tabela(df_saidas_inventario, "data_documento") if not df_saidas_inventario.is_empty() else padronizar_tabela(pl.DataFrame(), "")

    df_movimentos = pl.concat([df_entradas_clean, df_saidas_clean], how="vertical_relaxed")
    
    if df_movimentos.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    # Classificacao Direcional
    df_mov_tipado = classificar_direcao_fiscal(df_movimentos)

    # Processar Cronologia
    df_movimentacao = _calcular_movimentacao_cronologica(df_mov_tipado)
    
    total_registros = escrever_dataframe_ao_contrato(df_movimentacao, arquivo_saida, contrato)
    logger.info("mov_estoque: %s registros gerados", total_registros)
    return total_registros
