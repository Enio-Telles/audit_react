from __future__ import annotations

from pathlib import Path

import polars as pl

import audit_engine  # noqa: F401
from audit_engine.contratos.base import CONTRATOS
from audit_engine.tabelas.ajustes_e111.gerador import gerar_ajustes_e111
from audit_engine.tabelas.st_itens.gerador import gerar_st_itens
from audit_engine.utils.camada_silver import materializar_camadas_silver
from audit_engine.utils.camadas_cnpj import garantir_estrutura_camadas_cnpj


def _escrever_dependencias_produto(diretorio_parquets: Path) -> None:
    pl.DataFrame(
        [
            {
                "id_produto": 1,
                "descricao": "PRODUTO TESTE",
                "ncm": "12345678",
                "cest": "1234567",
                "unidade_principal": "UN",
                "qtd_total_nfe": 10,
                "valor_total": 100.0,
                "tipo": "ambos",
            }
        ]
    ).write_parquet(diretorio_parquets / "produtos.parquet")

    pl.DataFrame(
        [
            {
                "id_produto": 1,
                "id_agrupado": "AGR-1",
                "descricao_original": "PRODUTO TESTE",
                "descricao_padrao": "PRODUTO TESTE",
            }
        ]
    ).write_parquet(diretorio_parquets / "id_agrupados.parquet")

    pl.DataFrame(
        [
            {
                "id_agrupado": "AGR-1",
                "descricao_padrao": "PRODUTO TESTE",
                "ncm_padrao": "12345678",
                "cest_padrao": "1234567",
                "unid_ref": "UN",
                "fator_compra_ref": 1.0,
                "fator_venda_ref": 1.0,
                "qtd_total_nfe": 10,
                "valor_total": 100.0,
                "ids_membros": "[1]",
                "qtd_membros": 1,
                "status_conversao": "ok",
                "status_agregacao": "ok",
            }
        ]
    ).write_parquet(diretorio_parquets / "produtos_final.parquet")


def test_materializa_silver_st_e_gera_tabelas_complementares(tmp_path: Path):
    cnpj = "12345678000190"
    diretorio_cnpj = tmp_path / cnpj
    diretorios = garantir_estrutura_camadas_cnpj(diretorio_cnpj)

    pl.DataFrame(
        [
            {
                "periodo_efd": "2024/01",
                "data_entrega_efd_periodo": "2024-02-15",
                "cod_fin_efd": "0",
                "finalidade_efd": "0 - Original",
                "cnpj_referencia": cnpj,
                "chave_saida": "NFE123",
                "num_nf_saida": "1",
                "dt_doc_saida": "2024-01-10",
                "dt_e_s_saida": "2024-01-10",
                "cod_item": "PROD1",
                "descricao_item": "PRODUTO TESTE",
                "num_item_saida": "1",
                "cfop_saida": "5405",
                "unid_saida": "UN",
                "qtd_item_saida": 10.0,
                "vl_total_item": 100.0,
                "cod_mot_res": "1",
                "descricao_motivo_ressarcimento": "1 - Saida para outra UF",
                "chave_nfe_ultima_entrada": "NFEENTRADA1",
                "num_item_ultima_entrada": "1",
                "dt_ultima_entrada": "2024-01-05",
                "vl_unit_bc_st_entrada": 7.0,
                "vl_unit_icms_proprio_entrada": 1.0,
                "vl_unit_ressarcimento_st": 2.0,
                "vl_ressarc_credito_proprio": 10.0,
                "vl_ressarc_st_retido": 20.0,
                "vr_total_ressarcimento": 30.0,
            }
        ]
    ).write_parquet(diretorios["extraidos"] / "c176.parquet")

    pl.DataFrame(
        [
            {
                "tipo_operacao": "ENTRADA",
                "co_destinatario": cnpj,
                "co_emitente": "99887766000155",
                "cnpj_filtro": cnpj,
                "nsu": 1,
                "chave_acesso": "NFEENTRADA1",
                "prod_nitem": 1,
                "codigo_fonte": f"{cnpj}|PROD1",
                "nnf": 10,
                "dhemi": "2024-01-05",
                "dhsaient": "2024-01-05",
                "co_tp_nf": 0,
                "xnome_emit": "FORNECEDOR TESTE",
                "xnome_dest": "EMPRESA TESTE",
                "prod_cprod": "PROD1",
                "prod_cean": None,
                "prod_xprod": "PRODUTO TESTE",
                "prod_ncm": "12345678",
                "prod_cest": "1234567",
                "co_cfop": 1102,
                "prod_ucom": "CX",
                "prod_qcom": 20.0,
                "prod_vuncom": 5.0,
                "prod_vprod": 100.0,
                "prod_ceantrib": None,
                "prod_utrib": "UN",
                "prod_qtrib": 20.0,
                "prod_vuntrib": 5.0,
                "prod_vfrete": 0.0,
                "prod_vseg": 0.0,
                "prod_vdesc": 0.0,
                "prod_voutro": 0.0,
                "dt_gravacao": "2024-01-05",
            }
        ]
    ).write_parquet(diretorios["extraidos"] / "nfe.parquet")

    pl.DataFrame(
        [
            {
                "chave_acesso": "NFE123",
                "prod_nitem": "1",
                "codigo_fonte": f"{cnpj}|PROD1",
                "prod_cprod": "PROD1",
                "prod_xprod": "PRODUTO TESTE",
                "prod_ncm": "12345678",
                "prod_cest": "1234567",
                "co_cfop": "5405",
                "prod_qcom": 10.0,
                "prod_vprod": 100.0,
                "icms_cst": "60",
                "icms_csosn": None,
                "cnpj_filtro": cnpj,
                "co_emitente": cnpj,
                "co_destinatario": "99887766000155",
                "dhemi": "2024-01-10",
                "icms_vbcst": 90.0,
                "icms_vicmsst": 18.0,
                "icms_vicmssubstituto": 5.0,
                "icms_vicmsstret": 11.0,
                "icms_vbcfcpst": 90.0,
                "icms_pfcpst": 2.0,
                "icms_vfcpst": 1.8,
            }
        ]
    ).write_parquet(diretorios["extraidos"] / "nfe_dados_st.parquet")

    pl.DataFrame(
        [
            {
                "periodo_efd": "2024/01",
                "cnpj_referencia": cnpj,
                "codigo_ajuste": "SP000001",
                "descricao_codigo_ajuste": "AJUSTE TESTE",
                "descr_compl": "Complemento ajuste",
                "valor_ajuste": 55.5,
                "data_entrega_efd_periodo": "2024-02-15",
                "cod_fin_efd": "0",
            }
        ]
    ).write_parquet(diretorios["extraidos"] / "e111.parquet")

    _escrever_dependencias_produto(diretorios["parquets"])
    materializar_camadas_silver(diretorio_cnpj, cnpj)

    assert (diretorios["silver"] / "c176_xml.parquet").exists()
    assert (diretorios["silver"] / "nfe_dados_st.parquet").exists()
    assert (diretorios["silver"] / "e111_ajustes.parquet").exists()

    total_ajustes = gerar_ajustes_e111(
        diretorio_cnpj,
        diretorios["parquets"],
        diretorios["parquets"] / CONTRATOS["ajustes_e111"].saida,
        CONTRATOS["ajustes_e111"],
    )
    total_st = gerar_st_itens(
        diretorio_cnpj,
        diretorios["parquets"],
        diretorios["parquets"] / CONTRATOS["st_itens"].saida,
        CONTRATOS["st_itens"],
    )

    assert total_ajustes == 1
    assert total_st == 1

    df_ajustes = pl.read_parquet(diretorios["parquets"] / "ajustes_e111.parquet")
    df_st = pl.read_parquet(diretorios["parquets"] / "st_itens.parquet")
    df_c176_silver = pl.read_parquet(diretorios["silver"] / "c176_xml.parquet")

    assert df_ajustes.row(0, named=True)["ano"] == "2024"

    linha_c176 = df_c176_silver.row(0, named=True)
    assert linha_c176["periodo_efd"] == "2024/01"
    assert linha_c176["chave_saida"] == "NFE123"
    assert linha_c176["c176_num_item_ult_e_declarado"] == "1"
    assert linha_c176["unid_entrada_xml"] == "CX"
    assert linha_c176["qtd_entrada_xml"] == 20.0
    assert linha_c176["vl_total_entrada_xml"] == 100.0
    assert linha_c176["vl_unitario_entrada_xml"] == 5.0

    linha_st = df_st.row(0, named=True)
    assert linha_st["status_conciliacao"] == "conciliado"
    assert linha_st["id_agrupado"] == "AGR-1"
    assert linha_st["descricao_padrao"] == "PRODUTO TESTE"
    assert linha_st["bc_st_xml"] == 90.0
    assert linha_st["vl_total_ressarcimento"] == 30.0
