from __future__ import annotations

from pathlib import Path

import polars as pl

from scripts.comparar_paridade_externa import comparar_paridade_st, renderizar_relatorio_markdown


def _escrever_parquet(caminho: Path, linhas: list[dict[str, object]]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(linhas).write_parquet(caminho)


def test_compara_paridade_st_com_shape_bruto_divergente_e_shape_canonico_equivalente(tmp_path: Path) -> None:
    cnpj = "12345678000190"
    base_local = tmp_path / "local"
    base_externa = tmp_path / "externa"

    _escrever_parquet(
        base_local / cnpj / "extraidos" / "c176.parquet",
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
                "qtd_item_saida": 10,
                "vl_total_item": 100.0,
                "cod_mot_res": "1",
                "descricao_motivo_ressarcimento": "RESSARCIMENTO",
                "chave_nfe_ultima_entrada": "ENTRADA123",
                "num_item_ultima_entrada": "1",
                "dt_ultima_entrada": "2024-01-05",
                "vl_unit_bc_st_entrada": 7.0,
                "vl_unit_icms_proprio_entrada": 1.0,
                "vl_unit_ressarcimento_st": 2.0,
                "vl_ressarc_credito_proprio": 10.0,
                "vl_ressarc_st_retido": 20.0,
                "vr_total_ressarcimento": 30.0,
            }
        ],
    )
    _escrever_parquet(
        base_externa / cnpj / "arquivos_parquet" / f"c176_{cnpj}.parquet",
        [
            {
                "periodo_efd": "2024/01",
                "data_entrega_efd_periodo": "2024-02-15",
                "cod_fin_efd": "0",
                "finalidade_efd": "0 - Original",
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
                "descricao_motivo_ressarcimento": "RESSARCIMENTO",
                "chave_nfe_ultima_entrada": "ENTRADA123",
                "c176_num_item_ult_e_declarado": "1",
                "dt_ultima_entrada": "2024-01-05",
                "vl_unit_bc_st_entrada": 7.0,
                "vl_unit_icms_proprio_entrada": 1.0,
                "vl_unit_ressarcimento_st": 2.0,
                "vl_ressarc_credito_proprio": 10.0,
                "vl_ressarc_st_retido": 20.0,
                "vr_total_ressarcimento": 30.0,
            }
        ],
    )

    _escrever_parquet(
        base_local / cnpj / "extraidos" / "nfe_dados_st.parquet",
        [
            {
                "chave_acesso": "NFE123",
                "prod_nitem": 1,
                "codigo_fonte": f"{cnpj}|PROD1",
                "prod_cprod": "PROD1",
                "prod_xprod": "PRODUTO TESTE",
                "prod_ncm": "12345678",
                "prod_cest": "1234567",
                "co_cfop": 5405,
                "prod_qcom": 10.0,
                "prod_vprod": 100.0,
                "icms_cst": 60,
                "icms_csosn": 500,
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
        ],
    )
    _escrever_parquet(
        base_externa / cnpj / "arquivos_parquet" / f"nfe_dados_st_{cnpj}.parquet",
        [
            {
                "chave_acesso": "NFE123",
                "prod_nitem": 1,
                "prod_cprod": "PROD1",
                "icms_vbcst": 90.0,
                "icms_vicmsst": 18.0,
                "icms_vicmssubstituto": 5.0,
                "icms_vicmsstret": 11.0,
                "icms_vbcfcpst": 90.0,
                "icms_pfcpst": 2.0,
                "icms_vfcpst": 1.8,
            }
        ],
    )

    _escrever_parquet(
        base_local / cnpj / "extraidos" / "e111.parquet",
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
        ],
    )
    _escrever_parquet(
        base_externa / cnpj / "arquivos_parquet" / f"e111_{cnpj}.parquet",
        [
            {
                "periodo_efd": "2024/01",
                "codigo_ajuste": "SP000001",
                "descricao_codigo_ajuste": "AJUSTE TESTE",
                "descr_compl": "Complemento ajuste",
                "valor_ajuste": 55.5,
                "data_entrega_efd_periodo": "2024-02-15",
                "cod_fin_efd": "0",
            }
        ],
    )

    _escrever_parquet(
        base_local / cnpj / "silver" / "c176_xml.parquet",
        [
            {
                "id_linha_origem": "NFE123|1",
                "fonte": "c176",
                "periodo_efd": "2024/01",
                "data_entrega_efd_periodo": "2024-02-15",
                "cod_fin_efd": "0",
                "finalidade_efd": "0 - Original",
                "chave_documento": "NFE123",
                "chave_saida": "NFE123",
                "num_nf_saida": "1",
                "item_documento": "1",
                "num_item_saida": "1",
                "codigo_fonte": f"{cnpj}|PROD1",
                "codigo_produto": "PROD1",
                "cod_item_ref_saida": "PROD1",
                "descricao": "PRODUTO TESTE",
                "descricao_item": "PRODUTO TESTE",
                "cfop": "5405",
                "cfop_saida": "5405",
                "unid_saida": "UN",
                "data_documento": "2024-01-10",
                "dt_doc_saida": "2024-01-10",
                "dt_e_s_saida": "2024-01-10",
                "cnpj_referencia": cnpj,
                "quantidade": 10.0,
                "qtd_item_saida": 10.0,
                "valor_total": 100.0,
                "cod_mot_res": "1",
                "descricao_motivo_ressarcimento": "RESSARCIMENTO",
                "chave_nfe_ultima_entrada": "ENTRADA123",
                "num_item_ultima_entrada": "1",
                "c176_num_item_ult_e_declarado": "1",
                "dt_ultima_entrada": "2024-01-05",
                "prod_nitem": 1,
                "unid_entrada_xml": "CX",
                "qtd_entrada_xml": 20.0,
                "vl_total_entrada_xml": 100.0,
                "vl_unitario_entrada_xml": 5.0,
                "vl_unit_bc_st_entrada": 7.0,
                "vl_unit_icms_proprio_entrada": 1.0,
                "vl_unit_ressarcimento_st": 2.0,
                "vl_ressarc_credito_proprio": 10.0,
                "vl_ressarc_st_retido": 20.0,
                "vr_total_ressarcimento": 30.0,
            }
        ],
    )
    _escrever_parquet(
        base_externa / cnpj / "analises" / "produtos" / f"c176_xml_{cnpj}.parquet",
        [
            {
                "cnpj": cnpj,
                "periodo_efd": "2024/01",
                "data_entrega_efd_periodo": "2024-02-15",
                "cod_fin_efd": "0",
                "finalidade_efd": "0 - Original",
                "chave_saida": "NFE123",
                "num_nf_saida": "1",
                "dt_doc_saida": "2024-01-10",
                "dt_e_s_saida": "2024-01-10",
                "cod_item_ref_saida": "PROD1",
                "descricao_item": "PRODUTO TESTE",
                "num_item_saida": "1",
                "cfop_saida": "5405",
                "id_agrupado": "AGR-1",
                "descr_padrao": "PRODUTO TESTE",
                "unid_saida": "UN",
                "fator_saida": 1.0,
                "unid_ref": "UN",
                "qtd_item_saida": 10.0,
                "qtd_saida_unid_ref": 10.0,
                "cod_mot_res": "1",
                "descricao_motivo_ressarcimento": "RESSARCIMENTO",
                "chave_nfe_ultima_entrada": "ENTRADA123",
                "c176_num_item_ult_e_declarado": "1",
                "dt_ultima_entrada": "2024-01-05",
                "prod_nitem": 1,
                "unid_entrada_xml": "CX",
                "fator_entrada_xml": 1.0,
                "qtd_entrada_xml": 20.0,
                "qtd_entrada_xml_unid_ref": 20.0,
                "vl_total_entrada_xml": 100.0,
                "vl_unitario_entrada_xml": 5.0,
                "vl_unitario_entrada_xml_unid_ref": 5.0,
                "vl_unit_bc_st_entrada": 7.0,
                "vl_unit_bc_st_entrada_unid_ref": 7.0,
                "vl_unit_icms_proprio_entrada": 1.0,
                "vl_unit_icms_proprio_entrada_unid_ref": 1.0,
                "vl_unit_ressarcimento_st": 2.0,
                "vl_unit_ressarcimento_st_unid_ref": 2.0,
                "vl_ressarc_credito_proprio": 10.0,
                "vl_ressarc_st_retido": 20.0,
                "vr_total_ressarcimento": 30.0,
                "score_vinculo_entrada": 100,
                "diff_qtd_vinculo": 10.0,
                "regra_vinculo_entrada": "chave+item",
                "match_saida_id_agrupado": True,
                "match_entrada_xml": True,
            }
        ],
    )

    _escrever_parquet(
        base_local / cnpj / "parquets" / "id_agrupados.parquet",
        [{"id_produto": 1, "id_agrupado": "AGR-1", "descricao_original": "PRODUTO TESTE", "descricao_padrao": "PRODUTO TESTE"}],
    )
    _escrever_parquet(
        base_local / cnpj / "parquets" / "produtos_final.parquet",
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
        ],
    )
    _escrever_parquet(base_local / cnpj / "silver" / "nfe_dados_st.parquet", [{"id": "1"}])
    _escrever_parquet(base_local / cnpj / "silver" / "e111_ajustes.parquet", [{"id": "1"}])
    _escrever_parquet(base_local / cnpj / "parquets" / "ajustes_e111.parquet", [{"id": "1"}])
    _escrever_parquet(base_local / cnpj / "parquets" / "st_itens.parquet", [{"id": "1"}])

    relatorio = comparar_paridade_st(cnpj, base_local=base_local, base_externa=base_externa)

    assert relatorio["resumo_paridade_externa"]["status_geral"] == "equivalente"
    assert relatorio["resumo_paridade_externa"]["artefatos_equivalentes"] == 4
    assert relatorio["resumo_por_camada"]["bronze"]["equivalentes"] == 3
    assert relatorio["resumo_por_camada"]["silver"]["equivalentes"] == 1

    comparacao_c176 = relatorio["comparacoes_externas"]["extraidos/c176"]
    assert comparacao_c176["paridade_shape_bruto"]["colunas_iguais"] is False
    assert comparacao_c176["paridade_shape_canonico"]["colunas_iguais"] is True
    assert comparacao_c176["paridade_shape_canonico"]["schema_igual"] is True

    comparacao_nfe_st = relatorio["comparacoes_externas"]["extraidos/nfe_dados_st"]
    assert comparacao_nfe_st["camada"] == "bronze"
    assert comparacao_nfe_st["paridade_shape_bruto"]["colunas_iguais"] is False
    assert comparacao_nfe_st["paridade_shape_canonico"]["colunas_iguais"] is True
    assert comparacao_nfe_st["divergencia_residual_justificada"] is not None

    comparacao_c176_xml = relatorio["comparacoes_externas"]["silver/c176_xml"]
    assert comparacao_c176_xml["camada"] == "silver"
    assert comparacao_c176_xml["paridade_shape_bruto"]["colunas_iguais"] is False
    assert comparacao_c176_xml["paridade_shape_canonico"]["colunas_iguais"] is True
    assert comparacao_c176_xml["paridade_shape_canonico"]["schema_igual"] is True


def test_renderiza_relatorio_markdown_com_visoes_bruta_e_canonica(tmp_path: Path) -> None:
    cnpj = "00999999000100"
    base_local = tmp_path / "local"
    base_externa = tmp_path / "externa"

    _escrever_parquet(
        base_local / cnpj / "extraidos" / "c176.parquet",
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
                "qtd_item_saida": 10,
                "vl_total_item": 100.0,
                "cod_mot_res": "1",
                "descricao_motivo_ressarcimento": "RESSARCIMENTO",
                "chave_nfe_ultima_entrada": "ENTRADA123",
                "num_item_ultima_entrada": "1",
                "dt_ultima_entrada": "2024-01-05",
                "vl_unit_bc_st_entrada": 7.0,
                "vl_unit_icms_proprio_entrada": 1.0,
                "vl_unit_ressarcimento_st": 2.0,
                "vl_ressarc_credito_proprio": 10.0,
                "vl_ressarc_st_retido": 20.0,
                "vr_total_ressarcimento": 30.0,
            }
        ],
    )
    _escrever_parquet(
        base_externa / cnpj / "arquivos_parquet" / f"c176_{cnpj}.parquet",
        [
            {
                "periodo_efd": "2024/01",
                "data_entrega_efd_periodo": "2024-02-15",
                "cod_fin_efd": "0",
                "finalidade_efd": "0 - Original",
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
                "descricao_motivo_ressarcimento": "RESSARCIMENTO",
                "chave_nfe_ultima_entrada": "ENTRADA123",
                "c176_num_item_ult_e_declarado": "1",
                "dt_ultima_entrada": "2024-01-05",
                "vl_unit_bc_st_entrada": 7.0,
                "vl_unit_icms_proprio_entrada": 1.0,
                "vl_unit_ressarcimento_st": 2.0,
                "vl_ressarc_credito_proprio": 10.0,
                "vl_ressarc_st_retido": 20.0,
                "vr_total_ressarcimento": 30.0,
            }
        ],
    )

    relatorio = comparar_paridade_st(cnpj, base_local=base_local, base_externa=base_externa)
    markdown = renderizar_relatorio_markdown(relatorio)

    assert f"# Relatorio de Paridade ST - {cnpj}" in markdown
    assert "## Comparacoes Externas" in markdown
    assert "Bruto schema" in markdown
    assert "Canonico schema" in markdown
    assert "Divergencia residual" in markdown
    assert "## Resumo por Camada" in markdown
    assert "## Cadeia Local" in markdown
