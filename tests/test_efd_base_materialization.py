from pathlib import Path

import polars as pl

import backend.services.pipeline_runtime as pipeline_runtime
import transformacao.efd_base as efd_base


def _write_parquet(path: Path, data: dict[str, list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(data).write_parquet(path)


def test_materializar_base_efd_em_camada_canonica(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_cnpj = tmp_path / cnpj

    monkeypatch.setattr(efd_base, "CNPJ_ROOT", tmp_path)

    arquivos_parquet = pasta_cnpj / "arquivos_parquet"

    _write_parquet(
        arquivos_parquet / f"reg_0000_{cnpj}.parquet",
        {
            "id_arquivo": [1, 2],
            "cnpj": [cnpj, cnpj],
            "periodo": ["202401", "202402"],
            "dt_ini": ["2024-01-01", "2024-02-01"],
            "dt_fin": ["2024-01-31", "2024-02-29"],
        },
    )
    _write_parquet(
        arquivos_parquet / f"reg_0190_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "cod_unid": ["UN"],
            "descr": ["UNIDADE"],
        },
    )
    _write_parquet(
        arquivos_parquet / f"reg_0200_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "cod_item": ["ITEM1"],
            "descr_item": ["Produto 1"],
            "unid_inv": ["UN"],
            "cod_ncm": ["12345678"],
        },
    )
    _write_parquet(
        arquivos_parquet / f"reg_0220_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "cod_item": ["ITEM1"],
            "unid_conv": ["CX"],
            "fat_conv": [12.0],
        },
    )
    _write_parquet(
        arquivos_parquet / f"c100_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "chv_nfe": ["CHAVE1"],
            "num_doc": ["100"],
            "dt_doc": ["2024-01-10"],
            "vl_doc": [100.0],
        },
    )
    _write_parquet(
        arquivos_parquet / f"c170_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "chv_nfe": ["CHAVE1"],
            "num_item": [1],
            "cod_item": ["ITEM1"],
            "qtd": [2.0],
            "vl_item": [100.0],
        },
    )
    _write_parquet(
        arquivos_parquet / f"c190_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "chv_nfe": ["CHAVE1"],
            "cfop": ["5102"],
            "cst_icms": ["000"],
            "vl_opr": [100.0],
            "vl_bc_icms": [100.0],
            "vl_icms": [18.0],
        },
    )
    _write_parquet(
        arquivos_parquet / f"c176_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "chv_nfe": ["CHAVE1"],
            "num_item": [1],
            "vl_item": [100.0],
        },
    )
    _write_parquet(
        arquivos_parquet / f"h005_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "dt_inv": ["2024-01-31"],
            "vl_inv": [500.0],
        },
    )
    _write_parquet(
        arquivos_parquet / f"h010_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "dt_inv": ["2024-01-31"],
            "cod_item": ["ITEM1"],
            "qtd": [5.0],
            "vl_item": [100.0],
        },
    )
    _write_parquet(
        arquivos_parquet / f"h020_{cnpj}.parquet",
        {
            "cnpj": [cnpj],
            "periodo": ["202401"],
            "dt_inv": ["2024-01-31"],
            "cod_item": ["ITEM1"],
            "cst_icms": ["060"],
            "bc_icms": [80.0],
        },
    )

    assert efd_base.gerar_base_efd_arquivos_validos(cnpj) is True
    assert efd_base.gerar_base_efd_reg_0190_tipado(cnpj) is True
    assert efd_base.gerar_base_efd_reg_0200_tipado(cnpj) is True
    assert efd_base.gerar_base_efd_reg_0220_tipado(cnpj) is True
    assert efd_base.gerar_base_efd_reg_c100_tipado(cnpj) is True
    assert efd_base.gerar_base_efd_reg_c170_tipado(cnpj) is True
    assert efd_base.gerar_base_efd_reg_c190_tipado(cnpj) is True
    assert efd_base.gerar_base_efd_reg_c176_tipado(cnpj) is True
    assert efd_base.gerar_base_efd_bloco_h_tipado(cnpj) is True

    base_efd = pasta_cnpj / "base" / "efd"
    c170_df = pl.read_parquet(base_efd / "reg_c170_tipado" / "cnpj=12345678000190" / "periodo=202401" / "part-000.parquet")
    c190_df = pl.read_parquet(base_efd / "reg_c190_tipado" / "cnpj=12345678000190" / "periodo=202401" / "part-000.parquet")
    bloco_h_df = pl.read_parquet(base_efd / "bloco_h_tipado" / "cnpj=12345678000190" / "periodo=202401" / "part-000.parquet")

    assert "dataset_id" in c170_df.columns
    assert c170_df["dataset_id"][0] == "base__efd__reg_c170_tipado"
    assert c170_df["num_doc"][0] == "100"
    assert c190_df["cfop"][0] == "5102"
    assert bloco_h_df["cst_icms"][0] == "060"
    assert (base_efd / "reg_c170_tipado" / "_dataset.metadata.json").exists()


def test_pipeline_runtime_expoe_tabelas_base_efd():
    ids = {item["id"] for item in pipeline_runtime.TABELAS_DISPONIVEIS}
    assert "base__efd__arquivos_validos" in ids
    assert "base__efd__reg_c100_tipado" in ids
    assert "base__efd__reg_c170_tipado" in ids
    assert "base__efd__reg_c190_tipado" in ids
    assert "base__efd__bloco_h_tipado" in ids
