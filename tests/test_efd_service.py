from pathlib import Path

import polars as pl

from backend.services.efd_service import EfdService


def _write_partition(root: Path, cnpj: str, layer: str, dataset_name: str, data: pl.DataFrame) -> Path:
    target = root / "CNPJ" / cnpj / layer / "efd" / dataset_name
    target.mkdir(parents=True, exist_ok=True)
    data.write_parquet(target / "part-000.parquet")
    return target


def test_manifest_and_read_record_from_raw_dataset(tmp_path: Path):
    cnpj = "12345678000190"
    _write_partition(
        tmp_path,
        cnpj,
        "raw",
        "reg_0190",
        pl.DataFrame(
            {
                "cnpj": [cnpj, cnpj],
                "periodo": ["202401", "202401"],
                "cod_unid": ["UN", "CX"],
                "descr": ["Unidade", "Caixa"],
            }
        ),
    )

    service = EfdService(data_root=tmp_path)
    manifest = service.get_manifest("reg_0190", cnpj=cnpj)
    dataset = service.read_record("reg_0190", cnpj=cnpj, periodo="202401", page=1, page_size=10)

    assert manifest["record"] == "reg_0190"
    assert any(item["dataset_id"] == "raw__efd__reg_0190" for item in manifest["datasets"])
    assert dataset["dataset_id"] == "raw__efd__reg_0190"
    assert dataset["total"] == 2
    assert dataset["records"][0]["cod_unid"] == "UN"


def test_compare_periods_for_c170(tmp_path: Path):
    cnpj = "12345678000190"
    _write_partition(
        tmp_path,
        cnpj,
        "raw",
        "reg_c170",
        pl.DataFrame(
            {
                "cnpj": [cnpj, cnpj, cnpj],
                "periodo": ["202401", "202401", "202402"],
                "chv_nfe": ["A", "B", "B"],
                "num_item": [1, 1, 1],
                "cod_item": ["ITEM_A", "ITEM_B", "ITEM_B"],
            }
        ),
    )

    service = EfdService(data_root=tmp_path)
    payload = service.compare_periods("c170", cnpj=cnpj, periodo_a="202401", periodo_b="202402")

    assert payload["record"] == "c170"
    assert payload["summary"]["count_a"] == 2
    assert payload["summary"]["count_b"] == 1
    assert payload["summary"]["added"] == 0
    assert payload["summary"]["removed"] == 1
    assert payload["sample"]["removed_keys"] == ["A"]


def test_build_document_tree_and_row_provenance(tmp_path: Path):
    cnpj = "12345678000190"
    _write_partition(
        tmp_path,
        cnpj,
        "raw",
        "reg_c100",
        pl.DataFrame(
            {
                "cnpj": [cnpj],
                "periodo": ["202401"],
                "chv_nfe": ["NFE123"],
                "num_doc": ["0001"],
                "vl_doc": [100.0],
            }
        ),
    )
    _write_partition(
        tmp_path,
        cnpj,
        "raw",
        "reg_c170",
        pl.DataFrame(
            {
                "cnpj": [cnpj],
                "periodo": ["202401"],
                "chv_nfe": ["NFE123"],
                "num_item": [1],
                "cod_item": ["ITEM_1"],
                "vl_item": [100.0],
            }
        ),
    )
    _write_partition(
        tmp_path,
        cnpj,
        "raw",
        "reg_c190",
        pl.DataFrame(
            {
                "cnpj": [cnpj],
                "periodo": ["202401"],
                "chv_nfe": ["NFE123"],
                "cfop": ["5102"],
                "vl_opr": [100.0],
            }
        ),
    )
    _write_partition(
        tmp_path,
        cnpj,
        "raw",
        "reg_c176",
        pl.DataFrame(
            {
                "cnpj": [cnpj],
                "periodo": ["202401"],
                "chv_nfe": ["NFE123"],
                "num_item": [1],
                "vl_ult_e": [90.0],
            }
        ),
    )
    _write_partition(
        tmp_path,
        cnpj,
        "raw",
        "reg_c197",
        pl.DataFrame(
            {
                "cnpj": [cnpj],
                "periodo": ["202401"],
                "chv_nfe": ["NFE123"],
                "cod_aj": ["AJ001"],
                "descr_compl_aj": ["Ajuste teste"],
            }
        ),
    )

    service = EfdService(data_root=tmp_path)
    tree = service.build_document_tree(cnpj=cnpj, periodo="202401")
    provenance = service.row_provenance("c100", cnpj=cnpj, row_identifier="NFE123")

    assert tree["doc_key"] == "chv_nfe"
    assert len(tree["documents"]) == 1
    assert tree["documents"][0]["document"]["chv_nfe"] == "NFE123"
    assert len(tree["documents"][0]["items_c170"]) == 1
    assert len(tree["documents"][0]["summary_c190"]) == 1
    assert len(tree["documents"][0]["links_c176"]) == 1
    assert len(tree["documents"][0]["adjustments_c197"]) == 1
    assert provenance["record"] == "c100"
    assert provenance["row"]["chv_nfe"] == "NFE123"
