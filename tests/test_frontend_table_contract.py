from __future__ import annotations

from backend.routers.frontend_table_contract import build_table_payload


def test_build_table_payload_includes_detach_and_columns_metadata() -> None:
    legacy_page = {
        "page": 2,
        "page_size": 25,
        "total_rows": 3,
        "total_pages": 1,
        "all_columns": ["id_agrupado", "descricao", "valor_total"],
        "rows": [
            {"id_agrupado": "1", "descricao": "Produto A", "valor_total": 10.5},
            {"id_agrupado": "2", "descricao": "Produto B", "valor_total": 12.0},
        ],
        "_provenance": {"dataset_id": "mov_estoque", "resolved": True},
    }

    payload = build_table_payload(
        dataset_id="mov_estoque",
        bloco_fiscal="analise_fiscal",
        cnpj="12345678000199",
        title="Movimentação de estoque",
        legacy_page=legacy_page,
        filters_applied={"descricao": "Produto"},
        filters_supported={"descricao": {"type": "text"}},
        sort_by="descricao",
        sort_direction="asc",
        visible_columns=["id_agrupado", "descricao"],
    )

    assert payload["datasetId"] == "mov_estoque"
    assert payload["blocoFiscal"] == "analise_fiscal"
    assert payload["cnpj"] == "12345678000199"
    assert payload["pagination"]["page"] == 2
    assert payload["sorting"]["sort_by"] == "descricao"
    assert payload["detach"]["enabled"] is True
    assert payload["detach"]["view_state"]["datasetId"] == "mov_estoque"
    assert payload["detach"]["view_state"]["visible_columns"] == ["id_agrupado", "descricao"]
    assert payload["meta"]["provenance"]["dataset_id"] == "mov_estoque"

    columns = {column["key"]: column for column in payload["columns"]}
    assert columns["id_agrupado"]["visible"] is True
    assert columns["descricao"]["visible"] is True
    assert columns["valor_total"]["visible"] is False
    assert columns["valor_total"]["type"] == "number"


def test_build_table_payload_uses_all_columns_when_visible_columns_missing() -> None:
    payload = build_table_payload(
        dataset_id="nfe_entrada",
        bloco_fiscal="documentos_fiscais",
        cnpj="12345678000199",
        title="NFe Entrada",
        legacy_page={
            "page": 1,
            "page_size": 50,
            "total_rows": 1,
            "total_pages": 1,
            "all_columns": ["chave", "descricao"],
            "rows": [{"chave": "abc", "descricao": "Doc"}],
        },
        filters_applied={},
        filters_supported={},
        sort_by=None,
        sort_direction="asc",
        visible_columns=None,
    )

    visible_columns = payload["detach"]["view_state"]["visible_columns"]
    assert visible_columns == ["chave", "descricao"]
    assert all(column["visible"] is True for column in payload["columns"])
