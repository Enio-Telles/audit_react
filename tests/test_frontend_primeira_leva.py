from __future__ import annotations

from backend.routers import frontend_primeira_leva as router_module


class _FakeLegacyPage(dict):
    pass


def test_frontend_mov_estoque_wraps_legacy_payload(monkeypatch) -> None:
    def fake_mov_rows(*args, **kwargs):
        return _FakeLegacyPage(
            {
                "page": 1,
                "page_size": 50,
                "total_rows": 1,
                "total_pages": 1,
                "all_columns": ["id_agrupado", "descricao"],
                "rows": [{"id_agrupado": "10", "descricao": "Produto X"}],
                "_provenance": {"dataset_id": "mov_estoque", "resolved": True},
            }
        )

    monkeypatch.setattr(router_module, "mov_rows", fake_mov_rows)

    payload = router_module.frontend_mov_estoque(
        cnpj="12.345.678/0001-99",
        page=1,
        page_size=50,
        sort_by="descricao",
        sort_desc=False,
        filter_text="Produto",
        filter_column=None,
        filter_value=None,
        visible_columns="id_agrupado,descricao",
    )

    assert payload["datasetId"] == "mov_estoque"
    assert payload["blocoFiscal"] == "analise_fiscal"
    assert payload["cnpj"] == "12345678000199"
    assert payload["filters"]["applied"]["filter_text"] == "Produto"
    assert payload["detach"]["view_state"]["visible_columns"] == ["id_agrupado", "descricao"]
    assert payload["rows"][0]["descricao"] == "Produto X"


def test_frontend_nfe_entrada_wraps_document_payload(monkeypatch) -> None:
    def fake_nfe_rows(*args, **kwargs):
        return {
            "page": 1,
            "page_size": 50,
            "total_rows": 1,
            "total_pages": 1,
            "all_columns": ["chave", "descricao", "ncm"],
            "rows": [{"chave": "abc", "descricao": "NF-e", "ncm": "1234"}],
            "_provenance": {"dataset_id": "nfe_entrada", "resolved": True},
        }

    monkeypatch.setattr(router_module, "nfe_rows", fake_nfe_rows)

    payload = router_module.frontend_nfe_entrada(
        cnpj="12345678000199",
        page=1,
        page_size=50,
        sort_by="descricao",
        sort_desc=True,
        filter_text=None,
        filter_column="ncm",
        filter_value="1234",
        visible_columns="chave,descricao",
    )

    assert payload["datasetId"] == "nfe_entrada"
    assert payload["blocoFiscal"] == "documentos_fiscais"
    assert payload["filters"]["applied"]["ncm"] == "1234"
    assert payload["sorting"]["sort_direction"] == "desc"
    assert payload["detach"]["view_state"]["title"] == "NFe Entrada"
    assert payload["columns"][0]["key"] == "chave"
