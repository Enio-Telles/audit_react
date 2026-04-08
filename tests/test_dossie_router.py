from pathlib import Path
import asyncio
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))
sys.path.insert(0, str(Path("backend").resolve()))

from interface_grafica.services.dossie_catalog import listar_secoes_dossie
from routers import dossie as dossie_router


def salvar_parquet_teste(caminho_arquivo: Path, quantidade_linhas: int) -> None:
    """Cria um parquet minimo para validar o resumo do dossie sem depender de dados reais."""

    caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"linha": list(range(quantidade_linhas))}).write_parquet(caminho_arquivo)


def test_montar_resumo_secao_cadastro_reconhece_cache_existente(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    salvar_parquet_teste(
        cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "dados_cadastrais_12345678000190.parquet",
        1,
    )

    status, quantidade_linhas, data_atualizacao = dossie_router.montar_resumo_secao(
        "cadastro",
        "12345678000190",
    )

    assert status == "cached"
    assert quantidade_linhas == 1
    assert data_atualizacao is not None


def test_montar_resumo_documentos_prioriza_arquivos_agregados_sem_duplicar(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    base_cnpj = cnpj_root_teste / "12345678000190" / "arquivos_parquet"
    salvar_parquet_teste(base_cnpj / "nfe_agr_12345678000190.parquet", 7)
    salvar_parquet_teste(base_cnpj / "NFe_12345678000190.parquet", 99)
    salvar_parquet_teste(base_cnpj / "NFCe_12345678000190.parquet", 3)

    status, quantidade_linhas, _ = dossie_router.montar_resumo_secao(
        "documentos_fiscais",
        "12345678000190",
    )

    assert status == "cached"
    assert quantidade_linhas == 10


def test_get_secoes_retorna_idle_quando_nao_ha_artefatos_materializados(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    secoes = dossie_router.get_secoes("12345678000190")

    assert len(secoes) == len(listar_secoes_dossie())
    assert all(secao.status == "idle" for secao in secoes)
    assert all(secao.rowCount is None for secao in secoes)


def test_post_sync_secao_normaliza_cnpj_e_repassa_parametros(monkeypatch):
    chamadas: list[dict] = []

    async def executar_sync_secao_fake(cnpj: str, secao_id: str, parametros: dict | None = None):
        chamadas.append(
            {
                "cnpj": cnpj,
                "secao_id": secao_id,
                "parametros": parametros,
            }
        )
        return {"status": "success", "cnpj": cnpj, "secao_id": secao_id}

    modulo_fake = type(
        "ModuloExtracaoFake",
        (),
        {"executar_sync_secao": staticmethod(executar_sync_secao_fake)},
    )
    monkeypatch.setitem(sys.modules, "interface_grafica.services.dossie_extraction_service", modulo_fake)

    resposta = asyncio.run(
        dossie_router.post_sync_secao(
            "12.345.678/0001-90",
            "cadastro",
            dossie_router.SyncDossieRequest(parametros={"UF": "TO"}),
        )
    )

    assert resposta["status"] == "success"
    assert chamadas == [
        {
            "cnpj": "12345678000190",
            "secao_id": "cadastro",
            "parametros": {"UF": "TO"},
        }
    ]


def test_post_sync_secao_legado_reutiliza_mesma_regra(monkeypatch):
    chamadas: list[dict] = []

    async def sincronizar_secao_fake(cnpj: str, secao_id: str, payload=None):
        chamadas.append({"cnpj": cnpj, "secao_id": secao_id, "payload": payload})
        return {"status": "success"}

    monkeypatch.setattr(dossie_router, "sincronizar_secao_dossie", sincronizar_secao_fake)

    resposta = asyncio.run(
        dossie_router.post_sync_secao_legado(
            "12.345.678/0001-90",
            "socios",
            dossie_router.SyncDossieRequest(parametros={"limite": 50}),
        )
    )

    assert resposta == {"status": "success"}
    assert len(chamadas) == 1
    assert chamadas[0]["cnpj"] == "12.345.678/0001-90"
    assert chamadas[0]["secao_id"] == "socios"
    assert chamadas[0]["payload"].parametros == {"limite": 50}
