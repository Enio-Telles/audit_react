from pathlib import Path
import asyncio
import json
import os
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


def test_montar_resumo_secao_estoque_reaproveita_mov_estoque_materializada(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    caminho_mov = cnpj_root_teste / "12345678000190" / "analises" / "produtos" / "mov_estoque_12345678000190.parquet"
    salvar_parquet_teste(caminho_mov, 4)

    status, quantidade_linhas, data_atualizacao = dossie_router.montar_resumo_secao("estoque", "12345678000190")

    assert status == "cached"
    assert quantidade_linhas == 4
    assert data_atualizacao is not None

    secoes = dossie_router.get_secoes("12345678000190")
    secao_estoque = next(secao for secao in secoes if secao.id == "estoque")
    assert secao_estoque.sourceFiles is not None
    assert str(caminho_mov) in secao_estoque.sourceFiles


def test_get_dados_secao_ressarcimento_st_prioriza_item_materializado(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    caminho_item = (
        cnpj_root_teste
        / "12345678000190"
        / "analises"
        / "ressarcimento_st"
        / "ressarcimento_st_item_12345678000190.parquet"
    )
    caminho_item.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"chave": ["abc"], "valor": [10]}).write_parquet(caminho_item)

    resposta = dossie_router.get_dados_secao("12345678000190", "ressarcimento_st", limite=100)

    assert resposta.id == "ressarcimento_st"
    assert resposta.rowCount == 1
    assert resposta.rows[0]["chave"] == "abc"
    assert resposta.metadata is not None
    assert resposta.metadata["origem_dado"] == "cache_catalog"
    assert str(caminho_item) in resposta.metadata["arquivos_origem_considerados"]


def test_obter_arquivos_por_secao_prioriza_cache_canonico_mais_recente_com_parametros(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    base_dossie = cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "dossie"
    base_dossie.mkdir(parents=True, exist_ok=True)

    caminho_padrao = base_dossie / "dossie_12345678000190_contato_padrao.parquet"
    caminho_consolidado = base_dossie / "dossie_12345678000190_contato_consolidado.parquet"
    salvar_parquet_teste(caminho_padrao, 1)
    salvar_parquet_teste(caminho_consolidado, 2)
    stat_padrao = caminho_padrao.stat()
    os.utime(caminho_consolidado, (stat_padrao.st_atime + 10, stat_padrao.st_mtime + 10))

    class ResolucaoFake:
        cache_file_name = caminho_padrao.name

    monkeypatch.setattr(dossie_router, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    arquivos = dossie_router.obter_arquivos_por_secao("contato", "12345678000190")

    assert arquivos == [caminho_consolidado]


def test_montar_resumo_secao_prioriza_cache_canonico_sem_somar_artefatos_legados(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    base_cnpj = cnpj_root_teste / "12345678000190" / "arquivos_parquet"
    salvar_parquet_teste(base_cnpj / "nfe_agr_12345678000190.parquet", 7)
    salvar_parquet_teste(base_cnpj / "NFCe_12345678000190.parquet", 3)

    base_dossie = base_cnpj / "dossie"
    caminho_canonico = base_dossie / "dossie_12345678000190_documentos_fiscais_teste.parquet"
    salvar_parquet_teste(caminho_canonico, 5)

    class ResolucaoFake:
        cache_file_name = caminho_canonico.name

    monkeypatch.setattr(dossie_router, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    status, quantidade_linhas, _ = dossie_router.montar_resumo_secao(
        "documentos_fiscais",
        "12345678000190",
    )

    assert status == "cached"
    assert quantidade_linhas == 5


def test_get_secoes_retorna_idle_quando_nao_ha_artefatos_materializados(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    secoes = dossie_router.get_secoes("12345678000190")

    assert len(secoes) == len(listar_secoes_dossie())
    assert all(secao.status == "idle" for secao in secoes)
    assert all(secao.rowCount is None for secao in secoes)
    secao_cadastro = next(secao for secao in secoes if secao.id == "cadastro")
    secao_estoque = next(secao for secao in secoes if secao.id == "estoque")
    assert secao_cadastro.syncEnabled is True
    assert secao_estoque.syncEnabled is False


def test_get_dados_secao_retorna_cache_canonico_materializado(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    base_cnpj = cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "dossie"
    caminho_arquivo = base_cnpj / "dossie_12345678000190_contato_teste.parquet"
    caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "tipo_vinculo": ["EMPRESA_PRINCIPAL"],
            "nome_referencia": ["Empresa Teste"],
            "telefone": ["62999990000"],
        }
    ).write_parquet(caminho_arquivo)
    caminho_arquivo.with_suffix(".metadata.json").write_text(
        json.dumps(
            {
                "estrategia_execucao": "sql_consolidado",
                "sql_principal": "dossie_contato.sql",
                "sql_ids_reutilizados": ["dossie_contato.sql"],
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    class ResolucaoFake:
        cache_file_name = caminho_arquivo.name

    monkeypatch.setattr(dossie_router, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    resposta = dossie_router.get_dados_secao("12.345.678/0001-90", "contato", limite=100)

    assert resposta.id == "contato"
    assert resposta.rowCount == 1
    assert resposta.columns == ["tipo_vinculo", "nome_referencia", "telefone"]
    assert resposta.rows[0]["nome_referencia"] == "Empresa Teste"
    assert resposta.metadata is not None
    assert resposta.metadata["estrategia_execucao"] == "sql_consolidado"
    assert resposta.metadata["sql_principal"] == "dossie_contato.sql"


def test_get_secoes_expoe_estrategia_e_sql_principal_do_cache_canonico(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    base_dossie = cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "dossie"
    caminho_arquivo = base_dossie / "dossie_12345678000190_contato_resumo.parquet"
    salvar_parquet_teste(caminho_arquivo, 3)
    caminho_arquivo.with_suffix(".metadata.json").write_text(
        json.dumps(
            {
                "estrategia_execucao": "sql_consolidado",
                "sql_principal": "dossie_contato.sql",
                "comparacao_estrategia_alternativa": {
                    "convergencia_funcional": False,
                    "convergencia_basica": True,
                    "quantidade_chaves_faltantes": 2,
                    "quantidade_chaves_extras": 1,
                },
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    class ResolucaoFake:
        cache_file_name = caminho_arquivo.name

    monkeypatch.setattr(dossie_router, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    secoes = dossie_router.get_secoes("12345678000190")
    secao_contato = next(secao for secao in secoes if secao.id == "contato")

    assert secao_contato.status == "cached"
    assert secao_contato.rowCount == 3
    assert secao_contato.executionStrategy == "sql_consolidado"
    assert secao_contato.primarySql == "dossie_contato.sql"
    assert secao_contato.syncEnabled is True
    assert secao_contato.alternateStrategyComparison == "divergencia_funcional"
    assert secao_contato.alternateStrategyMissingKeys == 2
    assert secao_contato.alternateStrategyExtraKeys == 1


def test_get_dados_secao_falha_quando_cache_nao_existe(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    try:
        dossie_router.get_dados_secao("12345678000190", "contato", limite=100)
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 404
        assert "Nenhum cache materializado" in str(exc.detail)
    else:
        raise AssertionError("Era esperado erro 404 quando a secao nao possui cache materializado.")


def test_post_sync_secao_retorna_erro_tratavel_quando_executor_falha(monkeypatch):
    async def executar_sync_fake(**kwargs):
        raise RuntimeError("Oracle indisponivel")

    monkeypatch.setattr(dossie_router, "secao_permite_sincronizacao", lambda secao_id: True)
    original = sys.modules["interface_grafica.services.dossie_extraction_service"] if "interface_grafica.services.dossie_extraction_service" in sys.modules else None

    class ModuloFake:
        @staticmethod
        async def executar_sync_secao(**kwargs):
            return await executar_sync_fake(**kwargs)

    sys.modules["interface_grafica.services.dossie_extraction_service"] = ModuloFake()
    try:
        try:
            asyncio.run(dossie_router.post_sync_secao("12345678000190", "contato"))
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 502
            assert "Falha operacional ao sincronizar secao do dossie" in str(exc.detail)
        else:
            raise AssertionError("Era esperado erro HTTP tratavel quando o executor de sync falha.")
    finally:
        if original is not None:
            sys.modules["interface_grafica.services.dossie_extraction_service"] = original
        else:
            sys.modules.pop("interface_grafica.services.dossie_extraction_service", None)


def test_get_historico_comparacoes_contato_retorna_itens_mais_recentes(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    caminho_historico = (
        cnpj_root_teste
        / "12345678000190"
        / "arquivos_parquet"
        / "dossie"
        / "historico_comparacao_contato_12345678000190.jsonl"
    )
    caminho_historico.parent.mkdir(parents=True, exist_ok=True)
    caminho_historico.write_text(
        "\n".join(
            [
                json.dumps({"cache_key": "a1", "comparacao_estrategia_alternativa": {"convergencia_funcional": True}}, ensure_ascii=True),
                json.dumps({"cache_key": "a2", "comparacao_estrategia_alternativa": {"convergencia_funcional": False}}, ensure_ascii=True),
                json.dumps({"cache_key": "a3", "comparacao_estrategia_alternativa": {"convergencia_funcional": True}}, ensure_ascii=True),
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        dossie_router,
        "obter_caminho_historico_comparacao_contato",
        lambda cnpj: caminho_historico,
    )

    resposta = dossie_router.get_historico_comparacoes_contato("12.345.678/0001-90", limite=2)

    assert resposta.cnpj == "12345678000190"
    assert resposta.secaoId == "contato"
    assert resposta.historyFile.endswith("historico_comparacao_contato_12345678000190.jsonl")
    assert [item["cache_key"] for item in resposta.items] == ["a3", "a2"]


def test_get_resumo_comparacoes_contato_consolida_historico(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    caminho_historico = (
        cnpj_root_teste
        / "12345678000190"
        / "arquivos_parquet"
        / "dossie"
        / "historico_comparacao_contato_12345678000190.jsonl"
    )
    caminho_historico.parent.mkdir(parents=True, exist_ok=True)
    caminho_historico.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "cache_key": "a1",
                        "estrategia_execucao": "composicao_polars",
                        "sql_principal": "dados_cadastrais.sql",
                        "comparacao_estrategia_alternativa": {"convergencia_funcional": True},
                    },
                    ensure_ascii=True,
                ),
                json.dumps(
                    {
                        "cache_key": "a2",
                        "estrategia_execucao": "sql_consolidado",
                        "sql_principal": "dossie_contato.sql",
                        "comparacao_estrategia_alternativa": {"convergencia_funcional": False},
                    },
                    ensure_ascii=True,
                ),
                json.dumps(
                    {
                        "cache_key": "a3",
                        "estrategia_execucao": "sql_consolidado",
                        "sql_principal": "dossie_contato.sql",
                        "comparacao_estrategia_alternativa": {"convergencia_basica": True},
                    },
                    ensure_ascii=True,
                ),
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        dossie_router,
        "obter_caminho_historico_comparacao_contato",
        lambda cnpj: caminho_historico,
    )

    resposta = dossie_router.get_resumo_comparacoes_contato("12.345.678/0001-90")

    assert resposta.cnpj == "12345678000190"
    assert resposta.secaoId == "contato"
    assert resposta.totalComparacoes == 3
    assert resposta.convergenciasFuncionais == 1
    assert resposta.divergenciasFuncionais == 1
    assert resposta.convergenciasBasicas == 1
    assert resposta.divergenciasBasicas == 0
    assert resposta.ultimaEstrategia == "sql_consolidado"
    assert resposta.ultimaSqlPrincipal == "dossie_contato.sql"
    assert resposta.ultimoStatusComparacao == "convergencia_basica"
    assert resposta.ultimoCacheKey == "a3"
    assert resposta.historyFile.endswith("historico_comparacao_contato_12345678000190.jsonl")


def test_post_relatorio_comparacoes_contato_materializa_markdown(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dossie_router, "CNPJ_ROOT", cnpj_root_teste)

    caminho_historico = (
        cnpj_root_teste
        / "12345678000190"
        / "arquivos_parquet"
        / "dossie"
        / "historico_comparacao_contato_12345678000190.jsonl"
    )
    caminho_historico.parent.mkdir(parents=True, exist_ok=True)
    caminho_historico.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "cache_key": "a2",
                        "estrategia_execucao": "sql_consolidado",
                        "sql_principal": "dossie_contato.sql",
                        "comparacao_estrategia_alternativa": {"convergencia_funcional": False},
                    },
                    ensure_ascii=True,
                )
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        dossie_router,
        "obter_caminho_historico_comparacao_contato",
        lambda cnpj: caminho_historico,
    )

    resposta = dossie_router.post_relatorio_comparacoes_contato("12.345.678/0001-90")

    caminho_relatorio = Path(resposta.reportFile)
    assert resposta.cnpj == "12345678000190"
    assert resposta.secaoId == "contato"
    assert caminho_relatorio.exists()
    assert "Relatorio de Comparacao da Secao Contato - 12345678000190" in resposta.content
    assert "dossie_contato.sql" in resposta.content
    assert "divergencias funcionais" in resposta.content.lower()
    assert resposta.updatedAt is not None


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


def test_post_sync_secao_rejeita_secao_apenas_cache(monkeypatch):
    async def executar_sync_secao_fake(*args, **kwargs):
        raise AssertionError("O backend nao deveria tentar sincronizar secoes sem SQL mapeada.")

    modulo_fake = type(
        "ModuloExtracaoFake",
        (),
        {"executar_sync_secao": staticmethod(executar_sync_secao_fake)},
    )
    monkeypatch.setitem(sys.modules, "interface_grafica.services.dossie_extraction_service", modulo_fake)

    try:
        asyncio.run(dossie_router.post_sync_secao("12.345.678/0001-90", "estoque"))
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 400
        assert "leitura de cache" in str(exc.detail)
        assert "sincronizacao Oracle ativa" in str(exc.detail)
    else:
        raise AssertionError("Era esperado erro 400 para secao que opera apenas por cache.")


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
