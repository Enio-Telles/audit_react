from pathlib import Path
import json
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services import dossie_extraction_service as extraction_service
from interface_grafica.services import dossie_dataset_reuse as dataset_reuse


def test_extrair_chaves_funcionais_contato_agrega_fontes_por_entidade():
    dataframe_polars = pl.DataFrame(
        {
            "tipo_vinculo": ["EMPRESA_PRINCIPAL", "EMPRESA_PRINCIPAL", "SOCIO_ANTIGO", "SOCIO_ANTIGO"],
            "cpf_cnpj_referencia": ["12345678000190", "12345678000190", "11111111111", "11111111111"],
            "nome_referencia": ["Empresa Base", "Empresa Base", "Socio Antigo", "Socio Antigo"],
            "crc_contador": [None, None, None, None],
            "telefone": ["62999990000", None, None, "6233334444"],
            "telefone_nfe_nfce": [None, "62999990000", None, None],
            "email": [None, "contato@empresa.com", None, "socio.antigo@teste.com"],
            "endereco": ["Rua A", "Rua A", "Av B", None],
            "situacao_cadastral": [None, None, "BAIXADO", "BAIXADO"],
            "tabela_origem": ["BI.DM_PESSOA", "FAC", "SITAFE_SOCIO", "SITAFE_PESSOA"],
        }
    )
    dataframe_sql = pl.DataFrame(
        {
            "tipo_vinculo": ["EMPRESA_PRINCIPAL", "SOCIO_ANTIGO"],
            "cpf_cnpj_referencia": ["12345678000190", "11111111111"],
            "nome_referencia": ["Empresa Base", "Socio Antigo"],
            "crc_contador": [None, None],
            "telefone": ["62999990000", "6233334444"],
            "telefone_nfe_nfce": ["62999990000", None],
            "email": ["contato@empresa.com", "socio.antigo@teste.com"],
            "endereco": ["Rua A", "Av B"],
            "situacao_cadastral": [None, "BAIXADO"],
            "tabela_origem": ["dossie_contato.sql", "dossie_contato.sql"],
        }
    )

    chaves_polars = extraction_service._extrair_chaves_funcionais_contato(dataframe_polars)
    chaves_sql = extraction_service._extrair_chaves_funcionais_contato(dataframe_sql)

    assert chaves_polars == chaves_sql
    assert len(chaves_polars) == 2


def test_executar_sync_secao_reutiliza_datasets_e_compacta_multiplas_fontes(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(extraction_service, "CNPJ_ROOT", cnpj_root_teste)

    class ResolucaoFake:
        secao_id = "contato"
        cache_key = "abc123"
        cache_file_name = "dossie_12345678000190_contato_abc123.parquet"
        sql_ids = (
            "dados_cadastrais.sql",
            "dossie_contador.sql",
            "dossie_historico_fac.sql",
            "dossie_rascunho_fac_contador.sql",
            "dossie_req_inscricao_contador.sql",
            "dossie_historico_socios.sql",
        )

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    datasets_reutilizados = {
        "dados_cadastrais.sql": pl.DataFrame({"CNPJ": ["12345678000190"], "Nome": ["Empresa Base"]}),
        "dossie_contador.sql": pl.DataFrame({"nome": ["Contador Base"]}),
        "dossie_historico_fac.sql": pl.DataFrame({"no_contador": ["Contador FAC"], "cpf_contador": ["99999999000199"]}),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame({"no_contador": ["Contador Rascunho"], "cpf_contador": ["99999999000199"]}),
        "dossie_req_inscricao_contador.sql": pl.DataFrame({"no_contador": ["Contador Requerimento"], "cpf_contador": ["99999999000199"]}),
    }

    def carregar_dataset_fake(cnpj: str, sql_id: str):
        if sql_id not in datasets_reutilizados:
            return None
        return extraction_service.DatasetCompartilhadoDossie(
            sql_id=sql_id,
            dataframe=datasets_reutilizados[sql_id],
            caminho_origem=tmp_path / f"{sql_id}.parquet",
            reutilizado=True,
        )

    monkeypatch.setattr(extraction_service, "carregar_dataset_reutilizavel", carregar_dataset_fake)
    monkeypatch.setattr(extraction_service, "salvar_dataset_compartilhado", lambda *args, **kwargs: tmp_path / "shared.parquet")
    monkeypatch.setattr(extraction_service.SqlService, "read_sql", staticmethod(lambda sql_id: f"select * from {sql_id}"))
    monkeypatch.setattr(
        extraction_service.SqlService,
        "executar_sql",
        staticmethod(lambda *args, **kwargs: [{"nome": "Socio Base", "situacao": "SÓCIO ATUAL"}]),
    )
    monkeypatch.setattr(
        extraction_service,
        "compor_secao_dossie",
        lambda **kwargs: pl.DataFrame({"linha": [1, 2, 3]}),
    )

    resultado = extraction_service.executar_sync_secao_sync("12345678000190", "contato")

    assert resultado["status"] == "success"
    assert resultado["estrategia_execucao"] == "composicao_polars"
    assert resultado["sql_principal"] == "dados_cadastrais.sql"
    assert resultado["sql_ids_executados"] == ["dossie_historico_socios.sql"]
    assert resultado["sql_ids_reutilizados"] == [
        "dados_cadastrais.sql",
        "dossie_contador.sql",
        "dossie_historico_fac.sql",
        "dossie_rascunho_fac_contador.sql",
        "dossie_req_inscricao_contador.sql",
    ]
    assert resultado["linhas_extraidas"] == 3
    assert resultado["total_sql_ids"] == 6
    assert resultado["percentual_reuso_sql"] == 83.33
    assert resultado["impacto_cache_first"] == "reuso_parcial"
    assert isinstance(resultado["tempo_materializacao_ms"], int)
    assert isinstance(resultado["tempo_total_sync_ms"], int)

    caminho_cache = cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "dossie" / ResolucaoFake.cache_file_name
    assert caminho_cache.exists()
    caminho_metadata = caminho_cache.with_suffix(".metadata.json")
    assert caminho_metadata.exists()
    metadata_secao = json.loads(caminho_metadata.read_text(encoding="utf-8"))
    assert metadata_secao["estrategia_execucao"] == "composicao_polars"
    assert metadata_secao["sql_ids_executados"] == ["dossie_historico_socios.sql"]
    assert metadata_secao["total_sql_ids"] == 6
    assert metadata_secao["percentual_reuso_sql"] == 83.33
    assert metadata_secao["impacto_cache_first"] == "reuso_parcial"
    assert isinstance(metadata_secao["tempo_materializacao_ms"], int)
    assert isinstance(metadata_secao["tempo_total_sync_ms"], int)

    parametros_salvos = {}

    def salvar_dataset_fake(cnpj: str, sql_id: str, dataframe: pl.DataFrame, metadata=None):
        parametros_salvos["metadata"] = metadata
        return tmp_path / "shared.parquet"

    monkeypatch.setattr(extraction_service, "salvar_dataset_compartilhado", salvar_dataset_fake)

    extraction_service.executar_sync_secao_sync("12345678000190", "contato", parametros={"UF": "TO"})
    assert parametros_salvos["metadata"]["sql_id"] == "dossie_historico_socios.sql"
    assert parametros_salvos["metadata"]["parametros"]["CNPJ"] == "12345678000190"
    assert parametros_salvos["metadata"]["parametros"]["UF"] == "TO"


def test_executar_sync_secao_repassa_parametros_para_reuso_da_sql(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(extraction_service, "CNPJ_ROOT", cnpj_root_teste)

    class ResolucaoFake:
        secao_id = "enderecos"
        cache_key = "parametros_reuso"
        cache_file_name = "dossie_12345678000190_enderecos_parametros_reuso.parquet"
        sql_ids = ("fronteira.sql",)

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    chamadas: list[dict] = []

    def carregar_dataset_fake(cnpj: str, sql_id: str, parametros=None):
        chamadas.append({"cnpj": cnpj, "sql_id": sql_id, "parametros": parametros})
        return extraction_service.DatasetCompartilhadoDossie(
            sql_id=sql_id,
            dataframe=pl.DataFrame({"linha": [1]}),
            caminho_origem=tmp_path / "fronteira.parquet",
            reutilizado=True,
        )

    monkeypatch.setattr(extraction_service, "carregar_dataset_reutilizavel", carregar_dataset_fake)
    monkeypatch.setattr(extraction_service, "compor_secao_dossie", lambda **kwargs: pl.DataFrame({"linha": [1]}))

    resultado = extraction_service.executar_sync_secao_sync(
        "12345678000190",
        "enderecos",
        parametros={"data_limite_processamento": "31/03/2024"},
    )

    assert resultado["status"] == "success"
    assert chamadas == [
        {
            "cnpj": "12345678000190",
            "sql_id": "fronteira.sql",
            "parametros": {"data_limite_processamento": "31/03/2024"},
        }
    ]


def test_executar_sync_secao_reaproveita_todas_as_fontes_sem_consultar_oracle(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(extraction_service, "CNPJ_ROOT", cnpj_root_teste)

    class ResolucaoFake:
        secao_id = "contato"
        cache_key = "somente_reuso"
        cache_file_name = "dossie_12345678000190_contato_somente_reuso.parquet"
        sql_ids = (
            "dados_cadastrais.sql",
            "dossie_contador.sql",
        )

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    def carregar_dataset_fake(cnpj: str, sql_id: str):
        dataframes = {
            "dados_cadastrais.sql": pl.DataFrame({"CNPJ": ["12345678000190"], "Nome": ["Empresa Base"]}),
            "dossie_contador.sql": pl.DataFrame({"nome": ["Contador Base"]}),
        }
        dataframe = dataframes.get(sql_id)
        if dataframe is None:
            return None
        return extraction_service.DatasetCompartilhadoDossie(
            sql_id=sql_id,
            dataframe=dataframe,
            caminho_origem=tmp_path / f"{sql_id}.parquet",
            reutilizado=True,
        )

    monkeypatch.setattr(extraction_service, "carregar_dataset_reutilizavel", carregar_dataset_fake)
    monkeypatch.setattr(
        extraction_service,
        "compor_secao_dossie",
        lambda **kwargs: pl.DataFrame({"linha": [1]}),
    )

    def executar_sql_invalido(*args, **kwargs):
        raise AssertionError("Nao era esperado executar Oracle quando todos os datasets foram reutilizados.")

    monkeypatch.setattr(extraction_service.SqlService, "executar_sql", staticmethod(executar_sql_invalido))

    resultado = extraction_service.executar_sync_secao_sync("12345678000190", "contato")

    assert resultado["status"] == "success"
    assert resultado["sql_ids_executados"] == []
    assert resultado["sql_ids_reutilizados"] == ["dados_cadastrais.sql", "dossie_contador.sql"]
    assert resultado["total_sql_ids"] == 2
    assert resultado["percentual_reuso_sql"] == 100.0
    assert resultado["impacto_cache_first"] == "reuso_total"


def test_executar_sync_secao_falha_para_secao_sem_sqls(monkeypatch):
    class ResolucaoFake:
        secao_id = "inexistente"
        cache_key = "sem_sql"
        cache_file_name = "dossie_12345678000190_inexistente_sem_sql.parquet"
        sql_ids = ()

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    try:
        extraction_service.executar_sync_secao_sync("12345678000190", "inexistente")
    except ValueError as exc:
        assert "Nenhum SQL mapeado" in str(exc)
    else:
        raise AssertionError("Era esperado erro quando a secao nao possui SQLs mapeados.")


def test_executar_sync_secao_falha_quando_persistencia_parquet_retorna_false(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(extraction_service, "CNPJ_ROOT", cnpj_root_teste)

    class ResolucaoFake:
        secao_id = "contato"
        cache_key = "erro_persistencia"
        cache_file_name = "dossie_12345678000190_contato_erro_persistencia.parquet"
        sql_ids = ("dados_cadastrais.sql",)

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())
    monkeypatch.setattr(
        extraction_service,
        "carregar_dataset_reutilizavel",
        lambda *args, **kwargs: extraction_service.DatasetCompartilhadoDossie(
            sql_id="dados_cadastrais.sql",
            dataframe=pl.DataFrame({"cnpj": ["12345678000190"]}),
            caminho_origem=tmp_path / "dados_cadastrais.parquet",
            reutilizado=True,
        ),
    )
    monkeypatch.setattr(
        extraction_service,
        "compor_secao_dossie",
        lambda **kwargs: pl.DataFrame({"linha": [1]}),
    )
    monkeypatch.setattr(extraction_service, "salvar_para_parquet", lambda *args, **kwargs: False)

    try:
        extraction_service.executar_sync_secao_sync("12345678000190", "contato")
    except RuntimeError as exc:
        assert "Erro ao persistir" in str(exc)
    else:
        raise AssertionError("Era esperado erro quando a persistencia do parquet falha.")


def test_executar_sync_secao_contato_pode_forcar_sql_consolidado(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(extraction_service, "CNPJ_ROOT", cnpj_root_teste)

    class ResolucaoFake:
        secao_id = "contato"
        cache_key = "modo_consolidado"
        cache_file_name = "dossie_12345678000190_contato_modo_consolidado.parquet"
        sql_ids = (
            "dados_cadastrais.sql",
            "dossie_contador.sql",
            "dossie_historico_fac.sql",
        )

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    sqls_solicitados: list[str] = []

    def carregar_dataset_fake(cnpj: str, sql_id: str):
        sqls_solicitados.append(sql_id)
        if sql_id != "dossie_contato.sql":
            return None
        return extraction_service.DatasetCompartilhadoDossie(
            sql_id=sql_id,
            dataframe=pl.DataFrame(
                {
                    "tipo_vinculo": ["EMPRESA_PRINCIPAL"],
                    "cnpj_consultado": ["12345678000190"],
                    "cnpj_raiz": ["12345678"],
                    "cpf_cnpj_referencia": ["12345678000190"],
                    "nome_referencia": ["Empresa Consolidada"],
                    "origem_dado": ["dossie_contato.sql"],
                    "tabela_origem": ["BI.DM_PESSOA"],
                    "ordem_exibicao": [10],
                }
            ),
            caminho_origem=tmp_path / "dossie_contato.parquet",
            reutilizado=True,
        )

    monkeypatch.setattr(extraction_service, "carregar_dataset_reutilizavel", carregar_dataset_fake)
    monkeypatch.setattr(
        extraction_service,
        "compor_secao_dossie",
        lambda **kwargs: kwargs["datasets"]["dossie_contato.sql"],
    )

    resultado = extraction_service.executar_sync_secao_sync(
        "12345678000190",
        "contato",
        parametros={"usar_sql_consolidado": True},
    )

    assert resultado["status"] == "success"
    assert resultado["estrategia_execucao"] == "sql_consolidado"
    assert resultado["sql_principal"] == "dossie_contato.sql"
    assert resultado["metadata_file"].endswith(".metadata.json")
    assert resultado["sql_ids"] == ["dossie_contato.sql"]
    assert resultado["sql_ids_reutilizados"] == ["dossie_contato.sql"]
    assert resultado["sql_ids_executados"] == []
    assert sqls_solicitados == ["dossie_contato.sql"]


def test_executar_sync_secao_reexecuta_nfe_quando_so_existe_legacy_sem_shared_atual(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(extraction_service, "CNPJ_ROOT", cnpj_root_teste)
    monkeypatch.setattr(dataset_reuse, "CNPJ_ROOT", cnpj_root_teste)

    class ResolucaoFake:
        secao_id = "contato"
        cache_key = "forcar_shared_nfe"
        cache_file_name = "dossie_12345678000190_contato_forcar_shared_nfe.parquet"
        sql_ids = ("NFe.sql",)

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())

    caminho_legacy = cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "nfe_agr_12345678000190.parquet"
    caminho_legacy.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"email_dest": ["legado@teste.com"]}).write_parquet(caminho_legacy)

    chamadas_sql: list[str] = []

    def executar_sql_fake(sql: str, params=None, cnpj=None):
        chamadas_sql.append(sql)
        return [{"EMAIL_DEST": "atual@teste.com"}]

    monkeypatch.setattr(extraction_service.SqlService, "read_sql", staticmethod(lambda sql_id: f"select * from {sql_id}"))
    monkeypatch.setattr(extraction_service.SqlService, "executar_sql", staticmethod(executar_sql_fake))
    monkeypatch.setattr(
        extraction_service,
        "compor_secao_dossie",
        lambda **kwargs: kwargs["datasets"]["NFe.sql"],
    )

    resultado = extraction_service.executar_sync_secao_sync("12345678000190", "contato")

    assert resultado["sql_ids_executados"] == ["NFe.sql"]
    assert resultado["sql_ids_reutilizados"] == []
    assert len(chamadas_sql) == 1

    caminho_shared = cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "shared_sql" / "nfe_12345678000190.parquet"
    assert caminho_shared.exists()
    assert pl.read_parquet(caminho_shared).to_dicts() == [{"EMAIL_DEST": "atual@teste.com"}]


def test_executar_sync_secao_contato_registra_comparacao_com_estrategia_alternativa(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(extraction_service, "CNPJ_ROOT", cnpj_root_teste)

    base_dossie = cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "dossie"
    base_dossie.mkdir(parents=True, exist_ok=True)
    cache_referencia = base_dossie / "dossie_12345678000190_contato_referencia.parquet"
    pl.DataFrame(
        {
            "tipo_vinculo": ["EMPRESA_PRINCIPAL"],
            "cnpj_consultado": ["12345678000190"],
            "cnpj_raiz": ["12345678"],
            "cpf_cnpj_referencia": ["12345678000190"],
            "nome_referencia": ["Empresa Base"],
            "crc_contador": [None],
            "endereco": [None],
            "telefone": [None],
            "telefone_nfe_nfce": [None],
            "email": [None],
            "situacao_cadastral": [None],
            "indicador_matriz_filial": ["EMPRESA"],
            "origem_dado": ["dossie_contato.sql"],
            "tabela_origem": ["BI.DM_PESSOA"],
            "ordem_exibicao": [10],
        }
    ).write_parquet(cache_referencia)
    cache_referencia.with_suffix(".metadata.json").write_text(
        json.dumps(
            {
                "cnpj": "12345678000190",
                "secao_id": "contato",
                "estrategia_execucao": "sql_consolidado",
                "sql_principal": "dossie_contato.sql",
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    class ResolucaoFake:
        secao_id = "contato"
        cache_key = "comparacao_polars"
        cache_file_name = "dossie_12345678000190_contato_comparacao_polars.parquet"
        sql_ids = (
            "dados_cadastrais.sql",
            "dossie_contador.sql",
        )

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())
    monkeypatch.setattr(
        extraction_service,
        "carregar_dataset_reutilizavel",
        lambda *args, **kwargs: extraction_service.DatasetCompartilhadoDossie(
            sql_id="dados_cadastrais.sql",
            dataframe=pl.DataFrame({"cnpj": ["12345678000190"]}),
            caminho_origem=tmp_path / "dados_cadastrais.parquet",
            reutilizado=True,
        ) if kwargs.get("sql_id") == "dados_cadastrais.sql" else None,
    )

    def carregar_dataset_fake(cnpj: str, sql_id: str):
        if sql_id == "dados_cadastrais.sql":
            return extraction_service.DatasetCompartilhadoDossie(
                sql_id=sql_id,
                dataframe=pl.DataFrame({"cnpj": ["12345678000190"]}),
                caminho_origem=tmp_path / "dados_cadastrais.parquet",
                reutilizado=True,
            )
        if sql_id == "dossie_contador.sql":
            return extraction_service.DatasetCompartilhadoDossie(
                sql_id=sql_id,
                dataframe=pl.DataFrame({"nome": ["Contador Base"]}),
                caminho_origem=tmp_path / "dossie_contador.parquet",
                reutilizado=True,
            )
        return None

    monkeypatch.setattr(extraction_service, "carregar_dataset_reutilizavel", carregar_dataset_fake)
    monkeypatch.setattr(
        extraction_service,
        "compor_secao_dossie",
        lambda **kwargs: pl.read_parquet(cache_referencia),
    )

    resultado = extraction_service.executar_sync_secao_sync("12345678000190", "contato")

    assert resultado["comparacao_estrategia_alternativa"] is not None
    assert resultado["comparison_history_file"].endswith(".jsonl")
    assert resultado["comparacao_estrategia_alternativa"]["estrategia_referencia"] == "sql_consolidado"
    assert resultado["comparacao_estrategia_alternativa"]["convergencia_basica"] is True
    assert resultado["comparacao_estrategia_alternativa"]["convergencia_funcional"] is True
    assert resultado["comparacao_estrategia_alternativa"]["mesma_chave_funcional"] is True
    assert resultado["comparacao_estrategia_alternativa"]["quantidade_chaves_faltantes"] == 0
    assert resultado["comparacao_estrategia_alternativa"]["quantidade_chaves_extras"] == 0
    assert resultado["comparacao_estrategia_alternativa"]["campos_criticos_atual"]["tabela_origem"] == 1

    caminho_metadata = (base_dossie / ResolucaoFake.cache_file_name).with_suffix(".metadata.json")
    metadata_secao = json.loads(caminho_metadata.read_text(encoding="utf-8"))
    assert metadata_secao["comparacao_estrategia_alternativa"]["sql_principal_referencia"] == "dossie_contato.sql"
    caminho_historico = Path(resultado["comparison_history_file"])
    assert caminho_historico.exists()
    linhas_historico = caminho_historico.read_text(encoding="utf-8").strip().splitlines()
    assert len(linhas_historico) == 1
    registro_historico = json.loads(linhas_historico[0])
    assert registro_historico["estrategia_execucao"] == "composicao_polars"
    assert registro_historico["comparacao_estrategia_alternativa"]["convergencia_funcional"] is True


def test_executar_sync_secao_contato_registra_divergencia_funcional_quando_chaves_diferem(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(extraction_service, "CNPJ_ROOT", cnpj_root_teste)

    base_dossie = cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "dossie"
    base_dossie.mkdir(parents=True, exist_ok=True)
    cache_referencia = base_dossie / "dossie_12345678000190_contato_referencia_divergente.parquet"
    pl.DataFrame(
        {
            "tipo_vinculo": ["EMPRESA_PRINCIPAL"],
            "cnpj_consultado": ["12345678000190"],
            "cnpj_raiz": ["12345678"],
            "cpf_cnpj_referencia": ["12345678000190"],
            "nome_referencia": ["Empresa Base"],
            "crc_contador": [None],
            "endereco": ["Rua A"],
            "telefone": [None],
            "telefone_nfe_nfce": [None],
            "email": ["a@empresa.com"],
            "situacao_cadastral": [None],
            "indicador_matriz_filial": ["EMPRESA"],
            "origem_dado": ["dossie_contato.sql"],
            "tabela_origem": ["BI.DM_PESSOA"],
            "ordem_exibicao": [10],
        }
    ).write_parquet(cache_referencia)
    cache_referencia.with_suffix(".metadata.json").write_text(
        json.dumps(
            {
                "cnpj": "12345678000190",
                "secao_id": "contato",
                "estrategia_execucao": "sql_consolidado",
                "sql_principal": "dossie_contato.sql",
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    class ResolucaoFake:
        secao_id = "contato"
        cache_key = "comparacao_divergente"
        cache_file_name = "dossie_12345678000190_contato_comparacao_divergente.parquet"
        sql_ids = ("dados_cadastrais.sql", "dossie_contador.sql")

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())
    monkeypatch.setattr(
        extraction_service,
        "carregar_dataset_reutilizavel",
        lambda cnpj, sql_id: extraction_service.DatasetCompartilhadoDossie(
            sql_id=sql_id,
            dataframe=pl.DataFrame({"cnpj": ["12345678000190"]}),
            caminho_origem=tmp_path / f"{sql_id}.parquet",
            reutilizado=True,
        ),
    )
    monkeypatch.setattr(
        extraction_service,
        "compor_secao_dossie",
        lambda **kwargs: pl.DataFrame(
            {
                "tipo_vinculo": ["EMPRESA_PRINCIPAL"],
                "cnpj_consultado": ["12345678000190"],
                "cnpj_raiz": ["12345678"],
                "cpf_cnpj_referencia": ["12345678000190"],
                "nome_referencia": ["Empresa Divergente"],
                "crc_contador": [None],
                "endereco": [None],
                "telefone": [None],
                "telefone_nfe_nfce": [None],
                "email": [None],
                "situacao_cadastral": [None],
                "indicador_matriz_filial": ["EMPRESA"],
                "origem_dado": ["dados_cadastrais.sql"],
                "tabela_origem": ["BI.DM_PESSOA"],
                "ordem_exibicao": [10],
            }
        ),
    )

    resultado = extraction_service.executar_sync_secao_sync("12345678000190", "contato")

    assert resultado["comparacao_estrategia_alternativa"] is not None
    assert resultado["comparacao_estrategia_alternativa"]["convergencia_basica"] is True
    assert resultado["comparacao_estrategia_alternativa"]["convergencia_funcional"] is False
    assert resultado["comparacao_estrategia_alternativa"]["quantidade_chaves_faltantes"] == 1
    assert resultado["comparacao_estrategia_alternativa"]["quantidade_chaves_extras"] == 1
    assert resultado["comparacao_estrategia_alternativa"]["campos_criticos_atual"]["email"] == 0
    assert resultado["comparacao_estrategia_alternativa"]["campos_criticos_referencia"]["email"] == 1


def test_executar_sync_secao_reutiliza_cache_canonico_equivalente_sem_gravar_novo_parquet(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(extraction_service, "CNPJ_ROOT", cnpj_root_teste)

    base_dossie = cnpj_root_teste / "12345678000190" / "arquivos_parquet" / "dossie"
    base_dossie.mkdir(parents=True, exist_ok=True)

    dataframe_equivalente = pl.DataFrame(
        {
            "tipo_vinculo": ["EMPRESA_PRINCIPAL"],
            "cnpj_consultado": ["12345678000190"],
            "cnpj_raiz": ["12345678"],
            "cpf_cnpj_referencia": ["12345678000190"],
            "nome_referencia": ["Empresa Base"],
            "crc_contador": [None],
            "endereco": ["Rua A"],
            "telefone": [None],
            "telefone_nfe_nfce": [None],
            "email": [None],
            "situacao_cadastral": ["001 - ATIVA"],
            "indicador_matriz_filial": ["EMPRESA"],
            "origem_dado": ["dados_cadastrais.sql"],
            "tabela_origem": ["BI.DM_PESSOA"],
            "ordem_exibicao": [10],
        }
    )
    caminho_existente = base_dossie / "dossie_12345678000190_contato_existente.parquet"
    dataframe_equivalente.write_parquet(caminho_existente)
    assinatura = extraction_service._calcular_assinatura_conteudo_secao(dataframe_equivalente)
    caminho_existente.with_suffix(".metadata.json").write_text(
        json.dumps(
            {
                "cnpj": "12345678000190",
                "secao_id": "contato",
                "cache_file": str(caminho_existente),
                "cache_key": "cache_existente",
                "estrategia_execucao": "sql_consolidado",
                "sql_principal": "dossie_contato.sql",
                "assinatura_conteudo": assinatura,
                "cache_keys_equivalentes": ["cache_existente"],
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    class ResolucaoFake:
        secao_id = "contato"
        cache_key = "novo_cache_equivalente"
        cache_file_name = "dossie_12345678000190_contato_novo_cache_equivalente.parquet"
        sql_ids = ("dados_cadastrais.sql",)

    monkeypatch.setattr(extraction_service, "resolver_secao_dossie", lambda **_: ResolucaoFake())
    monkeypatch.setattr(
        extraction_service,
        "carregar_dataset_reutilizavel",
        lambda *args, **kwargs: extraction_service.DatasetCompartilhadoDossie(
            sql_id="dossie_contato.sql",
            dataframe=dataframe_equivalente,
            caminho_origem=tmp_path / "dossie_contato.parquet",
            reutilizado=True,
        ),
    )
    monkeypatch.setattr(
        extraction_service,
        "_resolver_sql_ids_efetivos",
        lambda **kwargs: ("dossie_contato.sql",),
    )
    monkeypatch.setattr(
        extraction_service,
        "compor_secao_dossie",
        lambda **kwargs: dataframe_equivalente,
    )

    chamadas_salvar: list[str] = []
    monkeypatch.setattr(
        extraction_service,
        "salvar_para_parquet",
        lambda *args, **kwargs: chamadas_salvar.append("salvou") or True,
    )

    resultado = extraction_service.executar_sync_secao_sync(
        "12345678000190",
        "contato",
        parametros={"usar_sql_consolidado": True},
    )

    assert resultado["cache_reutilizado"] is True
    assert resultado["cache_file"] == str(caminho_existente)
    assert resultado["impacto_cache_first"] == "cache_canonico_equivalente"
    assert chamadas_salvar == []
    assert not (base_dossie / ResolucaoFake.cache_file_name).exists()

    metadata_atualizada = json.loads(caminho_existente.with_suffix(".metadata.json").read_text(encoding="utf-8"))
    assert "novo_cache_equivalente" in metadata_atualizada["cache_keys_equivalentes"]
