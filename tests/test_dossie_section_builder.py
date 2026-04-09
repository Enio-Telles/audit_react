from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services import dossie_section_builder as builder


def test_compor_secao_contato_agrega_empresa_contador_socio_e_email(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    filial = cnpj_root_teste / "12345678000270" / "arquivos_parquet"
    filial.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "CNPJ": ["12345678000270"],
            "Nome": ["Filial Teste"],
            "Endereco": ["Rua B"],
            "Situacao da IE": ["001 - ATIVA"],
        }
    ).write_parquet(filial / "dados_cadastrais_12345678000270.parquet")

    datasets = {
        "dados_cadastrais.sql": pl.DataFrame(
            {
                "CNPJ": [cnpj],
                "Nome": ["Empresa Base"],
                "Endereco": ["Rua A"],
                "Situacao da IE": ["001 - ATIVA"],
            }
        ),
        "dossie_contador.sql": pl.DataFrame(
            {
                "situacao": ["Atual"],
                "co_cnpj_cpf_contador": ["99999999000199"],
                "nome": ["Contador Base"],
                "municipio": ["Goiania"],
                "uf": ["GO"],
            }
        ),
        "dossie_historico_fac.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "dossie_historico_socios.sql": pl.DataFrame(
            {
                "situacao": ["SOCIO ATUAL"],
                "co_cnpj_cpf": ["11111111111"],
                "nome": ["Socio Base"],
            }
        ),
        "NFe.sql": pl.DataFrame(
            {
                "co_emitente": ["99999999000199"],
                "fone_emit": ["6933334444"],
                "email_dest": ["contato@empresa.com"],
            }
        ),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)
    tipos_vinculo = resultado.get_column("tipo_vinculo").to_list()

    assert "EMPRESA_PRINCIPAL" in tipos_vinculo
    assert "FILIAL_RAIZ" in tipos_vinculo
    assert "CONTADOR_EMPRESA" in tipos_vinculo
    assert "SOCIO_ATUAL" in tipos_vinculo
    assert "EMAIL_NFE" in tipos_vinculo

    linha_contador = resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").to_dicts()[0]
    assert linha_contador["telefone_nfe_nfce"] == "6933334444"
    assert linha_contador["endereco"] == "Goiania, GO"
    assert "SITAFE.SITAFE_HISTORICO_CONTRIBUINTE" in linha_contador["tabela_origem"]


def test_compor_secao_contato_preserva_matriz_filial_e_registros_sem_contato(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000270"

    matriz = cnpj_root_teste / "12345678000190" / "arquivos_parquet"
    matriz.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "CNPJ": ["12345678000190"],
            "Nome": ["Matriz Teste"],
            "Endereco": ["Rua Matriz"],
            "Situacao da IE": ["001 - ATIVA"],
        }
    ).write_parquet(matriz / "dados_cadastrais_12345678000190.parquet")

    datasets = {
        "dados_cadastrais.sql": pl.DataFrame(
            {
                "CNPJ": [cnpj],
                "Nome": ["Filial Consultada"],
                "Endereco": ["Rua Filial"],
                "Situacao da IE": ["001 - ATIVA"],
            }
        ),
        "dossie_contador.sql": pl.DataFrame(
            {
                "situacao": ["Atual"],
                "co_cnpj_cpf_contador": ["99999999000199"],
                "nome": ["Contador Sem Fone"],
            }
        ),
        "dossie_historico_fac.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "dossie_historico_socios.sql": pl.DataFrame(),
        "NFe.sql": pl.DataFrame(),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)

    linha_matriz = resultado.filter(pl.col("tipo_vinculo") == "MATRIZ_RAIZ").to_dicts()[0]
    assert linha_matriz["cpf_cnpj_referencia"] == "12345678000190"
    assert linha_matriz["indicador_matriz_filial"] == "MATRIZ"
    assert linha_matriz["telefone"] is None
    assert linha_matriz["email"] is None
    assert "BI.DM_PESSOA" in linha_matriz["tabela_origem"]

    linha_contador = resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").to_dicts()[0]
    assert linha_contador["nome_referencia"] == "Contador Sem Fone"
    assert linha_contador["telefone"] is None
    assert linha_contador["telefone_nfe_nfce"] is None
    assert "BI.DM_PESSOA" in linha_contador["tabela_origem"]


def test_compor_secao_contato_faz_fallback_para_historico_fac_quando_contador_principal_ausente(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    datasets = {
        "dados_cadastrais.sql": pl.DataFrame(
            {
                "CNPJ": [cnpj],
                "Nome": ["Empresa Base"],
                "Endereco": ["Rua A"],
                "Situacao da IE": ["001 - ATIVA"],
            }
        ),
        "dossie_contador.sql": pl.DataFrame(),
        "dossie_historico_fac.sql": pl.DataFrame(
            {
                "cpf_contador": ["88888888000177"],
                "no_contador": ["Contador Via FAC"],
                "ult_fac": ["9"],
                "logradouro": ["Av FAC"],
                "num": ["100"],
                "municipio": ["Palmas"],
                "uf": ["TO"],
                "email": ["contador@fac.com"],
            }
        ),
        "dossie_historico_socios.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "NFe.sql": pl.DataFrame(
            {
                "co_emitente": ["88888888000177"],
                "fone_emit": ["62911112222"],
            }
        ),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)
    linha_contador = resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").to_dicts()[0]

    assert linha_contador["cpf_cnpj_referencia"] == "88888888000177"
    assert linha_contador["nome_referencia"] == "Contador Via FAC"
    assert linha_contador["situacao_cadastral"] == "FAC atual"
    assert linha_contador["origem_dado"] == "dossie_historico_fac.sql"
    assert linha_contador["telefone_nfe_nfce"] == "62911112222"
    assert linha_contador["endereco"] == "Av FAC, 100, Palmas, TO"
    assert linha_contador["email"] == "contador@fac.com"
    assert "SITAFE.SITAFE_HISTORICO_CONTRIBUINTE" in linha_contador["tabela_origem"]


def test_compor_secao_contato_materializa_fac_da_empresa_e_socio_antigo():
    cnpj = "12345678000190"
    datasets = {
        "dados_cadastrais.sql": pl.DataFrame(
            {
                "CNPJ": [cnpj],
                "Nome": ["Empresa Base"],
                "Endereco": ["Rua A"],
                "Situacao da IE": ["001 - ATIVA"],
            }
        ),
        "dossie_contador.sql": pl.DataFrame(),
        "dossie_historico_fac.sql": pl.DataFrame(
            {
                "nome": ["Empresa Base"],
                "logradouro": ["Av FAC"],
                "num": ["100"],
                "municipio": ["Palmas"],
                "uf": ["TO"],
                "telefone": ["6932221100"],
                "email": ["empresa@fac.com"],
            }
        ),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "dossie_historico_socios.sql": pl.DataFrame(
            {
                "situacao": ["SOCIO ATUAL", "SOCIO ANTIGO"],
                "co_cnpj_cpf": ["11111111111", "22222222222"],
                "nome": ["Socio Atual", "Socio Antigo"],
                "telefone": ["69911110000", None],
                "email": ["atual@socio.com", None],
                "endereco": ["Endereco Atual", None],
            }
        ),
        "NFe.sql": pl.DataFrame(),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)

    linha_empresa_fac = resultado.filter(pl.col("tipo_vinculo") == "EMPRESA_FAC_ATUAL").row(0, named=True)
    linha_socio_antigo = resultado.filter(pl.col("tipo_vinculo") == "SOCIO_ANTIGO").row(0, named=True)

    assert linha_empresa_fac["telefone"] == "6932221100"
    assert linha_empresa_fac["email"] == "empresa@fac.com"
    assert linha_empresa_fac["endereco"] == "Av FAC, 100, Palmas, TO"
    assert "SITAFE.SITAFE_PESSOA" in linha_empresa_fac["tabela_origem"]
    assert linha_socio_antigo["nome_referencia"] == "Socio Antigo"
    assert linha_socio_antigo["situacao_cadastral"] == "SOCIO ANTIGO"
    assert "SITAFE.SITAFE_HISTORICO_SOCIO" in linha_socio_antigo["tabela_origem"]


def test_compor_secao_contato_complementa_dados_do_contador_com_historico_fac(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    datasets = {
        "dados_cadastrais.sql": pl.DataFrame(
            {
                "CNPJ": [cnpj],
                "Nome": ["Empresa Base"],
                "Endereco": ["Rua A"],
                "Situacao da IE": ["001 - ATIVA"],
            }
        ),
        "dossie_contador.sql": pl.DataFrame(
            {
                "situacao": ["Atual"],
                "co_cnpj_cpf_contador": ["77777777000166"],
                "nome": ["Contador Misto"],
                "telefone": ["6933001100"],
                "email": ["sitafe@contador.com"],
            }
        ),
        "dossie_historico_fac.sql": pl.DataFrame(
            {
                "cpf_contador": ["77777777000166"],
                "no_contador": ["Contador Misto"],
                "ult_fac": ["9"],
                "logradouro": ["Rua FAC"],
                "num": ["55"],
                "municipio": ["Porto Velho"],
                "uf": ["RO"],
                "telefone": ["6933002200"],
                "email": ["misto@fac.com"],
                "email_corr": ["misto@fac.com"],
            }
        ),
        "dossie_historico_socios.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "NFe.sql": pl.DataFrame(),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)
    linha_contador = resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").to_dicts()[0]

    assert linha_contador["nome_referencia"] == "Contador Misto"
    assert linha_contador["origem_dado"] == "dossie_historico_fac.sql"
    assert linha_contador["endereco"] == "Rua FAC, 55, Porto Velho, RO"
    assert linha_contador["email"] == "misto@fac.com"
    assert linha_contador["telefone"] == "6933002200"
    assert "FAC atual: misto@fac.com" in linha_contador["emails_por_fonte"]
    assert "SITAFE_PESSOA: sitafe@contador.com" in linha_contador["emails_por_fonte"]
    assert "FAC atual: 6933002200" in linha_contador["telefones_por_fonte"]
    assert "SITAFE_PESSOA: 6933001100" in linha_contador["telefones_por_fonte"]
    assert "FAC atual" in linha_contador["fontes_contato"]
    assert "SITAFE_PESSOA" in linha_contador["fontes_contato"]
    assert "SITAFE.SITAFE_PESSOA" in linha_contador["tabela_origem"]


def test_compor_secao_contato_faz_fallback_para_rascunho_fac(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    datasets = {
        "dados_cadastrais.sql": pl.DataFrame({"CNPJ": [cnpj], "Nome": ["Empresa Base"]}),
        "dossie_contador.sql": pl.DataFrame(),
        "dossie_historico_fac.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(
            {
                "cpf_contador": ["66666666000155"],
                "no_contador": ["Contador Rascunho"],
                "logradouro": ["Rua Rascunho"],
                "bairro": ["Centro"],
                "municipio": ["Ariquemes"],
                "uf": ["RO"],
                "telefone": ["69999990000"],
                "email": ["rascunho@contador.com"],
                "crc_contador": ["CRC123"],
            }
        ),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "dossie_historico_socios.sql": pl.DataFrame(),
        "NFe.sql": pl.DataFrame(),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)
    linha_contador = resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").to_dicts()[0]

    assert linha_contador["origem_dado"] == "dossie_rascunho_fac_contador.sql"
    assert linha_contador["nome_referencia"] == "Contador Rascunho"
    assert linha_contador["endereco"] == "Rua Rascunho, Centro, Ariquemes, RO"
    assert linha_contador["telefone"] == "69999990000"
    assert linha_contador["email"] == "rascunho@contador.com"
    assert linha_contador["crc_contador"] == "CRC123"
    assert linha_contador["tabela_origem"] == "SITAFE.SITAFE_RASCUNHO_FAC"


def test_compor_secao_contato_faz_fallback_para_req_inscricao(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    datasets = {
        "dados_cadastrais.sql": pl.DataFrame({"CNPJ": [cnpj], "Nome": ["Empresa Base"]}),
        "dossie_contador.sql": pl.DataFrame(),
        "dossie_historico_fac.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(
            {
                "cpf_contador": ["55555555000144"],
                "no_contador": ["Contador Requerimento"],
                "telefone": ["6932100000"],
                "municipio": ["Ji-Parana"],
                "uf": ["RO"],
                "crc_contador": ["CRC999"],
            }
        ),
        "dossie_historico_socios.sql": pl.DataFrame(),
        "NFe.sql": pl.DataFrame(),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)
    linha_contador = resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").to_dicts()[0]

    assert linha_contador["origem_dado"] == "dossie_req_inscricao_contador.sql"
    assert linha_contador["nome_referencia"] == "Contador Requerimento"
    assert linha_contador["endereco"] == "Ji-Parana, RO"
    assert linha_contador["telefone"] == "6932100000"
    assert linha_contador["crc_contador"] == "CRC999"
    assert "SITAFE.SITAFE_REQ_INSCRICAO" in linha_contador["tabela_origem"]


def test_compor_secao_contato_usa_sql_consolidado_quando_disponivel():
    resultado = builder.compor_secao_dossie(
        "12345678000190",
        "contato",
        {
            "dossie_contato.sql": pl.DataFrame(
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
            )
        },
    )

    assert resultado.height == 1
    linha = resultado.to_dicts()[0]
    assert linha["nome_referencia"] == "Empresa Consolidada"
    assert linha["origem_dado"] == "dossie_contato.sql"
    assert linha["tabela_origem"] == "BI.DM_PESSOA"


def test_compor_secao_contato_prefere_dataset_sql_de_filiais_por_raiz_ao_scan_local(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    datasets = {
        "dados_cadastrais.sql": pl.DataFrame({"CNPJ": [cnpj], "Nome": ["Empresa Base"]}),
        "dossie_filiais_raiz.sql": pl.DataFrame(
            {
                "tipo_vinculo": ["FILIAL_RAIZ", "MATRIZ_RAIZ"],
                "cpf_cnpj_referencia": ["12345678000270", "12345678000100"],
                "nome_referencia": ["Filial SQL", "Matriz SQL"],
                "endereco": ["Rua Filial SQL", "Rua Matriz SQL"],
                "situacao_cadastral": ["001 - ATIVA", "001 - ATIVA"],
                "indicador_matriz_filial": ["FILIAL", "MATRIZ"],
                "origem_dado": ["dossie_filiais_raiz.sql", "dossie_filiais_raiz.sql"],
                "tabela_origem": ["BI.DM_PESSOA", "BI.DM_PESSOA"],
                "ordem_exibicao": [25, 20],
            }
        ),
        "dossie_contador.sql": pl.DataFrame(),
        "dossie_historico_fac.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "dossie_historico_socios.sql": pl.DataFrame(),
        "NFe.sql": pl.DataFrame(),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)
    filiais = resultado.filter(pl.col("tipo_vinculo").is_in(["FILIAL_RAIZ", "MATRIZ_RAIZ"])).select(
        ["tipo_vinculo", "cpf_cnpj_referencia", "nome_referencia", "origem_dado"]
    ).to_dicts()

    assert filiais == [
        {
            "tipo_vinculo": "MATRIZ_RAIZ",
            "cpf_cnpj_referencia": "12345678000100",
            "nome_referencia": "Matriz SQL",
            "origem_dado": "dossie_filiais_raiz.sql",
        },
        {
            "tipo_vinculo": "FILIAL_RAIZ",
            "cpf_cnpj_referencia": "12345678000270",
            "nome_referencia": "Filial SQL",
            "origem_dado": "dossie_filiais_raiz.sql",
        },
    ]


def test_compor_secao_contato_so_anexa_telefone_de_nota_com_reconciliacao_exata_do_contador(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    datasets = {
        "dados_cadastrais.sql": pl.DataFrame({"CNPJ": [cnpj], "Nome": ["Empresa Base"]}),
        "dossie_contador.sql": pl.DataFrame(
            {
                "situacao": ["Atual"],
                "co_cnpj_cpf_contador": ["11111111111"],
                "nome": ["Contador CPF"],
            }
        ),
        "dossie_historico_fac.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "dossie_historico_socios.sql": pl.DataFrame(),
        "NFe.sql": pl.DataFrame(
            {
                "co_emitente": ["11111111111", "1111111111", "22222222222"],
                "fone_emit": ["6299990000", "6299991111", "6299992222"],
            }
        ),
        "NFCe.sql": pl.DataFrame(
            {
                "co_destinatario": ["11111111111", "ABC", None],
                "fone_dest": ["6299993333", "6299994444", "6299995555"],
            }
        ),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)
    linha_contador = resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").to_dicts()[0]

    assert linha_contador["telefone_nfe_nfce"] == "6299990000, 6299993333"


def test_compor_secao_contato_nao_anexa_telefone_de_nota_quando_documento_do_contador_e_invalido(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    datasets = {
        "dados_cadastrais.sql": pl.DataFrame({"CNPJ": [cnpj], "Nome": ["Empresa Base"]}),
        "dossie_contador.sql": pl.DataFrame(
            {
                "situacao": ["Atual"],
                "co_cnpj_cpf_contador": ["12345"],
                "nome": ["Contador Invalido"],
            }
        ),
        "dossie_historico_fac.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "dossie_historico_socios.sql": pl.DataFrame(),
        "NFe.sql": pl.DataFrame(
            {
                "co_emitente": ["12345"],
                "fone_emit": ["6298887777"],
            }
        ),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)
    linha_contador = resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").to_dicts()[0]

    assert linha_contador["telefone_nfe_nfce"] is None


def test_normalizar_dataframe_evitar_colisao_de_colunas_apos_normalizacao():
    dataframe = pl.DataFrame(
        {
            "CO_INDIEDEST": [1],
            "co_indiedest_": [2],
        }
    )

    resultado = builder._normalizar_dataframe(dataframe)

    assert resultado.columns == ["co_indiedest", "co_indiedest_1"]
