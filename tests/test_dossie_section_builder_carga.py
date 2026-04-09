from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services import dossie_section_builder as builder


def test_compor_secao_contato_suporta_volume_alto_de_filiais_e_socios(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    quantidade_filiais = 120
    quantidade_socios = 80

    datasets = {
        "dados_cadastrais.sql": pl.DataFrame(
            {
                "CNPJ": [cnpj],
                "Nome": ["Empresa Base"],
                "Endereco": ["Rua A"],
                "Situacao da IE": ["001 - ATIVA"],
            }
        ),
        "dossie_filiais_raiz.sql": pl.DataFrame(
            {
                "tipo_vinculo": ["FILIAL_RAIZ"] * quantidade_filiais,
                "cpf_cnpj_referencia": [f"12345678{indice + 2:04d}90" for indice in range(quantidade_filiais)],
                "nome_referencia": [f"Filial {indice + 1}" for indice in range(quantidade_filiais)],
                "endereco": [f"Rua Filial {indice + 1}" for indice in range(quantidade_filiais)],
                "situacao_cadastral": ["001 - ATIVA"] * quantidade_filiais,
                "indicador_matriz_filial": ["FILIAL"] * quantidade_filiais,
                "origem_dado": ["dossie_filiais_raiz.sql"] * quantidade_filiais,
                "tabela_origem": ["BI.DM_PESSOA"] * quantidade_filiais,
                "ordem_exibicao": [25] * quantidade_filiais,
            }
        ),
        "dossie_contador.sql": pl.DataFrame(
            {
                "situacao": ["Atual"],
                "co_cnpj_cpf_contador": ["99999999000199"],
                "nome": ["Contador Base"],
            }
        ),
        "dossie_historico_fac.sql": pl.DataFrame(),
        "dossie_rascunho_fac_contador.sql": pl.DataFrame(),
        "dossie_req_inscricao_contador.sql": pl.DataFrame(),
        "dossie_historico_socios.sql": pl.DataFrame(
            {
                "situacao": ["SOCIO ATUAL"] * quantidade_socios,
                "co_cnpj_cpf": [f"{11111111111 + indice}" for indice in range(quantidade_socios)],
                "nome": [f"Socio {indice + 1}" for indice in range(quantidade_socios)],
            }
        ),
        "NFe.sql": pl.DataFrame(),
        "NFCe.sql": pl.DataFrame(),
    }

    resultado = builder.compor_secao_dossie(cnpj, "contato", datasets)

    assert resultado.filter(pl.col("tipo_vinculo") == "EMPRESA_PRINCIPAL").height == 1
    assert resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").height == 1
    assert resultado.filter(pl.col("tipo_vinculo") == "FILIAL_RAIZ").height == quantidade_filiais
    assert resultado.filter(pl.col("tipo_vinculo") == "SOCIO_ATUAL").height == quantidade_socios
    assert resultado.height == 1 + 1 + quantidade_filiais + quantidade_socios

    primeira_filial = resultado.filter(pl.col("tipo_vinculo") == "FILIAL_RAIZ").row(0, named=True)
    socios = resultado.filter(pl.col("tipo_vinculo") == "SOCIO_ATUAL")

    assert primeira_filial["nome_referencia"] == "Filial 1"
    assert primeira_filial["origem_dado"] == "dossie_filiais_raiz.sql"
    assert socios.filter(pl.col("nome_referencia") == f"Socio {quantidade_socios}").height == 1
    assert socios.filter(pl.col("origem_dado") == "dossie_historico_socios.sql").height == quantidade_socios


def test_compor_secao_contato_suporta_contador_sem_contato_completo_em_cenario_volumoso(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(builder, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    quantidade_filiais = 90

    datasets = {
        "dados_cadastrais.sql": pl.DataFrame(
            {
                "CNPJ": [cnpj],
                "Nome": ["Empresa Base"],
                "Endereco": ["Rua A"],
                "Situacao da IE": ["001 - ATIVA"],
            }
        ),
        "dossie_filiais_raiz.sql": pl.DataFrame(
            {
                "tipo_vinculo": ["FILIAL_RAIZ"] * quantidade_filiais,
                "cpf_cnpj_referencia": [f"12345678{indice + 2:04d}90" for indice in range(quantidade_filiais)],
                "nome_referencia": [f"Filial {indice + 1}" for indice in range(quantidade_filiais)],
                "endereco": [f"Rua Filial {indice + 1}" for indice in range(quantidade_filiais)],
                "situacao_cadastral": ["001 - ATIVA"] * quantidade_filiais,
                "indicador_matriz_filial": ["FILIAL"] * quantidade_filiais,
                "origem_dado": ["dossie_filiais_raiz.sql"] * quantidade_filiais,
                "tabela_origem": ["BI.DM_PESSOA"] * quantidade_filiais,
                "ordem_exibicao": [25] * quantidade_filiais,
            }
        ),
        "dossie_contador.sql": pl.DataFrame(
            {
                "situacao": ["Atual"],
                "co_cnpj_cpf_contador": ["99999999000199"],
                "nome": ["Contador Sem Contato"],
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
    linha_contador = resultado.filter(pl.col("tipo_vinculo") == "CONTADOR_EMPRESA").row(0, named=True)

    assert resultado.filter(pl.col("tipo_vinculo") == "FILIAL_RAIZ").height == quantidade_filiais
    assert linha_contador["nome_referencia"] == "Contador Sem Contato"
    assert linha_contador["telefone"] is None
    assert linha_contador["telefone_nfe_nfce"] is None
    assert linha_contador["email"] is None
    assert linha_contador["endereco"] is None
    assert linha_contador["origem_dado"] == "dossie_contador.sql"
