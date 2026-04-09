from __future__ import annotations

from datetime import date
from datetime import datetime
from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.composicao_dif_icms import atualizar_composicao_dif_icms
from transformacao.composicao_dif_icms import composicao_dif_icms
from transformacao.composicao_enderecos import atualizar_composicao_enderecos
from transformacao.composicao_fronteira import atualizar_composicao_fronteira
from transformacao.composicao_fronteira import composicao_fronteira
from utilitarios import dataset_registry as registry


def test_composicao_dif_icms_reproduz_filtro_de_debito_a_menor(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "CNPJ_ROOT", tmp_path)

    cnpj = "12345678000190"
    registry.registrar_dataset(
        cnpj,
        "nfe_base",
        pl.DataFrame(
            {
                "chave_acesso": ["NFE_OK", "NFE_IGNORADA"],
                "ide_serie": ["1", "1"],
                "nnf": ["10", "11"],
                "tot_vnf": [100.0, 80.0],
                "tot_vicms": [18.0, 12.0],
                "dhemi": [datetime(2024, 1, 10), datetime(2019, 12, 31)],
                "co_tp_nf": [1, 1],
                "infprot_cstat": ["100", "100"],
                "seq_nitem": ["1", "1"],
            }
        ),
    )
    registry.registrar_dataset(
        cnpj,
        "nfce_base",
        pl.DataFrame(
            {
                "chave_acesso": ["NFCE_OK"],
                "ide_serie": ["2"],
                "nnf": ["20"],
                "tot_vnf": [50.0],
                "tot_vicms": [9.0],
                "dhemi": [datetime(2024, 2, 5)],
                "infprot_cstat": ["150"],
                "seq_nitem": ["1"],
            }
        ),
    )
    registry.registrar_dataset(
        cnpj,
        "efd_c100",
        pl.DataFrame(
            {
                "chv_nfe": ["NFE_OK", "NFCE_OK"],
                "vl_icms": [10.0, 9.0],
                "ind_oper": ["1", "1"],
                "dt_doc": [date(2024, 1, 10), date(2024, 2, 5)],
            }
        ),
    )

    resultado = composicao_dif_icms(cnpj)

    assert resultado.select("chave_acesso").to_series().to_list() == ["NFE_OK"]
    assert resultado.to_dicts()[0]["diferenca_icms_nao_debitado"] == 8.0

    caminho = atualizar_composicao_dif_icms(cnpj)
    assert caminho is not None
    assert caminho.exists()
    metadata = registry.encontrar_dataset(cnpj, "dif_icms_nfe_efd")
    assert metadata is not None
    assert metadata.metadata is not None
    assert metadata.metadata["sql_id"] == "dif_ICMS_NFe_EFD.sql"


def test_composicao_fronteira_respeita_data_limite_e_tipo_operacao(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "CNPJ_ROOT", tmp_path)

    cnpj = "12345678000190"
    registry.registrar_dataset(
        cnpj,
        "nfe_base",
        pl.DataFrame(
            {
                "co_emitente": [cnpj, cnpj],
                "co_destinatario": ["99999999000199", "99999999000199"],
                "co_tp_nf": [1, 1],
                "chave_acesso": ["CHAVE_OK", "CHAVE_FORA"],
                "seq_nitem": ["1", "1"],
                "prod_cprod": ["A", "B"],
                "prod_xprod": ["Item A", "Item B"],
                "prod_ncm": ["1000", "2000"],
                "prod_cest": ["0101", "0202"],
                "prod_qcom": [1.0, 2.0],
                "prod_vprod": [15.0, 30.0],
                "icms_vbcst": [7.0, 8.0],
                "icms_vicmsst": [1.0, 2.0],
                "dhemi": [datetime(2024, 2, 10), datetime(2024, 4, 10)],
            }
        ),
    )
    registry.registrar_dataset(
        cnpj,
        "sitafe_calculo_item",
        pl.DataFrame(
            {
                "it_nu_chave_acesso": ["CHAVE_OK", "CHAVE_FORA"],
                "it_nu_item": ["1", "1"],
                "it_co_sefin": ["SEF1", "SEF2"],
                "it_co_rotina_calculo": ["ROT1", "ROT2"],
                "it_vl_icms": [3.5, 7.0],
            }
        ),
    )

    resultado = composicao_fronteira(cnpj, data_limite_processamento="31/03/2024")

    assert resultado.height == 1
    linha = resultado.to_dicts()[0]
    assert linha["chave_acesso"] == "CHAVE_OK"
    assert linha["tipo_operacao"] == "1 - SAIDA"

    caminho = atualizar_composicao_fronteira(cnpj, data_limite_processamento="31/03/2024")
    assert caminho is not None
    localizado = registry.encontrar_dataset(cnpj, "composicao_fronteira")
    assert localizado is not None
    assert localizado.metadata is not None
    assert localizado.metadata["parametros"]["data_limite_processamento"] == "2024-03-31"


def test_atualizar_composicao_enderecos_mantem_fallback_quando_cadastro_nao_tem_campos_detalhados(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "CNPJ_ROOT", tmp_path)

    cnpj = "12345678000190"
    registry.registrar_dataset(
        cnpj,
        "cadastral",
        pl.DataFrame(
            {
                "Endereço": ["Rua Exemplo Centro"],
                "Município": ["Porto Velho"],
                "UF": ["RO"],
            }
        ),
    )
    registry.registrar_dataset(
        cnpj,
        "nfe_base",
        pl.DataFrame(
            {
                "co_destinatario": [cnpj],
                "dhemi": [datetime(2024, 1, 1)],
                "xlgr_dest": ["Rua A"],
                "nro_dest": ["10"],
                "xcpl_dest": ["Sala 1"],
                "xbairro_dest": ["Centro"],
                "fone_dest": ["69999999999"],
                "cep_dest": ["76800000"],
                "xmun_dest": ["Porto Velho"],
                "co_uf_dest": ["RO"],
            }
        ),
    )

    caminho = atualizar_composicao_enderecos(cnpj)

    assert caminho is None
    assert registry.encontrar_dataset(cnpj, "composicao_enderecos") is None


def test_atualizar_composicao_enderecos_materializa_quando_cadastro_tem_campos_detalhados(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "CNPJ_ROOT", tmp_path)

    cnpj = "12345678000190"
    registry.registrar_dataset(
        cnpj,
        "cadastral",
        pl.DataFrame(
            {
                "DESC_ENDERECO": ["Rua Oficial"],
                "BAIRRO": ["Centro"],
                "NU_CEP": ["76800000"],
                "NO_MUNICIPIO": ["Porto Velho"],
                "CO_UF": ["RO"],
            }
        ),
    )
    registry.registrar_dataset(
        cnpj,
        "nfe_base",
        pl.DataFrame(
            {
                "co_destinatario": [cnpj],
                "dhemi": [datetime(2024, 1, 1)],
                "xlgr_dest": ["Rua A"],
                "nro_dest": ["10"],
                "xcpl_dest": ["Sala 1"],
                "xbairro_dest": ["Centro"],
                "fone_dest": ["69999999999"],
                "cep_dest": ["76800000"],
                "xmun_dest": ["Porto Velho"],
                "co_uf_dest": ["RO"],
            }
        ),
    )

    caminho = atualizar_composicao_enderecos(cnpj)

    assert caminho is not None
    assert caminho.exists()

    localizado = registry.encontrar_dataset(cnpj, "composicao_enderecos")
    assert localizado is not None
    dataframe = pl.read_parquet(localizado.caminho)
    assert dataframe.height == 2
    assert dataframe.to_dicts()[0] == {
        "origem": "DM_PESSOA/SITAFE",
        "ano_mes": "ATUAL",
        "logradouro": "Rua Oficial",
        "numero": None,
        "complemento": None,
        "bairro": "Centro",
        "fone": None,
        "cep": "76800000",
        "municipio": "Porto Velho",
        "uf": "RO",
    }
