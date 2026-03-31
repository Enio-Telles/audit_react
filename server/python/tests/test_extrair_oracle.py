from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from extrair_oracle import _montar_binds, _montar_dataframe_resultado, listar_consultas_versionadas
import mapeamento_sql_oracle
from oracle_client import _configurar_sessao_oracle


class CursorFalso:
    """Cursor minimo para validar montagem de binds sem Oracle real."""

    def __init__(self, nomes_binds: list[str]):
        self._nomes_binds = nomes_binds

    def bindnames(self) -> list[str]:
        return self._nomes_binds


class CursorSessaoFalso:
    """Cursor minimo para validar configuracao de sessao Oracle."""

    def __init__(self):
        self.comandos: list[str] = []
        self.fechado = False

    def execute(self, comando: str) -> None:
        self.comandos.append(comando)

    def close(self) -> None:
        self.fechado = True


class ConexaoSessaoFalsa:
    """Conexao minima para validar configuracao de sessao Oracle."""

    def __init__(self):
        self.cursor_instancia = CursorSessaoFalso()

    def cursor(self) -> CursorSessaoFalso:
        return self.cursor_instancia


def test_montar_binds_sem_data_limite_mantem_binds_temporais_nulos():
    cursor = CursorFalso(["CNPJ", "DATA_LIMITE_PROCESSAMENTO", "DATA_INICIAL", "DATA_FINAL"])

    binds = _montar_binds(cursor, "12345678000190", None)

    assert binds["CNPJ"] == "12345678000190"
    assert binds["DATA_LIMITE_PROCESSAMENTO"] is None
    assert binds["DATA_INICIAL"] is None
    assert binds["DATA_FINAL"] is None


def test_consultas_oracle_sem_data_limite_nao_usam_fallback_para_data_atual():
    caminhos = [
        Path("consultas/c100.sql"),
        Path("consultas/c170.sql"),
        Path("consultas/c176.sql"),
        Path("consultas/bloco_h.sql"),
        Path("consultas/e111.sql"),
        Path("consultas/nfe_dados_st.sql"),
        Path("consultas/reg0005.sql"),
        Path("consultas/reg0190.sql"),
        Path("consultas/reg0200.sql"),
        Path("consultas/reg0220.sql"),
    ]

    for caminho in caminhos:
        conteudo = caminho.read_text(encoding="utf-8")
        assert "NVL(TO_DATE(:DATA_LIMITE_PROCESSAMENTO, 'YYYY-MM-DD'), TRUNC(SYSDATE))" not in conteudo
        assert "TRUNC(SYSDATE)" not in conteudo


def test_listar_consultas_versionadas_inclui_trilha_st(tmp_path: Path):
    for nome in ["c176.sql", "nfe_dados_st.sql", "e111.sql"]:
        (tmp_path / nome).write_text("select 1 from dual", encoding="utf-8")

    consultas = listar_consultas_versionadas(str(tmp_path))

    assert "c176" in consultas
    assert "nfe_dados_st" in consultas
    assert "e111" in consultas


def test_montar_dataframe_resultado_lida_com_texto_apos_mais_de_cem_nulos():
    colunas = ["sequencia", "unid_inv"]
    dados = [(indice, None) for indice in range(101)] + [(101, "UN")]

    dataframe = _montar_dataframe_resultado(colunas, dados)

    assert dataframe.schema == {"sequencia": pl.Int64, "unid_inv": pl.String}
    assert dataframe.row(-1) == (101, "UN")


def test_mapeamento_sql_oracle_renderiza_placeholders_antes_da_analise(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    caminho_sql = tmp_path / "teste.sql"
    caminho_sql.write_text(
        "select * from {{FONTE_C170}} c170 inner join {{FONTE_REG0000}} r on r.id = c170.reg_0000_id",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        mapeamento_sql_oracle,
        "obter_mapeamento_fontes_oracle",
        lambda: {
            "FONTE_C170": "SPED.REG_C170_CUSTOM",
            "FONTE_REG0000": "SPED.REG_0000_CUSTOM",
        },
    )

    analise = mapeamento_sql_oracle.analisar_mapeamento_raiz_sql_oracle(tmp_path)
    tabelas_raiz = analise["arquivos_sql"][0]["tabelas_raiz"]

    assert "SPED.REG_C170_CUSTOM" in tabelas_raiz
    assert "SPED.REG_0000_CUSTOM" in tabelas_raiz


def test_configurar_sessao_oracle_define_nls_numeric_characters():
    conexao = ConexaoSessaoFalsa()

    _configurar_sessao_oracle(conexao)

    assert conexao.cursor_instancia.comandos == ["ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'"]
    assert conexao.cursor_instancia.fechado is True
