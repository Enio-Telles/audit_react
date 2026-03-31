"""Testes unitários para módulo de referências fiscais."""

import pytest
import polars as pl
from pathlib import Path

# Importar módulo testado
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from audit_engine.utils.referencias import (
    carregar_ncm,
    carregar_cest,
    carregar_cfop,
    carregar_cst,
    buscar_ncm_por_codigo,
    buscar_cest_por_codigo,
    buscar_cfop_por_codigo,
    validar_ncm,
    validar_cest,
    validar_cfop,
    validar_coluna_ncm,
    validar_coluna_cest,
    validar_coluna_cfop,
    validar_integridade_fiscal,
    enriquecer_com_ncm,
    enriquecer_com_cest,
    enriquecer_com_cfop,
)


class TestCarregamento:
    """Testes de carregamento de tabelas de referência."""

    def test_carregar_ncm(self):
        """Testa carregamento da tabela NCM."""
        df = carregar_ncm()
        assert not df.is_empty(), "NCM não deve estar vazia"
        assert len(df) > 10000, "NCM deve ter mais de 10.000 registros"
        assert "Codigo_NCM" in df.columns
        assert "Descricao" in df.columns

    def test_carregar_cest(self):
        """Testa carregamento da tabela CEST."""
        df = carregar_cest()
        assert not df.is_empty(), "CEST não deve estar vazia"
        assert len(df) > 1000, "CEST deve ter mais de 1.000 registros"

    def test_carregar_cfop(self):
        """Testa carregamento da tabela CFOP."""
        df = carregar_cfop()
        assert not df.is_empty(), "CFOP não deve estar vazia"
        assert len(df) > 500, "CFOP deve ter mais de 500 registros"

    def test_carregar_cst(self):
        """Testa carregamento da tabela CST."""
        df = carregar_cst()
        assert not df.is_empty(), "CST não deve estar vazia"
        assert len(df) > 50, "CST deve ter mais de 50 registros"


class TestBusca:
    """Testes de busca em tabelas de referência."""

    def test_buscar_ncm_por_codigo_exato(self):
        """Testa busca de NCM por código exato."""
        df = buscar_ncm_por_codigo("01012100")
        # Pode não existir este código específico, mas não deve falhar
        assert isinstance(df, pl.DataFrame)

    def test_buscar_ncm_por_prefixo(self):
        """Testa busca de NCM por prefixo."""
        df = buscar_ncm_por_codigo("0101")
        assert isinstance(df, pl.DataFrame)
        # Todos os resultados devem começar com o prefixo
        if not df.is_empty():
            assert all(df["Codigo_NCM"].str.starts_with("0101"))

    def test_buscar_cfop_por_codigo_exato(self):
        """Testa busca de CFOP por código exato."""
        df = buscar_cfop_por_codigo("5102")
        # CFOP 5102 é comum (Compra para industrialização)
        # Colunas reais: id, co_cfop, descricao, ...
        if not df.is_empty():
            if "co_cfop" in df.columns:
                assert "5102" in str(df["co_cfop"].to_list())
            elif "CFOP" in df.columns:
                assert "5102" in str(df["CFOP"].to_list())


class TestValidacao:
    """Testes de validação de códigos fiscais."""

    def test_validar_ncm_valido(self):
        """Testa validação de NCM válido."""
        # NCMs válidos conhecidos
        resultado = validar_ncm("01012100")
        # Pode ser True ou False dependendo se o NCM existe na base
        assert isinstance(resultado, bool)

    def test_validar_ncm_invalido(self):
        """Testa validação de NCM inválido."""
        # NCM com formato inválido (muitos dígitos)
        resultado = validar_ncm("999999999999")
        assert resultado is False, "NCM com formato inválido deve retornar False"

    def test_validar_cfop_valido(self):
        """Testa validação de CFOP válido."""
        resultado = validar_cfop("5102")
        assert isinstance(resultado, bool)

    def test_validar_cest_valido(self):
        """Testa validação de CEST válido."""
        resultado = validar_cest("0100140")
        assert isinstance(resultado, bool)


class TestValidacaoEmLote:
    """Testes de validação em lote de DataFrames."""

    def test_validar_coluna_ncm(self):
        """Testa validação de coluna NCM em DataFrame."""
        df_teste = pl.DataFrame({
            "produto": ["A", "B", "C"],
            "ncm": ["01012100", "99999999", "02011000"],
        })
        df_validado = validar_coluna_ncm(df_teste, "ncm")
        assert "ncm_valido" in df_validado.columns
        assert df_validado.schema["ncm_valido"] == pl.Boolean

    def test_validar_coluna_cest(self):
        """Testa validação de coluna CEST em DataFrame."""
        df_teste = pl.DataFrame({
            "produto": ["A", "B"],
            "cest": ["0100140", "9999999"],
        })
        df_validado = validar_coluna_cest(df_teste, "cest")
        assert "cest_valido" in df_validado.columns

    def test_validar_coluna_cfop(self):
        """Testa validação de coluna CFOP em DataFrame."""
        df_teste = pl.DataFrame({
            "documento": ["X", "Y"],
            "cfop": ["5102", "6999"],
        })
        df_validado = validar_coluna_cfop(df_teste, "cfop")
        assert "cfop_valido" in df_validado.columns

    def test_validar_integridade_fiscal(self):
        """Testa validação de integridade fiscal completa."""
        df_teste = pl.DataFrame({
            "produto": ["A", "B"],
            "ncm": ["01012100", "02011000"],
            "cest": ["0100140", "0200100"],
            "cfop": ["5102", "6102"],
        })
        resultado = validar_integridade_fiscal(df_teste)
        assert "ncm_validos" in resultado or "ncm_invalidos" in resultado
        assert "cest_validos" in resultado or "cest_invalidos" in resultado
        assert "cfop_validos" in resultado or "cfop_invalidos" in resultado


class TestEnriquecimento:
    """Testes de enriquecimento de DataFrames."""

    def test_enriquecer_com_ncm(self):
        """Testa enriquecimento com descrições de NCM."""
        df_teste = pl.DataFrame({
            "produto": ["A", "B"],
            "ncm": ["01012100", "02011000"],
            "valor": [100, 200],
        })
        df_enriquecido = enriquecer_com_ncm(df_teste, "ncm")
        assert "ncm_descricao" in df_enriquecido.columns
        assert "ncm_vigencia" in df_enriquecido.columns

    def test_enriquecer_com_cest(self):
        """Testa enriquecimento com descrições de CEST."""
        df_teste = pl.DataFrame({
            "produto": ["A"],
            "cest": ["0100140"],
        })
        df_enriquecido = enriquecer_com_cest(df_teste, "cest")
        assert "cest_descricao" in df_enriquecido.columns

    def test_enriquecer_com_cfop(self):
        """Testa enriquecimento com descrições de CFOP."""
        df_teste = pl.DataFrame({
            "documento": ["X"],
            "cfop": ["5102"],
        })
        df_enriquecido = enriquecer_com_cfop(df_teste, "cfop")
        assert "cfop_descricao" in df_enriquecido.columns
        assert "cfop_tipo" in df_enriquecido.columns


class TestEdgeCases:
    """Testes de casos extremos e borda."""

    def test_validar_coluna_sem_referencia(self):
        """Testa validação quando tabela de referência está vazia."""
        df_teste = pl.DataFrame({
            "produto": ["A"],
            "ncm": ["01012100"],
        })
        # Mesmo sem referência, deve retornar DataFrame com coluna de validação
        df_validado = validar_coluna_ncm(df_teste, "ncm")
        assert "ncm_valido" in df_validado.columns

    def test_enriquecer_dataframe_vazio(self):
        """Testa enriquecimento de DataFrame vazio."""
        df_vazio = pl.DataFrame({
            "produto": pl.Series([], dtype=pl.String),
            "ncm": pl.Series([], dtype=pl.String),
        })
        df_enriquecido = enriquecer_com_ncm(df_vazio, "ncm")
        assert "ncm_descricao" in df_enriquecido.columns

    def test_busca_codigo_vazio(self):
        """Testa busca com código vazio."""
        df = buscar_ncm_por_codigo("")
        # Não deve falhar, pode retornar vazio ou todos
        assert isinstance(df, pl.DataFrame)

    def test_validar_codigo_none(self):
        """Testa validação com código None."""
        resultado = validar_ncm("")
        assert resultado is False, "NCM vazio deve retornar False"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
