from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.fisconforme.processador_polars import ProcessadorPolars


def test_carregar_tabela_reutiliza_lazyframe_na_mesma_instancia(tmp_path):
    caminho_parquet = tmp_path / "pessoa.parquet"
    pl.DataFrame({"CO_CNPJ_CPF": ["123"]}).write_parquet(caminho_parquet)

    processador = ProcessadorPolars()
    processador.tabelas["pessoa"] = caminho_parquet

    primeiro = processador.carregar_tabela("pessoa")
    segundo = processador.carregar_tabela("pessoa")

    assert primeiro is segundo
