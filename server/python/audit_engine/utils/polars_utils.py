def tipo_para_polars(tipo: str):
    """
    Converte um tipo de dado string do contrato para um tipo de dado Polars.
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    mapa = {
        "string": pl.Utf8,
        "int": pl.Int64,
        "float": pl.Float64,
        "date": pl.Utf8,
        "bool": pl.Boolean,
    }
    return mapa.get(tipo, pl.Utf8)
