from __future__ import annotations

from pathlib import Path

from utilitarios.sql_cache import get_sql_catalog_cache
from utilitarios.sql_catalog import get_sql_id, resolve_sql_path


def _ler_sql_direto(arquivo: Path) -> str:
    encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1", "cp1250"]

    for enc in encodings:
        try:
            sql_txt = arquivo.read_text(encoding=enc)
            return sql_txt.strip().rstrip(";")
        except UnicodeDecodeError:
            continue
        except Exception:
            continue

    raise Exception(
        f"❌ ERRO FATAL: Não foi possível ler o arquivo '{arquivo.name}' com nenhum dos encodings disponíveis."
    )


def ler_sql(arquivo: str | Path, *, use_cache: bool = True) -> str:
    """Lê arquivo SQL com tratamento robusto de encoding e cache opcional."""
    arquivo_path = Path(arquivo)
    if not arquivo_path.exists():
        try:
            arquivo_path = resolve_sql_path(arquivo)
        except Exception:
            pass

    if not use_cache:
        return _ler_sql_direto(arquivo_path)

    sql_id = get_sql_id(arquivo_path)
    if sql_id is None:
        try:
            sql_id = str(arquivo)
        except Exception:
            sql_id = arquivo_path.name

    cache = get_sql_catalog_cache()
    cached = cache.get(sql_id, arquivo_path)
    if cached is not None:
        return cached

    sql_text = _ler_sql_direto(arquivo_path)
    cache.set(sql_id, arquivo_path, sql_text)
    return sql_text
