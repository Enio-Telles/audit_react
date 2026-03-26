import re
import os

modules = [
    "server/python/audit_engine/modulos/produtos.py",
    "server/python/audit_engine/modulos/agregacao.py",
    "server/python/audit_engine/modulos/conversao.py",
    "server/python/audit_engine/modulos/estoque.py"
]

for mod_file in modules:
    with open(mod_file, "r") as f:
        content = f.read()

    # Simple regex to split based on @registrar_gerador
    blocks = re.split(r"(@registrar_gerador\([^)]+\))", content)

    # Find all helper functions (those not starting with @registrar_gerador but used by them)
    # Actually, for simplicity we can just copy the whole file minus other generators
    # and adjust the imports.

    # A more robust way is to just use regex to extract the generator functions and let the rest be
    # But some files have `_tipo_para_polars` or other helpers.

    # Let's extract the imports and top-level definitions
    # It's better to just manually move or write a smarter parser.
