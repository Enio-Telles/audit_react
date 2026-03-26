import re
import os

from_modulos = {
    "produtos": ["produtos_unidades", "produtos"],
    "agregacao": ["produtos_agrupados", "id_agrupados"],
    "conversao": ["fatores_conversao", "produtos_final"],
    "estoque": ["nfe_entrada", "mov_estoque", "aba_mensal", "aba_anual", "produtos_selecionados"]
}

helpers_to_copy = {
    "produtos": ["_tipo_para_polars"],
    "agregacao": ["_tipo_para_polars", "_ler_agregacoes_manuais"],
    "conversao": ["_tipo_para_polars", "_ler_fatores_manuais"],
    "estoque": ["_tipo_para_polars"]
}

imports = """import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador

logger = logging.getLogger(__name__)

"""

def get_helper(mod_content, helper_name):
    # a bit hacky, simple regex to capture function definition to next def or end of file
    match = re.search(r"(def " + helper_name + r"\(.*?\):(?:\n(?:    .*\n|^\n)+)+)", mod_content, re.MULTILINE)
    if match:
        return match.group(1)
    return ""

for mod, funcs in from_modulos.items():
    mod_path = f"server/python/audit_engine/modulos/{mod}.py"
    with open(mod_path, "r") as f:
        mod_content = f.read()

    # extract each @registrar_gerador
    for func in funcs:
        # regex to capture from @registrar_gerador("nome") to the next @registrar_gerador or def _
        pattern = r"(@registrar_gerador\([\"']" + func + r"[\"']\).*?(?=\n@registrar_gerador|\ndef _|\Z))"
        match = re.search(pattern, mod_content, re.DOTALL)
        if match:
            generator_code = match.group(1)

            # create file content
            file_content = imports + generator_code + "\n\n"

            # append helpers
            for helper in helpers_to_copy.get(mod, []):
                helper_code = get_helper(mod_content, helper)
                if helper_code:
                    file_content += helper_code + "\n\n"

            out_path = f"server/python/audit_engine/tabelas/{func}/gerador.py"
            with open(out_path, "w") as out:
                out.write(file_content)
            print(f"Created {out_path}")
        else:
            print(f"Failed to find {func} in {mod}")
