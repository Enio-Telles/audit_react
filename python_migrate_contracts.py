import re
import os

with open("server/python/audit_engine/contratos/tabelas.py", "r") as f:
    content = f.read()

# Define the pattern to find each contract block
pattern = re.compile(r"registrar_contrato\(ContratoTabela\((.*?)\)\)", re.DOTALL)

for match in pattern.finditer(content):
    block_content = match.group(1)

    # Extract the name to determine the file path
    name_match = re.search(r'nome="([^"]+)"', block_content)
    if not name_match:
        continue

    nome = name_match.group(1)

    file_content = f"""from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

registrar_contrato(ContratoTabela(
{block_content}
))
"""

    filepath = f"server/python/audit_engine/tabelas/{nome}/contrato.py"
    with open(filepath, "w") as f_out:
        f_out.write(file_content)

    print(f"Created {filepath}")
