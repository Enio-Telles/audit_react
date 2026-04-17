from pathlib import Path

# Simulate CNPJ_ROOT resolution
cnpj_root = Path('/var/www/cnpj').resolve()
print(f"CNPJ_ROOT: {cnpj_root}")

req_path = "test.parquet"

# Original code behavior:
try:
    p = Path(req_path).resolve()
    print(f"Original p: {p}")
    is_rel = p.is_relative_to(cnpj_root)
    print(f"Original is_relative: {is_rel}")
except Exception as e:
    print(e)
