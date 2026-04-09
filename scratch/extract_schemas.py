import polars as pl
import json
from datetime import datetime, date
from pathlib import Path

cnpj = "84654326000394"
base_path = Path(r"c:\Sistema_react\dados\CNPJ") / cnpj
analises_path = base_path / "analises" / "produtos"
parquet_path = base_path / "arquivos_parquet"

files = {
    "produtos_final": analises_path / f"produtos_final_{cnpj}.parquet",
    "fatores_conversao": analises_path / f"fatores_conversao_{cnpj}.parquet",
    "mov_estoque": analises_path / f"mov_estoque_{cnpj}.parquet",
    "aba_mensal": analises_path / f"aba_mensal_{cnpj}.parquet",
    "aba_anual": analises_path / f"aba_anual_{cnpj}.parquet",
    "bloco_h": parquet_path / f"bloco_h_{cnpj}.parquet"
}

results = {}

for name, path in files.items():
    if path.exists():
        df = pl.read_parquet(path, n_rows=5)
        schema = {col: str(dtype) for col, dtype in df.schema.items()}
        sample = df.to_dicts()
        results[name] = {
            "schema": schema,
            "sample": sample
        }
    else:
        results[name] = "File not found"

def serialize(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

print(json.dumps(results, indent=2, default=serialize))
