import polars as pl
import os
from pathlib import Path

BASE_PATH = Path(r"c:\Sistema_react\dados\referencias\referencias\mapeamento")
KEYWORDS = ["http", "www", "download", ".gov.br", "sefaz", "receita", "sitaf", "ftp", "txt"]

def search_keywords():
    for root, dirs, files in os.walk(BASE_PATH):
        for file in files:
            if file.endswith(".parquet"):
                file_path = Path(root) / file
                try:
                    # Scan headers first to see if it's worth reading
                    df = pl.scan_parquet(file_path)
                    cols = df.collect_schema().names()
                    
                    # Read only relevant columns if they look like strings
                    # For simplicity, we read everything and check for strings
                    df = pl.read_parquet(file_path)
                    
                    for col in df.columns:
                        if df[col].dtype == pl.String:
                            for kw in KEYWORDS:
                                matches = df.filter(df[col].str.to_lowercase().str.contains(kw))
                                if len(matches) > 0:
                                    print(f"Found '{kw}' in {file_path} (column '{col}'):")
                                    # Print unique values that match
                                    unique_matches = matches[col].unique().limit(5).to_list()
                                    for m in unique_matches:
                                        print(f"  -> {m}")
                                    print("-" * 20)
                except Exception as e:
                    pass

if __name__ == "__main__":
    search_keywords()
