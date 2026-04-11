from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from utilitarios.delta_lake import resolve_storage_format, write_delta_table
from utilitarios.schema_registry import SchemaRegistry


def _safe_print(message: str) -> None:
    try:
        print(message)
    except UnicodeEncodeError:
        print(message.encode("ascii", errors="replace").decode("ascii"))


def salvar_para_parquet(
    df,
    caminho_saida: Path,
    nome_arquivo: str = None,
    schema=None,
    metadata: dict = None,
    formato: str | None = None,
    mode: str | None = None,
    partition_by: list[str] | None = None,
) -> bool:
    """
    Exporta um DataFrame/LazyFrame para Parquet ou Delta Lake.
    """
    try:
        delta_mode = mode or os.getenv("DELTA_WRITE_MODE", "overwrite")

        if nome_arquivo:
            if (formato or os.getenv("DATA_LAKE_FORMAT", "parquet")).lower() == "parquet" and not str(nome_arquivo).lower().endswith(".parquet"):
                nome_arquivo = f"{nome_arquivo}.parquet"
            arquivo = caminho_saida / nome_arquivo
        else:
            arquivo = caminho_saida

        arquivo.parent.mkdir(parents=True, exist_ok=True)
        formato_resolvido = resolve_storage_format(arquivo, formato)

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        if df.is_empty():
            _safe_print(f"   [!] Aviso: o DataFrame a ser salvo em {arquivo.name} esta vazio.")

        if formato_resolvido == "delta":
            if metadata:
                _safe_print("   [!] Aviso: metadata por coluna nao eh aplicada diretamente no formato Delta Lake.")
            destino_delta = write_delta_table(
                df,
                arquivo,
                mode=delta_mode,
                partition_by=partition_by,
                table_name=arquivo.stem,
            )
            _safe_print(f"   [OK] Delta Lake salvo com sucesso: {destino_delta}")
            return True

        if schema or metadata:
            table = df.to_arrow()

            if schema:
                try:
                    table = table.cast(schema)
                except Exception as e_schema:
                    _safe_print(
                        f"   [!] Aviso de schema: falha ao impor schema estrito em {arquivo.name}: {e_schema}"
                    )

            if metadata:
                new_fields = []
                for field in table.schema:
                    if field.name in metadata:
                        desc_value = str(metadata[field.name]).encode("utf-8")
                        new_meta = {
                            **(field.metadata or {}),
                            b"description": desc_value,
                            b"comment": desc_value,
                        }
                        new_fields.append(field.with_metadata(new_meta))
                    else:
                        new_fields.append(field)

                table = pa.Table.from_batches(
                    table.to_batches(),
                    pa.schema(new_fields, metadata=table.schema.metadata),
                )

            pq.write_table(table, arquivo, compression="snappy")
        else:
            df.write_parquet(arquivo, compression="snappy")

        registry = SchemaRegistry()
        registry.record_schema(
            arquivo.stem,
            df.schema,
            source_path=str(arquivo),
            metadata={"format": "parquet"},
        )

        _safe_print(f"   [OK] Parquet salvo com sucesso: {arquivo.name}")
        return True

    except Exception as e:
        nome = arquivo.name if "arquivo" in locals() else str(nome_arquivo or caminho_saida)
        if "arquivo" in locals() and formato_resolvido == "parquet":
            try:
                arquivo.unlink(missing_ok=True)
            except OSError:
                pass
        _safe_print(f"   [ERRO] Erro ao salvar arquivo {formato_resolvido.upper()} {nome}: {e}")
        return False
