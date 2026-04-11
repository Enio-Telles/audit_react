"""
Servico reutilizavel para leitura, parse e execucao de SQLs do catalogo local.

Fica em ``utilitarios`` para atender backend e pipeline sem dependencias da
estrutura antiga de ``interface_grafica``.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import polars as pl

from utilitarios.conectar_oracle import conectar
from utilitarios.extrair_parametros import extrair_parametros_sql
from utilitarios.sql_catalog import list_sql_entries, resolve_sql_path


WIDGET_DATE = "date"
WIDGET_TEXT = "text"


@dataclass
class ParamInfo:
    name: str
    widget_type: str = WIDGET_TEXT
    placeholder: str = ""


@dataclass
class SqlFileInfo:
    sql_id: str
    path: str
    display_name: str
    source_dir: str


class SqlService:
    def list_sql_files(self) -> list[SqlFileInfo]:
        return [
            SqlFileInfo(
                sql_id=entry.sql_id,
                path=str(entry.path),
                display_name=entry.display_name,
                source_dir=entry.source_label,
            )
            for entry in list_sql_entries()
        ]

    @staticmethod
    def read_sql(path_or_id: str) -> str:
        path = resolve_sql_path(path_or_id)
        for encoding in ("utf-8", "latin-1", "cp1252", "iso-8859-1"):
            try:
                return path.read_text(encoding=encoding).strip().rstrip(";")
            except UnicodeDecodeError:
                continue
        raise RuntimeError(f"Nao foi possivel ler o SQL: {path}")

    @staticmethod
    def extract_params(sql: str) -> list[ParamInfo]:
        raw_names = extrair_parametros_sql(sql)
        matches = re.findall(r"(?<!\[):([A-Za-z_]\w*)", sql)
        seen: set[str] = set()
        ordered: list[str] = []
        for name in matches:
            low = name.lower()
            if low in seen or name not in raw_names:
                continue
            seen.add(low)
            ordered.append(name)
        return [
            ParamInfo(
                name=name,
                widget_type=SqlService._infer_widget_type(name),
                placeholder=SqlService._infer_placeholder(name),
            )
            for name in ordered
        ]

    @staticmethod
    def _infer_widget_type(name: str) -> str:
        low = name.lower()
        if low.startswith(("data_", "dt_", "date_")) or low in {"data_limite_processamento"}:
            return WIDGET_DATE
        return WIDGET_TEXT

    @staticmethod
    def _infer_placeholder(name: str) -> str:
        low = name.lower()
        if "cnpj" in low:
            return "Somente digitos"
        if low.startswith(("data_", "dt_")):
            return "DD/MM/AAAA"
        return ""

    @staticmethod
    def build_binds(sql: str, values: dict[str, Any]) -> dict[str, Any]:
        provided = {k.lower(): v for k, v in values.items()}
        binds: dict[str, Any] = {}
        matches = re.findall(r"(?<!\[):([A-Za-z_]\w*)", sql)
        seen: set[str] = set()
        for name in matches:
            low = name.lower()
            if low in seen:
                continue
            seen.add(low)
            binds[name] = provided.get(low)
        return binds

    @staticmethod
    def construir_dataframe_resultado(registros: list[dict[str, Any]]) -> pl.DataFrame:
        if not registros:
            return pl.DataFrame()
        try:
            return pl.DataFrame(registros, infer_schema_length=None)
        except pl.exceptions.ComputeError:
            normalizados = SqlService._normalizar_registros_com_tipos_mistos(registros)
            return pl.DataFrame(normalizados, infer_schema_length=None)

    @staticmethod
    def _normalizar_registros_com_tipos_mistos(
        registros: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        colunas_mistas: set[str] = set()
        tipos_por_coluna: dict[str, set[type[Any]]] = {}
        for registro in registros:
            for coluna, valor in registro.items():
                if valor is None:
                    continue
                tipos = tipos_por_coluna.setdefault(coluna, set())
                tipos.add(type(valor))
                if len(tipos) > 1:
                    colunas_mistas.add(coluna)

        if not colunas_mistas:
            return registros

        saida: list[dict[str, Any]] = []
        for registro in registros:
            normalizado = dict(registro)
            for coluna in colunas_mistas:
                valor = normalizado.get(coluna)
                if valor is not None:
                    normalizado[coluna] = str(valor)
            saida.append(normalizado)
        return saida

    @staticmethod
    def executar_sql(
        sql: str,
        params: dict[str, Any] | None = None,
        cnpj: str | None = None,
    ) -> list[dict[str, Any]]:
        values = dict(params or {})
        if cnpj and not any(str(k).lower() == "cnpj" for k in values):
            values["CNPJ"] = cnpj
        binds = SqlService.build_binds(sql, values)

        conn = conectar()
        if conn is None:
            raise RuntimeError("Nao foi possivel estabelecer conexao com o Oracle.")

        try:
            with conn.cursor() as cursor:
                if binds:
                    cursor.execute(sql, binds)
                else:
                    cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description or []]
                rows = cursor.fetchall()
            if not rows:
                return []
            registros = [dict(zip(columns, row)) for row in rows]
            return SqlService.construir_dataframe_resultado(registros).to_dicts()
        finally:
            conn.close()
