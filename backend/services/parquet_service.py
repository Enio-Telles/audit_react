"""
Servico de manipulação de arquivos Parquet para o backend FastAPI.

Esta versao desacopla o backend da antiga `interface_grafica` e mantém apenas
o essencial para listagem, filtros, paginação e leitura seletiva.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl

from utilitarios.project_paths import CNPJ_ROOT

_ANALISES_PREFIXOS_PERMITIDOS = (
    "tb_documentos_",
    "item_unidades_",
    "itens_",
    "descricao_produtos_",
    "produtos_agrupados_",
    "map_produto_agrupado_",
    "produtos_final_",
    "c170_agr_",
    "bloco_h_agr_",
    "nfe_agr_",
    "nfce_agr_",
    "fatores_conversao_",
    "log_sem_preco_medio_compra_",
    "mov_estoque_",
    "c176_xml_",
    "aba_mensal_",
    "aba_anual_",
    "calculos_mensais_",
    "calculos_anuais_",
)


@dataclass
class FilterCondition:
    column: str
    operator: str
    value: str = ""


@dataclass
class PageResult:
    total_rows: int
    df_all_columns: pl.DataFrame
    df_visible: pl.DataFrame
    columns: list[str]
    visible_columns: list[str]


class ParquetService:
    def __init__(self, root: Path = CNPJ_ROOT) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def list_cnpjs(self) -> list[str]:
        if not self.root.exists():
            return []
        return sorted(
            p.name
            for p in self.root.iterdir()
            if p.is_dir() and p.name.isdigit() and len(p.name) >= 11
        )

    def cnpj_dir(self, cnpj: str) -> Path:
        return self.root / cnpj

    def list_parquet_files(self, cnpj: str) -> list[Path]:
        base = self.cnpj_dir(cnpj)
        if not base.exists():
            return []

        brutos = base / "arquivos_parquet"
        analises = base / "analises" / "produtos"
        old_prod = base / "produtos"

        files: list[Path] = []
        if brutos.exists():
            files.extend(brutos.glob("*.parquet"))
        if analises.exists():
            files.extend(analises.glob("*.parquet"))
        if old_prod.exists():
            files.extend(old_prod.glob("*.parquet"))
        files.extend(base.glob("*.parquet"))

        filtrados: list[Path] = []
        for path in set(files):
            parent_str = str(path.parent)
            if "arquivos_parquet" in parent_str:
                if any(
                    tag in path.name
                    for tag in ("_produtos_", "_enriquecido_", "_sem_id_agrupado_")
                ):
                    continue
                filtrados.append(path)
                continue
            if "analises" in parent_str or "produtos" in parent_str:
                if path.name.startswith(_ANALISES_PREFIXOS_PERMITIDOS):
                    filtrados.append(path)
                continue
            filtrados.append(path)

        return sorted(filtrados, key=lambda p: (str(p.parent), p.name))

    def get_schema(self, parquet_path: Path) -> list[str]:
        return list(pl.read_parquet_schema(parquet_path).names())

    @staticmethod
    def _normalize_operator(op: str) -> str:
        op_l = (op or "").strip().lower()
        if op_l.startswith("cont"):
            return "contem"
        if op_l.startswith("come"):
            return "comeca_com"
        if op_l.startswith("termina"):
            return "termina_com"
        if "nulo" in op_l:
            if "não" in op_l or "nao" in op_l or "nÃ" in op_l:
                return "nao_e_nulo"
            return "e_nulo"
        aliases = {
            "contem": {"contém", "contÃ©m", "contem"},
            "igual": {"igual"},
            "comeca_com": {"começa com", "comeÃ§a com", "comeca com"},
            "termina_com": {"termina com"},
            "maior": {">"},
            "maior_igual": {">="},
            "menor": {"<"},
            "menor_igual": {"<="},
            "e_nulo": {"é nulo", "Ã© nulo", "e nulo"},
            "nao_e_nulo": {"não é nulo", "nÃ£o Ã© nulo", "nao e nulo"},
        }
        for canonical, opts in aliases.items():
            if op_l in opts:
                return canonical
        return op_l

    @staticmethod
    def _is_list_dtype(dtype: object | None) -> bool:
        if dtype is None:
            return False
        try:
            return dtype.base_type() == pl.List
        except Exception:
            return str(dtype).startswith("List")

    def _text_expr(self, column: str, dtype: object | None) -> pl.Expr:
        col = pl.col(column)
        if self._is_list_dtype(dtype):
            return col.cast(pl.List(pl.Utf8), strict=False).list.join(" | ").fill_null("")
        return col.cast(pl.Utf8, strict=False).fill_null("")

    def _build_expr(self, cond: FilterCondition, dtype: object | None = None) -> pl.Expr:
        col = pl.col(cond.column)
        value = cond.value or ""
        op = self._normalize_operator(cond.operator)
        text_col = self._text_expr(cond.column, dtype)

        if op == "contem":
            return text_col.str.to_lowercase().str.contains(value.lower(), literal=True)
        if op == "igual":
            return text_col == value
        if op == "comeca_com":
            return text_col.str.to_lowercase().str.starts_with(value.lower())
        if op == "termina_com":
            return text_col.str.to_lowercase().str.ends_with(value.lower())
        if op == "e_nulo":
            return col.is_null() | (text_col == "")
        if op == "nao_e_nulo":
            return ~(col.is_null() | (text_col == ""))

        numeric_col = col.cast(pl.Float64, strict=False)
        try:
            numeric_value = float(value.replace(",", "."))
        except Exception:
            numeric_value = None

        if op in {"maior", "maior_igual", "menor", "menor_igual"} and numeric_value is not None:
            mapping = {
                "maior": numeric_col > numeric_value,
                "maior_igual": numeric_col >= numeric_value,
                "menor": numeric_col < numeric_value,
                "menor_igual": numeric_col <= numeric_value,
            }
            return mapping[op]

        return text_col == value

    def apply_filters(
        self,
        lf: pl.LazyFrame,
        conditions: Iterable[FilterCondition] | None,
        available_columns: dict[str, object] | None = None,
    ) -> pl.LazyFrame:
        filtered = lf
        if not conditions:
            return filtered
        if available_columns is None:
            schema = filtered.collect_schema()
            available_columns = {name: schema[name] for name in schema.names()}

        for cond in conditions:
            if not cond.column or cond.column not in available_columns:
                continue
            op_norm = self._normalize_operator(cond.operator)
            if op_norm not in {"e_nulo", "nao_e_nulo"} and cond.value == "":
                continue
            filtered = filtered.filter(self._build_expr(cond, available_columns.get(cond.column)))
        return filtered

    def build_lazyframe(
        self,
        parquet_path: Path,
        conditions: Iterable[FilterCondition] | None = None,
    ) -> pl.LazyFrame:
        lf = pl.scan_parquet(parquet_path)
        if conditions:
            schema = pl.read_parquet_schema(parquet_path)
            schema_map = {name: schema[name] for name in schema.names()}
            lf = self.apply_filters(lf, conditions, available_columns=schema_map)
        return lf

    def get_page(
        self,
        parquet_path: Path,
        conditions: list[FilterCondition] | None,
        visible_columns: list[str] | None,
        page: int,
        page_size: int = 200,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> PageResult:
        page = max(page, 1)
        lf_all = self.build_lazyframe(parquet_path, conditions or [])
        all_columns = self.get_schema(parquet_path)
        total_rows = int(lf_all.select(pl.len().alias("n")).collect().item())

        if sort_by and sort_by in all_columns:
            lf_all = lf_all.sort(sort_by, descending=sort_desc)

        offset = (page - 1) * page_size
        df_all = lf_all.slice(offset, page_size).collect()

        if not visible_columns:
            visible_columns = all_columns[:]
        df_visible = df_all.select([c for c in visible_columns if c in df_all.columns])

        return PageResult(
            total_rows=total_rows,
            df_all_columns=df_all,
            df_visible=df_visible,
            columns=all_columns,
            visible_columns=visible_columns,
        )

    paginate = get_page

    def load_dataset(
        self,
        parquet_path: Path,
        conditions: list[FilterCondition] | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        lf = self.build_lazyframe(parquet_path, conditions or [])
        if columns:
            lf = lf.select(columns)
        return lf.collect()

    def save_dataset(self, parquet_path: Path, df: pl.DataFrame) -> None:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(parquet_path)
