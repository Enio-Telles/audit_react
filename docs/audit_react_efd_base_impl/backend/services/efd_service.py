"""
Serviço canônico do domínio EFD.

Este módulo centraliza leitura de datasets EFD materializados, paginação,
manifest simplificado, comparação entre períodos, árvore documental e
proveniência por linha. A ideia é manter a borda HTTP fina e deixar a
maior parte das regras de exploração aqui.

Diretrizes:
- Ler preferencialmente Parquet materializado nas camadas raw/base/marts.
- Não consultar Oracle diretamente.
- Aplicar filtros cedo em LazyFrame.
- Expor metadados mínimos de rastreabilidade para a UI.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import polars as pl


@dataclass(frozen=True)
class EfdRecordConfig:
    record: str
    title: str
    layer_preference: Sequence[str]
    dataset_candidates: Sequence[str]
    key_candidates: Sequence[str]
    period_candidates: Sequence[str]
    cnpj_candidates: Sequence[str]
    upstream: Sequence[str]
    description: str


EFD_RECORDS: dict[str, EfdRecordConfig] = {
    "reg_0000": EfdRecordConfig(
        record="reg_0000",
        title="Registro 0000",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__arquivos_validos", "raw__efd__reg_0000"),
        key_candidates=("id_arquivo", "arquivo_id", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=(),
        description="Abertura do arquivo EFD, identificação do contribuinte e período.",
    ),
    "reg_0190": EfdRecordConfig(
        record="reg_0190",
        title="Registro 0190",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__reg_0190_tipado", "raw__efd__reg_0190"),
        key_candidates=("cod_unid", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("raw__efd__reg_0190",),
        description="Unidades de medida declaradas na EFD.",
    ),
    "reg_0200": EfdRecordConfig(
        record="reg_0200",
        title="Registro 0200",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__reg_0200_tipado", "raw__efd__reg_0200"),
        key_candidates=("cod_item", "produto_id", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("raw__efd__reg_0200",),
        description="Cadastro de itens/produtos da EFD.",
    ),
    "reg_0220": EfdRecordConfig(
        record="reg_0220",
        title="Registro 0220",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__reg_0220_tipado", "raw__efd__reg_0220"),
        key_candidates=("cod_item", "cod_unid_conv", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("raw__efd__reg_0220",),
        description="Fatores de conversão de unidade dentro da EFD.",
    ),
    "c100": EfdRecordConfig(
        record="c100",
        title="Registro C100",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__reg_c100_tipado", "raw__efd__reg_c100"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "num_doc", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_doc"),
        cnpj_candidates=("cnpj", "cnpj_emit", "cnpj_raiz"),
        upstream=("base__efd__arquivos_validos", "raw__efd__reg_c100"),
        description="Cabeçalho dos documentos fiscais da EFD.",
    ),
    "c170": EfdRecordConfig(
        record="c170",
        title="Registro C170",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__reg_c170_tipado", "raw__efd__reg_c170"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "num_item", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("base__efd__reg_c100_tipado", "raw__efd__reg_c170"),
        description="Itens dos documentos fiscais da EFD.",
    ),
    "c190": EfdRecordConfig(
        record="c190",
        title="Registro C190",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__reg_c190_tipado", "raw__efd__reg_c190"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "cfop", "cst_icms", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("base__efd__reg_c100_tipado", "raw__efd__reg_c190"),
        description="Resumo analítico de ICMS por documento na EFD.",
    ),
    "c176": EfdRecordConfig(
        record="c176",
        title="Registro C176",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__reg_c176_tipado", "raw__efd__reg_c176"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "num_item", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("base__efd__reg_c170_tipado", "raw__efd__reg_c176"),
        description="Relações de ressarcimento ST dentro da EFD.",
    ),
    "c197": EfdRecordConfig(
        record="c197",
        title="Registro C197",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__reg_c197_tipado", "raw__efd__reg_c197"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "cod_aj", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("base__efd__reg_c100_tipado", "raw__efd__reg_c197"),
        description="Ajustes e informações complementares do documento na EFD.",
    ),
    "h005": EfdRecordConfig(
        record="h005",
        title="Registro H005",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__bloco_h_tipado", "raw__efd__reg_h005"),
        key_candidates=("dt_inv", "id_inventario", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_inv"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("raw__efd__reg_h005", "raw__efd__reg_h010", "raw__efd__reg_h020"),
        description="Cabeçalho de inventário do Bloco H.",
    ),
    "h010": EfdRecordConfig(
        record="h010",
        title="Registro H010",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__bloco_h_tipado", "raw__efd__reg_h010"),
        key_candidates=("cod_item", "dt_inv", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_inv"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("raw__efd__reg_h010",),
        description="Itens do inventário do Bloco H.",
    ),
    "h020": EfdRecordConfig(
        record="h020",
        title="Registro H020",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__bloco_h_tipado", "raw__efd__reg_h020"),
        key_candidates=("cod_item", "dt_inv", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_inv"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("raw__efd__reg_h020",),
        description="Complemento fiscal do inventário do Bloco H.",
    ),
    "k200": EfdRecordConfig(
        record="k200",
        title="Registro K200",
        layer_preference=("base", "raw"),
        dataset_candidates=("base__efd__reg_k200_tipado", "raw__efd__reg_k200"),
        key_candidates=("cod_item", "dt_est", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_est"),
        cnpj_candidates=("cnpj", "cnpj_raiz"),
        upstream=("raw__efd__reg_k200",),
        description="Estoque escriturado no Bloco K.",
    ),
}


FIELD_DICTIONARY: dict[str, list[dict[str, str]]] = {
    "reg_0000": [
        {"field": "cnpj", "label": "CNPJ", "description": "CNPJ do contribuinte."},
        {"field": "dt_ini", "label": "Data inicial", "description": "Início do período da EFD."},
        {"field": "dt_fin", "label": "Data final", "description": "Fim do período da EFD."},
        {"field": "ind_perfil", "label": "Perfil", "description": "Perfil de apresentação da EFD."},
    ],
    "reg_0190": [
        {"field": "cod_unid", "label": "Unidade", "description": "Código da unidade de medida."},
        {"field": "descr", "label": "Descrição", "description": "Descrição da unidade declarada."},
    ],
    "reg_0200": [
        {"field": "cod_item", "label": "Código do item", "description": "Identificador do item no cadastro da EFD."},
        {"field": "descr_item", "label": "Descrição", "description": "Descrição principal do item."},
        {"field": "unid_inv", "label": "Unidade inventário", "description": "Unidade padrão declarada."},
        {"field": "cod_ncm", "label": "NCM", "description": "Classificação fiscal NCM."},
    ],
    "reg_0220": [
        {"field": "cod_item", "label": "Código do item", "description": "Item do cadastro 0200."},
        {"field": "unid_conv", "label": "Unidade convertida", "description": "Unidade alternativa."},
        {"field": "fat_conv", "label": "Fator", "description": "Fator de conversão da unidade."},
    ],
    "c100": [
        {"field": "chv_nfe", "label": "Chave da NF-e", "description": "Chave de acesso do documento fiscal."},
        {"field": "dt_doc", "label": "Data do documento", "description": "Data de emissão/entrada."},
        {"field": "cod_mod", "label": "Modelo", "description": "Modelo do documento fiscal."},
        {"field": "vl_doc", "label": "Valor total", "description": "Valor total do documento."},
    ],
    "c170": [
        {"field": "num_item", "label": "Número do item", "description": "Sequencial do item no documento."},
        {"field": "cod_item", "label": "Código do item", "description": "Código do produto escriturado."},
        {"field": "qtd", "label": "Quantidade", "description": "Quantidade escriturada."},
        {"field": "vl_item", "label": "Valor do item", "description": "Valor total do item."},
    ],
    "c190": [
        {"field": "cfop", "label": "CFOP", "description": "Código Fiscal de Operações."},
        {"field": "cst_icms", "label": "CST ICMS", "description": "Situação tributária do ICMS."},
        {"field": "vl_opr", "label": "Valor operação", "description": "Valor da operação."},
    ],
    "c176": [
        {"field": "num_item", "label": "Número do item", "description": "Item vinculado ao documento."},
        {"field": "chv_nfe_ult_e", "label": "Chave entrada", "description": "Chave da última entrada."},
        {"field": "vl_ult_e", "label": "Valor última entrada", "description": "Valor unitário da última entrada."},
    ],
    "c197": [
        {"field": "cod_aj", "label": "Código do ajuste", "description": "Código de ajuste/apuração."},
        {"field": "descr_compl_aj", "label": "Descrição", "description": "Descrição complementar do ajuste."},
        {"field": "vl_bc_icms", "label": "Base ICMS", "description": "Base de cálculo do ajuste."},
    ],
    "h005": [
        {"field": "dt_inv", "label": "Data inventário", "description": "Data de referência do inventário."},
        {"field": "vl_inv", "label": "Valor inventário", "description": "Valor total do inventário."},
    ],
    "h010": [
        {"field": "cod_item", "label": "Código do item", "description": "Item inventariado."},
        {"field": "qtd", "label": "Quantidade", "description": "Quantidade inventariada."},
        {"field": "vl_item", "label": "Valor do item", "description": "Valor do item em inventário."},
    ],
    "h020": [
        {"field": "cst_icms", "label": "CST ICMS", "description": "Situação tributária do inventário."},
        {"field": "bc_icms", "label": "Base ICMS", "description": "Base de cálculo do imposto no inventário."},
    ],
    "k200": [
        {"field": "dt_est", "label": "Data estoque", "description": "Data do saldo escriturado."},
        {"field": "cod_item", "label": "Código do item", "description": "Item em estoque."},
        {"field": "qtd", "label": "Quantidade", "description": "Quantidade em estoque declarada."},
    ],
}


class EfdService:
    def __init__(self, data_root: str | Path = "dados") -> None:
        self.data_root = Path(data_root)

    # ------------------------------------------------------------------
    # Paths / manifests
    # ------------------------------------------------------------------
    def list_records(self) -> list[dict[str, Any]]:
        return [
            {
                "record": cfg.record,
                "title": cfg.title,
                "description": cfg.description,
                "upstream": list(cfg.upstream),
                "dataset_candidates": list(cfg.dataset_candidates),
            }
            for cfg in EFD_RECORDS.values()
        ]

    def get_dictionary(self, record: str) -> list[dict[str, str]]:
        return FIELD_DICTIONARY.get(record.lower(), [])

    def get_manifest(self, record: str, cnpj: str | None = None) -> dict[str, Any]:
        cfg = self._get_config(record)
        discovered = []
        for dataset_id in cfg.dataset_candidates:
            item = self._resolve_dataset(dataset_id=dataset_id, cnpj=cnpj)
            if item is not None:
                discovered.append(item)
        return {
            "record": cfg.record,
            "title": cfg.title,
            "description": cfg.description,
            "upstream": list(cfg.upstream),
            "datasets": discovered,
            "dictionary_fields": len(self.get_dictionary(record)),
        }

    # ------------------------------------------------------------------
    # Dataset reading
    # ------------------------------------------------------------------
    def read_record(
        self,
        record: str,
        cnpj: str | None = None,
        periodo: str | None = None,
        filters: Mapping[str, Any] | None = None,
        columns: Sequence[str] | None = None,
        page: int = 1,
        page_size: int = 200,
        prefer_layer: str | None = None,
    ) -> dict[str, Any]:
        cfg = self._get_config(record)
        dataset = self._choose_dataset(cfg, cnpj=cnpj, prefer_layer=prefer_layer)
        lf = self._scan_dataset(dataset["path"])
        lf = self._apply_common_filters(lf, cfg, cnpj=cnpj, periodo=periodo, filters=filters)

        if columns:
            valid_columns = [col for col in columns if col in lf.collect_schema().names()]
            if valid_columns:
                lf = lf.select(valid_columns)

        total = int(lf.select(pl.len().alias("__count")).collect()["__count"][0])
        offset = max(page - 1, 0) * page_size
        page_lf = lf.slice(offset, page_size)
        df = page_lf.collect()

        return {
            "record": cfg.record,
            "dataset_id": dataset["dataset_id"],
            "layer": dataset["layer"],
            "path": str(dataset["path"]),
            "page": page,
            "page_size": page_size,
            "total": total,
            "columns": df.columns,
            "records": df.to_dicts(),
            "provenance": {
                "upstream": list(cfg.upstream),
                "periodo": periodo,
                "cnpj": cnpj,
            },
        }

    def compare_periods(
        self,
        record: str,
        cnpj: str,
        periodo_a: str,
        periodo_b: str,
        limit: int = 200,
        key_field: str | None = None,
    ) -> dict[str, Any]:
        cfg = self._get_config(record)
        dataset = self._choose_dataset(cfg, cnpj=cnpj)

        lf = self._scan_dataset(dataset["path"])
        lf = self._apply_common_filters(lf, cfg, cnpj=cnpj)

        schema_names = lf.collect_schema().names()
        periodo_col = self._pick_existing(schema_names, cfg.period_candidates)
        if periodo_col is None:
            raise ValueError(f"Dataset {dataset['dataset_id']} não possui coluna de período reconhecida.")

        key_col = key_field or self._pick_existing(schema_names, cfg.key_candidates)
        if key_col is None:
            raise ValueError(f"Dataset {dataset['dataset_id']} não possui chave reconhecida para comparação.")

        left = (
            lf.filter(pl.col(periodo_col).cast(pl.Utf8) == str(periodo_a))
            .select([pl.col(key_col).alias("__key"), *[pl.col(c) for c in schema_names if c != key_col]])
            .with_columns(pl.lit(periodo_a).alias("__periodo_ref"))
        )
        right = (
            lf.filter(pl.col(periodo_col).cast(pl.Utf8) == str(periodo_b))
            .select([pl.col(key_col).alias("__key"), *[pl.col(c) for c in schema_names if c != key_col]])
            .with_columns(pl.lit(periodo_b).alias("__periodo_ref"))
        )

        left_df = left.collect()
        right_df = right.collect()

        left_keys = set(left_df["__key"].to_list()) if "__key" in left_df.columns else set()
        right_keys = set(right_df["__key"].to_list()) if "__key" in right_df.columns else set()

        added_keys = list(right_keys - left_keys)[:limit]
        removed_keys = list(left_keys - right_keys)[:limit]
        same_keys = list(left_keys & right_keys)[:limit]

        return {
            "record": cfg.record,
            "dataset_id": dataset["dataset_id"],
            "periodo_a": periodo_a,
            "periodo_b": periodo_b,
            "key_field": key_col,
            "summary": {
                "count_a": len(left_keys),
                "count_b": len(right_keys),
                "added": len(right_keys - left_keys),
                "removed": len(left_keys - right_keys),
                "intersection": len(left_keys & right_keys),
            },
            "sample": {
                "added_keys": added_keys,
                "removed_keys": removed_keys,
                "intersection_keys": same_keys,
            },
        }

    def build_document_tree(
        self,
        cnpj: str,
        periodo: str | None = None,
        chave_documento: str | None = None,
        limit_docs: int = 50,
    ) -> dict[str, Any]:
        c100_cfg = self._get_config("c100")
        c170_cfg = self._get_config("c170")
        c190_cfg = self._get_config("c190")
        c176_cfg = self._get_config("c176")
        c197_cfg = self._get_config("c197")

        c100_dataset = self._choose_dataset(c100_cfg, cnpj=cnpj)
        c170_dataset = self._choose_dataset(c170_cfg, cnpj=cnpj)
        c190_dataset = self._choose_dataset(c190_cfg, cnpj=cnpj)
        c176_dataset = self._choose_dataset(c176_cfg, cnpj=cnpj)
        c197_dataset = self._choose_dataset(c197_cfg, cnpj=cnpj)

        lf_c100 = self._apply_common_filters(self._scan_dataset(c100_dataset["path"]), c100_cfg, cnpj=cnpj, periodo=periodo)
        lf_c170 = self._apply_common_filters(self._scan_dataset(c170_dataset["path"]), c170_cfg, cnpj=cnpj, periodo=periodo)
        lf_c190 = self._apply_common_filters(self._scan_dataset(c190_dataset["path"]), c190_cfg, cnpj=cnpj, periodo=periodo)
        lf_c176 = self._apply_common_filters(self._scan_dataset(c176_dataset["path"]), c176_cfg, cnpj=cnpj, periodo=periodo)
        lf_c197 = self._apply_common_filters(self._scan_dataset(c197_dataset["path"]), c197_cfg, cnpj=cnpj, periodo=periodo)

        doc_key = self._pick_existing(lf_c100.collect_schema().names(), ("chv_nfe", "chave_nfe", "id_doc", "num_doc"))
        if doc_key is None:
            raise ValueError("Não foi possível identificar a chave documental em C100.")

        if chave_documento:
            lf_c100 = lf_c100.filter(pl.col(doc_key).cast(pl.Utf8) == str(chave_documento))

        c100_df = lf_c100.slice(0, limit_docs).collect()
        if c100_df.is_empty():
            return {"documents": [], "doc_key": doc_key}

        doc_values = [str(x) for x in c100_df[doc_key].to_list()]
        c170_df = self._filter_by_doc_values(lf_c170, doc_values).collect()
        c190_df = self._filter_by_doc_values(lf_c190, doc_values).collect()
        c176_df = self._filter_by_doc_values(lf_c176, doc_values).collect()
        c197_df = self._filter_by_doc_values(lf_c197, doc_values).collect()

        trees = []
        for row in c100_df.to_dicts():
            doc_value = str(row.get(doc_key, ""))
            trees.append(
                {
                    "document": row,
                    "items_c170": [r for r in c170_df.to_dicts() if str(self._doc_value_from_row(r)) == doc_value],
                    "summary_c190": [r for r in c190_df.to_dicts() if str(self._doc_value_from_row(r)) == doc_value],
                    "links_c176": [r for r in c176_df.to_dicts() if str(self._doc_value_from_row(r)) == doc_value],
                    "adjustments_c197": [r for r in c197_df.to_dicts() if str(self._doc_value_from_row(r)) == doc_value],
                }
            )

        return {"doc_key": doc_key, "documents": trees}

    def row_provenance(
        self,
        record: str,
        cnpj: str | None,
        row_identifier: str,
        key_field: str | None = None,
        prefer_layer: str | None = None,
    ) -> dict[str, Any]:
        cfg = self._get_config(record)
        dataset = self._choose_dataset(cfg, cnpj=cnpj, prefer_layer=prefer_layer)
        lf = self._scan_dataset(dataset["path"])
        if cnpj:
            lf = self._apply_common_filters(lf, cfg, cnpj=cnpj)

        schema_names = lf.collect_schema().names()
        chosen_key = key_field or self._pick_existing(schema_names, cfg.key_candidates)
        if chosen_key is None:
            raise ValueError(f"Nenhuma chave disponível para {record}.")

        row_df = lf.filter(pl.col(chosen_key).cast(pl.Utf8) == str(row_identifier)).slice(0, 1).collect()
        row = row_df.to_dicts()[0] if not row_df.is_empty() else None

        return {
            "record": cfg.record,
            "dataset_id": dataset["dataset_id"],
            "layer": dataset["layer"],
            "path": str(dataset["path"]),
            "key_field": chosen_key,
            "row_identifier": row_identifier,
            "upstream": list(cfg.upstream),
            "row": row,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _get_config(self, record: str) -> EfdRecordConfig:
        key = record.lower()
        if key not in EFD_RECORDS:
            raise KeyError(f"Registro EFD não suportado: {record}")
        return EFD_RECORDS[key]

    def _resolve_dataset(self, dataset_id: str, cnpj: str | None = None) -> dict[str, Any] | None:
        layer = dataset_id.split("__", 1)[0]
        domain_and_name = dataset_id.split("__", 1)[1] if "__" in dataset_id else dataset_id
        # Preferência por contribuinte se existir estrutura dados/CNPJ/<cnpj>/<layer>/...
        candidates: list[Path] = []
        if cnpj:
            candidates.append(self.data_root / "CNPJ" / cnpj / layer / "efd" / domain_and_name)
            candidates.append(self.data_root / "CNPJ" / cnpj / layer / domain_and_name)
        candidates.append(self.data_root / layer / "efd" / domain_and_name)
        candidates.append(self.data_root / layer / domain_and_name)

        for path in candidates:
            if path.exists():
                return {"dataset_id": dataset_id, "layer": layer, "path": path}
        return None

    def _choose_dataset(
        self,
        cfg: EfdRecordConfig,
        cnpj: str | None = None,
        prefer_layer: str | None = None,
    ) -> dict[str, Any]:
        ordered = list(cfg.dataset_candidates)
        if prefer_layer:
            ordered = sorted(ordered, key=lambda ds: 0 if ds.startswith(f"{prefer_layer}__") else 1)
        for dataset_id in ordered:
            item = self._resolve_dataset(dataset_id=dataset_id, cnpj=cnpj)
            if item is not None:
                return item
        raise FileNotFoundError(f"Nenhum dataset encontrado para {cfg.record}: {cfg.dataset_candidates}")

    def _scan_dataset(self, path: Path) -> pl.LazyFrame:
        if path.is_dir():
            parquet_files = sorted(path.rglob("*.parquet"))
            if parquet_files:
                return pl.scan_parquet([str(p) for p in parquet_files])
            # fallback: permitir delta-like ou parquet particionado em diretório
            return pl.scan_parquet(str(path / "**" / "*.parquet"))
        return pl.scan_parquet(str(path))

    def _apply_common_filters(
        self,
        lf: pl.LazyFrame,
        cfg: EfdRecordConfig,
        cnpj: str | None = None,
        periodo: str | None = None,
        filters: Mapping[str, Any] | None = None,
    ) -> pl.LazyFrame:
        schema_names = lf.collect_schema().names()

        if cnpj:
            cnpj_col = self._pick_existing(schema_names, cfg.cnpj_candidates)
            if cnpj_col:
                lf = lf.filter(pl.col(cnpj_col).cast(pl.Utf8) == str(cnpj))

        if periodo:
            periodo_col = self._pick_existing(schema_names, cfg.period_candidates)
            if periodo_col:
                lf = lf.filter(pl.col(periodo_col).cast(pl.Utf8) == str(periodo))

        if filters:
            for field, value in filters.items():
                if field in schema_names and value is not None and value != "":
                    lf = lf.filter(pl.col(field).cast(pl.Utf8) == str(value))
        return lf

    def _pick_existing(self, columns: Sequence[str], candidates: Sequence[str]) -> str | None:
        lowered = {c.lower(): c for c in columns}
        for candidate in candidates:
            if candidate.lower() in lowered:
                return lowered[candidate.lower()]
        return None

    def _doc_value_from_row(self, row: Mapping[str, Any]) -> Any:
        for key in ("chv_nfe", "chave_nfe", "id_doc", "num_doc"):
            if key in row and row[key] not in (None, ""):
                return row[key]
        return None

    def _filter_by_doc_values(self, lf: pl.LazyFrame, doc_values: Sequence[str]) -> pl.LazyFrame:
        schema_names = lf.collect_schema().names()
        key = self._pick_existing(schema_names, ("chv_nfe", "chave_nfe", "id_doc", "num_doc"))
        if key is None:
            return lf.slice(0, 0)
        return lf.filter(pl.col(key).cast(pl.Utf8).is_in([str(v) for v in doc_values]))
