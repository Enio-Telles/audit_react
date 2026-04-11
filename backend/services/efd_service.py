"""
Servico canônico do dominio EFD.

Centraliza leitura de datasets EFD materializados, manifest por registro,
comparacao entre periodos, arvore documental e proveniencia de linha.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

import polars as pl

from utilitarios.project_paths import DATA_ROOT


@dataclass(frozen=True)
class EfdRecordConfig:
    record: str
    title: str
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
        dataset_candidates=("base__efd__arquivos_validos", "raw__efd__reg_0000", "efd_0000"),
        key_candidates=("id_arquivo", "arquivo_id", "row_id", "cnpj"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_ini"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=(),
        description="Abertura do arquivo EFD, identificacao do contribuinte e periodo.",
    ),
    "reg_0190": EfdRecordConfig(
        record="reg_0190",
        title="Registro 0190",
        dataset_candidates=("base__efd__reg_0190_tipado", "raw__efd__reg_0190", "efd_0190"),
        key_candidates=("cod_unid", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("raw__efd__reg_0190",),
        description="Unidades de medida declaradas na EFD.",
    ),
    "reg_0200": EfdRecordConfig(
        record="reg_0200",
        title="Registro 0200",
        dataset_candidates=("base__efd__reg_0200_tipado", "raw__efd__reg_0200", "efd_0200"),
        key_candidates=("cod_item", "produto_id", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("raw__efd__reg_0200",),
        description="Cadastro de itens/produtos da EFD.",
    ),
    "reg_0220": EfdRecordConfig(
        record="reg_0220",
        title="Registro 0220",
        dataset_candidates=("base__efd__reg_0220_tipado", "raw__efd__reg_0220"),
        key_candidates=("cod_item", "cod_unid_conv", "unid_conv", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("raw__efd__reg_0220",),
        description="Fatores de conversao de unidade dentro da EFD.",
    ),
    "c100": EfdRecordConfig(
        record="c100",
        title="Registro C100",
        dataset_candidates=("base__efd__reg_c100_tipado", "raw__efd__reg_c100", "efd_c100"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "num_doc", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_doc"),
        cnpj_candidates=("cnpj", "cnpj_emit", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("base__efd__arquivos_validos", "raw__efd__reg_c100"),
        description="Cabecalho dos documentos fiscais da EFD.",
    ),
    "c170": EfdRecordConfig(
        record="c170",
        title="Registro C170",
        dataset_candidates=("base__efd__reg_c170_tipado", "raw__efd__reg_c170", "efd_c170", "c170_xml"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "num_item", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("base__efd__reg_c100_tipado", "raw__efd__reg_c170"),
        description="Itens dos documentos fiscais da EFD.",
    ),
    "c190": EfdRecordConfig(
        record="c190",
        title="Registro C190",
        dataset_candidates=("base__efd__reg_c190_tipado", "raw__efd__reg_c190", "efd_c190"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "cfop", "cst_icms", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("base__efd__reg_c100_tipado", "raw__efd__reg_c190"),
        description="Resumo analitico de ICMS por documento na EFD.",
    ),
    "c176": EfdRecordConfig(
        record="c176",
        title="Registro C176",
        dataset_candidates=("base__efd__reg_c176_tipado", "raw__efd__reg_c176", "efd_c176", "c176_xml"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "num_item", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("base__efd__reg_c170_tipado", "raw__efd__reg_c176"),
        description="Relacoes de ressarcimento ST dentro da EFD.",
    ),
    "c197": EfdRecordConfig(
        record="c197",
        title="Registro C197",
        dataset_candidates=("base__efd__reg_c197_tipado", "raw__efd__reg_c197", "c197_legacy"),
        key_candidates=("chv_nfe", "chave_nfe", "id_doc", "cod_aj", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("base__efd__reg_c100_tipado", "raw__efd__reg_c197"),
        description="Ajustes e informacoes complementares do documento na EFD.",
    ),
    "h005": EfdRecordConfig(
        record="h005",
        title="Registro H005",
        dataset_candidates=("base__efd__bloco_h_tipado", "raw__efd__reg_h005", "bloco_h"),
        key_candidates=("dt_inv", "id_inventario", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_inv"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("raw__efd__reg_h005", "raw__efd__reg_h010", "raw__efd__reg_h020"),
        description="Cabecalho de inventario do Bloco H.",
    ),
    "h010": EfdRecordConfig(
        record="h010",
        title="Registro H010",
        dataset_candidates=("base__efd__bloco_h_tipado", "raw__efd__reg_h010", "bloco_h"),
        key_candidates=("cod_item", "dt_inv", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_inv"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("raw__efd__reg_h010",),
        description="Itens do inventario do Bloco H.",
    ),
    "h020": EfdRecordConfig(
        record="h020",
        title="Registro H020",
        dataset_candidates=("base__efd__bloco_h_tipado", "raw__efd__reg_h020", "bloco_h"),
        key_candidates=("cod_item", "dt_inv", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_inv"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("raw__efd__reg_h020",),
        description="Complemento fiscal do inventario do Bloco H.",
    ),
    "k200": EfdRecordConfig(
        record="k200",
        title="Registro K200",
        dataset_candidates=("base__efd__reg_k200_tipado", "raw__efd__reg_k200", "k200_legacy"),
        key_candidates=("cod_item", "dt_est", "row_id"),
        period_candidates=("periodo", "mes_ref", "aaaamm", "dt_est"),
        cnpj_candidates=("cnpj", "cnpj_raiz", "co_cnpj_cpf"),
        upstream=("raw__efd__reg_k200",),
        description="Estoque escriturado no Bloco K.",
    ),
}


FIELD_DICTIONARY: dict[str, list[dict[str, str]]] = {
    "reg_0000": [
        {"field": "cnpj", "label": "CNPJ", "description": "CNPJ do contribuinte."},
        {"field": "dt_ini", "label": "Data inicial", "description": "Inicio do periodo da EFD."},
        {"field": "dt_fin", "label": "Data final", "description": "Fim do periodo da EFD."},
        {"field": "ind_perfil", "label": "Perfil", "description": "Perfil de apresentacao da EFD."},
    ],
    "reg_0190": [
        {"field": "cod_unid", "label": "Unidade", "description": "Codigo da unidade de medida."},
        {"field": "descr", "label": "Descricao", "description": "Descricao da unidade declarada."},
    ],
    "reg_0200": [
        {"field": "cod_item", "label": "Codigo do item", "description": "Identificador do item no cadastro da EFD."},
        {"field": "descr_item", "label": "Descricao", "description": "Descricao principal do item."},
        {"field": "unid_inv", "label": "Unidade inventario", "description": "Unidade padrao declarada."},
        {"field": "cod_ncm", "label": "NCM", "description": "Classificacao fiscal NCM."},
    ],
    "reg_0220": [
        {"field": "cod_item", "label": "Codigo do item", "description": "Item do cadastro 0200."},
        {"field": "unid_conv", "label": "Unidade convertida", "description": "Unidade alternativa."},
        {"field": "fat_conv", "label": "Fator", "description": "Fator de conversao da unidade."},
    ],
    "c100": [
        {"field": "chv_nfe", "label": "Chave da NF-e", "description": "Chave de acesso do documento fiscal."},
        {"field": "dt_doc", "label": "Data do documento", "description": "Data de emissao ou entrada."},
        {"field": "cod_mod", "label": "Modelo", "description": "Modelo do documento fiscal."},
        {"field": "vl_doc", "label": "Valor total", "description": "Valor total do documento."},
    ],
    "c170": [
        {"field": "num_item", "label": "Numero do item", "description": "Sequencial do item no documento."},
        {"field": "cod_item", "label": "Codigo do item", "description": "Codigo do produto escriturado."},
        {"field": "qtd", "label": "Quantidade", "description": "Quantidade escriturada."},
        {"field": "vl_item", "label": "Valor do item", "description": "Valor total do item."},
    ],
    "c190": [
        {"field": "cfop", "label": "CFOP", "description": "Codigo Fiscal de Operacoes."},
        {"field": "cst_icms", "label": "CST ICMS", "description": "Situacao tributaria do ICMS."},
        {"field": "vl_opr", "label": "Valor operacao", "description": "Valor da operacao."},
    ],
    "c176": [
        {"field": "num_item", "label": "Numero do item", "description": "Item vinculado ao documento."},
        {"field": "chv_nfe_ult_e", "label": "Chave entrada", "description": "Chave da ultima entrada."},
        {"field": "vl_ult_e", "label": "Valor ultima entrada", "description": "Valor unitario da ultima entrada."},
    ],
    "c197": [
        {"field": "cod_aj", "label": "Codigo do ajuste", "description": "Codigo de ajuste ou apuracao."},
        {"field": "descr_compl_aj", "label": "Descricao", "description": "Descricao complementar do ajuste."},
        {"field": "vl_bc_icms", "label": "Base ICMS", "description": "Base de calculo do ajuste."},
    ],
    "h005": [
        {"field": "dt_inv", "label": "Data inventario", "description": "Data de referencia do inventario."},
        {"field": "vl_inv", "label": "Valor inventario", "description": "Valor total do inventario."},
    ],
    "h010": [
        {"field": "cod_item", "label": "Codigo do item", "description": "Item inventariado."},
        {"field": "qtd", "label": "Quantidade", "description": "Quantidade inventariada."},
        {"field": "vl_item", "label": "Valor do item", "description": "Valor do item em inventario."},
    ],
    "h020": [
        {"field": "cst_icms", "label": "CST ICMS", "description": "Situacao tributaria do inventario."},
        {"field": "bc_icms", "label": "Base ICMS", "description": "Base de calculo do imposto no inventario."},
    ],
    "k200": [
        {"field": "dt_est", "label": "Data estoque", "description": "Data do saldo escriturado."},
        {"field": "cod_item", "label": "Codigo do item", "description": "Item em estoque."},
        {"field": "qtd", "label": "Quantidade", "description": "Quantidade em estoque declarada."},
    ],
}


LEGACY_DATASET_PATTERNS: dict[str, tuple[str, ...]] = {
    "efd_0000": ("CNPJ/{cnpj}/arquivos_parquet/reg_0000_{cnpj}.parquet",),
    "efd_0190": ("CNPJ/{cnpj}/arquivos_parquet/reg_0190_{cnpj}.parquet",),
    "efd_0200": ("CNPJ/{cnpj}/arquivos_parquet/reg_0200_{cnpj}.parquet",),
    "efd_c100": ("CNPJ/{cnpj}/arquivos_parquet/c100_{cnpj}.parquet",),
    "efd_c170": ("CNPJ/{cnpj}/arquivos_parquet/c170_{cnpj}.parquet",),
    "efd_c176": ("CNPJ/{cnpj}/arquivos_parquet/c176_{cnpj}.parquet",),
    "efd_c190": ("CNPJ/{cnpj}/arquivos_parquet/c190_{cnpj}.parquet",),
    "c170_xml": (
        "CNPJ/{cnpj}/analises/produtos/c170_xml_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/c170_xml_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/fiscal/efd/c170_xml_{cnpj}.parquet",
    ),
    "c176_xml": (
        "CNPJ/{cnpj}/analises/produtos/c176_xml_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/c176_xml_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/fiscal/efd/c176_xml_{cnpj}.parquet",
    ),
    "c197_legacy": (
        "CNPJ/{cnpj}/arquivos_parquet/c197_agrupado_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/c197_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/fiscal/efd/c197_agrupado_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/fiscal/efd/c197_{cnpj}.parquet",
    ),
    "bloco_h": (
        "CNPJ/{cnpj}/analises/produtos/bloco_h_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/bloco_h_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/fiscal/efd/bloco_h_{cnpj}.parquet",
    ),
    "k200_legacy": (
        "CNPJ/{cnpj}/arquivos_parquet/k200_{cnpj}.parquet",
        "CNPJ/{cnpj}/arquivos_parquet/fiscal/efd/k200_{cnpj}.parquet",
        "CNPJ/{cnpj}/analises/produtos/k200_{cnpj}.parquet",
    ),
}


class EfdService:
    def __init__(self, data_root: str | Path = DATA_ROOT) -> None:
        self.data_root = Path(data_root)

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
        discovered: list[dict[str, Any]] = []
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
        lf = self._scan_dataset(Path(dataset["path"]))
        lf = self._apply_common_filters(lf, cfg, cnpj=cnpj, periodo=periodo, filters=filters)

        schema_names = lf.collect_schema().names()
        if columns:
            valid_columns = [col for col in columns if col in schema_names]
            if valid_columns:
                lf = lf.select(valid_columns)

        total = int(lf.select(pl.len().alias("__count")).collect()["__count"][0])
        offset = max(page - 1, 0) * page_size
        df = lf.slice(offset, page_size).collect()

        return {
            "record": cfg.record,
            "dataset_id": dataset["dataset_id"],
            "layer": dataset["layer"],
            "path": str(dataset["path"]),
            "page": page,
            "page_size": page_size,
            "total": total,
            "columns": df.columns,
            "records": [_safe_dict(row) for row in df.to_dicts()],
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
        lf = self._scan_dataset(Path(dataset["path"]))
        lf = self._apply_common_filters(lf, cfg, cnpj=cnpj)

        schema_names = lf.collect_schema().names()
        periodo_col = self._pick_existing(schema_names, cfg.period_candidates)
        if periodo_col is None:
            raise ValueError(f"Dataset {dataset['dataset_id']} nao possui coluna de periodo reconhecida.")

        key_col = key_field or self._pick_existing(schema_names, cfg.key_candidates)
        if key_col is None:
            raise ValueError(f"Dataset {dataset['dataset_id']} nao possui chave reconhecida para comparacao.")

        left_df = (
            lf.filter(pl.col(periodo_col).cast(pl.Utf8) == str(periodo_a))
            .select(pl.col(key_col).cast(pl.Utf8).alias("__key"))
            .collect()
        )
        right_df = (
            lf.filter(pl.col(periodo_col).cast(pl.Utf8) == str(periodo_b))
            .select(pl.col(key_col).cast(pl.Utf8).alias("__key"))
            .collect()
        )

        left_keys = set(left_df["__key"].drop_nulls().to_list()) if "__key" in left_df.columns else set()
        right_keys = set(right_df["__key"].drop_nulls().to_list()) if "__key" in right_df.columns else set()

        added_keys = sorted(right_keys - left_keys)[:limit]
        removed_keys = sorted(left_keys - right_keys)[:limit]
        same_keys = sorted(left_keys & right_keys)[:limit]

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
        c100_dataset = self._choose_dataset(c100_cfg, cnpj=cnpj)
        lf_c100 = self._apply_common_filters(
            self._scan_dataset(Path(c100_dataset["path"])),
            c100_cfg,
            cnpj=cnpj,
            periodo=periodo,
        )

        doc_key = self._pick_existing(lf_c100.collect_schema().names(), c100_cfg.key_candidates)
        if doc_key is None:
            raise ValueError("Nao foi possivel identificar a chave documental em C100.")

        if chave_documento:
            lf_c100 = lf_c100.filter(pl.col(doc_key).cast(pl.Utf8) == str(chave_documento))

        c100_df = lf_c100.slice(0, limit_docs).collect()
        if c100_df.is_empty():
            return {"doc_key": doc_key, "documents": []}

        doc_values = [str(value) for value in c100_df[doc_key].drop_nulls().to_list()]

        trees = []
        c170_rows = self._collect_rows_by_doc("c170", cnpj, periodo, doc_values)
        c190_rows = self._collect_rows_by_doc("c190", cnpj, periodo, doc_values)
        c176_rows = self._collect_rows_by_doc("c176", cnpj, periodo, doc_values)
        c197_rows = self._collect_rows_by_doc("c197", cnpj, periodo, doc_values)

        for row in c100_df.to_dicts():
            doc_value = str(row.get(doc_key, ""))
            trees.append(
                {
                    "document": _safe_dict(row),
                    "items_c170": [item for item in c170_rows if str(self._doc_value_from_row(item)) == doc_value],
                    "summary_c190": [item for item in c190_rows if str(self._doc_value_from_row(item)) == doc_value],
                    "links_c176": [item for item in c176_rows if str(self._doc_value_from_row(item)) == doc_value],
                    "adjustments_c197": [item for item in c197_rows if str(self._doc_value_from_row(item)) == doc_value],
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
        lf = self._scan_dataset(Path(dataset["path"]))
        if cnpj:
            lf = self._apply_common_filters(lf, cfg, cnpj=cnpj)

        schema_names = lf.collect_schema().names()
        chosen_key = key_field or self._pick_existing(schema_names, cfg.key_candidates)
        if chosen_key is None:
            raise ValueError(f"Nenhuma chave disponivel para {record}.")

        row_df = lf.filter(pl.col(chosen_key).cast(pl.Utf8) == str(row_identifier)).slice(0, 1).collect()
        row = _safe_dict(row_df.to_dicts()[0]) if not row_df.is_empty() else None

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

    def _get_config(self, record: str) -> EfdRecordConfig:
        key = record.lower()
        if key not in EFD_RECORDS:
            raise KeyError(f"Registro EFD nao suportado: {record}")
        return EFD_RECORDS[key]

    def _resolve_dataset(self, dataset_id: str, cnpj: str | None = None) -> dict[str, Any] | None:
        layer = dataset_id.split("__", 1)[0] if "__" in dataset_id else "legacy"
        for candidate in self._candidate_paths(dataset_id, cnpj):
            if candidate.exists():
                return {"dataset_id": dataset_id, "layer": layer, "path": candidate}
        return None

    def _candidate_paths(self, dataset_id: str, cnpj: str | None) -> list[Path]:
        candidates: list[Path] = []
        if "__" in dataset_id:
            layer, remainder = dataset_id.split("__", 1)
            base_name = dataset_id.replace(f"{layer}__efd__", "").replace(f"{layer}__", "")
            scoped: list[Path] = []
            if cnpj:
                scoped.extend(
                    [
                        self.data_root / "CNPJ" / cnpj / layer / "efd" / base_name,
                        self.data_root / "CNPJ" / cnpj / layer / "efd" / f"{base_name}.parquet",
                        self.data_root / "CNPJ" / cnpj / layer / "efd" / dataset_id,
                        self.data_root / "CNPJ" / cnpj / layer / "efd" / f"{dataset_id}.parquet",
                        self.data_root / "CNPJ" / cnpj / layer / base_name,
                        self.data_root / "CNPJ" / cnpj / layer / f"{base_name}.parquet",
                    ]
                )
            scoped.extend(
                [
                    self.data_root / layer / "efd" / base_name,
                    self.data_root / layer / "efd" / f"{base_name}.parquet",
                    self.data_root / layer / "efd" / dataset_id,
                    self.data_root / layer / "efd" / f"{dataset_id}.parquet",
                ]
            )
            candidates.extend(scoped)

        if cnpj and dataset_id in LEGACY_DATASET_PATTERNS:
            for pattern in LEGACY_DATASET_PATTERNS[dataset_id]:
                candidates.append(self.data_root / pattern.format(cnpj=cnpj))

        return self._deduplicate_paths(candidates)

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
            if not parquet_files:
                raise FileNotFoundError(f"Nenhum parquet encontrado em {path}")
            return pl.scan_parquet([str(item) for item in parquet_files])
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
                if field in schema_names and value not in (None, ""):
                    lf = lf.filter(
                        pl.col(field).cast(pl.Utf8, strict=False).fill_null("").str.contains(str(value), literal=True)
                    )
        return lf

    def _pick_existing(self, columns: Sequence[str], candidates: Sequence[str]) -> str | None:
        lowered = {col.lower(): col for col in columns}
        for candidate in candidates:
            if candidate.lower() in lowered:
                return lowered[candidate.lower()]
        return None

    def _doc_value_from_row(self, row: Mapping[str, Any]) -> Any:
        for key in ("chv_nfe", "chave_nfe", "id_doc", "num_doc"):
            if row.get(key) not in (None, ""):
                return row[key]
        return None

    def _collect_rows_by_doc(
        self,
        record: str,
        cnpj: str,
        periodo: str | None,
        doc_values: Sequence[str],
    ) -> list[dict[str, Any]]:
        cfg = self._get_config(record)
        dataset = self._resolve_first_available(cfg, cnpj)
        if dataset is None:
            return []

        lf = self._apply_common_filters(self._scan_dataset(Path(dataset["path"])), cfg, cnpj=cnpj, periodo=periodo)
        schema_names = lf.collect_schema().names()
        doc_key = self._pick_existing(schema_names, ("chv_nfe", "chave_nfe", "id_doc", "num_doc"))
        if doc_key is None:
            return []

        df = lf.filter(pl.col(doc_key).cast(pl.Utf8).is_in([str(v) for v in doc_values])).collect()
        return [_safe_dict(row) for row in df.to_dicts()]

    def _resolve_first_available(self, cfg: EfdRecordConfig, cnpj: str | None) -> dict[str, Any] | None:
        for dataset_id in cfg.dataset_candidates:
            item = self._resolve_dataset(dataset_id, cnpj=cnpj)
            if item is not None:
                return item
        return None

    def _deduplicate_paths(self, paths: Sequence[Path]) -> list[Path]:
        seen: set[str] = set()
        unique: list[Path] = []
        for path in paths:
            key = str(path).lower()
            if key in seen:
                continue
            seen.add(key)
            unique.append(path)
        return unique


def _safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, list):
        return [_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _safe_value(item) for key, item in value.items()}
    return value


def _safe_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _safe_value(value) for key, value in row.items()}
