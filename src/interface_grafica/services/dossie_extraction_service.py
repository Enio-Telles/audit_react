from __future__ import annotations

from pathlib import Path
from typing import Any
import asyncio
import hashlib
import json
import time

import polars as pl
from fastapi import HTTPException

from interface_grafica.services.dossie_cache_keys import VERSAO_PADRAO_DOSSIE
from interface_grafica.services.dossie_dataset_reuse import criar_metadata_dataset_compartilhado
from interface_grafica.services.dossie_dataset_reuse import DatasetCompartilhadoDossie
from interface_grafica.services.dossie_dataset_reuse import carregar_dataset_reutilizavel
from interface_grafica.services.dossie_dataset_reuse import salvar_dataset_compartilhado
from interface_grafica.services.dossie_dataset_reuse import obter_caminho_dataset_compartilhado
from interface_grafica.services.dossie_resolution import resolver_secao_dossie
from interface_grafica.services.dossie_section_builder import compor_secao_dossie
from interface_grafica.services.sql_service import SqlService
from utilitarios.project_paths import CNPJ_ROOT
from utilitarios.salvar_para_parquet import salvar_para_parquet


def obter_caminho_cache_dossie(cnpj: str, cache_file_name: str) -> Path:
    """Retorna o caminho canonico do cache parquet do dossie."""

    return CNPJ_ROOT / str(cnpj).strip() / "arquivos_parquet" / "dossie" / cache_file_name


def obter_caminho_metadata_cache_dossie(cnpj: str, cache_file_name: str) -> Path:
    """Retorna o sidecar JSON associado ao parquet final da secao do Dossie."""

    return obter_caminho_cache_dossie(cnpj, cache_file_name).with_suffix(".metadata.json")


def obter_caminho_historico_comparacao_contato(cnpj: str) -> Path:
    """Retorna o arquivo JSONL com o historico de comparacoes do contato por CNPJ."""

    return CNPJ_ROOT / str(cnpj).strip() / "arquivos_parquet" / "dossie" / f"historico_comparacao_contato_{str(cnpj).strip()}.jsonl"


def _montar_parametros_execucao(cnpj: str, parametros: dict[str, Any] | None = None) -> dict[str, Any]:
    """Prepara binds equivalentes para SQLs legadas com nomes diferentes."""

    cnpj_limpo = "".join(caractere for caractere in str(cnpj or "") if caractere.isdigit())
    parametros_reais = {
        "CNPJ": cnpj_limpo,
        "cnpj": cnpj_limpo,
        "CO_CNPJ_CPF": cnpj_limpo,
        "co_cnpj_cpf": cnpj_limpo,
    }
    if parametros:
        parametros_reais.update(parametros)
    return parametros_reais


def _deve_usar_sql_contato_consolidado(secao_id: str, parametros: dict[str, Any] | None = None) -> bool:
    """Ativa o SQL consolidado apenas quando solicitado explicitamente."""

    if secao_id != "contato" or not parametros:
        return False
    return bool(parametros.get("usar_sql_consolidado"))


def _resolver_sql_ids_efetivos(
    secao_id: str,
    sql_ids_resolvidos: tuple[str, ...],
    parametros: dict[str, Any] | None = None,
) -> tuple[str, ...]:
    """Define quais SQLs serao executados sem alterar o contrato padrao da resolucao."""

    if _deve_usar_sql_contato_consolidado(secao_id, parametros):
        return ("dossie_contato.sql",)
    return sql_ids_resolvidos


def _identificar_estrategia_execucao(secao_id: str, sql_ids_efetivos: tuple[str, ...]) -> str:
    """Resume a estrategia usada para auditoria do sync por secao."""

    if secao_id == "contato" and sql_ids_efetivos == ("dossie_contato.sql",):
        return "sql_consolidado"
    if len(sql_ids_efetivos) > 1:
        return "composicao_polars"
    return "sql_direto"


def _classificar_impacto_cache_first(total_sql_ids: int, sql_ids_reutilizados: list[str], cache_reutilizado: bool) -> str:
    """Resume o impacto da camada cache-first no sync da secao."""

    if cache_reutilizado:
        return "cache_canonico_equivalente"
    if total_sql_ids <= 0:
        return "indefinido"
    if len(sql_ids_reutilizados) == total_sql_ids:
        return "reuso_total"
    if len(sql_ids_reutilizados) > 0:
        return "reuso_parcial"
    return "sem_reuso"


def _salvar_metadata_secao_dossie(cnpj: str, cache_file_name: str, metadata: dict[str, Any]) -> Path | None:
    """Persiste o metadata sidecar do parquet final da secao materializada."""

    caminho_metadata = obter_caminho_metadata_cache_dossie(cnpj, cache_file_name)
    try:
        caminho_metadata.parent.mkdir(parents=True, exist_ok=True)
        caminho_metadata.write_text(json.dumps(metadata, ensure_ascii=True, indent=2), encoding="utf-8")
    except OSError:
        return None
    return caminho_metadata


def _registrar_historico_comparacao_contato(cnpj: str, registro: dict[str, Any]) -> Path | None:
    """Acumula em JSONL os eventos de comparacao entre estrategias do contato."""

    caminho_historico = obter_caminho_historico_comparacao_contato(cnpj)
    try:
        caminho_historico.parent.mkdir(parents=True, exist_ok=True)
        with caminho_historico.open("a", encoding="utf-8") as arquivo:
            arquivo.write(json.dumps(registro, ensure_ascii=True) + "\n")
    except OSError:
        return None
    return caminho_historico


def _calcular_assinatura_conteudo_secao(dataframe: pl.DataFrame) -> str:
    """Gera uma assinatura deterministica para evitar persistencia redundante."""

    payload = {
        "columns": dataframe.columns,
        "rows": dataframe.to_dicts(),
    }
    conteudo = json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(conteudo).hexdigest()


def _carregar_metadata_secao_dossie(caminho_arquivo: Path) -> dict[str, Any] | None:
    """Carrega o sidecar de uma secao do Dossie quando existir."""

    caminho_metadata = caminho_arquivo.with_suffix(".metadata.json")
    if not caminho_metadata.exists():
        return None

    try:
        return json.loads(caminho_metadata.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _reaproveitar_cache_canonico_equivalente(
    cnpj: str,
    secao_id: str,
    cache_file_name_destino: str,
    assinatura_conteudo: str,
    estrategia_execucao: str,
    sql_principal: str | None,
    cache_key_requisitado: str,
) -> tuple[Path, dict[str, Any]] | None:
    """Reutiliza um cache canonico equivalente da mesma secao quando ele ja existir."""

    caminho_destino = obter_caminho_cache_dossie(cnpj, cache_file_name_destino)
    for caminho_candidato in _listar_caches_secao_dossie(cnpj, secao_id):
        if caminho_candidato == caminho_destino:
            continue

        metadata_candidato = _carregar_metadata_secao_dossie(caminho_candidato)
        if not metadata_candidato:
            continue
        if metadata_candidato.get("estrategia_execucao") != estrategia_execucao:
            continue
        if metadata_candidato.get("sql_principal") != sql_principal:
            continue
        if metadata_candidato.get("assinatura_conteudo") != assinatura_conteudo:
            continue

        cache_keys_equivalentes = list(metadata_candidato.get("cache_keys_equivalentes") or [])
        cache_key_original = metadata_candidato.get("cache_key")
        if cache_key_original and cache_key_original not in cache_keys_equivalentes:
            cache_keys_equivalentes.append(cache_key_original)
        if cache_key_requisitado not in cache_keys_equivalentes:
            cache_keys_equivalentes.append(cache_key_requisitado)

        metadata_candidato["cache_keys_equivalentes"] = cache_keys_equivalentes
        metadata_candidato["ultima_reutilizacao_cache"] = cache_key_requisitado
        _salvar_metadata_secao_dossie(cnpj, caminho_candidato.name, metadata_candidato)
        return caminho_candidato, metadata_candidato

    return None


def _listar_caches_secao_dossie(cnpj: str, secao_id: str) -> list[Path]:
    """Lista caches parquet da secao para comparacoes entre estrategias."""

    pasta_dossie = CNPJ_ROOT / str(cnpj).strip() / "arquivos_parquet" / "dossie"
    if not pasta_dossie.exists():
        return []

    slug_secao = "_".join(
        parte
        for parte in str(secao_id).strip().lower().replace("/", " ").replace("-", " ").split()
        if parte
    )
    return sorted(
        pasta_dossie.glob(f"dossie_{str(cnpj).strip()}_{slug_secao}_*.parquet"),
        key=lambda caminho: caminho.stat().st_mtime,
        reverse=True,
    )


def _localizar_cache_contato_estrategia_alternativa(
    cnpj: str,
    caminho_cache_atual: Path,
    estrategia_execucao: str,
) -> tuple[Path, dict[str, Any]] | None:
    """Busca o cache mais recente da estrategia oposta para comparar contratos."""

    if estrategia_execucao not in {"sql_consolidado", "composicao_polars"}:
        return None

    for caminho_candidato in _listar_caches_secao_dossie(cnpj, "contato"):
        if caminho_candidato == caminho_cache_atual:
            continue
        metadata_candidato = _carregar_metadata_secao_dossie(caminho_candidato)
        if not metadata_candidato:
            continue
        estrategia_candidata = metadata_candidato.get("estrategia_execucao")
        if estrategia_candidata and estrategia_candidata != estrategia_execucao:
            return caminho_candidato, metadata_candidato
    return None


def _comparar_contrato_contato_com_estrategia_alternativa(
    caminho_cache_atual: Path,
    metadata_atual: dict[str, Any],
) -> dict[str, Any] | None:
    """Compara o contrato basico do contato com a ultima estrategia alternativa disponivel."""

    estrategia_execucao = str(metadata_atual.get("estrategia_execucao") or "")
    cnpj = str(metadata_atual.get("cnpj") or "")
    referencia = _localizar_cache_contato_estrategia_alternativa(cnpj, caminho_cache_atual, estrategia_execucao)
    if referencia is None:
        return None

    caminho_referencia, metadata_referencia = referencia
    dataframe_atual = pl.scan_parquet(caminho_cache_atual).collect()
    dataframe_referencia = pl.scan_parquet(caminho_referencia).collect()

    colunas_atuais = list(dataframe_atual.columns)
    colunas_referencia = list(dataframe_referencia.columns)
    tipos_atuais = sorted(
        dataframe_atual.get_column("tipo_vinculo").drop_nulls().cast(pl.Utf8, strict=False).unique().to_list()
    ) if "tipo_vinculo" in dataframe_atual.columns else []
    tipos_referencia = sorted(
        dataframe_referencia.get_column("tipo_vinculo").drop_nulls().cast(pl.Utf8, strict=False).unique().to_list()
    ) if "tipo_vinculo" in dataframe_referencia.columns else []

    mesma_ordem_colunas = colunas_atuais == colunas_referencia
    mesma_quantidade_linhas = dataframe_atual.height == dataframe_referencia.height
    mesmos_tipos_vinculo = tipos_atuais == tipos_referencia
    chaves_atuais = _extrair_chaves_funcionais_contato(dataframe_atual)
    chaves_referencia = _extrair_chaves_funcionais_contato(dataframe_referencia)
    campos_criticos_atuais = _resumir_preenchimento_campos_contato(dataframe_atual)
    campos_criticos_referencia = _resumir_preenchimento_campos_contato(dataframe_referencia)
    chaves_faltantes = sorted(chaves_referencia - chaves_atuais)
    chaves_extras = sorted(chaves_atuais - chaves_referencia)
    mesma_chave_funcional = chaves_atuais == chaves_referencia

    return {
        "estrategia_referencia": metadata_referencia.get("estrategia_execucao"),
        "cache_file_referencia": str(caminho_referencia),
        "sql_principal_referencia": metadata_referencia.get("sql_principal"),
        "mesma_ordem_colunas": mesma_ordem_colunas,
        "mesma_quantidade_linhas": mesma_quantidade_linhas,
        "mesmos_tipos_vinculo": mesmos_tipos_vinculo,
        "mesma_chave_funcional": mesma_chave_funcional,
        "linhas_referencia": int(dataframe_referencia.height),
        "tipos_vinculo_referencia": tipos_referencia,
        "amostra_chaves_faltantes": chaves_faltantes[:10],
        "amostra_chaves_extras": chaves_extras[:10],
        "quantidade_chaves_faltantes": len(chaves_faltantes),
        "quantidade_chaves_extras": len(chaves_extras),
        "campos_criticos_atual": campos_criticos_atuais,
        "campos_criticos_referencia": campos_criticos_referencia,
        "convergencia_basica": mesma_ordem_colunas and mesma_quantidade_linhas and mesmos_tipos_vinculo,
        "convergencia_funcional": mesma_chave_funcional,
    }


def _extrair_chaves_funcionais_contato(dataframe: pl.DataFrame) -> set[str]:
    """Gera a chave funcional minima para comparar registros entre estrategias."""

    if dataframe.is_empty():
        return set()

    colunas_chave = [
        coluna
        for coluna in ("tipo_vinculo", "cpf_cnpj_referencia", "nome_referencia", "email", "telefone_nfe_nfce")
        if coluna in dataframe.columns
    ]
    if not colunas_chave:
        return set()

    chaves: set[str] = set()
    for linha in dataframe.select(colunas_chave).to_dicts():
        partes = [str(linha.get(coluna) or "").strip() for coluna in colunas_chave]
        chaves.add("|".join(partes))
    return chaves


def _resumir_preenchimento_campos_contato(dataframe: pl.DataFrame) -> dict[str, int]:
    """Conta quantos registros possuem preenchimento em campos criticos do contato."""

    resumo: dict[str, int] = {}
    for coluna in ("telefone", "telefone_nfe_nfce", "email", "endereco", "tabela_origem"):
        if coluna not in dataframe.columns:
            resumo[coluna] = 0
            continue
        resumo[coluna] = int(
            dataframe.select(
                pl.col(coluna)
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                .ne("")
                .sum()
            ).item()
        )
    return resumo


def _executar_sql_ou_reutilizar(
    cnpj: str,
    sql_id: str,
    parametros: dict[str, Any] | None = None,
    versao_consulta: str | None = None,
) -> tuple[DatasetCompartilhadoDossie, bool]:
    """Carrega o dataset de uma SQL por reuso e executa Oracle apenas quando necessario."""

    try:
        dataset_reutilizado = carregar_dataset_reutilizavel(cnpj, sql_id, parametros=parametros)
    except TypeError as exc:
        if "parametros" not in str(exc):
            raise
        dataset_reutilizado = carregar_dataset_reutilizavel(cnpj, sql_id)
    if dataset_reutilizado is not None and not _deve_materializar_shared_sql_atual(cnpj, sql_id, dataset_reutilizado):
        return dataset_reutilizado, False

    sql_texto = SqlService.read_sql(sql_id)
    parametros_reais = _montar_parametros_execucao(cnpj, parametros)
    linhas_dict = SqlService.executar_sql(sql_texto, params=parametros_reais, cnpj=cnpj)
    dataframe = SqlService.construir_dataframe_resultado(linhas_dict) if linhas_dict else pl.DataFrame()
    metadata = criar_metadata_dataset_compartilhado(
        cnpj=cnpj,
        sql_id=sql_id,
        parametros=parametros_reais,
        versao_consulta=versao_consulta,
    )
    caminho_compartilhado = salvar_dataset_compartilhado(cnpj, sql_id, dataframe, metadata=metadata)

    return (
        DatasetCompartilhadoDossie(
            sql_id=sql_id,
            dataframe=dataframe,
            caminho_origem=caminho_compartilhado,
            reutilizado=False,
            metadata=metadata,
        ),
        True,
    )


def _deve_materializar_shared_sql_atual(
    cnpj: str,
    sql_id: str,
    dataset_reutilizado: DatasetCompartilhadoDossie,
) -> bool:
    """Forca a criacao do shared SQL atual quando o reuso vier apenas de legado fiscal."""

    if dataset_reutilizado.caminho_origem is None:
        return False

    sql_id_normalizado = str(sql_id).strip().lower()
    if sql_id_normalizado not in {"nfe.sql", "nfce.sql"}:
        return False

    return dataset_reutilizado.caminho_origem != obter_caminho_dataset_compartilhado(cnpj, sql_id)


def executar_sync_secao_sync(cnpj: str, secao_id: str, parametros: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Sincroniza proativamente uma secao do dossie:
    1. Resolve SQLs e chave de cache.
    2. Reutiliza datasets compartilhados quando existirem.
    3. Executa Oracle apenas para SQLs ausentes.
    4. Compoe o resultado final em Polars.
    5. Salva a secao em cache canonico.
    """

    inicio_total = time.perf_counter()
    resolucao = resolver_secao_dossie(
        cnpj=cnpj,
        secao_id=secao_id,
        parametros=parametros,
        versao_consulta=VERSAO_PADRAO_DOSSIE,
    )
    sql_ids_efetivos = _resolver_sql_ids_efetivos(
        secao_id=resolucao.secao_id,
        sql_ids_resolvidos=resolucao.sql_ids,
        parametros=parametros,
    )
    estrategia_execucao = _identificar_estrategia_execucao(resolucao.secao_id, sql_ids_efetivos)
    if not sql_ids_efetivos:
        raise ValueError(f"Nenhum SQL mapeado para a secao {secao_id}")

    datasets_por_sql: dict[str, pl.DataFrame] = {}
    sql_ids_executados: list[str] = []
    sql_ids_reutilizados: list[str] = []

    for sql_id in sql_ids_efetivos:
        try:
            dataset, executou_oracle = _executar_sql_ou_reutilizar(
                cnpj,
                sql_id,
                parametros,
                versao_consulta=VERSAO_PADRAO_DOSSIE,
            )
        except Exception as exc:
            raise ValueError(f"Erro ao obter dataset da consulta {sql_id}: {exc}") from exc

        datasets_por_sql[sql_id] = dataset.dataframe
        if executou_oracle:
            sql_ids_executados.append(sql_id)
        else:
            sql_ids_reutilizados.append(sql_id)

    inicio_materializacao = time.perf_counter()
    dataframe_secao = compor_secao_dossie(cnpj=cnpj, secao_id=secao_id, datasets=datasets_por_sql)
    caminho_arquivo = obter_caminho_cache_dossie(cnpj, resolucao.cache_file_name)
    tempo_materializacao_ms = int((time.perf_counter() - inicio_materializacao) * 1000)
    total_sql_ids = len(sql_ids_efetivos)
    percentual_reuso_sql = round((len(sql_ids_reutilizados) / total_sql_ids) * 100, 2) if total_sql_ids else 0.0
    metadata_secao = {
        "cnpj": str(cnpj).strip(),
        "secao_id": secao_id,
        "cache_key": resolucao.cache_key,
        "versao_consulta": VERSAO_PADRAO_DOSSIE,
        "estrategia_execucao": estrategia_execucao,
        "sql_principal": sql_ids_efetivos[0] if sql_ids_efetivos else None,
        "sql_ids": list(sql_ids_efetivos),
        "sql_ids_executados": sql_ids_executados,
        "sql_ids_reutilizados": sql_ids_reutilizados,
        "total_sql_ids": total_sql_ids,
        "percentual_reuso_sql": percentual_reuso_sql,
        "linhas_extraidas": int(dataframe_secao.height),
        "assinatura_conteudo": _calcular_assinatura_conteudo_secao(dataframe_secao),
        "tempo_materializacao_ms": tempo_materializacao_ms,
    }
    cache_equivalente = _reaproveitar_cache_canonico_equivalente(
        cnpj=cnpj,
        secao_id=secao_id,
        cache_file_name_destino=resolucao.cache_file_name,
        assinatura_conteudo=metadata_secao["assinatura_conteudo"],
        estrategia_execucao=estrategia_execucao,
        sql_principal=metadata_secao["sql_principal"],
        cache_key_requisitado=resolucao.cache_key,
    )
    cache_reutilizado = cache_equivalente is not None
    if cache_equivalente is not None:
        caminho_arquivo, metadata_existente = cache_equivalente
        metadata_secao["cache_file"] = str(caminho_arquivo)
        metadata_secao["cache_reutilizado"] = True
        metadata_secao["cache_keys_equivalentes"] = list(metadata_existente.get("cache_keys_equivalentes") or [])
    else:
        sucesso = salvar_para_parquet(
            dataframe_secao,
            caminho_saida=caminho_arquivo.parent,
            nome_arquivo=caminho_arquivo.name,
        )
        if not sucesso:
            raise RuntimeError("Erro ao persistir o resultado no diretorio cache.")
        metadata_secao["cache_file"] = str(caminho_arquivo)
        metadata_secao["cache_reutilizado"] = False
        metadata_secao["cache_keys_equivalentes"] = [resolucao.cache_key]

    metadata_secao["impacto_cache_first"] = _classificar_impacto_cache_first(
        total_sql_ids=total_sql_ids,
        sql_ids_reutilizados=sql_ids_reutilizados,
        cache_reutilizado=bool(metadata_secao["cache_reutilizado"]),
    )

    if secao_id == "contato":
        comparacao = _comparar_contrato_contato_com_estrategia_alternativa(caminho_arquivo, metadata_secao)
        if comparacao is not None:
            metadata_secao["comparacao_estrategia_alternativa"] = comparacao
            caminho_historico = _registrar_historico_comparacao_contato(
                cnpj,
                {
                    "cnpj": str(cnpj).strip(),
                    "cache_file": str(caminho_arquivo),
                    "cache_key": resolucao.cache_key,
                    "estrategia_execucao": estrategia_execucao,
                    "sql_principal": sql_ids_efetivos[0] if sql_ids_efetivos else None,
                    "comparacao_estrategia_alternativa": comparacao,
                },
            )
            if caminho_historico is not None:
                metadata_secao["comparison_history_file"] = str(caminho_historico)

    metadata_secao["tempo_total_sync_ms"] = int((time.perf_counter() - inicio_total) * 1000)
    _salvar_metadata_secao_dossie(cnpj, resolucao.cache_file_name, metadata_secao)

    return {
        "status": "success",
        "cnpj": cnpj,
        "secao_id": secao_id,
        "linhas_extraidas": int(dataframe_secao.height),
        "cache_file": str(caminho_arquivo),
        "cache_key": resolucao.cache_key,
        "cache_reutilizado": cache_reutilizado,
        "impacto_cache_first": metadata_secao["impacto_cache_first"],
        "versao_consulta": VERSAO_PADRAO_DOSSIE,
        "estrategia_execucao": estrategia_execucao,
        "sql_principal": sql_ids_efetivos[0] if sql_ids_efetivos else None,
        "sql_ids": list(sql_ids_efetivos),
        "sql_ids_executados": sql_ids_executados,
        "sql_ids_reutilizados": sql_ids_reutilizados,
        "total_sql_ids": total_sql_ids,
        "percentual_reuso_sql": percentual_reuso_sql,
        "tempo_materializacao_ms": tempo_materializacao_ms,
        "tempo_total_sync_ms": metadata_secao["tempo_total_sync_ms"],
        "comparacao_estrategia_alternativa": metadata_secao.get("comparacao_estrategia_alternativa"),
        "comparison_history_file": metadata_secao.get("comparison_history_file"),
        "metadata_file": str(caminho_arquivo.with_suffix(".metadata.json")),
        "updatedAt": caminho_arquivo.stat().st_mtime if caminho_arquivo.exists() else None,
    }


async def executar_sync_secao(cnpj: str, secao_id: str, parametros: dict[str, Any] | None = None) -> dict[str, Any]:
    """Wrapper assincrono para nao bloquear o event loop do FastAPI."""

    try:
        return await asyncio.to_thread(executar_sync_secao_sync, cnpj, secao_id, parametros)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
