from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional
import json

import polars as pl
from fastapi import APIRouter
from fastapi import HTTPException
from pydantic import BaseModel

from services.dossie_service import (
    executar_sync_secao,
    listar_secoes_dossie,
    listar_sql_prioritarias,
    obter_caminho_historico_comparacao_contato,
    obter_secao_dossie,
    resolver_secao_dossie,
)
from utilitarios.project_paths import CNPJ_ROOT

router = APIRouter()


class DossieSectionSummaryResponse(BaseModel):
    id: str
    title: str
    description: str
    sourceType: str
    syncEnabled: bool
    sourceFiles: Optional[list[str]] = None
    status: str
    rowCount: Optional[int] = None
    executionStrategy: Optional[str] = None
    primarySql: Optional[str] = None
    alternateStrategyComparison: Optional[str] = None
    alternateStrategyMissingKeys: Optional[int] = None
    alternateStrategyExtraKeys: Optional[int] = None
    updatedAt: Optional[str] = None


class DossieSectionDataResponse(BaseModel):
    id: str
    title: str
    columns: list[str]
    rows: list[dict]
    rowCount: int
    cacheFile: str
    metadata: Optional[dict] = None
    updatedAt: Optional[str] = None


class DossieComparisonHistoryResponse(BaseModel):
    cnpj: str
    secaoId: str
    items: list[dict]
    historyFile: str


class DossieComparisonSummaryResponse(BaseModel):
    cnpj: str
    secaoId: str
    totalComparacoes: int
    convergenciasFuncionais: int
    divergenciasFuncionais: int
    convergenciasBasicas: int
    divergenciasBasicas: int
    ultimaEstrategia: Optional[str] = None
    ultimaSqlPrincipal: Optional[str] = None
    ultimaEstrategiaReferencia: Optional[str] = None
    ultimaSqlPrincipalReferencia: Optional[str] = None
    ultimoStatusComparacao: Optional[str] = None
    ultimoCacheKey: Optional[str] = None
    ultimoTotalChavesFaltantes: Optional[int] = None
    ultimoTotalChavesExtras: Optional[int] = None
    ultimaAmostraChavesFaltantes: Optional[list[str]] = None
    ultimaAmostraChavesExtras: Optional[list[str]] = None
    ultimosCamposCriticosAtual: Optional[dict[str, int]] = None
    ultimosCamposCriticosReferencia: Optional[dict[str, int]] = None
    updatedAt: Optional[str] = None
    historyFile: str


class DossieComparisonReportResponse(BaseModel):
    cnpj: str
    secaoId: str
    reportFile: str
    updatedAt: Optional[str] = None
    content: str


class SyncDossieRequest(BaseModel):
    parametros: Optional[dict] = None


def normalizar_cnpj(cnpj: str) -> str:
    """Remove caracteres nao numericos para manter compatibilidade com a pasta do CNPJ."""

    return "".join(caractere for caractere in str(cnpj or "") if caractere.isdigit())


def contar_linhas_parquet(caminho_arquivo: Path) -> int:
    """Conta linhas via scan_parquet para evitar carga desnecessaria do arquivo inteiro."""

    return int(pl.scan_parquet(caminho_arquivo).select(pl.len()).collect().item())


def obter_data_atualizacao_arquivo(caminho_arquivo: Path) -> str | None:
    """Retorna a data de modificacao em ISO para facilitar rastreabilidade no frontend."""

    try:
        return datetime.fromtimestamp(caminho_arquivo.stat().st_mtime).isoformat()
    except OSError:
        return None


def carregar_metadata_cache_secao(caminho_arquivo: Path) -> dict | None:
    """Carrega o sidecar JSON do parquet materializado quando ele existir."""

    caminho_metadata = caminho_arquivo.with_suffix(".metadata.json")
    if not caminho_metadata.exists():
        return None

    try:
        return json.loads(caminho_metadata.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def escolher_primeiro_arquivo_existente(candidatos: list[Path]) -> Path | None:
    """Seleciona o primeiro arquivo existente respeitando a ordem de prioridade da secao."""

    for caminho_arquivo in candidatos:
        if caminho_arquivo.exists():
            return caminho_arquivo
    return None


def _normalizar_slug_secao(secao_id: str) -> str:
    """Replica o slug usado no nome do parquet canonico do Dossie."""

    return "_".join(
        parte
        for parte in str(secao_id).strip().lower().replace("/", " ").replace("-", " ").split()
        if parte
    )


def listar_caches_dossie_por_secao(secao_id: str, cnpj: str) -> list[Path]:
    """Lista caches canonicos da secao, inclusive materializacoes com parametros alternativos."""

    pasta_dossie = CNPJ_ROOT / cnpj / "arquivos_parquet" / "dossie"
    if not pasta_dossie.exists():
        return []

    candidatos: list[Path] = []
    try:
        resolucao = resolver_secao_dossie(cnpj=cnpj, secao_id=secao_id)
        candidatos.append(pasta_dossie / resolucao.cache_file_name)
    except Exception:
        pass

    slug_secao = _normalizar_slug_secao(secao_id)
    candidatos.extend(sorted(pasta_dossie.glob(f"dossie_{cnpj}_{slug_secao}_*.parquet")))

    arquivos_existentes = [caminho for caminho in dict.fromkeys(candidatos) if caminho.exists()]
    return sorted(
        arquivos_existentes,
        key=lambda caminho: caminho.stat().st_mtime,
        reverse=True,
    )


def resumir_arquivos_parquet(caminhos_arquivos: list[Path]) -> tuple[int, str | None]:
    """Soma linhas e retorna a data mais recente entre os arquivos realmente utilizados."""

    total_linhas = 0
    datas_atualizacao: list[str] = []
    for caminho_arquivo in caminhos_arquivos:
        total_linhas += contar_linhas_parquet(caminho_arquivo)
        data_atualizacao = obter_data_atualizacao_arquivo(caminho_arquivo)
        if data_atualizacao:
            datas_atualizacao.append(data_atualizacao)
    return total_linhas, max(datas_atualizacao) if datas_atualizacao else None


def obter_arquivos_secao_cadastro(cnpj: str) -> list[Path]:
    return [CNPJ_ROOT / cnpj / "arquivos_parquet" / f"dados_cadastrais_{cnpj}.parquet"]


def obter_arquivos_secao_documentos_fiscais(cnpj: str) -> list[Path]:
    candidatos_nfe = [
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"nfe_agr_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"NFe_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / "fiscal" / "documentos" / f"NFe_{cnpj}.parquet",
    ]
    candidatos_nfce = [
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"nfce_agr_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"NFCe_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / "fiscal" / "documentos" / f"NFCe_{cnpj}.parquet",
    ]
    encontrados = [
        escolher_primeiro_arquivo_existente(candidatos_nfe),
        escolher_primeiro_arquivo_existente(candidatos_nfce),
    ]
    return [caminho for caminho in encontrados if caminho is not None]


def obter_arquivos_secao_arrecadacao(cnpj: str) -> list[Path]:
    candidatos = [
        CNPJ_ROOT / cnpj / "arquivos_parquet" / f"E111_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / "fiscal" / "fronteira" / f"fronteira_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "arquivos_parquet" / "fisconforme" / "malhas" / f"Fisconforme_malha_cnpj_{cnpj}.parquet",
    ]
    return [caminho for caminho in candidatos if caminho.exists()]


def obter_arquivos_secao_estoque(cnpj: str) -> list[Path]:
    """Prioriza a camada cronologica principal de estoque ja materializada."""

    candidatos = [
        CNPJ_ROOT / cnpj / "analises" / "produtos" / f"mov_estoque_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "analises" / "produtos" / f"aba_mensal_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "analises" / "produtos" / f"aba_anual_{cnpj}.parquet",
    ]
    return [caminho for caminho in candidatos if caminho.exists()]


def obter_arquivos_secao_ressarcimento_st(cnpj: str) -> list[Path]:
    """Prioriza o item analitico e expande para os demais artefatos do dominio."""

    candidatos = [
        CNPJ_ROOT / cnpj / "analises" / "ressarcimento_st" / f"ressarcimento_st_item_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "analises" / "ressarcimento_st" / f"ressarcimento_st_mensal_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "analises" / "ressarcimento_st" / f"ressarcimento_st_conciliacao_{cnpj}.parquet",
        CNPJ_ROOT / cnpj / "analises" / "ressarcimento_st" / f"ressarcimento_st_validacoes_{cnpj}.parquet",
    ]
    return [caminho for caminho in candidatos if caminho.exists()]


def obter_arquivos_sql_prioritarios_materializados(secao_id: str, cnpj: str) -> list[Path]:
    """Reaproveita parquets brutos derivados dos SQLs prioritarios quando o cache canonico ainda nao existe."""

    secao = obter_secao_dossie(secao_id)
    if secao is None or not secao.sql_ids_prioritarios:
        return []

    pasta_base = CNPJ_ROOT / cnpj / "arquivos_parquet"
    candidatos: list[Path] = []
    for sql_id in secao.sql_ids_prioritarios:
        nome_base = Path(str(sql_id)).stem
        candidatos.append(pasta_base / f"{nome_base}_{cnpj}.parquet")

    return [caminho for caminho in dict.fromkeys(candidatos) if caminho.exists()]


def obter_arquivos_por_secao(secao_id: str, cnpj: str) -> list[Path]:
    """Centraliza o mapeamento das secoes do dossie para artefatos ja materializados."""

    caches_canonicos = listar_caches_dossie_por_secao(secao_id, cnpj)
    if caches_canonicos:
        return [caches_canonicos[0]]

    candidatos: list[Path] = []

    if secao_id == "cadastro":
        candidatos.extend(obter_arquivos_secao_cadastro(cnpj))
    elif secao_id == "documentos_fiscais":
        candidatos.extend(obter_arquivos_secao_documentos_fiscais(cnpj))
    elif secao_id == "arrecadacao":
        candidatos.extend(obter_arquivos_secao_arrecadacao(cnpj))
    elif secao_id == "estoque":
        candidatos.extend(obter_arquivos_secao_estoque(cnpj))
    elif secao_id == "ressarcimento_st":
        candidatos.extend(obter_arquivos_secao_ressarcimento_st(cnpj))

    if not candidatos:
        candidatos.extend(obter_arquivos_sql_prioritarios_materializados(secao_id, cnpj))

    return [caminho for caminho in list(dict.fromkeys(candidatos)) if caminho.exists()]


def montar_resumo_secao(secao_id: str, cnpj: str) -> tuple[str, int | None, str | None]:
    """Resume o estado atual da secao sem disparar novas consultas nem alterar cache existente."""

    caminhos_arquivos = obter_arquivos_por_secao(secao_id, cnpj)
    if not caminhos_arquivos:
        return "idle", None, None

    try:
        total_linhas, data_atualizacao = resumir_arquivos_parquet(caminhos_arquivos)
        return "cached", total_linhas, data_atualizacao
    except Exception:
        return "error", None, None


def secao_permite_sincronizacao(secao_id: str) -> bool:
    """Informa se a secao possui SQLs mapeadas para sync no contrato atual."""

    return bool(listar_sql_prioritarias(secao_id))


def sintetizar_metadata_secao_cache(secao_id: str, caminhos_arquivos: list[Path]) -> dict:
    """Gera metadata minima auditavel para secoes baseadas apenas em cache reutilizado."""

    tabelas_origem_por_secao = {
        "cadastro": ["BI.DM_PESSOA", "BI.DM_LOCALIDADE", "SITAFE.SITAFE_HISTORICO_SITUACAO"],
        "documentos_fiscais": ["BI.FATO_NFE_DETALHE", "BI.FATO_NFCE_DETALHE"],
        "arrecadacao": ["E111", "FRONTEIRA", "FISCONFORME.MALHAS"],
        "estoque": ["mov_estoque_<cnpj>.parquet", "aba_mensal_<cnpj>.parquet", "aba_anual_<cnpj>.parquet"],
        "ressarcimento_st": [
            "ressarcimento_st_item_<cnpj>.parquet",
            "ressarcimento_st_mensal_<cnpj>.parquet",
            "ressarcimento_st_conciliacao_<cnpj>.parquet",
            "ressarcimento_st_validacoes_<cnpj>.parquet",
        ],
    }
    return {
        "origem_dado": "cache_catalog",
        "secao_id": secao_id,
        "arquivos_origem_considerados": [str(caminho) for caminho in caminhos_arquivos],
        "tabela_origem": tabelas_origem_por_secao.get(secao_id, []),
    }


def resumir_metadata_secao(secao_id: str, cnpj: str) -> tuple[str | None, str | None, str | None, int | None, int | None]:
    """Extrai estrategia, SQL principal e resumo comparativo do cache canonico mais recente."""

    caminhos_arquivos = obter_arquivos_por_secao(secao_id, cnpj)
    if not caminhos_arquivos:
        return None, None, None, None, None

    metadata = carregar_metadata_cache_secao(caminhos_arquivos[0])
    if not metadata:
        return None, None, None, None, None

    estrategia_execucao = metadata.get("estrategia_execucao")
    sql_principal = metadata.get("sql_principal")
    comparacao = metadata.get("comparacao_estrategia_alternativa")
    comparacao_resumida = None
    quantidade_faltantes = None
    quantidade_extras = None
    if isinstance(comparacao, dict):
        if comparacao.get("convergencia_funcional") is True:
            comparacao_resumida = "convergencia_funcional"
        elif comparacao.get("convergencia_funcional") is False:
            comparacao_resumida = "divergencia_funcional"
        elif comparacao.get("convergencia_basica") is True:
            comparacao_resumida = "convergencia_basica"
        elif comparacao.get("convergencia_basica") is False:
            comparacao_resumida = "divergencia_basica"
        if isinstance(comparacao.get("quantidade_chaves_faltantes"), int):
            quantidade_faltantes = int(comparacao["quantidade_chaves_faltantes"])
        if isinstance(comparacao.get("quantidade_chaves_extras"), int):
            quantidade_extras = int(comparacao["quantidade_chaves_extras"])
    return (
        str(estrategia_execucao) if estrategia_execucao else None,
        str(sql_principal) if sql_principal else None,
        comparacao_resumida,
        quantidade_faltantes,
        quantidade_extras,
    )


def obter_arquivo_principal_secao(secao_id: str, cnpj: str) -> Path:
    """
    Resolve o parquet principal da secao sem disparar reextracao.

    A leitura detalhada sempre prioriza o cache canonico do Dossie quando ele existir.
    """

    caminhos_arquivos = obter_arquivos_por_secao(secao_id, cnpj)
    if not caminhos_arquivos:
        raise HTTPException(status_code=404, detail="Nenhum cache materializado foi encontrado para esta secao.")
    return caminhos_arquivos[0]


def carregar_dados_secao(secao_id: str, cnpj: str, limite: int = 500) -> DossieSectionDataResponse:
    """Carrega uma amostra auditavel da secao a partir do parquet principal ja persistido."""

    cnpj_normalizado = normalizar_cnpj(cnpj)
    secao = next((item for item in listar_secoes_dossie() if item.id == secao_id), None)
    if secao is None:
        raise HTTPException(status_code=404, detail="Secao do dossie nao encontrada.")

    caminhos_arquivos = obter_arquivos_por_secao(secao_id, cnpj_normalizado)
    if not caminhos_arquivos:
        raise HTTPException(status_code=404, detail="Nenhum cache materializado foi encontrado para esta secao.")
    caminho_arquivo = caminhos_arquivos[0]

    try:
        lazyframe = pl.scan_parquet(caminho_arquivo)
        row_count = int(lazyframe.select(pl.len()).collect().item())
        dataframe = lazyframe.limit(max(1, min(limite, 5000))).collect()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar dados da secao: {exc}") from exc

    metadata = carregar_metadata_cache_secao(caminho_arquivo)
    if metadata is None:
        metadata = sintetizar_metadata_secao_cache(secao_id, caminhos_arquivos)

    return DossieSectionDataResponse(
        id=secao.id,
        title=secao.titulo,
        columns=dataframe.columns,
        rows=dataframe.to_dicts(),
        rowCount=row_count,
        cacheFile=str(caminho_arquivo),
        metadata=metadata,
        updatedAt=obter_data_atualizacao_arquivo(caminho_arquivo),
    )


def carregar_historico_comparacao_contato(cnpj: str, limite: int = 20) -> DossieComparisonHistoryResponse:
    """Retorna as ultimas comparacoes registradas para a secao contato do CNPJ."""

    cnpj_normalizado = normalizar_cnpj(cnpj)
    caminho_historico = obter_caminho_historico_comparacao_contato(cnpj_normalizado)
    if not caminho_historico.exists():
        raise HTTPException(status_code=404, detail="Nenhum historico de comparacao foi encontrado para a secao contato.")

    try:
        linhas = caminho_historico.read_text(encoding="utf-8").splitlines()
        itens = [json.loads(linha) for linha in linhas if linha.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar historico de comparacao: {exc}") from exc

    limite_aplicado = max(1, min(limite, 200))
    return DossieComparisonHistoryResponse(
        cnpj=cnpj_normalizado,
        secaoId="contato",
        items=list(reversed(itens[-limite_aplicado:])),
        historyFile=str(caminho_historico),
    )


def resumir_historico_comparacao_contato(cnpj: str) -> DossieComparisonSummaryResponse:
    """Consolida o historico JSONL de comparacoes em um resumo auditavel por CNPJ."""

    cnpj_normalizado = normalizar_cnpj(cnpj)
    caminho_historico = obter_caminho_historico_comparacao_contato(cnpj_normalizado)
    if not caminho_historico.exists():
        raise HTTPException(status_code=404, detail="Nenhum historico de comparacao foi encontrado para a secao contato.")

    try:
        linhas = caminho_historico.read_text(encoding="utf-8").splitlines()
        itens = [json.loads(linha) for linha in linhas if linha.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail=f"Falha ao consolidar historico de comparacao: {exc}") from exc

    if not itens:
        raise HTTPException(status_code=404, detail="O historico de comparacao da secao contato esta vazio.")

    convergencias_funcionais = 0
    divergencias_funcionais = 0
    convergencias_basicas = 0
    divergencias_basicas = 0

    for item in itens:
        comparacao = item.get("comparacao_estrategia_alternativa")
        if not isinstance(comparacao, dict):
            continue
        if comparacao.get("convergencia_funcional") is True:
            convergencias_funcionais += 1
        elif comparacao.get("convergencia_funcional") is False:
            divergencias_funcionais += 1
        elif comparacao.get("convergencia_basica") is True:
            convergencias_basicas += 1
        elif comparacao.get("convergencia_basica") is False:
            divergencias_basicas += 1

    ultimo_item = itens[-1]
    ultima_comparacao = ultimo_item.get("comparacao_estrategia_alternativa")
    ultimo_status = None
    ultima_estrategia_referencia = None
    ultima_sql_principal_referencia = None
    ultimo_total_chaves_faltantes = None
    ultimo_total_chaves_extras = None
    ultima_amostra_chaves_faltantes = None
    ultima_amostra_chaves_extras = None
    ultimos_campos_criticos_atual = None
    ultimos_campos_criticos_referencia = None
    if isinstance(ultima_comparacao, dict):
        if ultima_comparacao.get("convergencia_funcional") is True:
            ultimo_status = "convergencia_funcional"
        elif ultima_comparacao.get("convergencia_funcional") is False:
            ultimo_status = "divergencia_funcional"
        elif ultima_comparacao.get("convergencia_basica") is True:
            ultimo_status = "convergencia_basica"
        elif ultima_comparacao.get("convergencia_basica") is False:
            ultimo_status = "divergencia_basica"
        if ultima_comparacao.get("estrategia_referencia"):
            ultima_estrategia_referencia = str(ultima_comparacao["estrategia_referencia"])
        if ultima_comparacao.get("sql_principal_referencia"):
            ultima_sql_principal_referencia = str(ultima_comparacao["sql_principal_referencia"])
        if isinstance(ultima_comparacao.get("quantidade_chaves_faltantes"), int):
            ultimo_total_chaves_faltantes = int(ultima_comparacao["quantidade_chaves_faltantes"])
        if isinstance(ultima_comparacao.get("quantidade_chaves_extras"), int):
            ultimo_total_chaves_extras = int(ultima_comparacao["quantidade_chaves_extras"])
        if isinstance(ultima_comparacao.get("amostra_chaves_faltantes"), list):
            ultima_amostra_chaves_faltantes = [str(item) for item in ultima_comparacao["amostra_chaves_faltantes"]]
        if isinstance(ultima_comparacao.get("amostra_chaves_extras"), list):
            ultima_amostra_chaves_extras = [str(item) for item in ultima_comparacao["amostra_chaves_extras"]]
        if isinstance(ultima_comparacao.get("campos_criticos_atual"), dict):
            ultimos_campos_criticos_atual = {
                str(chave): int(valor)
                for chave, valor in ultima_comparacao["campos_criticos_atual"].items()
                if isinstance(valor, int)
            }
        if isinstance(ultima_comparacao.get("campos_criticos_referencia"), dict):
            ultimos_campos_criticos_referencia = {
                str(chave): int(valor)
                for chave, valor in ultima_comparacao["campos_criticos_referencia"].items()
                if isinstance(valor, int)
            }

    return DossieComparisonSummaryResponse(
        cnpj=cnpj_normalizado,
        secaoId="contato",
        totalComparacoes=len(itens),
        convergenciasFuncionais=convergencias_funcionais,
        divergenciasFuncionais=divergencias_funcionais,
        convergenciasBasicas=convergencias_basicas,
        divergenciasBasicas=divergencias_basicas,
        ultimaEstrategia=str(ultimo_item.get("estrategia_execucao")) if ultimo_item.get("estrategia_execucao") else None,
        ultimaSqlPrincipal=str(ultimo_item.get("sql_principal")) if ultimo_item.get("sql_principal") else None,
        ultimaEstrategiaReferencia=ultima_estrategia_referencia,
        ultimaSqlPrincipalReferencia=ultima_sql_principal_referencia,
        ultimoStatusComparacao=ultimo_status,
        ultimoCacheKey=str(ultimo_item.get("cache_key")) if ultimo_item.get("cache_key") else None,
        ultimoTotalChavesFaltantes=ultimo_total_chaves_faltantes,
        ultimoTotalChavesExtras=ultimo_total_chaves_extras,
        ultimaAmostraChavesFaltantes=ultima_amostra_chaves_faltantes,
        ultimaAmostraChavesExtras=ultima_amostra_chaves_extras,
        ultimosCamposCriticosAtual=ultimos_campos_criticos_atual,
        ultimosCamposCriticosReferencia=ultimos_campos_criticos_referencia,
        updatedAt=obter_data_atualizacao_arquivo(caminho_historico),
        historyFile=str(caminho_historico),
    )


def obter_caminho_relatorio_comparacao_contato(cnpj: str) -> Path:
    """Resolve o caminho canonico do relatorio tecnico da secao contato por CNPJ."""

    return CNPJ_ROOT / cnpj / "arquivos_parquet" / "dossie" / f"relatorio_comparacao_contato_{cnpj}.md"


def _montar_linhas_delta_campos_criticos(resumo: DossieComparisonSummaryResponse) -> list[str]:
    """Formata o comparativo final de preenchimento dos campos criticos do contato."""

    campos_atual = resumo.ultimosCamposCriticosAtual or {}
    campos_referencia = resumo.ultimosCamposCriticosReferencia or {}
    chaves_ordenadas = sorted(set(campos_atual) | set(campos_referencia))
    if not chaves_ordenadas:
        return ["- Sem contagem de campos criticos registrada na ultima comparacao."]

    linhas: list[str] = []
    for campo in chaves_ordenadas:
        valor_atual = int(campos_atual.get(campo, 0))
        valor_referencia = int(campos_referencia.get(campo, 0))
        delta = valor_atual - valor_referencia
        linhas.append(
            f"- `{campo}`: atual=`{valor_atual}` | referencia=`{valor_referencia}` | delta=`{delta}`"
        )
    return linhas


def _formatar_texto_relatorio_comparacao(resumo: DossieComparisonSummaryResponse) -> str:
    """Gera markdown simples e auditavel para registrar o estado atual da convergencia."""

    linhas = [
        f"# Relatorio de Comparacao da Secao Contato - {resumo.cnpj}",
        "",
        f"- Secao: `{resumo.secaoId}`",
        f"- Total de comparacoes registradas: `{resumo.totalComparacoes}`",
        f"- Convergencias funcionais: `{resumo.convergenciasFuncionais}`",
        f"- Divergencias funcionais: `{resumo.divergenciasFuncionais}`",
        f"- Convergencias basicas: `{resumo.convergenciasBasicas}`",
        f"- Divergencias basicas: `{resumo.divergenciasBasicas}`",
        f"- Ultimo status de comparacao: `{resumo.ultimoStatusComparacao or 'nao informado'}`",
        f"- Ultima estrategia executada: `{resumo.ultimaEstrategia or 'nao informado'}`",
        f"- Ultima SQL principal: `{resumo.ultimaSqlPrincipal or 'nao informado'}`",
        f"- Ultima estrategia de referencia: `{resumo.ultimaEstrategiaReferencia or 'nao informado'}`",
        f"- Ultima SQL principal de referencia: `{resumo.ultimaSqlPrincipalReferencia or 'nao informado'}`",
        f"- Ultimo cache comparado: `{resumo.ultimoCacheKey or 'nao informado'}`",
        f"- Ultimas chaves faltantes: `{resumo.ultimoTotalChavesFaltantes if resumo.ultimoTotalChavesFaltantes is not None else 'nao informado'}`",
        f"- Ultimas chaves extras: `{resumo.ultimoTotalChavesExtras if resumo.ultimoTotalChavesExtras is not None else 'nao informado'}`",
        f"- Historico fonte: `{resumo.historyFile}`",
        f"- Atualizado em: `{resumo.updatedAt or 'nao informado'}`",
        "",
        "## Leitura Operacional",
        "",
    ]

    if resumo.divergenciasFuncionais > 0:
        linhas.append("- Existem divergencias funcionais registradas entre as estrategias comparadas.")
    elif resumo.divergenciasBasicas > 0:
        linhas.append("- Nao ha divergencia funcional registrada, mas ainda existem divergencias basicas no historico.")
    else:
        linhas.append("- O historico atual nao registra divergencias conhecidas entre as estrategias comparadas.")

    if resumo.ultimaSqlPrincipal:
        linhas.append(f"- A ultima SQL principal observada foi `{resumo.ultimaSqlPrincipal}`.")
    if resumo.ultimaEstrategia:
        linhas.append(f"- A ultima estrategia materializada foi `{resumo.ultimaEstrategia}`.")

    linhas.extend(
        [
            "",
            "## Ultima Comparacao Funcional",
            "",
            (
                f"- Amostra de chaves faltantes: `{' | '.join(resumo.ultimaAmostraChavesFaltantes)}`"
                if resumo.ultimaAmostraChavesFaltantes
                else "- Amostra de chaves faltantes: `nao informado`"
            ),
            (
                f"- Amostra de chaves extras: `{' | '.join(resumo.ultimaAmostraChavesExtras)}`"
                if resumo.ultimaAmostraChavesExtras
                else "- Amostra de chaves extras: `nao informado`"
            ),
            "",
            "## Cobertura de Campos Criticos",
            "",
            *_montar_linhas_delta_campos_criticos(resumo),
            "",
            "## Observacao",
            "",
            "- Este relatorio e derivado exclusivamente do historico JSONL da secao `contato`, sem reexecutar Oracle.",
        ]
    )
    return "\n".join(linhas) + "\n"


def gerar_relatorio_comparacao_contato(cnpj: str) -> DossieComparisonReportResponse:
    """Materializa um relatorio tecnico markdown a partir do historico consolidado da secao contato."""

    resumo = resumir_historico_comparacao_contato(cnpj)
    caminho_relatorio = obter_caminho_relatorio_comparacao_contato(resumo.cnpj)
    caminho_relatorio.parent.mkdir(parents=True, exist_ok=True)
    conteudo = _formatar_texto_relatorio_comparacao(resumo)
    try:
        caminho_relatorio.write_text(conteudo, encoding="utf-8")
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Falha ao persistir relatorio de comparacao: {exc}") from exc

    return DossieComparisonReportResponse(
        cnpj=resumo.cnpj,
        secaoId=resumo.secaoId,
        reportFile=str(caminho_relatorio),
        updatedAt=obter_data_atualizacao_arquivo(caminho_relatorio),
        content=conteudo,
    )


async def sincronizar_secao_dossie(
    cnpj: str,
    secao_id: str,
    payload: SyncDossieRequest | None = None,
):
    """Centraliza a sincronizacao do dossie para manter os contratos compativeis."""

    parametros = payload.parametros if payload else None
    cnpj_normalizado = normalizar_cnpj(cnpj)
    if not secao_permite_sincronizacao(secao_id):
        raise HTTPException(
            status_code=400,
            detail=(
                "Secao do dossie opera apenas por leitura de cache e nao possui "
                "sincronizacao Oracle ativa no contrato atual."
            ),
        )
    try:
        return await executar_sync_secao(cnpj=cnpj_normalizado, secao_id=secao_id, parametros=parametros)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=f"Falha operacional ao sincronizar secao do dossie: {exc}") from exc


@router.post("/{cnpj}/secoes/{secao_id}/sync")
async def post_sync_secao(cnpj: str, secao_id: str, payload: SyncDossieRequest | None = None):
    """Aciona a materializacao SQL no contrato canonico atual do backend."""

    return await sincronizar_secao_dossie(cnpj=cnpj, secao_id=secao_id, payload=payload)


@router.post("/{cnpj}/sync/{secao_id}")
async def post_sync_secao_legado(cnpj: str, secao_id: str, payload: SyncDossieRequest | None = None):
    """Mantem compatibilidade com o contrato originalmente descrito no plano."""

    return await sincronizar_secao_dossie(cnpj=cnpj, secao_id=secao_id, payload=payload)


@router.get("/{cnpj}/secoes", response_model=list[DossieSectionSummaryResponse])
def get_secoes(cnpj: str):
    """
    Lista as secoes do dossie com base nos artefatos ja persistidos do CNPJ.

    Regra conservadora:
    - nao dispara novas consultas Oracle;
    - nao altera arquivos existentes;
    - apenas resume o que ja foi materializado no workspace do projeto.
    """

    cnpj_normalizado = normalizar_cnpj(cnpj)
    resultado: list[DossieSectionSummaryResponse] = []

    for secao in listar_secoes_dossie():
        if secao.exige_cnpj and not cnpj_normalizado:
            continue

        status_secao, quantidade_linhas, data_atualizacao = montar_resumo_secao(secao.id, cnpj_normalizado)
        estrategia_execucao, sql_principal, comparacao_resumida, quantidade_faltantes, quantidade_extras = resumir_metadata_secao(secao.id, cnpj_normalizado)
        resultado.append(
            DossieSectionSummaryResponse(
                id=secao.id,
                title=secao.titulo,
                description=secao.descricao,
                sourceType=secao.tipo_fonte,
                syncEnabled=secao_permite_sincronizacao(secao.id),
                sourceFiles=[str(caminho) for caminho in obter_arquivos_por_secao(secao.id, cnpj_normalizado)] or None,
                status=status_secao,
                rowCount=quantidade_linhas,
                executionStrategy=estrategia_execucao,
                primarySql=sql_principal,
                alternateStrategyComparison=comparacao_resumida,
                alternateStrategyMissingKeys=quantidade_faltantes,
                alternateStrategyExtraKeys=quantidade_extras,
                updatedAt=data_atualizacao,
            )
        )

    return resultado


@router.get("/{cnpj}/secoes/{secao_id}/dados", response_model=DossieSectionDataResponse)
def get_dados_secao(cnpj: str, secao_id: str, limite: int = 500):
    """Expose a leitura do cache parquet para inspecao detalhada no frontend."""

    return carregar_dados_secao(secao_id=secao_id, cnpj=cnpj, limite=limite)


@router.get("/{cnpj}/secoes/contato/comparacoes", response_model=DossieComparisonHistoryResponse)
def get_historico_comparacoes_contato(cnpj: str, limite: int = 20):
    """Expose o historico JSONL de comparacoes do contato para a interface web."""

    return carregar_historico_comparacao_contato(cnpj=cnpj, limite=limite)


@router.get("/{cnpj}/secoes/contato/comparacoes/resumo", response_model=DossieComparisonSummaryResponse)
def get_resumo_comparacoes_contato(cnpj: str):
    """Expose um resumo consolidado do historico de comparacoes da secao contato."""

    return resumir_historico_comparacao_contato(cnpj=cnpj)


@router.post("/{cnpj}/secoes/contato/comparacoes/relatorio", response_model=DossieComparisonReportResponse)
def post_relatorio_comparacoes_contato(cnpj: str):
    """Gera e persiste um relatorio tecnico markdown da convergencia do contato por CNPJ."""

    return gerar_relatorio_comparacao_contato(cnpj=cnpj)
