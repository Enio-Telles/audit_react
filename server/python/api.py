"""API FastAPI do audit_react."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import polars as pl
from fastapi import Depends, FastAPI, HTTPException, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import audit_engine  # noqa: F401 - importa para registrar contratos e geradores
from audit_engine.contratos.base import CONTRATOS, listar_contratos, ordem_topologica
from audit_engine.pipeline.orquestrador import OrquestradorPipeline
from audit_engine.utils.camadas_cnpj import CAMADAS_CNPJ_PADRAO, garantir_estrutura_camadas_cnpj
from audit_engine.utils.manifesto_cnpj import gerar_manifesto_cnpj
from audit_engine.utils.parquet_io import exportar_csv, exportar_excel
from audit_engine.utils.referencias import (
    carregar_ncm,
    carregar_cest,
    carregar_cfop,
    carregar_cst,
    carregar_dominios_nfe,
    carregar_mapeamento_nfe,
    carregar_dominios_eventos_nfe,
    carregar_malhas_fisconforme,
    buscar_ncm_por_codigo,
    buscar_cest_por_codigo,
    buscar_cfop_por_codigo,
    validar_ncm,
    validar_cest,
    validar_cfop,
)
from configuracao_backend import (
    listar_resumos_configuracoes_oracle,
    obter_configuracao_backend,
    obter_configuracao_oracle,
)
from extrair_oracle import (
    detalhar_mapeamento_fontes_oracle,
    extrair_dados_cnpj,
    listar_consultas_versionadas,
    obter_mapeamento_fontes_oracle,
    salvar_mapeamento_fontes_oracle,
    separar_owner_objeto_oracle,
)
from consulta_cadastral import consultar_dados_cadastrais_documentos
from mapeamento_sql_oracle import analisar_mapeamento_raiz_sql_oracle
from oracle_client import (
    criar_conexao_oracle,
    listar_colunas_objeto_oracle,
    listar_objetos_oracle,
    testar_conexao_oracle,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Audit React API",
    description="API do sistema de auditoria fiscal",
    version="1.0.0",
)

allowed_origins = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,http://localhost:3000,http://localhost:8000",
    ).split(",")
    if origin.strip() and origin.strip() != "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_cfg = obter_configuracao_backend()
BASE_DIR = Path(_cfg.diretorio_base_cnpj)
BASE_DIR.mkdir(parents=True, exist_ok=True)


class ExecucaoRequest(BaseModel):
    cnpj: str
    consultas: list[str] = Field(default_factory=list)
    data_limite: Optional[str] = Field(
        default=None,
        description="Data maxima de processamento no formato YYYY-MM-DD. Nulo significa extrair todo o historico disponivel do CNPJ.",
    )
    tabelas_alvo: Optional[list[str]] = None
    executar_extracao: bool = True
    indice_oracle: Optional[int] = Field(default=None, ge=0, le=9)


class EdicaoFatorRequest(BaseModel):
    id_agrupado: str
    unid_ref: Optional[str] = None
    fator: Optional[float] = None
    fator_compra_ref: Optional[float] = None
    fator_venda_ref: Optional[float] = None


class AgregacaoRequest(BaseModel):
    ids_produtos: list[str]
    descricao_padrao: Optional[str] = None


class DesagregacaoRequest(BaseModel):
    id_grupo: str


class ConfiguracaoSistemaRequest(BaseModel):
    reprocessamento_automatico: Optional[bool] = None
    logs_detalhados: Optional[bool] = None
    exportacao_formatada: Optional[bool] = None
    diretorio_consultas_sql: Optional[str] = None
    oracle_indice_ativo: Optional[int] = Field(default=None, ge=0, le=9)


class MapeamentoFontesOracleRequest(BaseModel):
    mapeamentos: dict[str, Optional[str]]


class ConsultaCadastroRequest(BaseModel):
    documentos: list[str] = Field(default_factory=list)
    indice_oracle: Optional[int] = Field(default=None, ge=0, le=9)


def _normalizar_cnpj(cnpj: str) -> str:
    """Normaliza CNPJ removendo caracteres nao numericos."""
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    if len(cnpj_limpo) != 14:
        raise HTTPException(status_code=400, detail="CNPJ deve conter 14 digitos")
    return cnpj_limpo


def validar_cnpj(cnpj: str) -> str:
    """Dependência FastAPI para validar CNPJ e prevenir path traversal.
    
    Aceita apenas 14 dígitos numéricos. Rejeita qualquer outro formato.
    """
    if not re.match(r"^\d{14}$", cnpj):
        raise HTTPException(
            status_code=400,
            detail="CNPJ inválido: deve conter 14 dígitos numéricos"
        )
    return cnpj


def _normalizar_cpf(cpf: Any) -> str | None:
    """Normaliza CPF removendo caracteres nao numericos."""
    cpf_limpo = re.sub(r"\D", "", str(cpf or ""))
    if len(cpf_limpo) != 11:
        return None
    return cpf_limpo


def _normalizar_documento_consulta(documento: str) -> str:
    """Normaliza CPF ou CNPJ removendo caracteres nao numericos."""
    documento_limpo = re.sub(r"\D", "", str(documento or ""))
    if len(documento_limpo) not in {11, 14}:
        raise ValueError("Documento deve conter 11 digitos de CPF ou 14 digitos de CNPJ")
    return documento_limpo


def _validar_nome_tabela(nome_tabela: str) -> str:
    """Valida nome de tabela para prevenir path traversal."""
    if not re.fullmatch(r"[a-z0-9_]+", nome_tabela):
        raise HTTPException(status_code=400, detail="Nome de tabela invalido")
    return nome_tabela


def _obter_diretorio_cnpj(cnpj: str, criar: bool = False) -> Path:
    """Resolve diretorio raiz do CNPJ."""
    diretorio = BASE_DIR / cnpj
    if criar:
        diretorio.mkdir(parents=True, exist_ok=True)
    if not diretorio.exists():
        raise HTTPException(status_code=404, detail="CNPJ nao encontrado")
    return diretorio


def _garantir_estrutura_cnpj(diretorio_cnpj: Path) -> None:
    """Garante estrutura padrao de diretorios por CNPJ."""
    garantir_estrutura_camadas_cnpj(diretorio_cnpj)


def _validar_camada_armazenamento(camada: str) -> str:
    """Valida camada de armazenamento suportada pelos endpoints de consulta."""
    camada_normalizada = (camada or "parquets").strip().lower()
    if camada_normalizada not in CAMADAS_CNPJ_PADRAO:
        raise HTTPException(status_code=400, detail=f"Camada invalida: {camada_normalizada}")
    return camada_normalizada


def _carregar_json(caminho: Path, padrao: Any) -> Any:
    """Carrega JSON de disco com fallback para valor padrao."""
    if not caminho.exists():
        return padrao
    with caminho.open("r", encoding="utf-8") as arquivo:
        return json.load(arquivo)


def _salvar_json(caminho: Path, payload: Any) -> None:
    """Salva JSON em disco com indentacao padrao."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8") as arquivo:
        json.dump(payload, arquivo, ensure_ascii=False, indent=2)


def _selecionar_texto_mais_completo(candidatos: list[str]) -> str | None:
    """Escolhe o texto mais informativo dentre os candidatos validos."""
    textos_validos = [texto.strip() for texto in candidatos if isinstance(texto, str) and texto.strip()]
    if not textos_validos:
        return None
    return max(textos_validos, key=lambda texto: (len(texto), texto))


def _carregar_metadados_reg0000(diretorio_cnpj: Path) -> dict[str, Any]:
    """Le nome, IE e CPFs vinculados do reg0000 quando o parquet existir."""
    caminho_reg0000 = diretorio_cnpj / "extraidos" / "reg0000.parquet"
    if not caminho_reg0000.exists():
        return {
            "contribuinte": None,
            "ie": None,
            "cpfs_vinculados": [],
        }

    try:
        df_reg0000 = pl.read_parquet(caminho_reg0000, columns=["nome", "ie", "cpf"])
    except Exception as erro:  # noqa: BLE001
        logger.warning("Falha ao ler metadados do reg0000 para %s: %s", diretorio_cnpj.name, erro)
        return {
            "contribuinte": None,
            "ie": None,
            "cpfs_vinculados": [],
        }

    nomes = df_reg0000.get_column("nome").drop_nulls().to_list() if "nome" in df_reg0000.columns else []
    inscricoes = df_reg0000.get_column("ie").drop_nulls().to_list() if "ie" in df_reg0000.columns else []
    cpfs = df_reg0000.get_column("cpf").to_list() if "cpf" in df_reg0000.columns else []

    cpfs_vinculados = sorted({cpf for cpf in (_normalizar_cpf(valor) for valor in cpfs) if cpf})

    return {
        "contribuinte": _selecionar_texto_mais_completo([str(valor) for valor in nomes]),
        "ie": _selecionar_texto_mais_completo([str(valor) for valor in inscricoes]),
        "cpfs_vinculados": cpfs_vinculados,
    }


def _carregar_metadados_relatorio(diretorio_cnpj: Path) -> dict[str, Any]:
    """Le dados resumidos do relatorio salvo para enriquecer a selecao inicial."""
    caminho_dados = diretorio_cnpj / "relatorio" / "dados.json"
    if not caminho_dados.exists():
        return {
            "contribuinte": None,
            "ie": None,
            "dsf": None,
            "possui_relatorio": False,
        }

    dados_relatorio = _carregar_json(caminho_dados, {})

    return {
        "contribuinte": dados_relatorio.get("contribuinte"),
        "ie": dados_relatorio.get("ie"),
        "dsf": dados_relatorio.get("dsf"),
        "possui_relatorio": True,
    }


def _calcular_atualizacao_mais_recente(diretorio_cnpj: Path) -> str | None:
    """Calcula timestamp ISO do arquivo operacional mais recente do CNPJ."""
    arquivos_relevantes = [
        caminho
        for caminho in diretorio_cnpj.rglob("*")
        if caminho.is_file() and caminho.suffix.lower() in {".json", ".parquet", ".csv", ".xlsx", ".pdf", ".docx"}
    ]
    if not arquivos_relevantes:
        return None

    ultima_data = max(caminho.stat().st_mtime for caminho in arquivos_relevantes)
    return datetime.fromtimestamp(ultima_data, tz=timezone.utc).isoformat()


@lru_cache(maxsize=64)
def _ler_parquet_cached(caminho: str, mtime: float) -> pl.DataFrame:
    """Cache LRU de Parquet com invalidacao automatica por mtime.
    
    O mtime é usado como chave de invalidacao: quando o arquivo é modificado,
    o mtime muda e o cache é automaticamente invalidado.
    """
    return pl.read_parquet(caminho)


def _ler_tabela_com_cache(cnpj: str, nome_tabela: str, camada: str = "parquets") -> pl.DataFrame:
    """Le tabela Parquet com cache LRU.
    
    Se o arquivo não existir, lança FileNotFoundError.
    """
    diretorio_parquets = _obter_diretorio_cnpj(_normalizar_cnpj(cnpj)) / camada
    arquivo = diretorio_parquets / f"{nome_tabela}.parquet"
    
    if not arquivo.exists():
        raise FileNotFoundError(f"Tabela não encontrada: {arquivo}")
    
    mtime = arquivo.stat().st_mtime
    return _ler_parquet_cached(str(arquivo), mtime)


def _montar_alvos_analise() -> list[dict[str, Any]]:
    """Monta catalogo de CNPJs conhecidos no storage para selecao inicial."""
    total_tabelas_esperadas = len(CONTRATOS)
    nomes_tabelas_esperadas = {Path(contrato.saida).stem for contrato in CONTRATOS.values()}
    alvos: list[dict[str, Any]] = []

    for diretorio_cnpj in sorted(BASE_DIR.iterdir(), key=lambda item: item.name):
        if not diretorio_cnpj.is_dir() or not re.fullmatch(r"\d{14}", diretorio_cnpj.name):
            continue

        diretorio_parquets = diretorio_cnpj / "parquets"
        arquivos_parquet = sorted(diretorio_parquets.glob("*.parquet")) if diretorio_parquets.exists() else []
        nomes_parquets = {arquivo.stem for arquivo in arquivos_parquet}
        total_tabelas_ok = sum(1 for nome in nomes_tabelas_esperadas if nome in nomes_parquets)

        if total_tabelas_ok == total_tabelas_esperadas and total_tabelas_esperadas > 0:
            status_pipeline = "completo"
        elif total_tabelas_ok > 0:
            status_pipeline = "parcial"
        else:
            status_pipeline = "nao_iniciado"

        metadados_reg0000 = _carregar_metadados_reg0000(diretorio_cnpj)
        metadados_relatorio = _carregar_metadados_relatorio(diretorio_cnpj)

        alvos.append(
            {
                "cnpj": diretorio_cnpj.name,
                "contribuinte": _selecionar_texto_mais_completo(
                    [
                        str(metadados_relatorio.get("contribuinte") or "").strip(),
                        str(metadados_reg0000.get("contribuinte") or "").strip(),
                    ]
                ),
                "ie": _selecionar_texto_mais_completo(
                    [
                        str(metadados_relatorio.get("ie") or "").strip(),
                        str(metadados_reg0000.get("ie") or "").strip(),
                    ]
                ),
                "dsf": metadados_relatorio.get("dsf"),
                "cpfs_vinculados": metadados_reg0000["cpfs_vinculados"],
                "possui_relatorio": metadados_relatorio["possui_relatorio"],
                "possui_extraidos": (diretorio_cnpj / "extraidos").exists(),
                "total_parquets": len(arquivos_parquet),
                "total_tabelas_ok": total_tabelas_ok,
                "total_tabelas_esperadas": total_tabelas_esperadas,
                "status_pipeline": status_pipeline,
                "atualizado_em": _calcular_atualizacao_mais_recente(diretorio_cnpj),
            }
        )

    return sorted(
        alvos,
        key=lambda alvo: (
            0 if alvo["status_pipeline"] == "completo" else 1 if alvo["status_pipeline"] == "parcial" else 2,
            (alvo.get("contribuinte") or "").upper(),
            alvo["cnpj"],
        ),
    )


def _obter_arquivo_configuracoes_sistema() -> Path:
    """Resolve o caminho do arquivo de configuracoes persistidas."""
    return BASE_DIR / "_sistema" / "configuracoes.json"


def _carregar_configuracoes_sistema() -> dict[str, Any]:
    """Carrega configuracoes persistidas do sistema com fallback vazio."""
    return _carregar_json(_obter_arquivo_configuracoes_sistema(), {})


def _obter_diretorio_consultas_sql_ativo() -> str:
    """Resolve diretorio SQL efetivo priorizando configuracao persistida."""
    configuracoes_salvas = _carregar_configuracoes_sistema()
    diretorio_persistido = configuracoes_salvas.get("diretorio_consultas_sql")

    if isinstance(diretorio_persistido, str) and diretorio_persistido.strip():
        return diretorio_persistido.strip()

    return _cfg.diretorio_consultas_sql


def _listar_diretorios_sql_sugeridos() -> list[dict[str, str]]:
    """Lista diretorios SQL uteis para analise e extracao."""
    sugestoes: list[dict[str, str]] = []
    caminhos_vistos: set[str] = set()

    def adicionar(chave: str, rotulo: str, caminho: str) -> None:
        caminho_normalizado = str(Path(caminho))
        if caminho_normalizado in caminhos_vistos:
            return
        if not Path(caminho_normalizado).exists():
            return
        caminhos_vistos.add(caminho_normalizado)
        sugestoes.append({"chave": chave, "rotulo": rotulo, "caminho": caminho_normalizado})

    adicionar("ativo", "Diretorio SQL ativo", _obter_diretorio_consultas_sql_ativo())
    adicionar("embutido", "Consultas embutidas do repositorio", _cfg.diretorio_consultas_sql)
    # Caminho de referencia externa configuravel via variavel de ambiente
    referencia_externa = os.getenv("ORACLE_FONTE_REFERENCIA_SQL", r"C:\funcoes - Copia\sql")
    adicionar("referencia_externa", "Referencia externa do usuario", referencia_externa)

    return sugestoes


def _obter_indice_oracle_ativo(indice_override: int | None = None) -> int:
    """Resolve o indice Oracle ativo a partir do override ou das configuracoes persistidas."""
    if indice_override is not None:
        return indice_override

    configuracoes_salvas = _carregar_configuracoes_sistema()
    indice_ativo = configuracoes_salvas.get("oracle_indice_ativo", 0)
    try:
        indice_normalizado = int(indice_ativo)
    except (TypeError, ValueError) as erro:
        raise HTTPException(status_code=500, detail="Configuracao oracle_indice_ativo invalida") from erro

    if indice_normalizado < 0 or indice_normalizado > 9:
        raise HTTPException(status_code=500, detail="Configuracao oracle_indice_ativo fora do intervalo permitido")

    return indice_normalizado


def _montar_conexoes_oracle(indice_ativo: int) -> list[dict[str, Any]]:
    """Lista conexoes Oracle conhecidas sem expor credenciais."""
    conexoes = listar_resumos_configuracoes_oracle(indices_extras=[indice_ativo])
    return [
        {
            **conexao,
            "ativa": conexao["indice"] == indice_ativo,
        }
        for conexao in conexoes
    ]


def _normalizar_erros_execucao(
    resultado,
    resumo_extracao: Optional[dict[str, Any]] = None,
) -> tuple[list[str], list[str], list[str]]:
    """Separa erros do pipeline e da extracao para manter rastreabilidade."""
    erros_pipeline = list(resultado.erros)
    erros_extracao = []

    if resumo_extracao and isinstance(resumo_extracao.get("erros"), list):
        erros_extracao = [str(erro) for erro in resumo_extracao["erros"]]

    erros_total = [*erros_pipeline, *erros_extracao]
    return erros_pipeline, erros_extracao, erros_total


def _resumo_resultado_pipeline(resultado, resumo_extracao: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Normaliza resposta de resultado do orquestrador."""
    erros_pipeline, erros_extracao, erros_total = _normalizar_erros_execucao(resultado, resumo_extracao)

    return {
        "cnpj": resultado.cnpj,
        "status": resultado.status,
        "duracao_ms": resultado.duracao_total_ms,
        "tabelas_geradas": resultado.tabelas_geradas,
        "erros": erros_pipeline,
        "erros_pipeline": erros_pipeline,
        "erros_extracao": erros_extracao,
        "erros_total": erros_total,
        "etapas": [
            {
                "tabela": etapa.tabela,
                "status": etapa.status.value,
                "mensagem": etapa.mensagem,
                "duracao_ms": etapa.duracao_ms,
                "registros": etapa.registros_gerados,
                "arquivo_saida": etapa.arquivo_saida,
            }
            for etapa in resultado.etapas
        ],
    }


def _validar_mapeamentos_oracle(indice: int = 0) -> list[dict[str, Any]]:
    """Valida se as fontes Oracle configuradas existem e expoe amostra de colunas."""
    validacoes: list[dict[str, Any]] = []

    for item in detalhar_mapeamento_fontes_oracle():
        owner = item["owner"] or None
        objeto = item["objeto"]

        try:
            colunas = listar_colunas_objeto_oracle(
                objeto=objeto,
                owner=owner,
                limite=25,
                indice=indice,
            )
            validacoes.append(
                {
                    **item,
                    "existe": len(colunas) > 0,
                    "total_colunas_amostra": len(colunas),
                    "colunas_amostra": [coluna["column_name"] for coluna in colunas[:10]],
                    "erro": None,
                }
            )
        except Exception as erro:  # noqa: BLE001
            validacoes.append(
                {
                    **item,
                    "existe": False,
                    "total_colunas_amostra": 0,
                    "colunas_amostra": [],
                    "erro": str(erro),
                }
            )

    return validacoes


def _classificar_status_http_falha_extracao(erro: Exception) -> int:
    """Classifica falhas de extracao Oracle entre indisponibilidade e erro interno."""
    mensagem = str(erro).lower()
    termos_indisponibilidade = [
        "getaddrinfo",
        "name or service not known",
        "connection",
        "timed out",
        "timeout",
        "host",
        "service",
        "listener",
        "refused",
        "unreachable",
        "network",
    ]
    return 503 if any(termo in mensagem for termo in termos_indisponibilidade) else 500


def _montar_resposta_erro_oracle(
    mensagem_padrao: str,
    erro: Exception,
    indice_oracle: int,
    mensagem_conexao: str | None = None,
    extras: dict[str, Any] | None = None,
) -> JSONResponse:
    """Monta resposta JSON estruturada para falhas Oracle."""
    status_http = _classificar_status_http_falha_extracao(erro)
    detalhe = str(erro)
    mensagem = mensagem_padrao

    if status_http == 503:
        mensagem = mensagem_conexao or "Falha na conexao com o Oracle"

    payload = {
        "status": "erro",
        "mensagem": mensagem,
        "detalhe": detalhe,
        "indice_oracle": indice_oracle,
    }
    if extras:
        payload.update(extras)

    return JSONResponse(status_code=status_http, content=payload)


def _montar_resposta_falha_extracao(cnpj: str, erro: Exception, indice_oracle: int) -> JSONResponse:
    """Monta resposta JSON estruturada para falha de extracao Oracle."""
    return _montar_resposta_erro_oracle(
        mensagem_padrao="Falha na extracao Oracle",
        erro=erro,
        indice_oracle=indice_oracle,
        mensagem_conexao="Falha na conexao com o Oracle durante a extracao",
        extras={"cnpj": cnpj},
    )


def _obter_status_conectividade_oracle(indice_oracle: int) -> tuple[bool, str | None]:
    """Testa a conexao Oracle ativa e retorna estado booleano com detalhe de erro."""
    try:
        conexao = criar_conexao_oracle(indice=indice_oracle)
    except Exception as erro:  # noqa: BLE001
        return False, str(erro)

    conexao.close()
    return True, None


@app.get("/api/health")
async def health_check():
    """Retorna health check da API."""
    return {
        "status": "ok",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/sistema/status")
async def status_sistema():
    """Retorna status operacional minimo para frontend."""
    indice_oracle_ativo = _obter_indice_oracle_ativo()
    conexoes_oracle = _montar_conexoes_oracle(indice_oracle_ativo)
    diretorio_consultas_sql = _obter_diretorio_consultas_sql_ativo()

    try:
        obter_configuracao_oracle(indice=indice_oracle_ativo)
        oracle_configurada = True
        oracle_conectada, erro_oracle = _obter_status_conectividade_oracle(indice_oracle_ativo)
    except Exception as erro:  # noqa: BLE001
        oracle_configurada = False
        oracle_conectada = False
        erro_oracle = str(erro)

    return {
        "status": "ok",
        "api": "online",
        "oracle_configurada": oracle_configurada,
        "oracle_conectada": oracle_conectada,
        "oracle_indice_ativo": indice_oracle_ativo,
        "erro_oracle": erro_oracle,
        "conexoes_oracle": conexoes_oracle,
        "diretorio_base_cnpj": str(BASE_DIR),
        "consultas_disponiveis": listar_consultas_versionadas(diretorio_consultas=diretorio_consultas_sql),
        "fontes_oracle": obter_mapeamento_fontes_oracle(),
        "fontes_oracle_detalhadas": detalhar_mapeamento_fontes_oracle(),
    }


@app.get("/api/sistema/alvos")
async def listar_alvos_analise():
    """Lista CNPJs conhecidos no storage para a pagina inicial de selecao."""
    alvos = _montar_alvos_analise()
    cpfs_unicos = sorted({cpf for alvo in alvos for cpf in alvo["cpfs_vinculados"]})

    return {
        "status": "ok",
        "resumo": {
            "total_cnpjs": len(alvos),
            "total_cpfs_mapeados": len(cpfs_unicos),
            "total_cnpjs_com_pipeline": sum(1 for alvo in alvos if alvo["status_pipeline"] != "nao_iniciado"),
            "total_cnpjs_com_pipeline_completo": sum(1 for alvo in alvos if alvo["status_pipeline"] == "completo"),
            "total_cnpjs_com_relatorio": sum(1 for alvo in alvos if alvo["possui_relatorio"]),
        },
        "alvos": alvos,
    }


@app.get("/api/configuracoes")
async def obter_configuracoes():
    """Retorna configuracoes operacionais persistidas."""
    configuracoes_salvas = _carregar_configuracoes_sistema()

    return {
        "status": "ok",
        "configuracoes": {
            "reprocessamento_automatico": configuracoes_salvas.get("reprocessamento_automatico", True),
            "logs_detalhados": configuracoes_salvas.get("logs_detalhados", True),
            "exportacao_formatada": configuracoes_salvas.get("exportacao_formatada", True),
            "diretorio_consultas_sql": configuracoes_salvas.get(
                "diretorio_consultas_sql", _cfg.diretorio_consultas_sql
            ),
            "oracle_indice_ativo": configuracoes_salvas.get("oracle_indice_ativo", 0),
        },
    }


@app.put("/api/configuracoes")
async def salvar_configuracoes(request: ConfiguracaoSistemaRequest):
    """Persiste configuracoes operacionais do sistema."""
    arquivo_cfg = _obter_arquivo_configuracoes_sistema()
    atual = _carregar_configuracoes_sistema()

    payload = request.model_dump(exclude_none=True)
    atual.update(payload)
    _salvar_json(arquivo_cfg, atual)

    return {
        "status": "ok",
        "mensagem": "Configuracoes salvas com sucesso",
        "configuracoes": atual,
    }


@app.get("/api/consultas")
async def listar_consultas_endpoint():
    """Lista consultas SQL versionadas disponiveis no repositorio."""
    diretorio_consultas_sql = _obter_diretorio_consultas_sql_ativo()
    return {
        "status": "ok",
        "consultas": listar_consultas_versionadas(diretorio_consultas=diretorio_consultas_sql),
        "fontes_oracle": obter_mapeamento_fontes_oracle(),
        "diretorio_consultas_sql": diretorio_consultas_sql,
        "diretorios_sugeridos": _listar_diretorios_sql_sugeridos(),
    }


@app.post("/api/cadastro/consultar")
async def consultar_cadastro(request: ConsultaCadastroRequest):
    """Consulta dados cadastrais Oracle para uma lista de CPFs ou CNPJs."""
    documentos_invalidos: list[dict[str, Any]] = []
    documentos_validos: list[str] = []

    for documento in request.documentos:
        try:
            documentos_validos.append(_normalizar_documento_consulta(documento))
        except ValueError as erro:
            documentos_invalidos.append(
                {
                    "status": "invalido",
                    "tipo_documento": "desconhecido",
                    "documento_consultado": re.sub(r"\D", "", str(documento or "")),
                    "origem": "entrada",
                    "encontrado": False,
                    "mensagem": str(erro),
                    "registros": [],
                }
            )

    documentos_validos = list(dict.fromkeys(documentos_validos))
    indice_oracle = _obter_indice_oracle_ativo(request.indice_oracle)

    if not documentos_validos:
        return {
            "status": "ok",
            "documentos_processados": 0,
            "resultados": documentos_invalidos,
        }

    oracle_conectada, erro_oracle = _obter_status_conectividade_oracle(indice_oracle)
    if not oracle_conectada:
        return JSONResponse(
            status_code=503,
            content={
                "status": "erro",
                "mensagem": "Falha na conexao com o Oracle para consulta cadastral",
                "detalhe": erro_oracle or "Conexao Oracle indisponivel",
                "indice_oracle": indice_oracle,
                "documentos": documentos_validos,
            },
        )

    try:
        resultados_oracle = consultar_dados_cadastrais_documentos(documentos_validos, indice_oracle=indice_oracle)
    except Exception as erro:  # noqa: BLE001
        return _montar_resposta_erro_oracle(
            mensagem_padrao="Falha na consulta cadastral Oracle",
            erro=erro,
            indice_oracle=indice_oracle,
            mensagem_conexao="Falha na conexao com o Oracle para consulta cadastral",
            extras={"documentos": documentos_validos},
        )

    resultados_ordenados = sorted(
        [*resultados_oracle, *documentos_invalidos],
        key=lambda item: (
            0 if item["status"] == "ok" else 1,
            item["documento_consultado"],
        ),
    )

    return {
        "status": "ok",
        "documentos_processados": len(documentos_validos),
        "resultados": resultados_ordenados,
    }


@app.get("/api/oracle/mapeamento-raiz")
async def obter_mapeamento_raiz_oracle(diretorio: Optional[str] = Query(None)):
    """Analisa SQLs fiscais e devolve mapa raiz de fontes Oracle."""
    diretorio_analise = diretorio.strip() if diretorio and diretorio.strip() else None
    if diretorio_analise is None:
        sugestoes = _listar_diretorios_sql_sugeridos()
        sugestao_referencia = next((item for item in sugestoes if item["chave"] == "referencia_externa"), None)
        diretorio_analise = (
            sugestao_referencia["caminho"]
            if sugestao_referencia
            else sugestoes[0]["caminho"] if sugestoes else _obter_diretorio_consultas_sql_ativo()
        )

    try:
        analise = analisar_mapeamento_raiz_sql_oracle(diretorio_analise)
    except FileNotFoundError as erro:
        raise HTTPException(status_code=404, detail=str(erro)) from erro
    except Exception as erro:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Falha ao analisar SQLs Oracle: {erro}") from erro

    return {
        "status": "ok",
        **analise,
        "diretorio_ativo": _obter_diretorio_consultas_sql_ativo(),
        "diretorios_sugeridos": _listar_diretorios_sql_sugeridos(),
    }


@app.get("/api/oracle/conexao")
async def testar_conexao_oracle_endpoint(indice: Optional[int] = Query(None, ge=0, le=9)):
    """Testa conexao Oracle e retorna metadados basicos da sessao."""
    indice_oracle = _obter_indice_oracle_ativo(indice)
    try:
        resultado = testar_conexao_oracle(indice=indice_oracle)
        return {"status": "ok", "conexao": resultado}
    except Exception as erro:  # noqa: BLE001
        return _montar_resposta_erro_oracle(
            mensagem_padrao="Falha ao conectar no Oracle",
            erro=erro,
            indice_oracle=indice_oracle,
            mensagem_conexao="Falha na conexao com o Oracle",
        )


@app.get("/api/oracle/fontes")
async def listar_fontes_oracle(
    termo: Optional[str] = None,
    limite: int = Query(200, ge=1, le=1000),
    indice: Optional[int] = Query(None, ge=0, le=9),
):
    """Lista objetos Oracle candidatos para ajuste das consultas SQL."""
    indice_oracle = _obter_indice_oracle_ativo(indice)
    try:
        objetos = listar_objetos_oracle(termo=termo, limite=limite, indice=indice_oracle)
        return {"status": "ok", "objetos": objetos}
    except Exception as erro:  # noqa: BLE001
        return _montar_resposta_erro_oracle(
            mensagem_padrao="Falha ao listar fontes Oracle",
            erro=erro,
            indice_oracle=indice_oracle,
            mensagem_conexao="Falha na conexao com o Oracle ao listar fontes",
        )


@app.get("/api/oracle/mapeamentos")
async def listar_mapeamentos_oracle():
    """Lista placeholders Oracle com origem e fonte efetiva."""
    return {
        "status": "ok",
        "mapeamentos": detalhar_mapeamento_fontes_oracle(),
    }


@app.put("/api/oracle/mapeamentos")
async def salvar_mapeamentos_oracle(request: MapeamentoFontesOracleRequest):
    """Persiste overrides de fontes Oracle usados pelos SQLs versionados."""
    try:
        payload_normalizado: dict[str, Optional[str]] = {}
        for chave, valor in request.mapeamentos.items():
            chave_normalizada = str(chave).strip().upper()
            valor_normalizado = str(valor).strip().upper() if valor is not None else None

            if valor_normalizado:
                owner, objeto = separar_owner_objeto_oracle(valor_normalizado)
                valor_normalizado = f"{owner}.{objeto}" if owner else objeto

            payload_normalizado[chave_normalizada] = valor_normalizado

        salvar_mapeamento_fontes_oracle(payload_normalizado)
        return {
            "status": "ok",
            "mensagem": "Mapeamentos Oracle salvos com sucesso",
            "mapeamentos": detalhar_mapeamento_fontes_oracle(),
        }
    except ValueError as erro:
        raise HTTPException(status_code=400, detail=str(erro)) from erro
    except Exception as erro:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Falha ao salvar mapeamentos Oracle: {erro}") from erro


@app.get("/api/oracle/mapeamentos/validacao")
async def validar_mapeamentos_oracle(
    indice: Optional[int] = Query(None, ge=0, le=9),
):
    """Valida se os objetos Oracle mapeados existem e expõe amostra de colunas."""
    indice_oracle = _obter_indice_oracle_ativo(indice)
    try:
        validacoes = _validar_mapeamentos_oracle(indice=indice_oracle)
        return {
            "status": "ok",
            "validacoes": validacoes,
            "total_ok": sum(1 for item in validacoes if item["existe"] and not item["erro"]),
            "total_erro": sum(1 for item in validacoes if (not item["existe"]) or item["erro"]),
        }
    except Exception as erro:  # noqa: BLE001
        return _montar_resposta_erro_oracle(
            mensagem_padrao="Falha ao validar mapeamentos Oracle",
            erro=erro,
            indice_oracle=indice_oracle,
            mensagem_conexao="Falha na conexao com o Oracle ao validar mapeamentos",
        )


@app.get("/api/oracle/colunas/{objeto}")
async def listar_colunas_oracle(
    objeto: str,
    owner: Optional[str] = None,
    limite: int = Query(500, ge=1, le=2000),
    indice: Optional[int] = Query(None, ge=0, le=9),
):
    """Lista colunas de um objeto Oracle para apoiar mapeamento de SQL."""
    indice_oracle = _obter_indice_oracle_ativo(indice)
    try:
        colunas = listar_colunas_objeto_oracle(
            objeto=objeto,
            owner=owner,
            limite=limite,
            indice=indice_oracle,
        )
        return {"status": "ok", "colunas": colunas}
    except Exception as erro:  # noqa: BLE001
        return _montar_resposta_erro_oracle(
            mensagem_padrao="Falha ao listar colunas Oracle",
            erro=erro,
            indice_oracle=indice_oracle,
            mensagem_conexao="Falha na conexao com o Oracle ao listar colunas",
        )


@app.get("/api/contratos")
async def listar_contratos_endpoint():
    """Lista contratos de tabelas registrados no pipeline."""
    contratos = listar_contratos()
    return {
        "status": "ok",
        "contratos": [
            {
                "nome": contrato.nome,
                "descricao": contrato.descricao,
                "modulo": contrato.modulo,
                "funcao": contrato.funcao,
                "dependencias": contrato.dependencias,
                "saida": contrato.saida,
                "colunas": [
                    {
                        "nome": coluna.nome,
                        "tipo": coluna.tipo.value,
                        "descricao": coluna.descricao,
                        "obrigatoria": coluna.obrigatoria,
                    }
                    for coluna in contrato.colunas
                ],
            }
            for contrato in contratos
        ],
    }


@app.get("/api/contratos/ordem")
async def ordem_execucao():
    """Retorna ordem topologica de execucao."""
    return {
        "status": "ok",
        "ordem": ordem_topologica(),
    }


# =============================================================================
# ENDPOINTS DE TABELAS DE REFERÊNCIA
# =============================================================================

@app.get("/api/referencias/ncm")
async def listar_ncm(
    codigo: Optional[str] = Query(None, description="Filtrar por código NCM (exato ou parcial)"),
    limite: int = Query(1000, ge=1, le=10000, description="Limite de registros"),
):
    """Lista tabela NCM com filtro opcional por código."""
    df = carregar_ncm()
    if df.is_empty():
        return {"status": "ok", "dados": [], "total": 0}

    if codigo:
        codigo_limpo = codigo.replace(".", "").replace("-", "").strip()
        df = df.filter(pl.col("Codigo_NCM").str.starts_with(codigo_limpo))

    df = df.limit(limite)
    return {
        "status": "ok",
        "dados": df.to_dicts(),
        "total": len(df),
    }


@app.get("/api/referencias/ncm/{codigo}")
async def buscar_ncm(codigo: str):
    """Busca NCM por código exato."""
    df = buscar_ncm_por_codigo(codigo)
    if df.is_empty():
        raise HTTPException(status_code=404, detail=f"NCM {codigo} não encontrado")

    return {
        "status": "ok",
        "dados": df.to_dicts()[0],
        "valido": validar_ncm(codigo),
    }


@app.get("/api/referencias/cest")
async def listar_cest(
    codigo: Optional[str] = Query(None, description="Filtrar por código CEST"),
    limite: int = Query(1000, ge=1, le=10000),
):
    """Lista tabela CEST com filtro opcional por código."""
    df = carregar_cest()
    if df.is_empty():
        return {"status": "ok", "dados": [], "total": 0}

    if codigo:
        codigo_limpo = codigo.replace(".", "").strip()
        if "Codigo_CEST" in df.columns:
            df = df.filter(pl.col("Codigo_CEST").str.starts_with(codigo_limpo))
        else:
            df = df.filter(pl.col("codigo").str.starts_with(codigo_limpo))

    df = df.limit(limite)
    return {
        "status": "ok",
        "dados": df.to_dicts(),
        "total": len(df),
    }


@app.get("/api/referencias/cest/{codigo}")
async def buscar_cest(codigo: str):
    """Busca CEST por código exato."""
    df = buscar_cest_por_codigo(codigo)
    if df.is_empty():
        raise HTTPException(status_code=404, detail=f"CEST {codigo} não encontrado")

    return {
        "status": "ok",
        "dados": df.to_dicts()[0],
        "valido": validar_cest(codigo),
    }


@app.get("/api/referencias/cfop")
async def listar_cfop(
    codigo: Optional[str] = Query(None, description="Filtrar por código CFOP"),
    limite: int = Query(1000, ge=1, le=10000),
):
    """Lista tabela CFOP com filtro opcional por código."""
    df = carregar_cfop()
    if df.is_empty():
        return {"status": "ok", "dados": [], "total": 0}

    if codigo:
        codigo_limpo = codigo.replace(".", "").strip()
        if "CFOP" in df.columns:
            df = df.filter(pl.col("CFOP").str.starts_with(codigo_limpo))
        else:
            df = df.filter(pl.col("codigo").str.starts_with(codigo_limpo))

    df = df.limit(limite)
    return {
        "status": "ok",
        "dados": df.to_dicts(),
        "total": len(df),
    }


@app.get("/api/referencias/cfop/{codigo}")
async def buscar_cfop(codigo: str):
    """Busca CFOP por código exato."""
    df = buscar_cfop_por_codigo(codigo)
    if df.is_empty():
        raise HTTPException(status_code=404, detail=f"CFOP {codigo} não encontrado")

    return {
        "status": "ok",
        "dados": df.to_dicts()[0],
        "valido": validar_cfop(codigo),
    }


@app.get("/api/referencias/cst")
async def listar_cst(
    codigo: Optional[str] = Query(None, description="Filtrar por código CST"),
    limite: int = Query(500, ge=1, le=10000),
):
    """Lista tabela CST com filtro opcional por código."""
    df = carregar_cst()
    if df.is_empty():
        return {"status": "ok", "dados": [], "total": 0}

    if codigo:
        codigo_limpo = codigo.replace(".", "").strip()
        if "cst" in df.columns:
            df = df.filter(pl.col("cst").str.starts_with(codigo_limpo))

    df = df.limit(limite)
    return {
        "status": "ok",
        "dados": df.to_dicts(),
        "total": len(df),
    }


@app.get("/api/referencias/nfe/dominios")
async def listar_dominios_nfe():
    """Lista todos os domínios de NFe disponíveis."""
    dominios = carregar_dominios_nfe()
    return {
        "status": "ok",
        "dominios": {
            chave: {
                "total_registros": len(df),
                "colunas": df.columns,
            }
            for chave, df in dominios.items()
        },
    }


@app.get("/api/referencias/nfe/dominios/{nome}")
async def obter_dominio_nfe(nome: str):
    """Obtém um domínio específico de NFe."""
    dominios = carregar_dominios_nfe()
    if nome not in dominios:
        raise HTTPException(status_code=404, detail=f"Domínio {nome} não encontrado")

    df = dominios[nome]
    return {
        "status": "ok",
        "nome": nome,
        "dados": df.to_dicts(),
        "total": len(df),
    }


@app.get("/api/referencias/nfe/mapeamento")
async def obter_mapeamento_nfe():
    """Obtém mapeamento de campos de NFe."""
    df = carregar_mapeamento_nfe()
    if df.is_empty():
        return {"status": "ok", "dados": [], "total": 0}

    return {
        "status": "ok",
        "dados": df.to_dicts(),
        "total": len(df),
    }


@app.get("/api/referencias/nfe/eventos")
async def listar_eventos_nfe():
    """Lista tipos de eventos de NFe."""
    eventos = carregar_dominios_eventos_nfe()
    return {
        "status": "ok",
        "eventos": {
            chave: {
                "total_registros": len(df),
                "colunas": df.columns,
            }
            for chave, df in eventos.items()
        },
    }


@app.get("/api/referencias/fisconforme/malhas")
async def listar_malhas_fisconforme(limite: int = Query(500, ge=1, le=10000)):
    """Lista malhas de fiscalização do Fisconforme."""
    df = carregar_malhas_fisconforme()
    if df.is_empty():
        return {"status": "ok", "dados": [], "total": 0}

    df = df.limit(limite)
    return {
        "status": "ok",
        "dados": df.to_dicts(),
        "total": len(df),
    }


@app.post("/api/pipeline/executar")
async def executar_pipeline(request: ExecucaoRequest):
    """Executa extracao Oracle opcional e pipeline completo/parcial.

    Quando `data_limite` vier nula, a extracao considera todo o historico disponivel
    do CNPJ nas consultas Oracle versionadas.
    """
    cnpj = _normalizar_cnpj(request.cnpj)
    diretorio_cnpj = _obter_diretorio_cnpj(cnpj, criar=True)
    _garantir_estrutura_cnpj(diretorio_cnpj)
    indice_oracle = _obter_indice_oracle_ativo(request.indice_oracle)

    resumo_extracao: Optional[dict[str, Any]] = None

    if request.executar_extracao and request.consultas:
        try:
            conexao_disponivel, erro_conexao = _obter_status_conectividade_oracle(indice_oracle)
            if not conexao_disponivel:
                raise RuntimeError(erro_conexao or "Falha ao abrir conexao Oracle")

            resumo_extracao = extrair_dados_cnpj(
                cnpj_input=cnpj,
                consultas_alvo=request.consultas,
                data_limite=request.data_limite,
                diretorio_consultas=_obter_diretorio_consultas_sql_ativo(),
                indice_oracle=indice_oracle,
            )
        except Exception as erro:  # noqa: BLE001
            logger.exception("Falha na extracao Oracle para o CNPJ %s", cnpj)
            return _montar_resposta_falha_extracao(cnpj, erro, indice_oracle)

    orquestrador = OrquestradorPipeline(diretorio_cnpj, cnpj)
    resultado = orquestrador.executar_pipeline_completo(tabelas_alvo=request.tabelas_alvo)

    resposta = _resumo_resultado_pipeline(resultado, resumo_extracao)
    resposta["status"] = "concluido_com_erros" if resposta["erros_total"] else "concluido"
    resposta["extracao"] = resumo_extracao

    return resposta


@app.post("/api/pipeline/reprocessar")
async def reprocessar_pipeline(cnpj: str = Depends(validar_cnpj), tabela_editada: str = ...):
    """Reprocessa tabela editada e dependentes em cascata."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diretorio_cnpj = _obter_diretorio_cnpj(cnpj_limpo)

    orquestrador = OrquestradorPipeline(diretorio_cnpj, cnpj_limpo)
    resultado = orquestrador.reprocessar_a_partir_de(tabela_editada)

    return _resumo_resultado_pipeline(resultado)


@app.get("/api/pipeline/status/{cnpj}")
async def status_pipeline(cnpj: str = Depends(validar_cnpj)):
    """Retorna integridade atual dos parquets do CNPJ."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diretorio_cnpj = _obter_diretorio_cnpj(cnpj_limpo)

    orquestrador = OrquestradorPipeline(diretorio_cnpj, cnpj_limpo)
    integridade = orquestrador.verificar_integridade()

    return {
        "status": "ok",
        "cnpj": cnpj_limpo,
        "tabelas": integridade,
        "completo": all(integridade.values()),
    }


@app.get("/api/tabelas/{cnpj}")
async def listar_tabelas(cnpj: str = Depends(validar_cnpj), camada: str = Query("parquets")):
    """Lista tabelas parquet disponiveis no CNPJ."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    camada_validada = _validar_camada_armazenamento(camada)
    diretorio_parquets = _obter_diretorio_cnpj(cnpj_limpo) / camada_validada

    if not diretorio_parquets.exists():
        return {"status": "ok", "camada": camada_validada, "tabelas": []}

    tabelas: list[dict[str, Any]] = []
    for arquivo in sorted(diretorio_parquets.glob("*.parquet")):
        contrato = CONTRATOS.get(arquivo.stem)

        try:
            # ⚡ BOLT: Otimização de performance. Evita carregar o Parquet na memória inteiramente.
            # Usa lazy evaluation (read_parquet_schema e scan_parquet) para obter dados sem ler as linhas.
            try:
                schema = pl.read_parquet_schema(arquivo)
                colunas = list(schema.names())
                # Contagem otimizada usando lazy frame (pushdown no engine do polars)
                total_registros = pl.scan_parquet(arquivo).select(pl.len()).collect().item()
            except Exception:
                # Fallback seguro para testes unitarios que usam mocks do polars e corrompem o collect
                dataframe = pl.read_parquet(arquivo)
                total_registros = len(dataframe)
                colunas = dataframe.columns
        except Exception as erro:  # noqa: BLE001
            logger.warning("Falha ao ler metadados de %s: %s", arquivo, erro)
            total_registros = 0
            colunas = []

        tabelas.append(
            {
                "nome": arquivo.stem,
                "caminho": str(arquivo),
                "registros": total_registros,
                "colunas": colunas,
                "tamanho_bytes": arquivo.stat().st_size,
                "atualizado_em": datetime.fromtimestamp(arquivo.stat().st_mtime, tz=timezone.utc).isoformat(),
                "descricao": contrato.descricao if contrato else "",
                "camada": camada_validada,
            }
        )

    return {"status": "ok", "camada": camada_validada, "tabelas": tabelas}


@app.get("/api/tabelas/{cnpj}/{nome_tabela}")
async def ler_tabela(
    cnpj: str = Depends(validar_cnpj),
    nome_tabela: str = ...,
    camada: str = Query("parquets"),
    pagina: int = Query(1, ge=1),
    por_pagina: int = Query(50, ge=1, le=1000),
    filtro_coluna: Optional[str] = None,
    filtro_valor: Optional[str] = None,
    ordenar_por: Optional[str] = None,
    ordem: str = Query("asc", pattern="^(asc|desc)$"),
):
    """Le tabela parquet com paginacao e filtros."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    nome_tabela_valido = _validar_nome_tabela(nome_tabela)
    camada_validada = _validar_camada_armazenamento(camada)

    diretorio_parquets = _obter_diretorio_cnpj(cnpj_limpo) / camada_validada
    arquivo = (diretorio_parquets / f"{nome_tabela_valido}.parquet").resolve()

    try:
        arquivo.relative_to(diretorio_parquets.resolve())
    except ValueError as erro:
        raise HTTPException(status_code=400, detail="Caminho de tabela invalido") from erro

    if not arquivo.exists():
        raise HTTPException(status_code=404, detail=f"Tabela {nome_tabela_valido} nao encontrada")

    lf = pl.scan_parquet(arquivo)
    colunas_disponiveis = lf.collect_schema().names()

    if filtro_coluna and filtro_valor and filtro_coluna in colunas_disponiveis:
        lf = lf.filter(
            pl.col(filtro_coluna).cast(pl.Utf8, strict=False).str.contains(filtro_valor, literal=True)
        )

    if ordenar_por and ordenar_por in colunas_disponiveis:
        lf = lf.sort(ordenar_por, descending=(ordem == "desc"))

    total = lf.select(pl.len()).collect().item()
    inicio = (pagina - 1) * por_pagina
    dados = lf.slice(inicio, por_pagina).collect()

    colunas_finais = dados.columns
    dados_dict = dados.to_dicts()
    schema_dict = {coluna: str(dados[coluna].dtype) for coluna in colunas_finais}

    return {
        "status": "ok",
        "camada": camada_validada,
        "colunas": colunas_finais,
        "dados": dados_dict,
        "total_registros": total,
        "pagina": pagina,
        "por_pagina": por_pagina,
        "total_paginas": (total + por_pagina - 1) // por_pagina,
        "schema": schema_dict,
    }


@app.get("/api/storage/{cnpj}/manifesto")
async def obter_manifesto_cnpj(cnpj: str = Depends(validar_cnpj)):
    """Retorna manifesto operacional do CNPJ por camada."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diretorio_cnpj = _obter_diretorio_cnpj(cnpj_limpo)
    return {
        "status": "ok",
        "manifesto": gerar_manifesto_cnpj(diretorio_cnpj, cnpj=cnpj_limpo),
    }


@app.post("/api/agregacao/agregar")
async def agregar_produtos(cnpj: str = Depends(validar_cnpj), request: AgregacaoRequest = ...):
    """Registra edicao manual de agregacao e reprocessa dependentes."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diretorio_cnpj = _obter_diretorio_cnpj(cnpj_limpo)

    ids_produtos = [str(item).strip() for item in request.ids_produtos if str(item).strip()]
    if len(ids_produtos) < 2:
        raise HTTPException(status_code=400, detail="Informe ao menos dois IDs de produto para agregar")

    arquivo_produtos = diretorio_cnpj / "parquets" / "produtos.parquet"
    if not arquivo_produtos.exists():
        raise HTTPException(status_code=404, detail="Tabela produtos.parquet nao encontrada")

    df_produtos = pl.read_parquet(arquivo_produtos)
    coluna_identificador = "id_item" if "id_item" in df_produtos.columns else "id_produto" if "id_produto" in df_produtos.columns else None
    if coluna_identificador is None:
        raise HTTPException(status_code=400, detail="Tabela produtos sem coluna de identificador publico")

    ids_disponiveis = set(df_produtos[coluna_identificador].cast(pl.String, strict=False).to_list())
    ids_invalidos = [item for item in ids_produtos if item not in ids_disponiveis]
    if ids_invalidos:
        raise HTTPException(status_code=400, detail=f"IDs de produto invalidos: {ids_invalidos}")

    descricao_padrao = request.descricao_padrao
    if not descricao_padrao:
        df_primeiro = df_produtos.filter(pl.col(coluna_identificador).cast(pl.String, strict=False) == ids_produtos[0])
        descricao_padrao = str(df_primeiro["descricao"][0]) if not df_primeiro.is_empty() else ids_produtos[0]

    arquivo_edicoes = diretorio_cnpj / "edicoes" / "agregacao.json"
    edicoes = _carregar_json(arquivo_edicoes, {})

    ids_atuais = [str(item) for item in edicoes.get(descricao_padrao, [])]
    ids_combinados = sorted(set(ids_atuais + ids_produtos))
    edicoes[descricao_padrao] = ids_combinados
    _salvar_json(arquivo_edicoes, edicoes)

    orquestrador = OrquestradorPipeline(diretorio_cnpj, cnpj_limpo)
    resultado = orquestrador.reprocessar_a_partir_de("produtos_agrupados")

    return {
        "status": "ok",
        "mensagem": "Agregacao registrada e reprocessamento concluido",
        "descricao_padrao": descricao_padrao,
        "ids_produtos": ids_combinados,
        "pipeline": _resumo_resultado_pipeline(resultado),
    }


@app.post("/api/agregacao/desagregar")
async def desagregar_grupo(cnpj: str = Depends(validar_cnpj), request: DesagregacaoRequest = ...):
    """Remove edicao manual de agregacao e reprocessa dependentes."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diretorio_cnpj = _obter_diretorio_cnpj(cnpj_limpo)

    arquivo_edicoes = diretorio_cnpj / "edicoes" / "agregacao.json"
    edicoes = _carregar_json(arquivo_edicoes, {})

    chave_alvo = request.id_grupo
    if chave_alvo not in edicoes:
        arquivo_agrupados = diretorio_cnpj / "parquets" / "produtos_agrupados.parquet"
        if arquivo_agrupados.exists():
            df_agrupados = pl.read_parquet(arquivo_agrupados)
            candidatos = df_agrupados.filter(pl.col("id_agrupado") == request.id_grupo)
            if not candidatos.is_empty():
                coluna_descricao = (
                    "descricao_padrao"
                    if "descricao_padrao" in candidatos.columns
                    else "descr_padrao"
                    if "descr_padrao" in candidatos.columns
                    else None
                )
                if coluna_descricao:
                    chave_alvo = str(candidatos[coluna_descricao][0])

    if chave_alvo not in edicoes:
        raise HTTPException(status_code=404, detail="Grupo nao encontrado nas edicoes manuais")

    ids_removidos = edicoes.pop(chave_alvo)
    _salvar_json(arquivo_edicoes, edicoes)

    orquestrador = OrquestradorPipeline(diretorio_cnpj, cnpj_limpo)
    resultado = orquestrador.reprocessar_a_partir_de("produtos_agrupados")

    return {
        "status": "ok",
        "mensagem": "Grupo removido e pipeline reprocessado",
        "id_grupo": request.id_grupo,
        "descricao_padrao": chave_alvo,
        "ids_produtos": ids_removidos,
        "pipeline": _resumo_resultado_pipeline(resultado),
    }


@app.put("/api/conversao/fator")
async def editar_fator(cnpj: str = Depends(validar_cnpj), request: EdicaoFatorRequest = ...):
    """Registra edicao manual de fator e reprocessa dependentes."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diretorio_cnpj = _obter_diretorio_cnpj(cnpj_limpo)

    if (
        request.unid_ref is None
        and request.fator is None
        and request.fator_compra_ref is None
        and request.fator_venda_ref is None
    ):
        raise HTTPException(status_code=400, detail="Informe ao menos um campo para edicao do fator")

    arquivo_edicoes = diretorio_cnpj / "edicoes" / "fatores.json"
    edicoes = _carregar_json(arquivo_edicoes, {})

    registro = edicoes.get(request.id_agrupado, {})

    if request.unid_ref is not None:
        registro["unid_ref"] = request.unid_ref

    if request.fator is not None:
        registro["fator_compra_ref"] = request.fator
        registro["fator_venda_ref"] = request.fator

    if request.fator_compra_ref is not None:
        registro["fator_compra_ref"] = request.fator_compra_ref

    if request.fator_venda_ref is not None:
        registro["fator_venda_ref"] = request.fator_venda_ref

    edicoes[request.id_agrupado] = registro
    _salvar_json(arquivo_edicoes, edicoes)

    orquestrador = OrquestradorPipeline(diretorio_cnpj, cnpj_limpo)
    resultado = orquestrador.reprocessar_a_partir_de("fatores_conversao")

    return {
        "status": "ok",
        "mensagem": "Fator atualizado e pipeline reprocessado",
        "id_agrupado": request.id_agrupado,
        "edicao": registro,
        "pipeline": _resumo_resultado_pipeline(resultado),
    }


@app.post("/api/conversao/recalcular")
async def recalcular_derivados(cnpj: str = Depends(validar_cnpj)):
    """Reprocessa cadeia de tabelas dependentes de fatores."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diretorio_cnpj = _obter_diretorio_cnpj(cnpj_limpo)

    orquestrador = OrquestradorPipeline(diretorio_cnpj, cnpj_limpo)
    resultado = orquestrador.reprocessar_a_partir_de("fatores_conversao")

    return {
        "status": "ok",
        "mensagem": "Recalculo concluido",
        "pipeline": _resumo_resultado_pipeline(resultado),
    }


@app.get("/api/exportar/{cnpj}/{nome_tabela}")
async def exportar_tabela(
    cnpj: str = Depends(validar_cnpj),
    nome_tabela: str = ...,
    formato: str = Query("xlsx", pattern="^(xlsx|csv|parquet)$"),
):
    """Exporta tabela parquet para xlsx, csv ou parquet."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    nome_tabela_valido = _validar_nome_tabela(nome_tabela)

    diretorio_cnpj = _obter_diretorio_cnpj(cnpj_limpo)
    diretorio_parquets = diretorio_cnpj / "parquets"
    diretorio_exportacoes = diretorio_cnpj / "exportacoes"
    diretorio_exportacoes.mkdir(parents=True, exist_ok=True)

    origem = diretorio_parquets / f"{nome_tabela_valido}.parquet"
    if not origem.exists():
        raise HTTPException(status_code=404, detail=f"Tabela {nome_tabela_valido} nao encontrada")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destino = diretorio_exportacoes / f"{nome_tabela_valido}_{timestamp}.{formato}"

    if formato == "parquet":
        shutil.copy2(origem, destino)
    elif formato == "csv":
        exportar_csv(origem, destino)
    else:
        exportar_excel(origem, destino, formatado=True)

    media_types = {
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "csv": "text/csv",
        "parquet": "application/octet-stream",
    }

    return FileResponse(
        path=destino,
        filename=destino.name,
        media_type=media_types[formato],
    )


# ============================================================
# RELATORIOS FISCAIS CONCLUSIVOS
# ============================================================

from relatorio_fiscal import (
    carregar_dados_relatorio,
    carregar_json as rf_carregar_json,
    carregar_dsfs_efetivas,
    diagnosticar_prontidao_relatorios,
    listar_cnpjs_com_relatorio as rf_listar_cnpjs_com_relatorio,
    listar_dets_disponiveis,
    salvar_json as rf_salvar_json,
    obter_caminho_auditor,
    obter_caminho_relatorio_cnpj,
    obter_diretorio_relatorio_cnpj,
    carregar_dsfs as rf_carregar_dsfs,
    salvar_dsf as rf_salvar_dsf,
    gerar_docx_individual,
    gerar_pdf_individual,
    gerar_pdf_geral,
    normalizar_manifestacoes as rf_normalizar_manifestacoes,
    resumir_manifestacoes as rf_resumir_manifestacoes,
)


class AuditorRequest(BaseModel):
    nome: str
    cargo: str = "Auditor Fiscal de Tributos Estaduais"
    matricula: str
    orgao: str = "SEFIN/CRE/GEFIS - Gerencia de Fiscalizacao"
    endereco: str = "Avenida Farquar, n. 2986 - Palacio Rio Madeira - Bairro Pedrinhas - CEP 76.801-470 - Porto Velho/RO"
    local_data: Optional[str] = None


class ManifestacoesRequest(BaseModel):
    regularizou_integralmente: bool = False
    apresentou_contestacao: bool = False
    solicitou_prorrogacao: bool = False
    nao_apresentou_manifestacao: bool = True


class RelatorioRequest(BaseModel):
    cnpj: str = ""
    contribuinte: str
    ie: str = ""
    dsf: str = ""
    notificacao_det: str = ""
    manifestacao: str = "Nao apresentou manifestacao"
    manifestacoes: Optional[ManifestacoesRequest] = None
    contatos_realizados: str = ""
    decisao_fiscal: str = ""
    desfecho: str = ""
    arquivos_notificacao_incluidos: Optional[list[str]] = None


class DsfRequest(BaseModel):
    numero: str
    descricao: str = ""
    cnpjs: list[str] = []


class GerarRelatorioGeralRequest(BaseModel):
    dsf: Optional[str] = None
    cnpjs: list[str] = []
    incluir_dets: bool = True


def _resposta_erro_relatorio(
    mensagem: str,
    *,
    status_code: int,
    codigo: str,
    detalhe: str = "",
    diagnostico: Optional[dict[str, Any]] = None,
) -> JSONResponse:
    """Retorna erro operacional estruturado para fluxos de relatorio."""
    payload = {
        "status": "erro",
        "mensagem": mensagem,
        "erro": {
            "codigo": codigo,
            "mensagem": mensagem,
            "detalhe": detalhe or mensagem,
        },
    }
    if diagnostico:
        payload["diagnostico"] = diagnostico
    return JSONResponse(status_code=status_code, content=payload)


# ---- Auditor ----

@app.get("/api/relatorio/auditor")
async def obter_auditor():
    """Retorna dados do auditor persistidos."""
    caminho = obter_caminho_auditor(BASE_DIR)
    dados = rf_carregar_json(caminho, {})
    return {"status": "ok", "auditor": dados}


@app.put("/api/relatorio/auditor")
async def salvar_auditor(request: AuditorRequest):
    """Salva dados do auditor de forma centralizada."""
    caminho = obter_caminho_auditor(BASE_DIR)
    payload = request.model_dump()
    if not payload.get("local_data"):
        payload["local_data"] = datetime.now().strftime("Porto Velho, %d de %B de %Y")
    rf_salvar_json(caminho, payload)
    return {"status": "ok", "mensagem": "Dados do auditor salvos", "auditor": payload}


# ---- DSF ----

@app.get("/api/relatorio/diagnostico")
async def obter_diagnostico_relatorios():
    """Resume prontidao das dependencias e do storage de relatorios."""
    diagnostico = diagnosticar_prontidao_relatorios(BASE_DIR)

    cnpj_referencia = "37671507000187"
    try:
        diretorio_cnpj = _obter_diretorio_cnpj(cnpj_referencia)
        orquestrador = OrquestradorPipeline(diretorio_cnpj, cnpj_referencia)
        integridade = orquestrador.verificar_integridade()
        diagnostico["pipeline_local"] = {
            "cnpj_referencia": cnpj_referencia,
            "completo": all(integridade.values()),
            "tabelas": integridade,
            "total_tabelas_ok": sum(1 for valor in integridade.values() if valor),
            "total_tabelas": len(integridade),
        }
    except HTTPException:
        diagnostico["pipeline_local"] = {
            "cnpj_referencia": cnpj_referencia,
            "completo": False,
            "tabelas": {},
            "total_tabelas_ok": 0,
            "total_tabelas": len(CONTRATOS),
        }

    return {"status": "ok", **diagnostico}

@app.get("/api/relatorio/dsf")
async def listar_todas_dsfs():
    """Lista todas as DSFs cadastradas com seus CNPJs vinculados."""
    dsfs = list(carregar_dsfs_efetivas(BASE_DIR).values())
    return {"status": "ok", "dsfs": dsfs}


@app.get("/api/relatorio/dsf/{numero}")
async def obter_dsf_por_numero(numero: str):
    """Retorna uma DSF especifica pelo numero."""
    dsfs = carregar_dsfs_efetivas(BASE_DIR)
    dsf = dsfs.get(numero)
    if not dsf:
        return {"status": "ok", "dsf": None}
    return {"status": "ok", "dsf": dsf}


@app.put("/api/relatorio/dsf/{numero}")
async def salvar_dsf_endpoint(numero: str, request: DsfRequest):
    """Salva ou atualiza uma DSF com seus CNPJs vinculados."""
    cnpjs_limpos = [re.sub(r"\D", "", c) for c in request.cnpjs if len(re.sub(r"\D", "", c)) == 14]
    dsf = rf_salvar_dsf(BASE_DIR, numero, request.descricao, cnpjs_limpos)
    return {"status": "ok", "mensagem": f"DSF {numero} salva com {len(cnpjs_limpos)} CNPJs", "dsf": dsf}


@app.delete("/api/relatorio/dsf/{numero}")
async def excluir_dsf(numero: str):
    """Exclui uma DSF."""
    dsfs = rf_carregar_dsfs(BASE_DIR)
    if numero in dsfs:
        del dsfs[numero]
        from relatorio_fiscal import salvar_dsfs as rf_salvar_dsfs_all
        rf_salvar_dsfs_all(BASE_DIR, dsfs)
    return {"status": "ok", "mensagem": f"DSF {numero} excluida"}


# ---- Relatorio por CNPJ ----

@app.get("/api/relatorio/listar-cnpjs-com-relatorio")
async def listar_cnpjs_com_relatorio():
    """Lista todos os CNPJs que possuem dados de relatorio preenchidos."""
    return {"status": "ok", "cnpjs": rf_listar_cnpjs_com_relatorio(BASE_DIR)}


@app.get("/api/relatorio/cnpj/{cnpj}")
async def obter_relatorio_cnpj(cnpj: str = Depends(validar_cnpj)):
    """Retorna dados do relatorio de um CNPJ."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    dados = carregar_dados_relatorio(BASE_DIR, cnpj_limpo)
    return {"status": "ok", "cnpj": cnpj_limpo, "dados": dados}


@app.put("/api/relatorio/cnpj/{cnpj}")
async def salvar_relatorio_cnpj(cnpj: str = Depends(validar_cnpj), request: RelatorioRequest = ...):
    """Salva dados do relatorio de um CNPJ."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    caminho = obter_caminho_relatorio_cnpj(BASE_DIR, cnpj_limpo)
    manifestacoes = rf_normalizar_manifestacoes(
        request.manifestacoes.model_dump() if request.manifestacoes else None,
        request.manifestacao,
    )
    payload = {
        "cnpj": cnpj_limpo,
        "contribuinte": request.contribuinte,
        "ie": request.ie,
        "dsf": request.dsf,
        "notificacao_det": request.notificacao_det,
        "manifestacoes": manifestacoes,
        "manifestacao": rf_resumir_manifestacoes(manifestacoes),
        "contatos_realizados": request.contatos_realizados,
        "decisao_fiscal": request.decisao_fiscal,
        "desfecho": request.desfecho,
    }
    if request.arquivos_notificacao_incluidos is not None:
        payload["arquivos_notificacao_incluidos"] = request.arquivos_notificacao_incluidos
    rf_salvar_json(caminho, payload)
    return {
        "status": "ok",
        "mensagem": f"Relatorio do CNPJ {cnpj_limpo} salvo",
        "dados": carregar_dados_relatorio(BASE_DIR, cnpj_limpo),
    }


@app.get("/api/relatorio/cnpj/{cnpj}/listar-dets")
async def listar_dets_cnpj(cnpj: str = Depends(validar_cnpj)):
    """Lista PDFs DET disponiveis na pasta do CNPJ."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    dets = listar_dets_disponiveis(BASE_DIR, cnpj_limpo)
    return {"status": "ok", "cnpj": cnpj_limpo, "dets": dets}


@app.post("/api/relatorio/cnpj/{cnpj}/upload-det")
async def upload_det_cnpj(cnpj: str = Depends(validar_cnpj), arquivo: UploadFile = File(...)):
    """Recebe upload de PDF DET via multipart/form-data."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diretorio = obter_diretorio_relatorio_cnpj(BASE_DIR, cnpj_limpo)

    if not arquivo.filename or not arquivo.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Apenas arquivos PDF sao aceitos")

    nome_arquivo = Path(arquivo.filename).name
    destino = diretorio / nome_arquivo
    if not destino.resolve().is_relative_to(diretorio.resolve()):
        raise HTTPException(status_code=400, detail="Caminho de arquivo invalido")

    conteudo = await arquivo.read()
    with destino.open("wb") as f:
        f.write(conteudo)

    caminho_dados = obter_caminho_relatorio_cnpj(BASE_DIR, cnpj_limpo)
    dados_existentes = rf_carregar_json(caminho_dados, {})
    if isinstance(dados_existentes.get("arquivos_notificacao_incluidos"), list):
        arquivos = [
            nome
            for nome in dados_existentes["arquivos_notificacao_incluidos"]
            if isinstance(nome, str)
        ]
        if nome_arquivo not in arquivos:
            arquivos.append(nome_arquivo)
            dados_existentes["arquivos_notificacao_incluidos"] = arquivos
            rf_salvar_json(caminho_dados, dados_existentes)

    return {
        "status": "ok",
        "mensagem": f"DET '{nome_arquivo}' salvo para CNPJ {cnpj_limpo}",
        "arquivo": {
            "nome": nome_arquivo,
            "caminho": str(destino),
            "tamanho_bytes": len(conteudo),
        },
    }


# ---- Geracao de PDFs ----

@app.post("/api/relatorio/cnpj/{cnpj}/gerar-docx")
async def gerar_docx_cnpj(cnpj: str = Depends(validar_cnpj)):
    """Gera DOCX do relatorio individual do CNPJ preenchendo o modelo Word."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diagnostico = diagnosticar_prontidao_relatorios(BASE_DIR)
    modelo_individual = diagnostico.get("modelos_docx", {}).get("individual", {})

    auditor = rf_carregar_json(obter_caminho_auditor(BASE_DIR), {})
    if not auditor.get("nome"):
        return _resposta_erro_relatorio(
            "Dados do auditor nao configurados",
            status_code=400,
            codigo="auditor_nao_configurado",
            diagnostico=diagnostico,
        )
    if not modelo_individual.get("pronto"):
        return _resposta_erro_relatorio(
            "Modelo Word individual indisponivel",
            status_code=503,
            codigo="relatorio_modelo_individual_indisponivel",
            detalhe=modelo_individual.get("mensagem", "Modelo individual nao encontrado"),
            diagnostico=diagnostico,
        )

    dados_relatorio = carregar_dados_relatorio(BASE_DIR, cnpj_limpo)
    if not dados_relatorio.get("contribuinte"):
        return _resposta_erro_relatorio(
            f"Dados do relatorio do CNPJ {cnpj_limpo} nao preenchidos",
            status_code=400,
            codigo="relatorio_cnpj_nao_preenchido",
            diagnostico=diagnostico,
        )

    diretorio = obter_diretorio_relatorio_cnpj(BASE_DIR, cnpj_limpo)
    nome_curto = dados_relatorio["contribuinte"].split()[0]
    output_filename = f"Relatorio_Final_{nome_curto}_{cnpj_limpo}.docx"
    output_path = str(diretorio / output_filename)

    try:
        gerar_docx_individual(dados_relatorio, auditor, output_path)
    except Exception as erro:
        logger.error("Erro ao gerar DOCX para CNPJ %s: %s", cnpj_limpo, erro)
        return _resposta_erro_relatorio(
            "Erro ao gerar DOCX individual",
            status_code=503,
            codigo="relatorio_docx_individual_indisponivel",
            detalhe=str(erro),
            diagnostico=diagnostico,
        )

    return FileResponse(
        path=output_path,
        filename=output_filename,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


# ---- Geracao de PDFs ----

@app.post("/api/relatorio/cnpj/{cnpj}/gerar-pdf")
async def gerar_pdf_cnpj(cnpj: str = Depends(validar_cnpj)):
    """Gera PDF do relatorio individual do CNPJ e mescla com DET se disponivel."""
    cnpj_limpo = _normalizar_cnpj(cnpj)
    diagnostico = diagnosticar_prontidao_relatorios(BASE_DIR)
    modelo_individual = diagnostico.get("modelos_docx", {}).get("individual", {})

    auditor = rf_carregar_json(obter_caminho_auditor(BASE_DIR), {})
    if not auditor.get("nome"):
        return _resposta_erro_relatorio(
            "Dados do auditor nao configurados",
            status_code=400,
            codigo="auditor_nao_configurado",
            diagnostico=diagnostico,
        )
    if not modelo_individual.get("pronto"):
        return _resposta_erro_relatorio(
            "Modelo Word individual indisponivel",
            status_code=503,
            codigo="relatorio_modelo_individual_indisponivel",
            detalhe=modelo_individual.get("mensagem", "Modelo individual nao encontrado"),
            diagnostico=diagnostico,
        )

    dados_relatorio = carregar_dados_relatorio(BASE_DIR, cnpj_limpo)
    if not dados_relatorio.get("contribuinte"):
        return _resposta_erro_relatorio(
            f"Dados do relatorio do CNPJ {cnpj_limpo} nao preenchidos",
            status_code=400,
            codigo="relatorio_cnpj_nao_preenchido",
            diagnostico=diagnostico,
        )

    diretorio = obter_diretorio_relatorio_cnpj(BASE_DIR, cnpj_limpo)
    pdfs_notificacao = dados_relatorio.get("caminhos_notificacao_incluidos", [])

    nome_curto = dados_relatorio["contribuinte"].split()[0]
    output_filename = f"Relatorio_Final_{nome_curto}_{cnpj_limpo}.pdf"
    output_path = str(diretorio / output_filename)

    try:
        gerar_pdf_individual(dados_relatorio, auditor, output_path, pdfs_notificacao)
    except Exception as erro:
        logger.error("Erro ao gerar PDF para CNPJ %s: %s", cnpj_limpo, erro)
        return _resposta_erro_relatorio(
            "Erro ao gerar PDF individual",
            status_code=503,
            codigo="relatorio_pdf_individual_indisponivel",
            detalhe=str(erro),
            diagnostico=diagnostico,
        )

    return FileResponse(
        path=output_path,
        filename=output_filename,
        media_type="application/pdf",
    )


@app.post("/api/relatorio/gerar-geral")
async def gerar_relatorio_geral(request: GerarRelatorioGeralRequest):
    """Gera PDF do relatorio geral consolidado.

    Aceita uma lista explicita de CNPJs ou um numero de DSF para resolver os CNPJs vinculados.
    """
    diagnostico = diagnosticar_prontidao_relatorios(BASE_DIR)
    modelo_geral = diagnostico.get("modelos_docx", {}).get("geral", {})
    auditor = rf_carregar_json(obter_caminho_auditor(BASE_DIR), {})
    if not auditor.get("nome"):
        return _resposta_erro_relatorio(
            "Dados do auditor nao configurados",
            status_code=400,
            codigo="auditor_nao_configurado",
            diagnostico=diagnostico,
        )
    if not modelo_geral.get("pronto"):
        return _resposta_erro_relatorio(
            "Modelo Word geral indisponivel",
            status_code=503,
            codigo="relatorio_modelo_geral_indisponivel",
            detalhe=modelo_geral.get("mensagem", "Modelo geral nao encontrado"),
            diagnostico=diagnostico,
        )

    # Resolver CNPJs: prioridade para DSF, fallback para lista explicita
    cnpjs_para_gerar = list(request.cnpjs)
    if request.dsf:
        dsfs = carregar_dsfs_efetivas(BASE_DIR)
        dsf_data = dsfs.get(request.dsf)
        if dsf_data and dsf_data.get("cnpjs"):
            cnpjs_para_gerar = dsf_data["cnpjs"]

    empresas = []
    for cnpj_raw in cnpjs_para_gerar:
        cnpj_limpo = re.sub(r"\D", "", cnpj_raw)
        if len(cnpj_limpo) != 14:
            continue
        dados = carregar_dados_relatorio(BASE_DIR, cnpj_limpo)
        if dados.get("contribuinte"):
            empresas.append(dados)

    if not empresas:
        return _resposta_erro_relatorio(
            "Nenhum CNPJ com dados de relatorio preenchidos",
            status_code=400,
            codigo="relatorio_geral_sem_empresas",
            diagnostico=diagnostico,
        )

    output_dir = BASE_DIR.parent / "_config"
    output_dir.mkdir(parents=True, exist_ok=True)

    dsf_label = request.dsf or "geral"
    output_filename = f"Relatorio_Geral_Consolidado_{dsf_label}.pdf"
    output_path = str(output_dir / output_filename)

    try:
        gerar_pdf_geral(empresas, auditor, output_path, request.incluir_dets)
    except Exception as erro:
        logger.error("Erro ao gerar relatorio geral: %s", erro)
        return _resposta_erro_relatorio(
            "Erro ao gerar relatorio geral",
            status_code=503,
            codigo="relatorio_pdf_geral_indisponivel",
            detalhe=str(erro),
            diagnostico=diagnostico,
        )

    return FileResponse(
        path=output_path,
        filename=output_filename,
        media_type="application/pdf",
    )


# ============================================================
# SERVIDOR DE ARQUIVOS ESTÁTICOS (PRODUÇÃO)
# ============================================================
# Monta o diretório de build do Vite para servir em produção
# e implementa fallback para index.html (SPA routing)

import os

# Diretório de build do Vite (configurado em vite.config.ts)
BUILD_DIR = Path(__file__).parent.parent.parent / "dist" / "public"


def _serve_index_html() -> HTMLResponse:
    """Retorna index.html para SPA routing."""
    index_path = BUILD_DIR / "index.html"
    if not index_path.exists():
        return HTMLResponse(
            content="<html><body><h1>Frontend em desenvolvimento</h1><p>Execute <code>pnpm dev</code> para iniciar o Vite.</p></body></html>",
            status_code=200,
        )
    with open(index_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


# Monta arquivos estáticos do build do Vite
# Nota: Em desenvolvimento, use o Vite diretamente (pnpm dev)
if BUILD_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(BUILD_DIR / "assets"), html=False), name="assets")


# Handler para SPA routing - serve index.html para rotas desconhecidas
@app.get("/{path:path}")
async def spa_fallback(path: str):
    """Fallback para index.html em rotas não-API (SPA routing)."""
    # Se o caminho for um arquivo estático existente, retorna o arquivo
    if path.startswith("assets/"):
        arquivo_estatico = BUILD_DIR / path
        if arquivo_estatico.exists() and arquivo_estatico.is_file():
            return FileResponse(path=arquivo_estatico)
    
    # Para todas as outras rotas, retorna index.html (client-side routing)
    return _serve_index_html()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
