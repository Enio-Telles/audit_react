"""Orquestrador de pipeline de tabelas por CNPJ."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

from ..contratos.base import CONTRATOS, obter_contrato, ordem_topologica
from ..utils.camada_silver import materializar_camadas_silver
from ..utils.camadas_cnpj import garantir_estrutura_camadas_cnpj
from ..utils.manifesto_cnpj import gerar_manifesto_cnpj

logger = logging.getLogger(__name__)


class StatusEtapa(Enum):
    PENDENTE = "pendente"
    EXECUTANDO = "executando"
    CONCLUIDA = "concluida"
    ERRO = "erro"
    PULADA = "pulada"


@dataclass
class ResultadoEtapa:
    tabela: str
    status: StatusEtapa
    mensagem: str = ""
    duracao_ms: int = 0
    registros_gerados: int = 0
    arquivo_saida: str = ""


@dataclass
class ResultadoPipeline:
    cnpj: str
    status: str = "concluido"
    etapas: List[ResultadoEtapa] = field(default_factory=list)
    erros: List[str] = field(default_factory=list)
    inicio: float = 0
    fim: float = 0

    @property
    def duracao_total_ms(self) -> int:
        return int((self.fim - self.inicio) * 1000) if self.fim else 0

    @property
    def tabelas_geradas(self) -> List[str]:
        return [item.tabela for item in self.etapas if item.status == StatusEtapa.CONCLUIDA]


_GERADORES: Dict[str, Callable] = {}


def registrar_gerador(nome_tabela: str):
    """Registra funcao geradora no catalogo global do pipeline."""

    def decorator(func: Callable):
        _GERADORES[nome_tabela] = func
        return func

    return decorator


class OrquestradorPipeline:
    """Executa pipeline completo/parcial e reprocessamento em cascata."""

    def __init__(self, diretorio_cnpj: Path, cnpj: str):
        self.diretorio_cnpj = diretorio_cnpj
        self.cnpj = cnpj
        garantir_estrutura_camadas_cnpj(diretorio_cnpj)
        self.diretorio_parquets = diretorio_cnpj / "parquets"
        self.diretorio_silver = diretorio_cnpj / "silver"
        self._callback_progresso: Optional[Callable] = None

    def set_callback_progresso(self, callback: Callable):
        """Define callback opcional para reportar progresso da execucao."""
        self._callback_progresso = callback

    def _reportar_progresso(self, etapa: str, status: StatusEtapa, mensagem: str = ""):
        if self._callback_progresso:
            self._callback_progresso(etapa, status.value, mensagem)

    def _validar_tabela(self, nome_tabela: str) -> None:
        """Valida se tabela existe no catalogo de contratos."""
        if nome_tabela not in CONTRATOS:
            raise KeyError(f"Tabela nao registrada no pipeline: {nome_tabela}")

    def executar_pipeline_completo(
        self,
        tabelas_alvo: Optional[List[str]] = None,
    ) -> ResultadoPipeline:
        """Executa pipeline completo ou parcial."""
        resultado = ResultadoPipeline(cnpj=self.cnpj, inicio=time.time())
        self._materializar_silver()

        ordem_execucao = ordem_topologica()

        if tabelas_alvo:
            for tabela in tabelas_alvo:
                self._validar_tabela(tabela)

            tabelas_necessarias = self._resolver_dependencias(tabelas_alvo)
            ordem_execucao = [tabela for tabela in ordem_execucao if tabela in tabelas_necessarias]

        logger.info("Pipeline iniciado para CNPJ %s", self.cnpj)
        logger.info("Ordem de execucao: %s", ordem_execucao)

        for nome_tabela in ordem_execucao:
            etapa_resultado = self._executar_etapa(nome_tabela)
            resultado.etapas.append(etapa_resultado)

            if etapa_resultado.status == StatusEtapa.ERRO:
                mensagem = f"Erro em {nome_tabela}: {etapa_resultado.mensagem}"
                resultado.erros.append(mensagem)
                logger.warning("%s", mensagem)

        resultado.fim = time.time()

        if resultado.erros:
            resultado.status = "concluido_com_erros"

        logger.info(
            "Pipeline concluido em %sms. Tabelas geradas: %s/%s",
            resultado.duracao_total_ms,
            len(resultado.tabelas_geradas),
            len(ordem_execucao),
        )
        self.gerar_manifesto()

        return resultado

    def _materializar_silver(self) -> None:
        """Prepara as tabelas intermediarias reaproveitaveis do pipeline."""
        self._reportar_progresso("silver", StatusEtapa.EXECUTANDO, "Materializando camada silver")
        materializar_camadas_silver(self.diretorio_cnpj, self.cnpj)
        self._reportar_progresso("silver", StatusEtapa.CONCLUIDA)

    def _executar_etapa(self, nome_tabela: str) -> ResultadoEtapa:
        """Executa uma etapa individual respeitando dependencias declaradas."""
        contrato = obter_contrato(nome_tabela)
        gerador = _GERADORES.get(nome_tabela)

        self._reportar_progresso(nome_tabela, StatusEtapa.EXECUTANDO)

        if not gerador:
            mensagem = "Gerador nao registrado"
            self._reportar_progresso(nome_tabela, StatusEtapa.PULADA, mensagem)
            return ResultadoEtapa(
                tabela=nome_tabela,
                status=StatusEtapa.PULADA,
                mensagem=mensagem,
            )

        for dependencia in contrato.dependencias:
            arquivo_dependencia = self.diretorio_parquets / CONTRATOS[dependencia].saida
            if not arquivo_dependencia.exists():
                mensagem = f"Dependencia nao encontrada: {dependencia}"
                self._reportar_progresso(nome_tabela, StatusEtapa.ERRO, mensagem)
                return ResultadoEtapa(
                    tabela=nome_tabela,
                    status=StatusEtapa.ERRO,
                    mensagem=mensagem,
                )

        inicio = time.time()
        try:
            arquivo_saida = self.diretorio_parquets / contrato.saida
            registros = gerador(
                diretorio_cnpj=self.diretorio_cnpj,
                diretorio_parquets=self.diretorio_parquets,
                arquivo_saida=arquivo_saida,
                contrato=contrato,
            )
            duracao = int((time.time() - inicio) * 1000)

            self._reportar_progresso(nome_tabela, StatusEtapa.CONCLUIDA)

            return ResultadoEtapa(
                tabela=nome_tabela,
                status=StatusEtapa.CONCLUIDA,
                duracao_ms=duracao,
                registros_gerados=registros,
                arquivo_saida=str(arquivo_saida),
            )
        except Exception as erro:  # noqa: BLE001
            duracao = int((time.time() - inicio) * 1000)
            mensagem = str(erro)
            self._reportar_progresso(nome_tabela, StatusEtapa.ERRO, mensagem)
            logger.exception("Falha na tabela %s", nome_tabela)

            return ResultadoEtapa(
                tabela=nome_tabela,
                status=StatusEtapa.ERRO,
                mensagem=mensagem,
                duracao_ms=duracao,
            )

    def reprocessar_a_partir_de(self, tabela_editada: str) -> ResultadoPipeline:
        """Reprocessa tabela editada e todos os dependentes transitivos."""
        self._validar_tabela(tabela_editada)

        dependentes = self._resolver_dependentes(tabela_editada)
        alvos_reprocessamento = {tabela_editada, *dependentes}

        logger.info(
            "Reprocessando a partir de %s. Alvos: %s",
            tabela_editada,
            sorted(alvos_reprocessamento),
        )

        return self.executar_pipeline_completo(tabelas_alvo=sorted(alvos_reprocessamento))

    def _resolver_dependencias(self, tabelas: List[str]) -> Set[str]:
        """Resolve dependencias transitivas das tabelas alvo."""
        necessarias: Set[str] = set()

        def visitar(nome_tabela: str):
            if nome_tabela in necessarias:
                return
            necessarias.add(nome_tabela)
            contrato = CONTRATOS.get(nome_tabela)
            if contrato:
                for dependencia in contrato.dependencias:
                    visitar(dependencia)

        for tabela in tabelas:
            visitar(tabela)

        return necessarias

    def _resolver_dependentes(self, tabela: str) -> Set[str]:
        """Resolve todas as tabelas que dependem da tabela informada."""
        dependentes: Set[str] = set()

        def visitar(nome_tabela: str):
            for tabela_contrato, contrato in CONTRATOS.items():
                if nome_tabela in contrato.dependencias and tabela_contrato not in dependentes:
                    dependentes.add(tabela_contrato)
                    visitar(tabela_contrato)

        visitar(tabela)
        return dependentes

    def verificar_integridade(self) -> Dict[str, bool]:
        """Verifica se arquivos parquet de todas as tabelas existem."""
        return {
            nome_tabela: (self.diretorio_parquets / contrato.saida).exists()
            for nome_tabela, contrato in CONTRATOS.items()
        }

    def gerar_manifesto(self) -> dict:
        """Gera manifesto consolidado do CNPJ com metadados das camadas."""
        return gerar_manifesto_cnpj(self.diretorio_cnpj, cnpj=self.cnpj)
