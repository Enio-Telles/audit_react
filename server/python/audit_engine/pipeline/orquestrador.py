"""
Orquestrador de Pipeline — audit_engine
Executa tabelas na ordem topológica, gerencia dependências e reprocessamento.
Baseado no orquestrador_pipeline.py do audit_pyside.
"""
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

from ..contratos.tabelas import (
    CONTRATOS,
    ContratoTabela,
    obter_contrato,
    ordem_topologica,
)

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
        return [e.tabela for e in self.etapas if e.status == StatusEtapa.CONCLUIDA]


# Registro de funções geradoras
_GERADORES: Dict[str, Callable] = {}


def registrar_gerador(nome_tabela: str):
    """Decorator para registrar uma função geradora de tabela."""
    def decorator(func: Callable):
        _GERADORES[nome_tabela] = func
        return func
    return decorator


class OrquestradorPipeline:
    """
    Orquestra a execução do pipeline de tabelas.
    
    Responsabilidades:
    - Resolver ordem de execução via topologia de dependências
    - Executar geradores na ordem correta
    - Gerenciar reprocessamento parcial (cascade)
    - Reportar progresso e erros
    """

    def __init__(self, diretorio_cnpj: Path, cnpj: str):
        self.diretorio_cnpj = diretorio_cnpj
        self.cnpj = cnpj
        self.diretorio_parquets = diretorio_cnpj / "parquets"
        self.diretorio_parquets.mkdir(parents=True, exist_ok=True)
        self._callback_progresso: Optional[Callable] = None

    def set_callback_progresso(self, callback: Callable):
        """Define callback para reportar progresso."""
        self._callback_progresso = callback

    def _reportar_progresso(self, etapa: str, status: StatusEtapa, mensagem: str = ""):
        if self._callback_progresso:
            self._callback_progresso(etapa, status.value, mensagem)

    def executar_pipeline_completo(
        self,
        tabelas_alvo: Optional[List[str]] = None,
    ) -> ResultadoPipeline:
        """
        Executa o pipeline completo ou parcial.
        
        Args:
            tabelas_alvo: Se fornecido, executa apenas essas tabelas e suas dependências.
        """
        resultado = ResultadoPipeline(cnpj=self.cnpj, inicio=time.time())
        
        # Determinar ordem de execução
        ordem = ordem_topologica()
        
        if tabelas_alvo:
            # Filtrar apenas tabelas necessárias (alvo + dependências)
            necessarias = self._resolver_dependencias(tabelas_alvo)
            ordem = [t for t in ordem if t in necessarias]

        logger.info(f"Pipeline iniciado para CNPJ {self.cnpj}")
        logger.info(f"Ordem de execução: {ordem}")

        for nome_tabela in ordem:
            etapa_resultado = self._executar_etapa(nome_tabela)
            resultado.etapas.append(etapa_resultado)

            if etapa_resultado.status == StatusEtapa.ERRO:
                resultado.erros.append(
                    f"Erro em {nome_tabela}: {etapa_resultado.mensagem}"
                )
                # Continuar com próximas tabelas que não dependem desta
                logger.warning(f"Erro em {nome_tabela}, continuando pipeline...")

        resultado.fim = time.time()
        
        if resultado.erros:
            resultado.status = "concluido_com_erros"
        
        logger.info(
            f"Pipeline concluído em {resultado.duracao_total_ms}ms. "
            f"Tabelas geradas: {len(resultado.tabelas_geradas)}/{len(ordem)}"
        )
        
        return resultado

    def _executar_etapa(self, nome_tabela: str) -> ResultadoEtapa:
        """Executa uma única etapa do pipeline."""
        contrato = obter_contrato(nome_tabela)
        gerador = _GERADORES.get(nome_tabela)

        self._reportar_progresso(nome_tabela, StatusEtapa.EXECUTANDO)

        if not gerador:
            logger.warning(f"Gerador não registrado para {nome_tabela}, pulando...")
            self._reportar_progresso(nome_tabela, StatusEtapa.PULADA, "Gerador não registrado")
            return ResultadoEtapa(
                tabela=nome_tabela,
                status=StatusEtapa.PULADA,
                mensagem="Gerador não registrado",
            )

        # Verificar se dependências foram geradas
        for dep in contrato.dependencias:
            arquivo_dep = self.diretorio_parquets / CONTRATOS[dep].saida
            if not arquivo_dep.exists():
                msg = f"Dependência não encontrada: {dep}"
                self._reportar_progresso(nome_tabela, StatusEtapa.ERRO, msg)
                return ResultadoEtapa(
                    tabela=nome_tabela,
                    status=StatusEtapa.ERRO,
                    mensagem=msg,
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
            logger.info(f"  {nome_tabela}: {registros} registros em {duracao}ms")
            
            return ResultadoEtapa(
                tabela=nome_tabela,
                status=StatusEtapa.CONCLUIDA,
                duracao_ms=duracao,
                registros_gerados=registros,
                arquivo_saida=str(arquivo_saida),
            )
        except Exception as e:
            duracao = int((time.time() - inicio) * 1000)
            msg = str(e)
            self._reportar_progresso(nome_tabela, StatusEtapa.ERRO, msg)
            logger.error(f"  Erro em {nome_tabela}: {msg}")
            
            return ResultadoEtapa(
                tabela=nome_tabela,
                status=StatusEtapa.ERRO,
                mensagem=msg,
                duracao_ms=duracao,
            )

    def reprocessar_a_partir_de(self, tabela_editada: str) -> ResultadoPipeline:
        """
        Reprocessa todas as tabelas que dependem (direta ou indiretamente)
        da tabela editada. Usado após edição manual de fatores/agregação.
        """
        dependentes = self._resolver_dependentes(tabela_editada)
        logger.info(f"Reprocessando a partir de {tabela_editada}: {dependentes}")
        return self.executar_pipeline_completo(tabelas_alvo=list(dependentes))

    def _resolver_dependencias(self, tabelas: List[str]) -> Set[str]:
        """Resolve todas as dependências transitivas de um conjunto de tabelas."""
        necessarias: Set[str] = set()
        
        def resolver(nome: str):
            if nome in necessarias:
                return
            necessarias.add(nome)
            contrato = CONTRATOS.get(nome)
            if contrato:
                for dep in contrato.dependencias:
                    resolver(dep)
        
        for t in tabelas:
            resolver(t)
        
        return necessarias

    def _resolver_dependentes(self, tabela: str) -> Set[str]:
        """Resolve todas as tabelas que dependem (direta ou indiretamente) de uma tabela."""
        dependentes: Set[str] = set()
        
        def resolver(nome: str):
            for nome_t, contrato in CONTRATOS.items():
                if nome in contrato.dependencias and nome_t not in dependentes:
                    dependentes.add(nome_t)
                    resolver(nome_t)
        
        resolver(tabela)
        return dependentes

    def verificar_integridade(self) -> Dict[str, bool]:
        """Verifica se todos os parquets existem e estão íntegros."""
        resultado = {}
        for nome, contrato in CONTRATOS.items():
            arquivo = self.diretorio_parquets / contrato.saida
            resultado[nome] = arquivo.exists()
        return resultado
