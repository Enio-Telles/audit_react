"""
Contratos de Tabelas — audit_engine
Define schemas, dependências e metadados de cada tabela do pipeline.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class TipoColuna(Enum):
    STRING = "string"
    INT = "int"
    FLOAT = "float"
    DATE = "date"
    BOOL = "bool"


@dataclass
class ColunaSchema:
    nome: str
    tipo: TipoColuna
    descricao: str = ""
    obrigatoria: bool = True
    valor_padrao: Optional[str] = None


@dataclass
class ContratoTabela:
    """Define o contrato de uma tabela do pipeline."""
    nome: str
    descricao: str
    modulo: str
    funcao: str
    colunas: List[ColunaSchema] = field(default_factory=list)
    dependencias: List[str] = field(default_factory=list)
    saida: str = ""  # nome do arquivo parquet de saída

    @property
    def nomes_colunas(self) -> List[str]:
        return [c.nome for c in self.colunas]


# === Registro de Contratos ===

CONTRATOS: Dict[str, ContratoTabela] = {}


def registrar_contrato(contrato: ContratoTabela) -> ContratoTabela:
    """Registra um contrato no catálogo global."""
    CONTRATOS[contrato.nome] = contrato
    return contrato


def obter_contrato(nome: str) -> ContratoTabela:
    """Obtém um contrato pelo nome da tabela."""
    if nome not in CONTRATOS:
        raise KeyError(f"Contrato não encontrado: {nome}")
    return CONTRATOS[nome]


def listar_contratos() -> List[ContratoTabela]:
    """Lista todos os contratos registrados."""
    return list(CONTRATOS.values())


def ordem_topologica() -> List[str]:
    """Retorna a ordem de execução das tabelas respeitando dependências."""
    visitados = set()
    ordem = []

    def visitar(nome: str):
        if nome in visitados:
            return
        visitados.add(nome)
        contrato = CONTRATOS.get(nome)
        if contrato:
            for dep in contrato.dependencias:
                visitar(dep)
            ordem.append(nome)

    for nome in CONTRATOS:
        visitar(nome)

    return ordem
