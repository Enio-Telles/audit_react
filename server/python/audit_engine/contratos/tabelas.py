"""
Contratos de Tabelas — audit_engine
Define schemas, dependências e metadados de cada tabela do pipeline.
Baseado na estrutura modular do audit_pyside.
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


# === Definição dos Contratos ===

registrar_contrato(ContratoTabela(
    nome="produtos_unidades",
    descricao="Tabela base com produtos e suas unidades de medida, extraída do cruzamento NFe x EFD",
    modulo="modulos.produtos",
    funcao="gerar_produtos_unidades",
    dependencias=[],
    saida="produtos_unidades.parquet",
    colunas=[
        ColunaSchema("id_produto", TipoColuna.INT, "ID único do produto"),
        ColunaSchema("descricao", TipoColuna.STRING, "Descrição do produto"),
        ColunaSchema("ncm", TipoColuna.STRING, "Código NCM"),
        ColunaSchema("cest", TipoColuna.STRING, "Código CEST", obrigatoria=False),
        ColunaSchema("gtin", TipoColuna.STRING, "Código de barras GTIN", obrigatoria=False),
        ColunaSchema("unid_compra", TipoColuna.STRING, "Unidade de compra"),
        ColunaSchema("unid_venda", TipoColuna.STRING, "Unidade de venda"),
        ColunaSchema("qtd_nfe_compra", TipoColuna.INT, "Quantidade de NFe de compra"),
        ColunaSchema("qtd_nfe_venda", TipoColuna.INT, "Quantidade de NFe de venda"),
        ColunaSchema("qtd_efd", TipoColuna.INT, "Quantidade de registros EFD"),
        ColunaSchema("valor_total_compra", TipoColuna.FLOAT, "Valor total de compras"),
        ColunaSchema("valor_total_venda", TipoColuna.FLOAT, "Valor total de vendas"),
    ],
))

registrar_contrato(ContratoTabela(
    nome="produtos",
    descricao="Tabela de produtos consolidada (sem duplicatas de unidade)",
    modulo="modulos.produtos",
    funcao="gerar_produtos",
    dependencias=["produtos_unidades"],
    saida="produtos.parquet",
    colunas=[
        ColunaSchema("id_produto", TipoColuna.INT, "ID único do produto"),
        ColunaSchema("descricao", TipoColuna.STRING, "Descrição do produto"),
        ColunaSchema("ncm", TipoColuna.STRING, "Código NCM"),
        ColunaSchema("cest", TipoColuna.STRING, "Código CEST", obrigatoria=False),
        ColunaSchema("unidade_principal", TipoColuna.STRING, "Unidade principal"),
        ColunaSchema("qtd_total_nfe", TipoColuna.INT, "Total de NFe"),
        ColunaSchema("valor_total", TipoColuna.FLOAT, "Valor total"),
        ColunaSchema("tipo", TipoColuna.STRING, "Tipo: compra/venda/ambos"),
    ],
))

registrar_contrato(ContratoTabela(
    nome="produtos_agrupados",
    descricao="Tabela de produtos após agregação (De/Para)",
    modulo="modulos.agregacao",
    funcao="gerar_produtos_agrupados",
    dependencias=["produtos"],
    saida="produtos_agrupados.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao_padrao", TipoColuna.STRING, "Descrição padrão do grupo"),
        ColunaSchema("ncm_padrao", TipoColuna.STRING, "NCM padrão"),
        ColunaSchema("cest_padrao", TipoColuna.STRING, "CEST padrão", obrigatoria=False),
        ColunaSchema("ids_membros", TipoColuna.STRING, "IDs dos produtos membros (JSON)"),
        ColunaSchema("qtd_membros", TipoColuna.INT, "Quantidade de membros"),
        ColunaSchema("qtd_total_nfe", TipoColuna.INT, "Total de NFe do grupo"),
        ColunaSchema("valor_total", TipoColuna.FLOAT, "Valor total do grupo"),
        ColunaSchema("unid_compra", TipoColuna.STRING, "Unidade de compra predominante"),
        ColunaSchema("unid_venda", TipoColuna.STRING, "Unidade de venda predominante"),
        ColunaSchema("origem", TipoColuna.STRING, "Origem: manual/automatico"),
        ColunaSchema("criado_em", TipoColuna.DATE, "Data de criação"),
        ColunaSchema("editado_em", TipoColuna.DATE, "Data de última edição", obrigatoria=False),
        ColunaSchema("status", TipoColuna.STRING, "Status: ativo/inativo"),
    ],
))

registrar_contrato(ContratoTabela(
    nome="fatores_conversao",
    descricao="Fatores de conversão entre unidades de compra, venda e referência",
    modulo="modulos.conversao",
    funcao="gerar_fatores_conversao",
    dependencias=["produtos_agrupados"],
    saida="fatores_conversao.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao_padrao", TipoColuna.STRING, "Descrição padrão"),
        ColunaSchema("unid_compra", TipoColuna.STRING, "Unidade de compra"),
        ColunaSchema("unid_venda", TipoColuna.STRING, "Unidade de venda"),
        ColunaSchema("unid_ref", TipoColuna.STRING, "Unidade de referência"),
        ColunaSchema("fator_compra_ref", TipoColuna.FLOAT, "Fator compra → referência"),
        ColunaSchema("fator_venda_ref", TipoColuna.FLOAT, "Fator venda → referência"),
        ColunaSchema("origem_fator", TipoColuna.STRING, "Origem: reg0220/manual/calculado"),
        ColunaSchema("status", TipoColuna.STRING, "Status: ok/pendente"),
        ColunaSchema("editado_em", TipoColuna.DATE, "Data de última edição", obrigatoria=False),
    ],
))

registrar_contrato(ContratoTabela(
    nome="produtos_final",
    descricao="Tabela final de produtos com fatores de conversão aplicados",
    modulo="modulos.conversao",
    funcao="gerar_produtos_final",
    dependencias=["produtos_agrupados", "fatores_conversao"],
    saida="produtos_final.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao_padrao", TipoColuna.STRING, "Descrição padrão"),
        ColunaSchema("ncm_padrao", TipoColuna.STRING, "NCM padrão"),
        ColunaSchema("cest_padrao", TipoColuna.STRING, "CEST padrão", obrigatoria=False),
        ColunaSchema("unid_ref", TipoColuna.STRING, "Unidade de referência"),
        ColunaSchema("fator_compra_ref", TipoColuna.FLOAT, "Fator compra → referência"),
        ColunaSchema("fator_venda_ref", TipoColuna.FLOAT, "Fator venda → referência"),
        ColunaSchema("qtd_total_nfe", TipoColuna.INT, "Total de NFe"),
        ColunaSchema("valor_total", TipoColuna.FLOAT, "Valor total"),
        ColunaSchema("ids_membros", TipoColuna.STRING, "IDs dos produtos membros (JSON)"),
        ColunaSchema("qtd_membros", TipoColuna.INT, "Quantidade de membros"),
        ColunaSchema("status_conversao", TipoColuna.STRING, "Status da conversão"),
        ColunaSchema("status_agregacao", TipoColuna.STRING, "Status da agregação"),
    ],
))

registrar_contrato(ContratoTabela(
    nome="nfe_entrada",
    descricao="NFe de entrada enriquecidas com classificação CO SEFIN",
    modulo="modulos.estoque",
    funcao="gerar_nfe_entrada",
    dependencias=["produtos_final"],
    saida="nfe_entrada.parquet",
    colunas=[
        ColunaSchema("chave_nfe", TipoColuna.STRING, "Chave de acesso da NFe"),
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("data_emissao", TipoColuna.DATE, "Data de emissão"),
        ColunaSchema("cfop", TipoColuna.STRING, "CFOP"),
        ColunaSchema("quantidade", TipoColuna.FLOAT, "Quantidade"),
        ColunaSchema("unidade", TipoColuna.STRING, "Unidade"),
        ColunaSchema("qtd_ref", TipoColuna.FLOAT, "Quantidade na unidade de referência"),
        ColunaSchema("valor_unitario", TipoColuna.FLOAT, "Valor unitário"),
        ColunaSchema("valor_total", TipoColuna.FLOAT, "Valor total"),
        ColunaSchema("cnpj_emitente", TipoColuna.STRING, "CNPJ do emitente"),
    ],
))

registrar_contrato(ContratoTabela(
    nome="mov_estoque",
    descricao="Movimentação de estoque consolidada (entradas + saídas + inventário)",
    modulo="modulos.estoque",
    funcao="gerar_mov_estoque",
    dependencias=["nfe_entrada", "produtos_final"],
    saida="mov_estoque.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao", TipoColuna.STRING, "Descrição"),
        ColunaSchema("tipo", TipoColuna.STRING, "Tipo: ENTRADA/SAIDA/INVENTARIO"),
        ColunaSchema("data", TipoColuna.DATE, "Data do movimento"),
        ColunaSchema("quantidade", TipoColuna.FLOAT, "Quantidade na unidade de referência"),
        ColunaSchema("valor_unitario", TipoColuna.FLOAT, "Valor unitário"),
        ColunaSchema("valor_total", TipoColuna.FLOAT, "Valor total"),
        ColunaSchema("saldo", TipoColuna.FLOAT, "Saldo acumulado"),
        ColunaSchema("custo_medio", TipoColuna.FLOAT, "Custo médio ponderado"),
        ColunaSchema("cfop", TipoColuna.STRING, "CFOP", obrigatoria=False),
        ColunaSchema("origem", TipoColuna.STRING, "Origem: nfe/efd/inventario"),
    ],
))

registrar_contrato(ContratoTabela(
    nome="aba_mensal",
    descricao="Consolidação mensal de estoque por produto agrupado",
    modulo="modulos.estoque",
    funcao="gerar_aba_mensal",
    dependencias=["mov_estoque"],
    saida="aba_mensal.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao", TipoColuna.STRING, "Descrição"),
        ColunaSchema("mes", TipoColuna.STRING, "Mês (YYYY-MM)"),
        ColunaSchema("saldo_inicial", TipoColuna.FLOAT, "Saldo inicial do mês"),
        ColunaSchema("entradas", TipoColuna.FLOAT, "Total de entradas no mês"),
        ColunaSchema("saidas", TipoColuna.FLOAT, "Total de saídas no mês"),
        ColunaSchema("saldo_final", TipoColuna.FLOAT, "Saldo final do mês"),
        ColunaSchema("custo_medio", TipoColuna.FLOAT, "Custo médio ponderado"),
        ColunaSchema("valor_estoque", TipoColuna.FLOAT, "Valor do estoque"),
        ColunaSchema("qtd_movimentos", TipoColuna.INT, "Quantidade de movimentos"),
        ColunaSchema("omissao", TipoColuna.BOOL, "Indica omissão de estoque"),
    ],
))

registrar_contrato(ContratoTabela(
    nome="aba_anual",
    descricao="Consolidação anual de estoque por produto agrupado",
    modulo="modulos.estoque",
    funcao="gerar_aba_anual",
    dependencias=["aba_mensal"],
    saida="aba_anual.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao", TipoColuna.STRING, "Descrição"),
        ColunaSchema("ano", TipoColuna.STRING, "Ano (YYYY)"),
        ColunaSchema("saldo_inicial_ano", TipoColuna.FLOAT, "Saldo inicial do ano"),
        ColunaSchema("total_entradas", TipoColuna.FLOAT, "Total de entradas no ano"),
        ColunaSchema("total_saidas", TipoColuna.FLOAT, "Total de saídas no ano"),
        ColunaSchema("saldo_final_ano", TipoColuna.FLOAT, "Saldo final do ano"),
        ColunaSchema("custo_medio_anual", TipoColuna.FLOAT, "Custo médio anual"),
        ColunaSchema("valor_estoque_final", TipoColuna.FLOAT, "Valor do estoque final"),
        ColunaSchema("meses_com_omissao", TipoColuna.INT, "Meses com omissão"),
        ColunaSchema("total_omissao", TipoColuna.FLOAT, "Valor total de omissões"),
    ],
))

registrar_contrato(ContratoTabela(
    nome="id_agrupados",
    descricao="Mapeamento de IDs originais para IDs agrupados",
    modulo="modulos.agregacao",
    funcao="gerar_id_agrupados",
    dependencias=["produtos_agrupados"],
    saida="id_agrupados.parquet",
    colunas=[
        ColunaSchema("id_produto", TipoColuna.INT, "ID original do produto"),
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao_original", TipoColuna.STRING, "Descrição original"),
        ColunaSchema("descricao_padrao", TipoColuna.STRING, "Descrição padrão do grupo"),
    ],
))

registrar_contrato(ContratoTabela(
    nome="produtos_selecionados",
    descricao="Produtos selecionados para análise detalhada de estoque",
    modulo="modulos.estoque",
    funcao="gerar_produtos_selecionados",
    dependencias=["produtos_final"],
    saida="produtos_selecionados.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao_padrao", TipoColuna.STRING, "Descrição padrão"),
        ColunaSchema("ncm_padrao", TipoColuna.STRING, "NCM padrão"),
        ColunaSchema("selecionado", TipoColuna.BOOL, "Se está selecionado para análise"),
        ColunaSchema("motivo", TipoColuna.STRING, "Motivo da seleção", obrigatoria=False),
    ],
))
