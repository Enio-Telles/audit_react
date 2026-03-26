from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

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
