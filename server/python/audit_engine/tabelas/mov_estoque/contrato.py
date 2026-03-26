from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

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
