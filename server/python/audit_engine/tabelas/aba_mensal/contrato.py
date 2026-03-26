from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

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
