from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

registrar_contrato(ContratoTabela(

    nome="mov_estoque",
    descricao="Movimentação de estoque consolidada (entradas + saídas + inventário)",
    modulo="modulos.estoque",
    funcao="gerar_mov_estoque",
    dependencias=["nfe_entrada", "produtos_final", "id_agrupados"],
    saida="mov_estoque.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao", TipoColuna.STRING, "Descrição"),
        ColunaSchema("tipo", TipoColuna.STRING, "Tipo: 0 - ESTOQUE INICIAL / 1 - ENTRADA / 2 - SAIDAS / 3 - ESTOQUE FINAL"),
        ColunaSchema("data", TipoColuna.DATE, "Data do movimento", obrigatoria=False),
        ColunaSchema("ano", TipoColuna.INT, "Ano da movimentação", obrigatoria=False),
        ColunaSchema("mes", TipoColuna.INT, "Mês da movimentação", obrigatoria=False),
        ColunaSchema("q_conv", TipoColuna.FLOAT, "Quantidade convertida"),
        ColunaSchema("valor_unitario", TipoColuna.FLOAT, "Valor unitário original", obrigatoria=False),
        ColunaSchema("valor_total", TipoColuna.FLOAT, "Valor total", obrigatoria=False),
        ColunaSchema("saldo_estoque_anual", TipoColuna.FLOAT, "Saldo acumulado"),
        ColunaSchema("custo_medio_anual", TipoColuna.FLOAT, "Custo médio ponderado"),
        ColunaSchema("entr_desac_anual", TipoColuna.FLOAT, "Entradas desacobertadas"),
        ColunaSchema("cfop", TipoColuna.STRING, "CFOP", obrigatoria=False),
        ColunaSchema("origem", TipoColuna.STRING, "Origem: nfe/efd/inventario/gerado"),
        ColunaSchema("excluir_estoque", TipoColuna.BOOL, "Flag de exclusão do estoque", obrigatoria=False),
        ColunaSchema("__qtd_decl_final_audit__", TipoColuna.FLOAT, "Qtd final declarada", obrigatoria=False),
    ],

))
