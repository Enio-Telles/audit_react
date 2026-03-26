from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

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
