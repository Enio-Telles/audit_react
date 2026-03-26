from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

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
