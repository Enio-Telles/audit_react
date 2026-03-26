from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

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
