from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

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
