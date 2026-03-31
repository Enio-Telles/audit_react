from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

registrar_contrato(ContratoTabela(

    nome="nfe_entrada",
    descricao="NFe de entrada enriquecidas com classificação CO SEFIN",
    modulo="modulos.estoque",
    funcao="gerar_nfe_entrada",
    dependencias=["produtos_final", "id_agrupados"],
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
