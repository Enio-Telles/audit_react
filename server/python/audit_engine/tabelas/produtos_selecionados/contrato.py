from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

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
