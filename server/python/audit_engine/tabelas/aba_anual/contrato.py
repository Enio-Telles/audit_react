from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

registrar_contrato(ContratoTabela(

    nome="aba_anual",
    descricao="Consolidação anual de estoque por produto agrupado",
    modulo="modulos.estoque",
    funcao="gerar_aba_anual",
    dependencias=["aba_mensal"],
    saida="aba_anual.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao", TipoColuna.STRING, "Descrição"),
        ColunaSchema("ano", TipoColuna.STRING, "Ano (YYYY)"),
        ColunaSchema("saldo_inicial_ano", TipoColuna.FLOAT, "Saldo inicial do ano"),
        ColunaSchema("total_entradas", TipoColuna.FLOAT, "Total de entradas no ano"),
        ColunaSchema("total_saidas", TipoColuna.FLOAT, "Total de saídas no ano"),
        ColunaSchema("saldo_final_ano", TipoColuna.FLOAT, "Saldo final do ano"),
        ColunaSchema("custo_medio_anual", TipoColuna.FLOAT, "Custo médio anual"),
        ColunaSchema("valor_estoque_final", TipoColuna.FLOAT, "Valor do estoque final"),
        ColunaSchema("meses_com_omissao", TipoColuna.INT, "Meses com omissão"),
        ColunaSchema("total_omissao", TipoColuna.FLOAT, "Valor total de omissões"),
    ],

))
