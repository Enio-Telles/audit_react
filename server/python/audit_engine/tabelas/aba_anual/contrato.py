from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

registrar_contrato(ContratoTabela(

    nome="aba_anual",
    descricao="Consolidação anual do estoque confrontando inventário declarado vs calculado",
    modulo="modulos.estoque",
    funcao="gerar_aba_anual",
    dependencias=["mov_estoque"],
    saida="aba_anual.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao", TipoColuna.STRING, "Descrição do Produto"),
        ColunaSchema("ano", TipoColuna.INT, "Ano da movimentação"),
        ColunaSchema("estoque_inicial", TipoColuna.FLOAT, "Totalização de Estoque Inicial do Ano"),
        ColunaSchema("entradas", TipoColuna.FLOAT, "Totalização de Entradas no Ano"),
        ColunaSchema("saidas", TipoColuna.FLOAT, "Totalização de Saídas no Ano"),
        ColunaSchema("estoque_final", TipoColuna.FLOAT, "Inventário físico declarado no final do período"),
        ColunaSchema("saldo_final", TipoColuna.FLOAT, "Saldo físico sistêmico apurado no fim do período"),
        ColunaSchema("entradas_desacob", TipoColuna.FLOAT, "Omissão de Entradas no Ano"),
        ColunaSchema("saidas_desacob", TipoColuna.FLOAT, "Omissão de Saídas por falta de inventário final"),
        ColunaSchema("estoque_final_desacob", TipoColuna.FLOAT, "Omissão por excesso não declarado de estoque final"),
        ColunaSchema("pme", TipoColuna.FLOAT, "Preço Médio de Entrada no ano"),
        ColunaSchema("pms", TipoColuna.FLOAT, "Preço Médio de Saída no ano"),
        ColunaSchema("ICMS_saidas_desac", TipoColuna.FLOAT, "ICMS Tributado sobre saídas desacobertadas", obrigatoria=False),
        ColunaSchema("ICMS_estoque_desac", TipoColuna.FLOAT, "ICMS Tributado sobre excesso de estoque", obrigatoria=False),
    ],

))
