from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

registrar_contrato(ContratoTabela(

    nome="aba_mensal",
    descricao="Resumo mensal do fluxo cronológico de estoque, com análise fiscal e tributária (ICMS/ST)",
    modulo="modulos.estoque",
    funcao="gerar_aba_mensal",
    dependencias=["mov_estoque"],
    saida="aba_mensal.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao", TipoColuna.STRING, "Descrição do Produto"),
        ColunaSchema("ano", TipoColuna.INT, "Ano da movimentação"),
        ColunaSchema("mes", TipoColuna.INT, "Mês numérico da movimentação"),
        ColunaSchema("ano_mes", TipoColuna.STRING, "Mês (YYYY-MM)", obrigatoria=False),
        ColunaSchema("valor_entradas", TipoColuna.FLOAT, "Total do valor de entradas no mês"),
        ColunaSchema("qtd_entradas", TipoColuna.FLOAT, "Quantidade física convertida de entradas"),
        ColunaSchema("valor_saidas", TipoColuna.FLOAT, "Total do valor de saídas no mês"),
        ColunaSchema("qtd_saidas", TipoColuna.FLOAT, "Quantidade física convertida de saídas"),
        ColunaSchema("pme_mes", TipoColuna.FLOAT, "Preço Médio de Entrada válido no mês"),
        ColunaSchema("pms_mes", TipoColuna.FLOAT, "Preço Médio de Saída válido no mês"),
        ColunaSchema("entradas_desacob", TipoColuna.FLOAT, "Soma mensal das omissões detectadas na mov_estoque"),
        ColunaSchema("ICMS_entr_desacob", TipoColuna.FLOAT, "ICMS apurado sobre entradas desacobertadas", obrigatoria=False),
        ColunaSchema("saldo_mes", TipoColuna.FLOAT, "Saldo remanescente físico no fechamento cronológico do mês"),
        ColunaSchema("custo_medio_mes", TipoColuna.FLOAT, "Último custo médio apurado no fechamento do mês"),
        ColunaSchema("valor_estoque", TipoColuna.FLOAT, "Valor final ponderado do estoque (saldo * custo)"),
        ColunaSchema("ST", TipoColuna.STRING, "Registro literal da vigência ST cruzada no exercício do mês", obrigatoria=False),
        ColunaSchema("MVA", TipoColuna.FLOAT, "Margem de Valor Agregado quando incidente", obrigatoria=False),
        ColunaSchema("MVA_ajustado", TipoColuna.FLOAT, "MVA Efetivo ou Ajustado tributário", obrigatoria=False),
    ],

))
