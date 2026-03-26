from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato

registrar_contrato(ContratoTabela(

    nome="produtos_agrupados",
    descricao="Tabela de produtos após agregação (De/Para)",
    modulo="modulos.agregacao",
    funcao="gerar_produtos_agrupados",
    dependencias=["produtos"],
    saida="produtos_agrupados.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID do grupo"),
        ColunaSchema("descricao_padrao", TipoColuna.STRING, "Descrição padrão do grupo"),
        ColunaSchema("ncm_padrao", TipoColuna.STRING, "NCM padrão"),
        ColunaSchema("cest_padrao", TipoColuna.STRING, "CEST padrão", obrigatoria=False),
        ColunaSchema("ids_membros", TipoColuna.STRING, "IDs dos produtos membros (JSON)"),
        ColunaSchema("qtd_membros", TipoColuna.INT, "Quantidade de membros"),
        ColunaSchema("qtd_total_nfe", TipoColuna.INT, "Total de NFe do grupo"),
        ColunaSchema("valor_total", TipoColuna.FLOAT, "Valor total do grupo"),
        ColunaSchema("unid_compra", TipoColuna.STRING, "Unidade de compra predominante"),
        ColunaSchema("unid_venda", TipoColuna.STRING, "Unidade de venda predominante"),
        ColunaSchema("origem", TipoColuna.STRING, "Origem: manual/automatico"),
        ColunaSchema("criado_em", TipoColuna.DATE, "Data de criação"),
        ColunaSchema("editado_em", TipoColuna.DATE, "Data de última edição", obrigatoria=False),
        ColunaSchema("status", TipoColuna.STRING, "Status: ativo/inativo"),
    ],

))
