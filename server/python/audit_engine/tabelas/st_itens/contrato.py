from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato


registrar_contrato(
    ContratoTabela(
        nome="st_itens",
        descricao="Consolidacao auditavel de ST por documento e item com base em XML e C176",
        modulo="tabelas.st_itens",
        funcao="gerar_st_itens",
        dependencias=["produtos", "id_agrupados", "produtos_final"],
        saida="st_itens.parquet",
        colunas=[
            ColunaSchema("id_linha_origem", TipoColuna.STRING, "Chave canonica da linha documental"),
            ColunaSchema("chave_documento", TipoColuna.STRING, "Chave ou identificador do documento"),
            ColunaSchema("item_documento", TipoColuna.STRING, "Numero do item no documento"),
            ColunaSchema("cnpj_referencia", TipoColuna.STRING, "CNPJ analisado"),
            ColunaSchema("data_documento", TipoColuna.STRING, "Data do documento"),
            ColunaSchema("id_agrupado", TipoColuna.STRING, "Produto agrupado vinculado", obrigatoria=False),
            ColunaSchema("codigo_fonte", TipoColuna.STRING, "Chave do produto antes do agrupamento"),
            ColunaSchema("codigo_produto", TipoColuna.STRING, "Codigo original do item"),
            ColunaSchema("descricao", TipoColuna.STRING, "Descricao documental do item"),
            ColunaSchema("descricao_padrao", TipoColuna.STRING, "Descricao padronizada do grupo", obrigatoria=False),
            ColunaSchema("ncm", TipoColuna.STRING, "NCM do item", obrigatoria=False),
            ColunaSchema("cest", TipoColuna.STRING, "CEST do item", obrigatoria=False),
            ColunaSchema("cfop", TipoColuna.STRING, "CFOP do item", obrigatoria=False),
            ColunaSchema("cst", TipoColuna.STRING, "CST do item", obrigatoria=False),
            ColunaSchema("csosn", TipoColuna.STRING, "CSOSN do item", obrigatoria=False),
            ColunaSchema("quantidade", TipoColuna.FLOAT, "Quantidade documental do item"),
            ColunaSchema("valor_total", TipoColuna.FLOAT, "Valor total documental do item"),
            ColunaSchema("bc_st_xml", TipoColuna.FLOAT, "Base ST apurada no XML"),
            ColunaSchema("vl_st_xml", TipoColuna.FLOAT, "Valor ST apurado no XML"),
            ColunaSchema("vl_icms_substituto", TipoColuna.FLOAT, "ICMS do substituto para conciliacao"),
            ColunaSchema("vl_st_retido", TipoColuna.FLOAT, "Valor ST retido anteriormente"),
            ColunaSchema("bc_fcp_st", TipoColuna.FLOAT, "Base FCP ST"),
            ColunaSchema("p_fcp_st", TipoColuna.FLOAT, "Aliquota FCP ST"),
            ColunaSchema("vl_fcp_st", TipoColuna.FLOAT, "Valor FCP ST"),
            ColunaSchema("vl_ressarc_credito_proprio", TipoColuna.FLOAT, "Credito proprio calculado a partir do C176"),
            ColunaSchema("vl_ressarc_st_retido", TipoColuna.FLOAT, "Valor ST retido a ressarcir no C176"),
            ColunaSchema("vl_total_ressarcimento", TipoColuna.FLOAT, "Valor total de ressarcimento apurado no C176"),
            ColunaSchema("origem_st", TipoColuna.STRING, "Origem predominante da trilha ST"),
            ColunaSchema("status_conciliacao", TipoColuna.STRING, "Status da conciliacao entre XML e C176"),
        ],
    )
)
