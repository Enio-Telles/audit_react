from ...contratos.base import ColunaSchema, ContratoTabela, TipoColuna, registrar_contrato


registrar_contrato(
    ContratoTabela(
        nome="ajustes_e111",
        descricao="Trilha auditavel de ajustes E111 por competencia fiscal",
        modulo="tabelas.ajustes_e111",
        funcao="gerar_ajustes_e111",
        dependencias=[],
        saida="ajustes_e111.parquet",
        colunas=[
            ColunaSchema("periodo_efd", TipoColuna.STRING, "Competencia EFD no formato YYYY/MM"),
            ColunaSchema("ano", TipoColuna.STRING, "Ano fiscal derivado da competencia"),
            ColunaSchema("cnpj_referencia", TipoColuna.STRING, "CNPJ analisado"),
            ColunaSchema("codigo_ajuste", TipoColuna.STRING, "Codigo do ajuste E111"),
            ColunaSchema("descricao_codigo_ajuste", TipoColuna.STRING, "Descricao do codigo de ajuste", obrigatoria=False),
            ColunaSchema("descricao_complementar", TipoColuna.STRING, "Descricao complementar declarada", obrigatoria=False),
            ColunaSchema("valor_ajuste", TipoColuna.FLOAT, "Valor monetario do ajuste"),
            ColunaSchema("data_entrega_efd_periodo", TipoColuna.STRING, "Data da entrega EFD vigente para a competencia", obrigatoria=False),
            ColunaSchema("cod_fin_efd", TipoColuna.STRING, "Finalidade do arquivo EFD", obrigatoria=False),
        ],
    )
)
