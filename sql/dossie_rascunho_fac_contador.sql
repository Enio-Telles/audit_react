WITH contribuinte AS (
    SELECT co_cad_icms
    FROM bi.dm_pessoa
    WHERE co_cnpj_cpf = :CNPJ
),
rascunho_ordenado AS (
    SELECT
        SUBSTR(rf.gr_ident_contador, 2, 14) AS cpf_contador,
        rf.it_no_contador AS no_contador,
        rf.gr_numero_crc AS crc_contador,
        rf.it_co_correio_eletro_contador AS email,
        rf.it_tx_logradouro_contador AS logradouro,
        rf.it_no_bairro_contador AS bairro,
        rf.it_no_municipio_contador AS municipio,
        rf.it_sg_uf_contador AS uf,
        TRIM(NVL(rf.it_nu_ddd_contador, '') || NVL(rf.it_nu_telefone_contador, '')) AS telefone,
        rf.it_da_referencia AS da_referencia,
        rf.it_da_transacao AS da_transacao,
        ROW_NUMBER() OVER (
            ORDER BY rf.it_da_transacao DESC, rf.it_ho_transacao DESC, rf.tuk DESC
        ) AS rn
    FROM sitafe.sitafe_rascunho_fac rf
    WHERE rf.it_nu_inscricao_estadual IN (SELECT co_cad_icms FROM contribuinte)
)
SELECT
    cpf_contador,
    no_contador,
    crc_contador,
    email,
    logradouro,
    bairro,
    municipio,
    uf,
    telefone,
    da_referencia,
    da_transacao
FROM rascunho_ordenado
WHERE rn = 1
