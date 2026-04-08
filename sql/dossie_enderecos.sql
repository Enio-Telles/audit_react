SELECT
    'DM_PESSOA/SITAFE' origem,
    'ATUAL' ano_mes,
    t.desc_endereco logradouro,
    NULL numero,
    NULL complemento,
    t.bairro bairro,
    NULL fone,
    t.nu_cep cep,
    localid.no_municipio municipio,
    localid.co_uf uf
FROM bi.dm_pessoa t
LEFT JOIN bi.dm_localidade localid ON t.co_municipio = localid.co_municipio
WHERE co_cnpj_cpf = :CNPJ
UNION ALL
SELECT * FROM (
    SELECT
        'NFE' origem,
        EXTRACT(YEAR FROM dhemi) || '/' || EXTRACT(MONTH FROM dhemi) ano_mes,
        UPPER(xlgr_dest) logradouro,
        UPPER(nro_dest) numero,
        UPPER(xcpl_dest) complemento,
        UPPER(xbairro_dest) bairro,
        UPPER(fone_dest) fone,
        UPPER(cep_dest) cep,
        UPPER(xmun_dest) muncipio,
        UPPER(co_uf_dest) uf
    FROM bi.fato_nfe_detalhe t
    WHERE t.co_destinatario = :CNPJ
    GROUP BY
        UPPER(xlgr_dest), UPPER(nro_dest), UPPER(xcpl_dest), UPPER(xbairro_dest),
        UPPER(fone_dest), UPPER(xmun_dest), UPPER(cep_dest), UPPER(co_uf_dest),
        EXTRACT(YEAR FROM dhemi) || '/' || EXTRACT(MONTH FROM dhemi)
    ORDER BY EXTRACT(YEAR FROM dhemi) || '/' || EXTRACT(MONTH FROM dhemi) DESC
)
