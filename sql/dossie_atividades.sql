SELECT
    base.tipo,
    base.co_cnae,
    cnae.no_cnae
FROM (
    SELECT 'SECUND' tipo, t.co_cnae_secundaria co_cnae
    FROM bi.dm_cnae_secundaria t WHERE t.co_cnpj_cpf = :CNPJ
    UNION
    SELECT 'PRINCIPAL' tipo, t.co_cnae co_cnae
    FROM bi.dm_pessoa t WHERE t.co_cnpj_cpf = :CNPJ
) base
LEFT JOIN bi.dm_cnae cnae ON base.co_cnae = cnae.co_cnae
ORDER BY base.tipo ASC, base.co_cnae
