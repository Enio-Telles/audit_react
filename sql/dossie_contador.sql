SELECT
    CASE 
        WHEN b.fim_ref IS NULL AND b.co_cnpj_cpf_contador = '   -   ' THEN 'Atual - sem contador indicado' 
        WHEN b.fim_ref IS NULL AND b.co_cnpj_cpf_contador != '   -   ' THEN 'Atual' 
        WHEN b.fim_ref IS NOT NULL AND b.co_cnpj_cpf_contador = '   -   ' THEN 'Anterior - Período sem indicação de contador' 
        ELSE 'Anterior' 
    END situacao,
    b.co_cnpj_cpf_contador co_cnpj_cpf_contador,
    p.no_razao_social nome,
    l.no_municipio municipio,
    l.co_uf uf,
    b.ini_ref inicio,
    b.fim_ref fim
FROM (
    SELECT
        it_nu_inscricao_estadual ie,
        cnpj,
        CASE WHEN SUBSTR(gr_ident_contador, 2) IS NULL THEN '   -   ' ELSE SUBSTR(gr_ident_contador, 2) END co_cnpj_cpf_contador,
        TO_DATE(it_da_referencia, 'yyyymmdd') ini_ref,
        CASE
            WHEN LEAD(it_da_referencia) OVER(ORDER BY it_nu_fac) IS NULL THEN NULL
            ELSE TO_DATE(LEAD(it_da_referencia) OVER(ORDER BY it_nu_fac), 'yyyymmdd')
        END fim_ref
    FROM (
        SELECT
            c.it_nu_inscricao_estadual,
            SUBSTR(c.gr_identificacao, 2) cnpj,
            c.it_nu_fac,
            c.it_da_referencia,
            c.gr_ident_contador,
            CASE
                WHEN ROW_NUMBER() OVER(ORDER BY c.it_nu_fac) = 1 THEN 1
                WHEN c.gr_ident_contador != LAG(c.gr_ident_contador) OVER(ORDER BY c.it_nu_fac) THEN 1
                ELSE 0
            END usar
        FROM sitafe.sitafe_historico_contribuinte c
        WHERE c.it_nu_inscricao_estadual IN (SELECT co_cad_icms FROM bi.dm_pessoa WHERE co_cnpj_cpf = :CNPJ)
        ORDER BY c.it_nu_inscricao_estadual, c.it_nu_fac
    ) WHERE usar = 1
    ORDER BY CASE
        WHEN LEAD(it_da_referencia) OVER(ORDER BY it_nu_fac) IS NULL THEN '99999999'
        ELSE LEAD(it_da_referencia) OVER(ORDER BY it_nu_fac)
    END DESC
) b
LEFT JOIN bi.dm_pessoa p ON b.co_cnpj_cpf_contador = p.co_cnpj_cpf
LEFT JOIN bi.dm_localidade l ON p.co_municipio = l.co_municipio
