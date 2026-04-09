SELECT
    t.it_da_transacao da_trans,
    t.it_da_referencia da_ref,
    t.it_nu_fac nu_fac,
    t.it_nu_inscricao_estadual IE,
    CONVERT(sit_pessoa.it_no_pessoa, 'AL32UTF8', 'WE8MSWIN1252') Nome,
    CONVERT(sit_pessoa.it_no_fantasia, 'AL32UTF8', 'WE8MSWIN1252') No_fantasia,
    t.it_co_regime_pagamento rp,
    t.it_va_capital_social c_soc,
    t.it_co_atividade_economica atv_pr,
    tg.it_co_tipo_logradouro tl,
    CONVERT(tg.it_no_logradouro, 'AL32UTF8', 'WE8MSWIN1252') logradouro,
    sit_pessoa.it_ed_numero num,
    CONVERT(localid.no_municipio, 'AL32UTF8', 'WE8MSWIN1252') municipio,
    sit_pessoa.it_sg_uf uf,
    CONVERT(sit_pessoa.it_co_correio_eletronico, 'AL32UTF8', 'WE8MSWIN1252') email,
    CONVERT(sit_pessoa.it_co_correio_eletro_corresp, 'AL32UTF8', 'WE8MSWIN1252') email_corr,
    SUBSTR(t.gr_ident_contador, 2, 14) cpf_contador,
    CONVERT(pessoa.no_razao_social, 'AL32UTF8', 'WE8MSWIN1252') no_contador,
    t.it_in_ultima_fac ult_fac
FROM sitafe.sitafe_historico_contribuinte t
LEFT JOIN sitafe.sitafe_pessoa sit_pessoa ON t.it_nu_fac = sit_pessoa.it_nu_fac
LEFT JOIN sitafe.sitafe_tabelas_cadastro tg ON sit_pessoa.it_co_logradouro = tg.it_co_logradouro
LEFT JOIN bi.dm_localidade localid ON sit_pessoa.it_co_municipio = localid.co_municipio
LEFT JOIN bi.dm_pessoa pessoa ON SUBSTR(t.gr_ident_contador, 2, 14) = pessoa.co_cnpj_cpf
WHERE t.it_nu_inscricao_estadual IN (SELECT co_cad_icms FROM bi.dm_pessoa WHERE co_cnpj_cpf = :CNPJ)
ORDER BY t.it_da_transacao DESC
