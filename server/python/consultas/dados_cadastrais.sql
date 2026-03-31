SELECT
    t.co_cnpj_cpf AS documento,
    t.co_cad_icms AS ie,
    t.no_razao_social AS nome,
    t.no_fantasia AS nome_fantasia,
    t.desc_endereco || ' ' || t.bairro AS endereco,
    localid.no_municipio AS municipio,
    localid.co_uf AS uf,
    t.co_regime_pagto || ' - ' || rp.no_regime_pagamento AS regime_pagamento,
    CASE
        WHEN t.in_situacao = '001' THEN t.in_situacao || ' - ' || s.desc_situacao
        ELSE t.in_situacao || ' - ' || CONVERT(s.desc_situacao, 'AL32UTF8', 'WE8MSWIN1252')
    END AS situacao_ie,
    t.da_inicio_atividade AS data_inicio_atividade,
    TO_DATE(us.data_ult_sit, 'YYYYMMDD') AS data_ultima_situacao,
    TO_CHAR(
        TRUNC(
            MONTHS_BETWEEN(
                CASE
                    WHEN t.in_situacao = '001' THEN SYSDATE
                    ELSE TO_DATE(us.data_ult_sit, 'YYYYMMDD')
                END,
                t.da_inicio_atividade
            ),
            2
        )
    ) || ' meses' AS periodo_atividade,
    'https://portalcontribuinte.sefin.ro.gov.br/Publico/parametropublica.jsp?NuDevedor=' || t.co_cad_icms AS url_redesim
FROM bi.dm_pessoa t
LEFT JOIN bi.dm_localidade localid
    ON t.co_municipio = localid.co_municipio
LEFT JOIN bi.dm_regime_pagto_descricao rp
    ON t.co_regime_pagto = rp.co_regime_pagamento
LEFT JOIN (
    SELECT
        co_situacao_contribuinte AS co_situacao,
        no_situacao_contribuinte AS desc_situacao
    FROM bi.dm_situacao_contribuinte
) s
    ON t.in_situacao = s.co_situacao
LEFT JOIN (
    SELECT
        MAX(u.it_da_transacao) AS data_ult_sit,
        u.it_nu_inscricao_estadual
    FROM sitafe.sitafe_historico_gr_situacao t_hist
    LEFT JOIN sitafe.sitafe_historico_situacao u
        ON t_hist.tuk = u.tuk
    WHERE t_hist.it_co_situacao_contribuinte NOT IN ('030', '150', '005')
      AND u.it_co_usuario NOT IN ('INTERNET', 'P30015AC   ')
    GROUP BY u.it_nu_inscricao_estadual
) us
    ON t.co_cad_icms = us.it_nu_inscricao_estadual
WHERE t.co_cnpj_cpf = :CO_CNPJ_CPF
