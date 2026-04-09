WITH s_auto AS (
    SELECT bi.dm_pessoa.co_cnpj_cpf, bi.dm_pessoa.co_cad_icms ie
    FROM bi.dm_pessoa
    WHERE bi.dm_pessoa.co_cnpj_cpf = :CNPJ
), 
hist_socio AS (
    SELECT
        shs.gr_identificacao,
        shs.it_nu_inscricao_estadual,
        MIN(shs.it_da_inicio_part_societaria) da_entrada,
        MAX(shs.it_da_fim_part_societaria) da_saida
    FROM s_auto
    LEFT JOIN sitafe.sitafe_historico_socio shs ON shs.it_nu_inscricao_estadual = s_auto.ie
    GROUP BY shs.gr_identificacao, shs.it_nu_inscricao_estadual
),
ult_socio AS (
    SELECT shs.gr_identificacao, shs.it_nu_inscricao_estadual
    FROM s_auto
    LEFT JOIN sitafe.sitafe_historico_socio shs ON shs.it_nu_inscricao_estadual = s_auto.ie
        AND shs.it_in_ultima_fac = '9'
        AND (shs.it_da_fim_part_societaria = '        ' OR shs.it_da_fim_part_societaria > TO_CHAR(SYSDATE, 'yyyymmdd'))
    GROUP BY shs.gr_identificacao, shs.it_nu_inscricao_estadual
), 
tabela AS (
    SELECT
        CASE
            WHEN ult_socio.gr_identificacao IS NOT NULL THEN 'SÓCIO ATUAL'
            ELSE 'SÓCIO ANTIGO'
        END part_atual,
        SUBSTR(p.gr_identificacao, 2) cpfcnpj,
        p.it_no_pessoa,
        CASE
            WHEN tg.it_no_logradouro IS NOT NULL OR p.it_ed_numero IS NOT NULL OR loc.no_municipio IS NOT NULL OR p.it_sg_uf IS NOT NULL THEN
                TRIM(
                    CONVERT(tg.it_no_logradouro, 'AL32UTF8', 'WE8MSWIN1252')
                    || CASE WHEN p.it_ed_numero IS NOT NULL THEN ', ' || p.it_ed_numero ELSE '' END
                    || CASE WHEN loc.no_municipio IS NOT NULL THEN ', ' || CONVERT(loc.no_municipio, 'AL32UTF8', 'WE8MSWIN1252') ELSE '' END
                    || CASE WHEN p.it_sg_uf IS NOT NULL THEN ', ' || p.it_sg_uf ELSE '' END
                )
            WHEN p.it_tx_logradouro_corresp IS NOT NULL OR p.it_no_bairro_corresp IS NOT NULL OR loc_corresp.no_municipio IS NOT NULL THEN
                TRIM(
                    CONVERT(p.it_tx_logradouro_corresp, 'AL32UTF8', 'WE8MSWIN1252')
                    || CASE WHEN p.it_no_bairro_corresp IS NOT NULL THEN ', ' || CONVERT(p.it_no_bairro_corresp, 'AL32UTF8', 'WE8MSWIN1252') ELSE '' END
                    || CASE WHEN loc_corresp.no_municipio IS NOT NULL THEN ', ' || CONVERT(loc_corresp.no_municipio, 'AL32UTF8', 'WE8MSWIN1252') ELSE '' END
                    || CASE WHEN p.it_sg_uf IS NOT NULL THEN ', ' || p.it_sg_uf ELSE '' END
                )
        END endereco,
        NULLIF(TRIM(NVL(p.it_nu_ddd, '') || NVL(p.it_nu_telefone, '')), '') telefone,
        COALESCE(
            NULLIF(TRIM(CONVERT(p.it_co_correio_eletronico, 'AL32UTF8', 'WE8MSWIN1252')), ''),
            NULLIF(TRIM(CONVERT(p.it_co_correio_eletro_corresp, 'AL32UTF8', 'WE8MSWIN1252')), '')
        ) email,
        CASE
            WHEN hist_socio.da_entrada != '         ' THEN TO_DATE(hist_socio.da_entrada, 'YYYYMMDD')
        END it_da_inicio_part_societaria,
        CASE
            WHEN ult_socio.gr_identificacao IS NOT NULL THEN 'SÓCIO ATUAL'
            WHEN hist_socio.da_saida = '        ' THEN 'NÃO INFORMADO'
            ELSE TO_CHAR(TO_DATE(hist_socio.da_saida, 'YYYYMMDD'))
        END fim_part_societaria_real
    FROM hist_socio
    LEFT JOIN ult_socio ON hist_socio.gr_identificacao = ult_socio.gr_identificacao
        AND hist_socio.it_nu_inscricao_estadual = ult_socio.it_nu_inscricao_estadual
    LEFT JOIN sitafe.sitafe_pessoa p ON p.gr_identificacao = hist_socio.gr_identificacao AND p.it_in_ultima_situacao = '9'
    LEFT JOIN sitafe.sitafe_tabelas_cadastro tg ON p.it_co_logradouro = tg.it_co_logradouro
    LEFT JOIN bi.dm_localidade loc ON p.it_co_municipio = loc.co_municipio
    LEFT JOIN bi.dm_localidade loc_corresp ON p.it_co_municipio_corresp = loc_corresp.co_municipio
)
SELECT
    tabela.part_atual situacao,
    tabela.cpfcnpj co_cnpj_cpf,
    tabela.it_no_pessoa nome,
    tabela.endereco,
    tabela.telefone,
    tabela.email,
    tabela.it_da_inicio_part_societaria da_inicio,
    tabela.fim_part_societaria_real da_fim,
    'http://www.portaltransparencia.gov.br/pessoa-fisica/busca/lista?termo=' || tabela.cpfcnpj portal_transparencia
FROM tabela
ORDER BY 1, 4 DESC, 5 DESC, 3 ASC
