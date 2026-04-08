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
)
SELECT
    tabela.part_atual situacao,
    tabela.cpfcnpj co_cnpj_cpf,
    tabela.it_no_pessoa nome,
    tabela.it_da_inicio_part_societaria da_inicio,
    tabela.fim_part_societaria_real da_fim,
    'http://www.portaltransparencia.gov.br/pessoa-fisica/busca/lista?termo=' || tabela.cpfcnpj portal_transparencia
FROM tabela
ORDER BY 1, 4 DESC, 5 DESC, 3 ASC
