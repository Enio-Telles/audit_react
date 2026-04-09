WITH parametros AS (
    SELECT
        REGEXP_REPLACE(:CNPJ, '[^0-9]', '') AS cnpj_consultado,
        SUBSTR(REGEXP_REPLACE(:CNPJ, '[^0-9]', ''), 1, 8) AS cnpj_raiz
    FROM dual
)
SELECT
    CASE
        WHEN SUBSTR(p.co_cnpj_cpf, 9, 4) = '0001' THEN 'MATRIZ_RAIZ'
        ELSE 'FILIAL_RAIZ'
    END AS tipo_vinculo,
    prm.cnpj_consultado,
    prm.cnpj_raiz,
    p.co_cnpj_cpf AS cpf_cnpj_referencia,
    p.no_razao_social AS nome_referencia,
    p.desc_endereco || ' ' || p.bairro AS endereco,
    CASE
        WHEN p.in_situacao = '001' THEN p.in_situacao || ' - ' || s.no_situacao_contribuinte
        ELSE p.in_situacao || ' - ' || s.no_situacao_contribuinte
    END AS situacao_cadastral,
    CASE
        WHEN SUBSTR(p.co_cnpj_cpf, 9, 4) = '0001' THEN 'MATRIZ'
        ELSE 'FILIAL'
    END AS indicador_matriz_filial,
    'dados_cadastrais.sql' AS origem_dado,
    'BI.DM_PESSOA; BI.DM_SITUACAO_CONTRIBUINTE' AS tabela_origem,
    CASE
        WHEN SUBSTR(p.co_cnpj_cpf, 9, 4) = '0001' THEN 20
        ELSE 25
    END AS ordem_exibicao
FROM parametros prm
JOIN bi.dm_pessoa p
    ON SUBSTR(p.co_cnpj_cpf, 1, 8) = prm.cnpj_raiz
LEFT JOIN bi.dm_situacao_contribuinte s
    ON p.in_situacao = s.co_situacao_contribuinte
WHERE p.co_cnpj_cpf <> prm.cnpj_consultado
