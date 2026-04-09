WITH contribuinte AS (
    SELECT co_cad_icms
    FROM bi.dm_pessoa
    WHERE co_cnpj_cpf = :CNPJ
),
requerimento_ordenado AS (
    SELECT
        SUBSTR(ri.gr_identificacao_contador, 2, 14) AS cpf_contador,
        ri.it_no_contador AS no_contador,
        ri.it_nu_crc AS crc_contador,
        TRIM(NVL(ri.it_nu_ddd, '') || NVL(ri.it_nu_telefone, '')) AS telefone,
        loc.no_municipio AS municipio,
        loc.co_uf AS uf,
        ri.it_da_transacao AS da_transacao,
        ROW_NUMBER() OVER (
            ORDER BY ri.it_da_transacao DESC, ri.it_ho_transacao DESC, ri.tuk DESC
        ) AS rn
    FROM sitafe.sitafe_req_inscricao ri
    LEFT JOIN bi.dm_localidade loc ON ri.it_co_municipio = loc.co_municipio
    WHERE ri.it_nu_inscricao_estadual IN (SELECT co_cad_icms FROM contribuinte)
)
SELECT
    cpf_contador,
    no_contador,
    crc_contador,
    telefone,
    municipio,
    uf,
    da_transacao
FROM requerimento_ordenado
WHERE rn = 1
