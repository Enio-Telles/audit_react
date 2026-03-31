/*
  Adaptada de C:\funcoes - Copia\sql\c170.sql.
  Decomposta para extrair apenas a base do item C170, preservando o filtro fiscal
  por arquivo EFD valido e deixando o enriquecimento com C100/0200 para recomposicao em Polars.
*/
WITH parametros AS (
    SELECT
        :CNPJ AS cnpj_filtro
    FROM dual
),
arquivos_validos AS (
    SELECT reg_0000_id, dt_ini, cnpj
    FROM (
        SELECT
            r.id AS reg_0000_id,
            r.dt_ini,
            r.cnpj,
            ROW_NUMBER() OVER (
                PARTITION BY r.cnpj, r.dt_ini
                ORDER BY r.data_entrega DESC, r.id DESC
            ) AS rn
        FROM {{FONTE_REG0000}} r
        CROSS JOIN parametros p
        WHERE r.cnpj = p.cnpj_filtro
          AND (
                :DATA_LIMITE_PROCESSAMENTO IS NULL
                OR r.data_entrega <= TO_DATE(:DATA_LIMITE_PROCESSAMENTO, 'YYYY-MM-DD')
          )
    )
    WHERE rn = 1
)
SELECT
    TO_CHAR(arq.dt_ini, 'YYYY/MM') AS periodo_efd,
    arq.cnpj,
    arq.reg_0000_id,
    c170.reg_c100_id,
    c170.num_item,
    c170.cod_item,
    c170.descr_compl,
    c170.cfop,
    c170.cst_icms,
    NVL(c170.qtd, 0) AS qtd,
    c170.unid,
    NVL(c170.vl_item, 0) AS vl_item,
    NVL(c170.vl_desc, 0) AS vl_desc,
    NVL(c170.vl_icms, 0) AS vl_icms,
    NVL(c170.vl_bc_icms, 0) AS vl_bc_icms,
    c170.aliq_icms,
    NVL(c170.vl_bc_icms_st, 0) AS vl_bc_icms_st,
    NVL(c170.vl_icms_st, 0) AS vl_icms_st,
    c170.aliq_st
FROM {{FONTE_C170}} c170
INNER JOIN arquivos_validos arq
    ON arq.reg_0000_id = c170.reg_0000_id
ORDER BY arq.dt_ini, c170.reg_c100_id, c170.num_item
