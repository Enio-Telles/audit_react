/*
  Adaptada de C:\funcoes - Copia\sql\c100.sql.
  Usa a regra de versionamento do REG_0000 para manter apenas o arquivo EFD mais recente por periodo.
*/
WITH parametros AS (
    SELECT
        :CNPJ AS cnpj_filtro
    FROM dual
),
arquivos_validos AS (
    SELECT reg_0000_id, dt_ini, data_entrega
    FROM (
        SELECT
            r.id AS reg_0000_id,
            r.dt_ini,
            r.data_entrega,
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
    c100.id AS reg_c100_id,
    c100.reg_0000_id,
    arq.data_entrega AS data_entrega_efd_periodo,
    c100.cod_part,
    c100.cod_mod,
    c100.cod_sit,
    c100.ser,
    c100.num_doc,
    TRIM(c100.chv_nfe) AS chv_nfe,
    CASE WHEN REGEXP_LIKE(c100.dt_doc, '^\d{8}$') THEN TO_DATE(c100.dt_doc, 'DDMMYYYY') END AS dt_doc,
    CASE WHEN REGEXP_LIKE(c100.dt_e_s, '^\d{8}$') THEN TO_DATE(c100.dt_e_s, 'DDMMYYYY') END AS dt_e_s,
    c100.ind_oper,
    c100.ind_emit,
    c100.vl_doc,
    c100.vl_desc,
    c100.vl_merc,
    c100.vl_frt,
    c100.vl_seg,
    c100.vl_out_da,
    c100.vl_bc_icms,
    c100.vl_icms,
    c100.vl_bc_icms_st,
    c100.vl_icms_st,
    c100.vl_ipi,
    c100.vl_pis,
    c100.vl_cofins
FROM {{FONTE_C100}} c100
INNER JOIN arquivos_validos arq
    ON c100.reg_0000_id = arq.reg_0000_id
ORDER BY dt_doc, ser, num_doc
