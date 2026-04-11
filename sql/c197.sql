WITH PARAMETROS AS (
    -- 1. Padronização dos inputs e definição da Data de Corte
    SELECT 
        :CNPJ AS cnpj_filtro,
        TO_DATE(:data_inicial, 'DD/MM/YYYY') AS dt_ini_filtro,
        TO_DATE(:data_final,   'DD/MM/YYYY') AS dt_fim_filtro,
        NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte
    FROM dual
),

TODOS_ARQUIVOS_VALIDOS AS (
    -- 2. Seleção do arquivo mais recente (Original ou Substituto) respeitando a Data de Corte
    SELECT 
        r.ID              AS reg_0000_id,
        r.DT_INI          AS dt_ini,
        r.DT_FIN          AS dt_fin,
        r.CNPJ            AS cnpj,
        r.DATA_ENTREGA    AS data_entrega,
        r.COD_FIN         AS cod_fin_efd,
        p.dt_corte, p.dt_ini_filtro, p.dt_fim_filtro,
        ROW_NUMBER() OVER (
            PARTITION BY r.CNPJ, r.DT_INI 
            ORDER BY r.DATA_ENTREGA DESC
        ) AS rn
    FROM sped.reg_0000 r
    JOIN PARAMETROS p ON r.CNPJ = p.cnpj_filtro
    WHERE r.DATA_ENTREGA <= p.dt_corte
),

ARQUIVOS_DO_PERIODO AS (
    -- 3a. Arquivos EFD cujo período de apuração está dentro do filtro
    SELECT * FROM TODOS_ARQUIVOS_VALIDOS
    WHERE rn = 1 
      AND dt_ini BETWEEN dt_ini_filtro AND dt_fim_filtro
),

OUTROS_ARQUIVOS AS (
    -- 3b. Arquivos EFD de outros períodos (para buscar notas extemporâneas)
    SELECT * FROM TODOS_ARQUIVOS_VALIDOS
    WHERE rn = 1 
      AND (dt_ini < dt_ini_filtro OR dt_ini > dt_fim_filtro)
),

BASE_C197 AS (
    -- 4a. Ajustes do Período (Registros C197 dentro dos arquivos EFD do próprio período)
    SELECT
        arq.dt_ini                                                   AS periodo_efd_dt,
        arq.data_entrega,
        arq.cod_fin_efd,
        CASE c100.ind_oper WHEN '0' THEN '0 - Entrada' WHEN '1' THEN '1 - Saída' END AS ind_oper_desc,
        c100.chv_nfe,
        c100.num_doc,
        -- Conversão segura de datas
        CASE WHEN c100.dt_doc IS NOT NULL AND REGEXP_LIKE(c100.dt_doc, '^\d{8}$') THEN TO_DATE(c100.dt_doc, 'DDMMYYYY') ELSE NULL END AS dt_doc,
        CASE WHEN c100.dt_e_s IS NOT NULL AND REGEXP_LIKE(c100.dt_e_s, '^\d{8}$') THEN TO_DATE(c100.dt_e_s, 'DDMMYYYY') ELSE NULL END AS dt_e_s,
        c197.cod_item,
        c197.cod_aj,
        c197.descr_compl_aj,
        c197.vl_bc_icms   AS vl_bc_icms_c197,
        c197.aliq_icms    AS aliq_icms_c197,
        c197.vl_icms      AS vl_icms_c197,
        c197.vl_outros    AS vl_outros_c197,
        c100.vl_bc_icms   AS vl_bc_icms_c100,
        c100.vl_icms      AS vl_icms_c100,
        c100.vl_bc_icms_st AS vl_bc_icms_st_c100,
        c100.vl_icms_st   AS vl_icms_st_c100
    FROM sped.reg_c197 c197
    INNER JOIN sped.reg_c100 c100 ON c197.reg_c100_id = c100.id
    INNER JOIN ARQUIVOS_DO_PERIODO arq ON c100.reg_0000_id = arq.reg_0000_id

    UNION ALL

    -- 4b. Ajustes Extemporâneos (Registros C197 em arquivos de outros períodos, mas com data do documento DENTRO do período auditado)
    SELECT
        arq.dt_ini                                                   AS periodo_efd_dt,
        arq.data_entrega,
        arq.cod_fin_efd,
        CASE c100.ind_oper WHEN '0' THEN '0 - Entrada' WHEN '1' THEN '1 - Saída' END AS ind_oper_desc,
        c100.chv_nfe,
        c100.num_doc,
        -- Conversão segura de datas
        CASE WHEN c100.dt_doc IS NOT NULL AND REGEXP_LIKE(c100.dt_doc, '^\d{8}$') THEN TO_DATE(c100.dt_doc, 'DDMMYYYY') ELSE NULL END AS dt_doc,
        CASE WHEN c100.dt_e_s IS NOT NULL AND REGEXP_LIKE(c100.dt_e_s, '^\d{8}$') THEN TO_DATE(c100.dt_e_s, 'DDMMYYYY') ELSE NULL END AS dt_e_s,
        c197.cod_item,
        c197.cod_aj,
        c197.descr_compl_aj,
        c197.vl_bc_icms   AS vl_bc_icms_c197,
        c197.aliq_icms    AS aliq_icms_c197,
        c197.vl_icms      AS vl_icms_c197,
        c197.vl_outros    AS vl_outros_c197,
        c100.vl_bc_icms   AS vl_bc_icms_c100,
        c100.vl_icms      AS vl_icms_c100,
        c100.vl_bc_icms_st AS vl_bc_icms_st_c100,
        c100.vl_icms_st   AS vl_icms_st_c100
    FROM sped.reg_c197 c197
    INNER JOIN sped.reg_c100 c100 ON c197.reg_c100_id = c100.id
    INNER JOIN OUTROS_ARQUIVOS arq ON c100.reg_0000_id = arq.reg_0000_id
    WHERE GREATEST(
            CASE WHEN c100.dt_doc IS NOT NULL AND REGEXP_LIKE(c100.dt_doc, '^\d{8}$') THEN TO_DATE(c100.dt_doc, 'DDMMYYYY') ELSE NULL END,
            NVL(
              CASE WHEN c100.dt_e_s IS NOT NULL AND REGEXP_LIKE(c100.dt_e_s, '^\d{8}$') THEN TO_DATE(c100.dt_e_s, 'DDMMYYYY') ELSE NULL END,
              CASE WHEN c100.dt_doc IS NOT NULL AND REGEXP_LIKE(c100.dt_doc, '^\d{8}$') THEN TO_DATE(c100.dt_doc, 'DDMMYYYY') ELSE NULL END
            )
          ) BETWEEN arq.dt_ini_filtro AND arq.dt_fim_filtro
)

-- 5. Consulta Final com formatação
SELECT
    ind_oper_desc,
    chv_nfe,
    num_doc,
    dt_doc,
    dt_e_s,
    cod_item,
    cod_aj,
    descr_compl_aj,
    vl_bc_icms_c197,
    aliq_icms_c197,
    vl_icms_c197,
    vl_outros_c197,
    vl_bc_icms_c100,
    vl_icms_c100,
    vl_bc_icms_st_c100,
    vl_icms_st_c100,
    TO_CHAR(periodo_efd_dt, 'YYYY/MM') AS periodo_efd,
    data_entrega                       AS data_entrega_efd,
    cod_fin_efd
FROM BASE_C197
ORDER BY periodo_efd_dt, dt_doc, num_doc;