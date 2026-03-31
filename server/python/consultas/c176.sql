/*
  Adaptada de C:\funcoes - Copia\sql\c176.sql.
  Extrai a trilha de ressarcimento/ST do C176 por CNPJ, mantendo o recorte
  do arquivo EFD valido e deixando a reconciliacao fina para Polars.
*/
WITH parametros AS (
    SELECT :CNPJ AS cnpj_filtro
    FROM dual
),
arquivos_validos AS (
    SELECT reg_0000_id, dt_ini, cnpj, data_entrega, cod_fin_efd
    FROM (
        SELECT
            r.id AS reg_0000_id,
            r.dt_ini,
            r.cnpj,
            r.data_entrega,
            r.cod_fin AS cod_fin_efd,
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
    arq.data_entrega AS data_entrega_efd_periodo,
    arq.cod_fin_efd,
    CASE arq.cod_fin_efd
        WHEN '0' THEN '0 - Original'
        WHEN '1' THEN '1 - Substituto'
        ELSE TO_CHAR(arq.cod_fin_efd)
    END AS finalidade_efd,
    arq.cnpj AS cnpj_referencia,
    c100.chv_nfe AS chave_saida,
    c100.num_doc AS num_nf_saida,
    c100.dt_doc AS dt_doc_saida,
    c100.dt_e_s AS dt_e_s_saida,
    c170.cod_item AS cod_item,
    c170.descr_compl AS descricao_item,
    c170.num_item AS num_item_saida,
    c170.cfop AS cfop_saida,
    c170.unid AS unid_saida,
    NVL(c170.qtd, 0) AS qtd_item_saida,
    NVL(c170.vl_item, 0) AS vl_total_item,
    c176.cod_mot_res AS cod_mot_res,
    CASE c176.cod_mot_res
        WHEN '1' THEN '1 - Saida para outra UF'
        WHEN '2' THEN '2 - Isencao ou nao incidencia'
        WHEN '3' THEN '3 - Perda ou deterioracao'
        WHEN '4' THEN '4 - Furto ou roubo'
        WHEN '5' THEN '5 - Exportacao'
        WHEN '6' THEN '6 - Venda interna p/ Simples Nacional'
        WHEN '9' THEN '9 - Outros'
        ELSE c176.cod_mot_res
    END AS descricao_motivo_ressarcimento,
    c176.chave_nfe_ult AS chave_nfe_ultima_entrada,
    c176.num_item_ult_e AS num_item_ultima_entrada,
    CASE
        WHEN c176.dt_ult_e IS NOT NULL
         AND REGEXP_LIKE(c176.dt_ult_e, '^\d{8}$')
        THEN TO_DATE(c176.dt_ult_e, 'DDMMYYYY')
        ELSE NULL
    END AS dt_ultima_entrada,
    NVL(c176.vl_unit_ult_e, 0) AS vl_unit_bc_st_entrada,
    NVL(c176.vl_unit_icms_ult_e, 0) AS vl_unit_icms_proprio_entrada,
    NVL(c176.vl_unit_res, 0) AS vl_unit_ressarcimento_st,
    NVL(c170.qtd, 0) * NVL(c176.vl_unit_icms_ult_e, 0) AS vl_ressarc_credito_proprio,
    NVL(c170.qtd, 0) * NVL(c176.vl_unit_res, 0) AS vl_ressarc_st_retido,
    CASE
        WHEN NVL(c170.vl_icms, 0) > 0
        THEN (NVL(c170.qtd, 0) * NVL(c176.vl_unit_res, 0))
           + (NVL(c170.qtd, 0) * NVL(c176.vl_unit_icms_ult_e, 0))
        ELSE (NVL(c170.qtd, 0) * NVL(c176.vl_unit_res, 0))
    END AS vr_total_ressarcimento
FROM {{FONTE_C176}} c176
INNER JOIN arquivos_validos arq
    ON arq.reg_0000_id = c176.reg_0000_id
INNER JOIN {{FONTE_C100}} c100
    ON c100.id = c176.reg_c100_id
INNER JOIN {{FONTE_C170}} c170
    ON c170.id = c176.reg_c170_id
ORDER BY arq.dt_ini, c100.chv_nfe, c170.num_item
