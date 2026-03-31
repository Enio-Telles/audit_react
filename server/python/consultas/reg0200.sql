/*
  Adaptada de C:\funcoes - Copia\sql\reg_0200.sql.
  Mantem a regra de versionamento por periodo e incorpora 0205 e 0220 no mesmo dataset.
*/
WITH parametros AS (
    SELECT
        :CNPJ AS cnpj_filtro,
        TO_DATE(:DATA_INICIAL, 'YYYY-MM-DD') AS dt_ini_filtro,
        TO_DATE(:DATA_FINAL, 'YYYY-MM-DD') AS dt_fim_filtro
    FROM dual
),
ranking_0000 AS (
    SELECT
        r.id AS reg_0000_id,
        r.cnpj,
        r.cod_fin,
        r.dt_ini,
        r.data_entrega,
        ROW_NUMBER() OVER (
            PARTITION BY r.cnpj, r.dt_ini
            ORDER BY r.data_entrega DESC, r.id DESC
        ) AS rn
    FROM {{FONTE_REG0000}} r
    INNER JOIN parametros p
        ON r.cnpj = p.cnpj_filtro
    WHERE (
            :DATA_LIMITE_PROCESSAMENTO IS NULL
            OR r.data_entrega <= TO_DATE(:DATA_LIMITE_PROCESSAMENTO, 'YYYY-MM-DD')
    )
      AND (
            :DATA_INICIAL IS NULL
            OR r.dt_ini >= p.dt_ini_filtro
    )
      AND (
            :DATA_FINAL IS NULL
            OR r.dt_ini <= p.dt_fim_filtro
    )
),
arquivos_validos AS (
    SELECT reg_0000_id, dt_ini, cod_fin, data_entrega
    FROM ranking_0000
    WHERE rn = 1
),
dados_0200 AS (
    SELECT r200.*
    FROM {{FONTE_REG0200}} r200
    INNER JOIN arquivos_validos av
        ON r200.reg_0000_id = av.reg_0000_id
),
dados_0205 AS (
    SELECT
        r205.descr_ant_item,
        r205.dt_fim,
        r205.dt_ini,
        r205.cod_ant_item,
        r205.reg_0000_id,
        r205.reg_0200_id
    FROM {{FONTE_REG0205}} r205
    INNER JOIN arquivos_validos av
        ON r205.reg_0000_id = av.reg_0000_id
),
dados_0220 AS (
    SELECT
        r220.reg_0000_id,
        r220.reg_0200_id,
        r220.unid_conv,
        r220.fat_conv
    FROM {{FONTE_REG0220}} r220
    INNER JOIN arquivos_validos av
        ON r220.reg_0000_id = av.reg_0000_id
)
SELECT
    TO_CHAR(arq.dt_ini, 'MM/YYYY') AS periodo_efd,
    r200.id AS reg_0200_id,
    r200.reg_0000_id,
    r200.cod_item,
    r200.cod_ant_item,
    r205.cod_ant_item AS r0205_cod_ant_item,
    r200.descr_item,
    r200.aliq_icms,
    r200.unid_inv,
    r205.descr_ant_item,
    r205.dt_ini AS dt_ini_ant_item,
    r205.dt_fim AS dt_fim_ant_item,
    r220.unid_conv,
    r220.fat_conv,
    r200.cod_barra,
    r200.cod_ncm,
    r200.cest,
    r200.tipo_item,
    r200.cod_gen,
    arq.cod_fin AS cod_fin_efd,
    arq.data_entrega AS data_entrega_efd_periodo
FROM arquivos_validos arq
INNER JOIN dados_0200 r200
    ON arq.reg_0000_id = r200.reg_0000_id
LEFT JOIN dados_0205 r205
    ON r205.reg_0000_id = r200.reg_0000_id
   AND r205.reg_0200_id = r200.id
LEFT JOIN dados_0220 r220
    ON r220.reg_0000_id = r200.reg_0000_id
   AND r220.reg_0200_id = r200.id
ORDER BY arq.dt_ini ASC
