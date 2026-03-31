/*
  Derivada de C:\funcoes - Copia\sql\reg_0200.sql.
  Isola os fatores de conversao 0220 com o contexto do item 0200 e a versao valida do arquivo EFD.
*/
WITH parametros AS (
    SELECT
        :CNPJ AS cnpj_filtro,
        TO_DATE(:DATA_INICIAL, 'YYYY-MM-DD') AS dt_ini_filtro,
        TO_DATE(:DATA_FINAL, 'YYYY-MM-DD') AS dt_fim_filtro
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
    )
    WHERE rn = 1
)
SELECT
    TO_CHAR(arq.dt_ini, 'MM/YYYY') AS periodo_efd,
    r200.cod_item,
    r200.descr_item,
    r200.unid_inv,
    r220.unid_conv,
    r220.fat_conv,
    arq.data_entrega AS data_entrega_efd_periodo
FROM {{FONTE_REG0220}} r220
INNER JOIN arquivos_validos arq
    ON r220.reg_0000_id = arq.reg_0000_id
INNER JOIN {{FONTE_REG0200}} r200
    ON r200.reg_0000_id = r220.reg_0000_id
   AND r200.id = r220.reg_0200_id
ORDER BY arq.dt_ini ASC, r200.cod_item, r220.unid_conv
