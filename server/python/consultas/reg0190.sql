/*
  Adaptada de C:\funcoes - Copia\sql\reg_0190.sql.
  Mantem a tabela de unidades do SPED associada apenas aos arquivos validos do contribuinte.
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
    TO_CHAR(arq.dt_ini, 'MM/YYYY') AS periodo_efd,
    r0190.*,
    arq.data_entrega AS data_entrega_efd_periodo
FROM {{FONTE_REG0190}} r0190
INNER JOIN arquivos_validos arq
    ON r0190.reg_0000_id = arq.reg_0000_id
ORDER BY arq.dt_ini ASC
