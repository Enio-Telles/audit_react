/*
  Adaptada de C:\funcoes - Copia\sql\E111.sql.
  Extrai ajustes de apuracao E111 por CNPJ respeitando a ultima versao valida
  do arquivo EFD em cada competencia.
*/
WITH parametros AS (
    SELECT :CNPJ AS cnpj_filtro
    FROM dual
),
arquivos_validos AS (
    SELECT reg_0000_id, cnpj, cod_fin_efd, dt_ini, data_entrega
    FROM (
        SELECT
            r.id AS reg_0000_id,
            r.cnpj,
            r.cod_fin AS cod_fin_efd,
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
    arq.cnpj AS cnpj_referencia,
    e111.cod_aj_apur AS codigo_ajuste,
    aj.no_cod_aj AS descricao_codigo_ajuste,
    e111.descr_compl_aj AS descr_compl,
    NVL(e111.vl_aj_apur, 0) AS valor_ajuste,
    arq.data_entrega AS data_entrega_efd_periodo,
    arq.cod_fin_efd
FROM arquivos_validos arq
INNER JOIN {{FONTE_E111}} e111
    ON e111.reg_0000_id = arq.reg_0000_id
LEFT JOIN {{FONTE_DM_EFD_AJUSTES}} aj
    ON RTRIM(aj.co_cod_aj) = e111.cod_aj_apur
ORDER BY periodo_efd, e111.cod_aj_apur
