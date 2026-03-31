/*
  Adaptada de C:\funcoes - Copia\sql\bloco_h.sql.
  Mantem o relacionamento H005 -> H010 -> H020 e enriquece com o cadastro 0200.
*/
WITH parametros AS (
    SELECT
        :CNPJ AS cnpj_filtro
    FROM dual
),
arquivos_ranking AS (
    SELECT
        r.id AS reg_0000_id,
        r.cnpj,
        r.dt_ini,
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
),
cte_h005 AS (
    SELECT
        h005.id AS reg_h005_id,
        h005.reg_0000_id,
        TO_DATE(h005.dt_inv, 'DDMMYYYY') AS dt_inv,
        NVL(CAST(h005.vl_inv AS NUMBER), 0) AS vl_inv_total,
        h005.mot_inv AS cod_mot_inv
    FROM {{FONTE_BLOCO_H_CAB}} h005
    WHERE h005.reg_0000_id IN (
        SELECT reg_0000_id FROM arquivos_ranking WHERE rn = 1
    )
),
cte_h010 AS (
    SELECT
        h010.id AS reg_h010_id,
        h010.reg_h005_id,
        h010.reg_0000_id,
        h010.cod_item AS codigo_produto_original,
        REGEXP_REPLACE(h010.cod_item, '[^[:alnum:]]', '') AS codigo_produto_limpo,
        h010.unid AS unidade_medida,
        NVL(CAST(h010.qtd AS NUMBER), 0) AS quantidade,
        NVL(CAST(h010.vl_unit AS NUMBER), 0) AS valor_unitario,
        NVL(CAST(h010.vl_item AS NUMBER), 0) AS valor_item,
        h010.ind_prop,
        h010.cod_part,
        h010.txt_compl
    FROM {{FONTE_BLOCO_H_ITEM}} h010
    WHERE h010.reg_0000_id IN (
        SELECT reg_0000_id FROM arquivos_ranking WHERE rn = 1
    )
),
cte_h020 AS (
    SELECT
        h020.reg_h010_id,
        h020.reg_0000_id,
        h020.cst_icms,
        NVL(CAST(h020.bc_icms AS NUMBER), 0) AS bc_icms,
        NVL(CAST(h020.vl_icms AS NUMBER), 0) AS vl_icms
    FROM {{FONTE_BLOCO_H_TRIB}} h020
    WHERE h020.reg_0000_id IN (
        SELECT reg_0000_id FROM arquivos_ranking WHERE rn = 1
    )
),
cte_0200 AS (
    SELECT
        r0200.reg_0000_id,
        r0200.cod_item,
        r0200.descr_item AS descricao_produto,
        r0200.tipo_item,
        r0200.cod_ncm,
        r0200.cest,
        r0200.cod_barra
    FROM {{FONTE_REG0200}} r0200
    WHERE r0200.reg_0000_id IN (
        SELECT reg_0000_id FROM arquivos_ranking WHERE rn = 1
    )
)
SELECT
    arq.cnpj,
    h005.dt_inv,
    h005.cod_mot_inv,
    h005.vl_inv_total AS valor_total_inventario_h005,
    h010.codigo_produto_original AS codigo_produto,
    arq.cnpj || '|' || h010.codigo_produto_original AS codigo_fonte,
    r0200.descricao_produto,
    r0200.cod_ncm,
    r0200.cest,
    r0200.cod_barra,
    r0200.tipo_item,
    h010.unidade_medida,
    h010.quantidade,
    h010.valor_unitario,
    h010.valor_item,
    h010.ind_prop AS indicador_propriedade,
    h010.cod_part AS participante_terceiro,
    h010.txt_compl AS obs_complementar,
    h020.cst_icms,
    h020.bc_icms,
    h020.vl_icms
FROM arquivos_ranking arq
INNER JOIN cte_h005 h005
    ON h005.reg_0000_id = arq.reg_0000_id
INNER JOIN cte_h010 h010
    ON h010.reg_h005_id = h005.reg_h005_id
   AND h010.reg_0000_id = arq.reg_0000_id
LEFT JOIN cte_h020 h020
    ON h020.reg_h010_id = h010.reg_h010_id
   AND h020.reg_0000_id = arq.reg_0000_id
LEFT JOIN cte_0200 r0200
    ON r0200.reg_0000_id = arq.reg_0000_id
   AND r0200.cod_item = h010.codigo_produto_original
WHERE arq.rn = 1
ORDER BY h005.dt_inv DESC, h010.codigo_produto_limpo
