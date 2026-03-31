/*
  Adaptada de C:\funcoes - Copia\sql\NFCe.sql.
  Mantem a classificacao de entrada/saida e os campos usados pelo pipeline.
*/
WITH parametros AS (
    SELECT :CNPJ AS cnpj_filtro FROM dual
)
SELECT
    CASE
        WHEN d.co_emitente = p.cnpj_filtro AND d.co_tp_nf = 1 THEN '1 - SAIDA'
        WHEN d.co_emitente = p.cnpj_filtro AND d.co_tp_nf = 0 THEN '0 - ENTRADA'
        WHEN d.co_destinatario = p.cnpj_filtro AND d.co_tp_nf = 1 THEN '0 - ENTRADA'
        WHEN d.co_destinatario = p.cnpj_filtro AND d.co_tp_nf = 0 THEN '1 - SAIDA'
        ELSE 'INDEFINIDO'
    END AS tipo_operacao,
    d.co_destinatario,
    d.co_emitente,
    p.cnpj_filtro,
    d.nsu,
    d.chave_acesso,
    d.prod_nitem,
    d.co_emitente || '|' || d.prod_cprod AS codigo_fonte,
    d.nnf,
    d.dhemi,
    d.co_tp_nf,
    d.xnome_emit,
    d.xnome_dest,
    d.prod_cprod,
    d.prod_cean,
    d.prod_xprod,
    d.prod_ncm,
    d.prod_cest,
    d.co_cfop,
    d.prod_ucom,
    d.prod_qcom,
    d.prod_vuncom,
    d.prod_vprod,
    d.prod_ceantrib,
    d.prod_utrib,
    d.prod_qtrib,
    d.prod_vuntrib,
    d.prod_vfrete,
    d.prod_vseg,
    d.prod_vdesc,
    d.prod_voutro,
    d.dt_gravacao
FROM {{FONTE_NFCE}} d
CROSS JOIN parametros p
WHERE (
        d.co_destinatario = p.cnpj_filtro
        OR d.co_emitente = p.cnpj_filtro
    )
  AND (
        :DATA_LIMITE_PROCESSAMENTO IS NULL
        OR TRUNC(d.dt_gravacao) <= TO_DATE(:DATA_LIMITE_PROCESSAMENTO, 'YYYY-MM-DD')
    )
