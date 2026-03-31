/*
  Adaptada de C:\funcoes - Copia\sql\Nfe_dados_ST.sql.
  Extrai a base de ICMS ST e FCP ST por item de NF-e, combinando os campos
  documentais do detalhe estruturado com os valores de ST presentes no XML.
*/
WITH parametros AS (
    SELECT :CNPJ AS cnpj_filtro
    FROM dual
),
lista_nfes AS (
    SELECT DISTINCT
        d.chave_acesso
    FROM {{FONTE_NFE}} d
    CROSS JOIN parametros p
    WHERE (
            d.co_destinatario = p.cnpj_filtro
            OR d.co_emitente = p.cnpj_filtro
        )
      AND d.infprot_cstat IN (100, 150)
      AND (
            :DATA_LIMITE_PROCESSAMENTO IS NULL
            OR TRUNC(d.dt_gravacao) <= TO_DATE(:DATA_LIMITE_PROCESSAMENTO, 'YYYY-MM-DD')
      )
)
SELECT
    d.chave_acesso,
    d.prod_nitem,
    d.co_emitente || '|' || d.prod_cprod AS codigo_fonte,
    d.prod_cprod,
    d.prod_xprod,
    d.prod_ncm,
    d.prod_cest,
    d.co_cfop,
    d.prod_qcom,
    d.prod_vprod,
    d.icms_cst,
    d.icms_csosn,
    p.cnpj_filtro,
    d.co_emitente,
    d.co_destinatario,
    d.dhemi,
    xml_item.icms_vbcst,
    xml_item.icms_vicmsst,
    xml_item.icms_vicmssubstituto,
    xml_item.icms_vicmsstret,
    xml_item.icms_vbcfcpst,
    xml_item.icms_pfcpst,
    xml_item.icms_vfcpst
FROM {{FONTE_NFE_XML}} x
INNER JOIN lista_nfes l
    ON l.chave_acesso = x.chave_acesso
INNER JOIN {{FONTE_NFE}} d
    ON d.chave_acesso = x.chave_acesso
CROSS JOIN parametros p
CROSS JOIN XMLTABLE(
    XMLNAMESPACES(DEFAULT 'http://www.portalfiscal.inf.br/nfe'),
    '//det' PASSING x.xml
    COLUMNS
        prod_nitem NUMBER PATH '@nItem',
        icms_vbcst NUMBER PATH 'imposto/ICMS//vBCST' DEFAULT 0,
        icms_vicmsst NUMBER PATH 'imposto/ICMS//vICMSST' DEFAULT 0,
        icms_vicmssubstituto NUMBER PATH 'imposto/ICMS//vICMSSubstituto' DEFAULT 0,
        icms_vicmsstret NUMBER PATH 'imposto/ICMS//vICMSSTRet' DEFAULT 0,
        icms_vbcfcpst NUMBER PATH 'imposto/ICMS//vBCFCPST' DEFAULT 0,
        icms_pfcpst NUMBER PATH 'imposto/ICMS//pFCPST' DEFAULT 0,
        icms_vfcpst NUMBER PATH 'imposto/ICMS//vFCPST' DEFAULT 0
) xml_item
WHERE d.prod_nitem = xml_item.prod_nitem
ORDER BY d.chave_acesso, d.prod_nitem
