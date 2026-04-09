WITH parametros AS (
    SELECT
        REGEXP_REPLACE(:CNPJ, '[^0-9]', '') AS cnpj_consultado,
        SUBSTR(REGEXP_REPLACE(:CNPJ, '[^0-9]', ''), 1, 8) AS cnpj_raiz
    FROM dual
),
contribuinte AS (
    SELECT
        p.co_cnpj_cpf AS cnpj_consultado,
        SUBSTR(p.co_cnpj_cpf, 1, 8) AS cnpj_raiz,
        p.co_cad_icms AS ie,
        p.no_razao_social AS nome_referencia,
        p.desc_endereco || ' ' || p.bairro AS endereco,
        CASE
            WHEN p.in_situacao = '001' THEN p.in_situacao || ' - ' || s.no_situacao_contribuinte
            ELSE p.in_situacao || ' - ' || s.no_situacao_contribuinte
        END AS situacao_cadastral
    FROM bi.dm_pessoa p
    LEFT JOIN bi.dm_situacao_contribuinte s
        ON p.in_situacao = s.co_situacao_contribuinte
    JOIN parametros prm
        ON p.co_cnpj_cpf = prm.cnpj_consultado
),
filiais_raiz AS (
    SELECT
        CASE
            WHEN SUBSTR(p.co_cnpj_cpf, 9, 4) = '0001' THEN 'MATRIZ_RAIZ'
            ELSE 'FILIAL_RAIZ'
        END AS tipo_vinculo,
        prm.cnpj_consultado,
        prm.cnpj_raiz,
        p.co_cnpj_cpf AS cpf_cnpj_referencia,
        p.no_razao_social AS nome_referencia,
        p.desc_endereco || ' ' || p.bairro AS endereco,
        CASE
            WHEN p.in_situacao = '001' THEN p.in_situacao || ' - ' || s.no_situacao_contribuinte
            ELSE p.in_situacao || ' - ' || s.no_situacao_contribuinte
        END AS situacao_cadastral,
        CASE
            WHEN SUBSTR(p.co_cnpj_cpf, 9, 4) = '0001' THEN 'MATRIZ'
            ELSE 'FILIAL'
        END AS indicador_matriz_filial
    FROM parametros prm
    JOIN bi.dm_pessoa p
        ON SUBSTR(p.co_cnpj_cpf, 1, 8) = prm.cnpj_raiz
    LEFT JOIN bi.dm_situacao_contribuinte s
        ON p.in_situacao = s.co_situacao_contribuinte
    WHERE p.co_cnpj_cpf <> prm.cnpj_consultado
),
contador_principal AS (
    SELECT
        SUBSTR(h.gr_ident_contador, 2) AS cpf_contador,
        p.no_razao_social AS nome_contador,
        l.no_municipio || ', ' || l.co_uf AS endereco,
        CAST(NULL AS VARCHAR2(70)) AS email,
        CAST(NULL AS VARCHAR2(20)) AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        'dossie_contador.sql' AS origem_dado,
        'SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; BI.DM_PESSOA; BI.DM_LOCALIDADE' AS tabela_origem,
        2 AS prioridade
    FROM contribuinte c
    JOIN sitafe.sitafe_historico_contribuinte h
        ON h.it_nu_inscricao_estadual = c.ie
    LEFT JOIN bi.dm_pessoa p
        ON SUBSTR(h.gr_ident_contador, 2) = p.co_cnpj_cpf
    LEFT JOIN bi.dm_localidade l
        ON p.co_municipio = l.co_municipio
    WHERE h.gr_ident_contador IS NOT NULL
),
contador_historico_fac AS (
    SELECT
        hf.cpf_contador,
        hf.no_contador AS nome_contador,
        hf.logradouro || ', ' || hf.num || ', ' || hf.municipio || ', ' || hf.uf AS endereco,
        COALESCE(TRIM(hf.email), TRIM(hf.email_corr)) AS email,
        CAST(NULL AS VARCHAR2(20)) AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        'dossie_historico_fac.sql' AS origem_dado,
        'SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; SITAFE.SITAFE_PESSOA; BI.DM_PESSOA' AS tabela_origem,
        1 AS prioridade
    FROM (
        SELECT
            t.cpf_contador AS cpf_contador,
            t.no_contador,
            t.logradouro,
            t.num,
            t.municipio,
            t.uf,
            t.email,
            t.email_corr,
            ROW_NUMBER() OVER (
                PARTITION BY t.cpf_contador, t.no_contador
                ORDER BY
                    CASE
                        WHEN TRIM(t.email) IS NOT NULL OR TRIM(t.email_corr) IS NOT NULL THEN 0
                        ELSE 1
                    END,
                    t.da_trans DESC,
                    t.nu_fac DESC
            ) AS rn
        FROM (
            SELECT
                h.it_da_transacao AS da_trans,
                h.it_nu_fac AS nu_fac,
                SUBSTR(h.gr_ident_contador, 2) AS cpf_contador,
                p.no_razao_social AS no_contador,
                tg.it_no_logradouro AS logradouro,
                sp.it_ed_numero AS num,
                loc.no_municipio AS municipio,
                sp.it_sg_uf AS uf,
                sp.it_co_correio_eletronico AS email,
                sp.it_co_correio_eletro_corresp AS email_corr
            FROM contribuinte c
            JOIN sitafe.sitafe_historico_contribuinte h
                ON h.it_nu_inscricao_estadual = c.ie
            LEFT JOIN sitafe.sitafe_pessoa sp
                ON h.it_nu_fac = sp.it_nu_fac
            LEFT JOIN sitafe.sitafe_tabelas_cadastro tg
                ON sp.it_co_logradouro = tg.it_co_logradouro
            LEFT JOIN bi.dm_localidade loc
                ON sp.it_co_municipio = loc.co_municipio
            LEFT JOIN bi.dm_pessoa p
                ON SUBSTR(h.gr_ident_contador, 2) = p.co_cnpj_cpf
        ) t
    ) hf
    WHERE hf.rn = 1
),
contador_rascunho_fac AS (
    SELECT
        rf.cpf_contador,
        rf.no_contador AS nome_contador,
        rf.logradouro || ', ' || rf.bairro || ', ' || rf.municipio || ', ' || rf.uf AS endereco,
        rf.email,
        rf.telefone,
        rf.crc_contador,
        'dossie_rascunho_fac_contador.sql' AS origem_dado,
        'SITAFE.SITAFE_RASCUNHO_FAC' AS tabela_origem,
        3 AS prioridade
    FROM (
        SELECT
            SUBSTR(rf.gr_ident_contador, 2) AS cpf_contador,
            rf.it_no_contador AS no_contador,
            rf.it_tx_logradouro_contador AS logradouro,
            rf.it_no_bairro_contador AS bairro,
            rf.it_no_municipio_contador AS municipio,
            rf.it_sg_uf_contador AS uf,
            rf.it_co_correio_eletro_contador AS email,
            TRIM(NVL(rf.it_nu_ddd_contador, '') || NVL(rf.it_nu_telefone_contador, '')) AS telefone,
            rf.gr_numero_crc AS crc_contador,
            ROW_NUMBER() OVER (ORDER BY rf.it_da_transacao DESC, rf.it_ho_transacao DESC, rf.tuk DESC) AS rn
        FROM contribuinte c
        JOIN sitafe.sitafe_rascunho_fac rf
            ON rf.it_nu_inscricao_estadual = c.ie
    ) rf
    WHERE rf.rn = 1
),
contador_req_inscricao AS (
    SELECT
        ri.cpf_contador,
        ri.no_contador AS nome_contador,
        ri.municipio || ', ' || ri.uf AS endereco,
        CAST(NULL AS VARCHAR2(70)) AS email,
        ri.telefone,
        ri.crc_contador,
        'dossie_req_inscricao_contador.sql' AS origem_dado,
        'SITAFE.SITAFE_REQ_INSCRICAO; BI.DM_LOCALIDADE' AS tabela_origem,
        4 AS prioridade
    FROM (
        SELECT
            SUBSTR(ri.gr_identificacao_contador, 2) AS cpf_contador,
            ri.it_no_contador AS no_contador,
            loc.no_municipio AS municipio,
            loc.co_uf AS uf,
            TRIM(NVL(ri.it_nu_ddd, '') || NVL(ri.it_nu_telefone, '')) AS telefone,
            ri.it_nu_crc AS crc_contador,
            ROW_NUMBER() OVER (ORDER BY ri.it_da_transacao DESC, ri.it_ho_transacao DESC, ri.tuk DESC) AS rn
        FROM contribuinte c
        JOIN sitafe.sitafe_req_inscricao ri
            ON ri.it_nu_inscricao_estadual = c.ie
        LEFT JOIN bi.dm_localidade loc
            ON ri.it_co_municipio = loc.co_municipio
    ) ri
    WHERE ri.rn = 1
),
contador_unificado AS (
    SELECT * FROM contador_principal
    UNION ALL
    SELECT * FROM contador_historico_fac
    UNION ALL
    SELECT * FROM contador_rascunho_fac
    UNION ALL
    SELECT * FROM contador_req_inscricao
),
contador_contatos_por_fonte AS (
    SELECT
        chave_contador,
        LISTAGG(email_fonte, ' | ') WITHIN GROUP (ORDER BY prioridade, email_fonte) AS emails_por_fonte,
        LISTAGG(telefone_fonte, ' | ') WITHIN GROUP (ORDER BY prioridade, telefone_fonte) AS telefones_por_fonte,
        LISTAGG(rotulo_origem, ' | ') WITHIN GROUP (ORDER BY prioridade, rotulo_origem) AS fontes_contato
    FROM (
        SELECT DISTINCT
            COALESCE(REGEXP_REPLACE(cu.cpf_contador, '[^0-9]', ''), cu.nome_contador) AS chave_contador,
            cu.prioridade,
            CASE cu.origem_dado
                WHEN 'dossie_historico_fac.sql' THEN 'FAC atual'
                WHEN 'dossie_contador.sql' THEN 'SITAFE_PESSOA'
                WHEN 'dossie_rascunho_fac_contador.sql' THEN 'Rascunho FAC'
                WHEN 'dossie_req_inscricao_contador.sql' THEN 'Requerimento'
                ELSE cu.origem_dado
            END AS rotulo_origem,
            CASE
                WHEN cu.email IS NOT NULL AND TRIM(cu.email) IS NOT NULL THEN
                    CASE cu.origem_dado
                        WHEN 'dossie_historico_fac.sql' THEN 'FAC atual'
                        WHEN 'dossie_contador.sql' THEN 'SITAFE_PESSOA'
                        WHEN 'dossie_rascunho_fac_contador.sql' THEN 'Rascunho FAC'
                        WHEN 'dossie_req_inscricao_contador.sql' THEN 'Requerimento'
                        ELSE cu.origem_dado
                    END || ': ' || TRIM(cu.email)
            END AS email_fonte,
            CASE
                WHEN cu.telefone IS NOT NULL AND TRIM(cu.telefone) IS NOT NULL THEN
                    CASE cu.origem_dado
                        WHEN 'dossie_historico_fac.sql' THEN 'FAC atual'
                        WHEN 'dossie_contador.sql' THEN 'SITAFE_PESSOA'
                        WHEN 'dossie_rascunho_fac_contador.sql' THEN 'Rascunho FAC'
                        WHEN 'dossie_req_inscricao_contador.sql' THEN 'Requerimento'
                        ELSE cu.origem_dado
                    END || ': ' || TRIM(cu.telefone)
            END AS telefone_fonte
        FROM contador_unificado cu
        WHERE TRIM(cu.cpf_contador) IS NOT NULL
           OR TRIM(cu.nome_contador) IS NOT NULL
    )
    GROUP BY chave_contador
),
contador_priorizado AS (
    SELECT
        MAX(cpf_contador) KEEP (
            DENSE_RANK FIRST ORDER BY CASE WHEN cpf_contador IS NOT NULL THEN 0 ELSE 1 END, prioridade
        ) AS cpf_contador,
        MAX(nome_contador) KEEP (
            DENSE_RANK FIRST ORDER BY CASE WHEN nome_contador IS NOT NULL THEN 0 ELSE 1 END, prioridade
        ) AS nome_contador,
        MAX(endereco) KEEP (
            DENSE_RANK FIRST ORDER BY CASE WHEN endereco IS NOT NULL THEN 0 ELSE 1 END, prioridade
        ) AS endereco,
        MAX(email) KEEP (
            DENSE_RANK FIRST ORDER BY CASE WHEN email IS NOT NULL THEN 0 ELSE 1 END, prioridade
        ) AS email,
        MAX(telefone) KEEP (
            DENSE_RANK FIRST ORDER BY CASE WHEN telefone IS NOT NULL THEN 0 ELSE 1 END, prioridade
        ) AS telefone,
        MAX(crc_contador) KEEP (
            DENSE_RANK FIRST ORDER BY CASE WHEN crc_contador IS NOT NULL THEN 0 ELSE 1 END, prioridade
        ) AS crc_contador,
        MAX(origem_dado) KEEP (
            DENSE_RANK FIRST ORDER BY prioridade
        ) AS origem_dado,
        MAX(tabela_origem) KEEP (
            DENSE_RANK FIRST ORDER BY prioridade
        ) AS tabela_origem,
        MAX(ccpf.emails_por_fonte) AS emails_por_fonte,
        MAX(ccpf.telefones_por_fonte) AS telefones_por_fonte,
        MAX(ccpf.fontes_contato) AS fontes_contato,
        1 AS rn
    FROM (
        SELECT
            REGEXP_REPLACE(cpf_contador, '[^0-9]', '') AS cpf_contador,
            nome_contador,
            endereco,
            email,
            telefone,
            crc_contador,
            origem_dado,
            tabela_origem,
            prioridade,
            COALESCE(REGEXP_REPLACE(cpf_contador, '[^0-9]', ''), nome_contador) AS chave_contador
        FROM contador_unificado
        WHERE TRIM(cpf_contador) IS NOT NULL
           OR TRIM(nome_contador) IS NOT NULL
    ) base
    LEFT JOIN contador_contatos_por_fonte ccpf
        ON base.chave_contador = ccpf.chave_contador
    GROUP BY chave_contador
),
socios_atuais AS (
    SELECT
        'SOCIO_ATUAL' AS tipo_vinculo,
        prm.cnpj_consultado,
        prm.cnpj_raiz,
        SUBSTR(p.gr_identificacao, 2) AS cpf_cnpj_referencia,
        p.it_no_pessoa AS nome_referencia,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        CAST(NULL AS VARCHAR2(200)) AS endereco,
        CAST(NULL AS VARCHAR2(20)) AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS telefone_nfe_nfce,
        CAST(NULL AS VARCHAR2(70)) AS email,
        CAST(NULL AS VARCHAR2(4000)) AS telefones_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS emails_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS fontes_contato,
        'SOCIO ATUAL' AS situacao_cadastral,
        'SOCIO' AS indicador_matriz_filial,
        'dossie_historico_socios.sql' AS origem_dado,
        'SITAFE.SITAFE_HISTORICO_SOCIO; SITAFE.SITAFE_PESSOA' AS tabela_origem,
        40 AS ordem_exibicao
    FROM parametros prm
    JOIN contribuinte c
        ON prm.cnpj_consultado = c.cnpj_consultado
    JOIN sitafe.sitafe_historico_socio hs
        ON hs.it_nu_inscricao_estadual = c.ie
        AND hs.it_in_ultima_fac = '9'
        AND (hs.it_da_fim_part_societaria = '        ' OR hs.it_da_fim_part_societaria > TO_CHAR(SYSDATE, 'yyyymmdd'))
    LEFT JOIN sitafe.sitafe_pessoa p
        ON p.gr_identificacao = hs.gr_identificacao
        AND p.it_in_ultima_situacao = '9'
),
emails_nfe AS (
    SELECT DISTINCT
        'EMAIL_NFE' AS tipo_vinculo,
        prm.cnpj_consultado,
        prm.cnpj_raiz,
        prm.cnpj_consultado AS cpf_cnpj_referencia,
        'Email observado em documento fiscal' AS nome_referencia,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        CAST(NULL AS VARCHAR2(200)) AS endereco,
        CAST(NULL AS VARCHAR2(20)) AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS telefone_nfe_nfce,
        n.email_dest AS email,
        CAST(NULL AS VARCHAR2(4000)) AS telefones_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS emails_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS fontes_contato,
        CAST(NULL AS VARCHAR2(200)) AS situacao_cadastral,
        'EMPRESA' AS indicador_matriz_filial,
        'NFe.sql' AS origem_dado,
        'BI.FATO_NFE_DETALHE' AS tabela_origem,
        50 AS ordem_exibicao
    FROM parametros prm
    JOIN bi.fato_nfe_detalhe n
        ON (n.co_emitente = prm.cnpj_consultado OR n.co_destinatario = prm.cnpj_consultado)
    WHERE n.email_dest IS NOT NULL
      AND TRIM(n.email_dest) IS NOT NULL
      AND n.dhemi >= DATE '2020-01-01'
),
emails_nfce AS (
    SELECT DISTINCT
        'EMAIL_NFE' AS tipo_vinculo,
        prm.cnpj_consultado,
        prm.cnpj_raiz,
        prm.cnpj_consultado AS cpf_cnpj_referencia,
        'Email observado em documento fiscal' AS nome_referencia,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        CAST(NULL AS VARCHAR2(200)) AS endereco,
        CAST(NULL AS VARCHAR2(20)) AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS telefone_nfe_nfce,
        CAST(NULL AS VARCHAR2(70)) AS email,
        CAST(NULL AS VARCHAR2(4000)) AS telefones_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS emails_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS fontes_contato,
        CAST(NULL AS VARCHAR2(200)) AS situacao_cadastral,
        'EMPRESA' AS indicador_matriz_filial,
        'NFCe.sql' AS origem_dado,
        'BI.FATO_NFCE_DETALHE' AS tabela_origem,
        55 AS ordem_exibicao
    FROM parametros prm
    WHERE 1 = 0
),
fones_contador_notas AS (
    SELECT
        documento,
        LISTAGG(telefone, ', ') WITHIN GROUP (ORDER BY telefone) AS telefone_nfe_nfce
    FROM (
        SELECT DISTINCT
            cp.cpf_contador AS documento,
            REGEXP_REPLACE(n.fone_emit, '[^0-9]', '') AS telefone
        FROM contador_priorizado cp
        JOIN bi.fato_nfe_detalhe n
            ON cp.cpf_contador = n.co_emitente
        WHERE cp.rn = 1
          AND n.fone_emit IS NOT NULL
        UNION ALL
        SELECT DISTINCT
            cp.cpf_contador AS documento,
            REGEXP_REPLACE(n.fone_dest, '[^0-9]', '') AS telefone
        FROM contador_priorizado cp
        JOIN bi.fato_nfe_detalhe n
            ON cp.cpf_contador = n.co_destinatario
        WHERE cp.rn = 1
          AND n.fone_dest IS NOT NULL
        UNION ALL
        SELECT DISTINCT
            cp.cpf_contador AS documento,
            REGEXP_REPLACE(n.fone_emit, '[^0-9]', '') AS telefone
        FROM contador_priorizado cp
        JOIN bi.fato_nfce_detalhe n
            ON cp.cpf_contador = n.co_emitente
        WHERE cp.rn = 1
          AND n.fone_emit IS NOT NULL
        UNION ALL
        SELECT DISTINCT
            cp.cpf_contador AS documento,
            REGEXP_REPLACE(n.fone_dest, '[^0-9]', '') AS telefone
        FROM contador_priorizado cp
        JOIN bi.fato_nfce_detalhe n
            ON cp.cpf_contador = n.co_destinatario
        WHERE cp.rn = 1
          AND n.fone_dest IS NOT NULL
    )
    WHERE telefone IS NOT NULL
      AND telefone <> ''
      AND LENGTH(documento) IN (11, 14)
      AND LENGTH(telefone) >= 8
    GROUP BY documento
),
resultado_final AS (
    SELECT
        'EMPRESA_PRINCIPAL' AS tipo_vinculo,
        c.cnpj_consultado,
        c.cnpj_raiz,
        c.cnpj_consultado AS cpf_cnpj_referencia,
        c.nome_referencia,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        c.endereco,
        CAST(NULL AS VARCHAR2(20)) AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS telefone_nfe_nfce,
        CAST(NULL AS VARCHAR2(70)) AS email,
        CAST(NULL AS VARCHAR2(4000)) AS telefones_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS emails_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS fontes_contato,
        c.situacao_cadastral,
        'EMPRESA' AS indicador_matriz_filial,
        'dados_cadastrais.sql' AS origem_dado,
        'BI.DM_PESSOA; BI.DM_SITUACAO_CONTRIBUINTE' AS tabela_origem,
        10 AS ordem_exibicao
    FROM contribuinte c

    UNION ALL

    SELECT
        f.tipo_vinculo,
        f.cnpj_consultado,
        f.cnpj_raiz,
        f.cpf_cnpj_referencia,
        f.nome_referencia,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        f.endereco,
        CAST(NULL AS VARCHAR2(20)) AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS telefone_nfe_nfce,
        CAST(NULL AS VARCHAR2(70)) AS email,
        CAST(NULL AS VARCHAR2(4000)) AS telefones_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS emails_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS fontes_contato,
        f.situacao_cadastral,
        f.indicador_matriz_filial,
        'dados_cadastrais.sql' AS origem_dado,
        'BI.DM_PESSOA; BI.DM_SITUACAO_CONTRIBUINTE' AS tabela_origem,
        CASE WHEN f.tipo_vinculo = 'MATRIZ_RAIZ' THEN 20 ELSE 25 END AS ordem_exibicao
    FROM filiais_raiz f

    UNION ALL

    SELECT
        'CONTADOR_EMPRESA' AS tipo_vinculo,
        prm.cnpj_consultado,
        prm.cnpj_raiz,
        cp.cpf_contador AS cpf_cnpj_referencia,
        cp.nome_contador AS nome_referencia,
        cp.crc_contador,
        cp.endereco,
        cp.telefone,
        fn.telefone_nfe_nfce,
        cp.email,
        cp.telefones_por_fonte,
        cp.emails_por_fonte,
        cp.fontes_contato,
        CAST(NULL AS VARCHAR2(200)) AS situacao_cadastral,
        'CONTADOR' AS indicador_matriz_filial,
        cp.origem_dado,
        cp.tabela_origem,
        30 AS ordem_exibicao
    FROM parametros prm
    JOIN contador_priorizado cp
        ON cp.rn = 1
    LEFT JOIN fones_contador_notas fn
        ON cp.cpf_contador = fn.documento

    UNION ALL

    SELECT * FROM socios_atuais
    UNION ALL
    SELECT * FROM emails_nfe
    UNION ALL
    SELECT * FROM emails_nfce
)
SELECT
    tipo_vinculo,
    cnpj_consultado,
    cnpj_raiz,
    cpf_cnpj_referencia,
    nome_referencia,
    crc_contador,
    endereco,
    telefone,
    telefone_nfe_nfce,
    email,
    telefones_por_fonte,
    emails_por_fonte,
    fontes_contato,
    situacao_cadastral,
    indicador_matriz_filial,
    origem_dado,
    tabela_origem,
    ordem_exibicao
FROM resultado_final
ORDER BY ordem_exibicao, tipo_vinculo, nome_referencia, cpf_cnpj_referencia
