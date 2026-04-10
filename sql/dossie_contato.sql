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
        h.it_da_referencia AS referencia_vinculo,
        CASE
            WHEN LEAD(h.it_da_referencia) OVER (PARTITION BY h.it_nu_inscricao_estadual ORDER BY h.it_nu_fac) IS NULL
                 AND SUBSTR(h.gr_ident_contador, 2) IS NOT NULL THEN 'Atual'
            WHEN LEAD(h.it_da_referencia) OVER (PARTITION BY h.it_nu_inscricao_estadual ORDER BY h.it_nu_fac) IS NULL
                 AND SUBSTR(h.gr_ident_contador, 2) IS NULL THEN 'Atual - sem contador indicado'
            WHEN SUBSTR(h.gr_ident_contador, 2) IS NULL THEN 'Anterior - periodo sem indicacao de contador'
            ELSE 'Anterior'
        END AS situacao_vinculo,
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
        COALESCE(
            hf.logradouro || ', ' || hf.num || ', ' || hf.municipio || ', ' || hf.uf,
            hf.logradouro_corr || ', ' || hf.bairro_corr || ', ' || hf.municipio_corr || ', ' || hf.uf_corr
        ) AS endereco,
        COALESCE(TRIM(hf.email), TRIM(hf.email_corr)) AS email,
        COALESCE(NULLIF(TRIM(hf.telefone), ''), NULLIF(TRIM(hf.telefone_corr), '')) AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        hf.da_trans AS referencia_vinculo,
        CASE
            WHEN hf.ult_fac = '9' THEN 'FAC atual'
            ELSE 'Historico FAC'
        END AS situacao_vinculo,
        'dossie_historico_fac.sql' AS origem_dado,
        'SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; SITAFE.SITAFE_PESSOA; SITAFE.SITAFE_TABELAS_CADASTRO; BI.DM_PESSOA; BI.DM_LOCALIDADE' AS tabela_origem,
        1 AS prioridade
    FROM (
        SELECT
            t.cpf_contador AS cpf_contador,
            t.no_contador,
            t.logradouro,
            t.num,
            t.municipio,
            t.uf,
            t.telefone,
            t.email,
            t.email_corr,
            t.logradouro_corr,
            t.bairro_corr,
            t.municipio_corr,
            t.uf_corr,
            t.telefone_corr,
            t.ult_fac,
            t.da_trans,
            ROW_NUMBER() OVER (
                PARTITION BY t.cpf_contador, t.no_contador
                ORDER BY
                    CASE
                        WHEN TRIM(t.email) IS NOT NULL OR TRIM(t.email_corr) IS NOT NULL OR TRIM(t.telefone) IS NOT NULL OR TRIM(t.telefone_corr) IS NOT NULL THEN 0
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
                TRIM(NVL(sp.it_nu_ddd, '') || NVL(sp.it_nu_telefone, '')) AS telefone,
                sp.it_co_correio_eletronico AS email,
                sp.it_co_correio_eletro_corresp AS email_corr,
                sp.it_tx_logradouro_corresp AS logradouro_corr,
                sp.it_no_bairro_corresp AS bairro_corr,
                loc_corresp.no_municipio AS municipio_corr,
                sp.it_sg_uf AS uf_corr,
                TRIM(NVL(sp.it_nu_ddd_corresp, '') || NVL(sp.it_nu_telefone_corresp, '')) AS telefone_corr,
                h.it_in_ultima_fac AS ult_fac
            FROM contribuinte c
            JOIN sitafe.sitafe_historico_contribuinte h
                ON h.it_nu_inscricao_estadual = c.ie
            LEFT JOIN sitafe.sitafe_pessoa sp
                ON h.it_nu_fac = sp.it_nu_fac
            LEFT JOIN sitafe.sitafe_tabelas_cadastro tg
                ON sp.it_co_logradouro = tg.it_co_logradouro
            LEFT JOIN bi.dm_localidade loc
                ON sp.it_co_municipio = loc.co_municipio
            LEFT JOIN bi.dm_localidade loc_corresp
                ON sp.it_co_municipio_corresp = loc_corresp.co_municipio
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
        rf.da_transacao AS referencia_vinculo,
        'Rascunho FAC' AS situacao_vinculo,
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
            rf.it_da_transacao AS da_transacao,
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
        ri.da_transacao AS referencia_vinculo,
        'Requerimento de Inscricao' AS situacao_vinculo,
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
            ri.it_da_transacao AS da_transacao,
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
        LISTAGG(rotulo_origem, ' | ') WITHIN GROUP (ORDER BY prioridade, rotulo_origem) AS fontes_contato,
        LISTAGG(situacao_vinculo, ' | ') WITHIN GROUP (ORDER BY prioridade, situacao_vinculo) AS situacoes_vinculo
    FROM (
        SELECT DISTINCT
            COALESCE(REGEXP_REPLACE(cu.cpf_contador, '[^0-9]', ''), cu.nome_contador) AS chave_contador,
            cu.prioridade,
            cu.situacao_vinculo,
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
        MAX(ccpf.situacoes_vinculo) AS situacoes_vinculo,
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
            situacao_vinculo,
            COALESCE(REGEXP_REPLACE(cpf_contador, '[^0-9]', ''), nome_contador) AS chave_contador
        FROM contador_unificado
        WHERE TRIM(cpf_contador) IS NOT NULL
           OR TRIM(nome_contador) IS NOT NULL
    ) base
    LEFT JOIN contador_contatos_por_fonte ccpf
        ON base.chave_contador = ccpf.chave_contador
    GROUP BY chave_contador
),
socios_historico AS (
    SELECT
        CASE
            WHEN hs.it_in_ultima_fac = '9'
                 AND (hs.it_da_fim_part_societaria = '        ' OR hs.it_da_fim_part_societaria > TO_CHAR(SYSDATE, 'yyyymmdd'))
            THEN 'SOCIO_ATUAL'
            ELSE 'SOCIO_ANTIGO'
        END AS tipo_vinculo,
        prm.cnpj_consultado,
        prm.cnpj_raiz,
        SUBSTR(p.gr_identificacao, 2) AS cpf_cnpj_referencia,
        p.it_no_pessoa AS nome_referencia,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        CASE
            WHEN tg.it_no_logradouro IS NOT NULL OR p.it_ed_numero IS NOT NULL OR loc.no_municipio IS NOT NULL OR p.it_sg_uf IS NOT NULL THEN
                TRIM(
                    tg.it_no_logradouro
                    || CASE WHEN p.it_ed_numero IS NOT NULL THEN ', ' || p.it_ed_numero ELSE '' END
                    || CASE WHEN loc.no_municipio IS NOT NULL THEN ', ' || loc.no_municipio ELSE '' END
                    || CASE WHEN p.it_sg_uf IS NOT NULL THEN ', ' || p.it_sg_uf ELSE '' END
                )
            WHEN p.it_tx_logradouro_corresp IS NOT NULL OR p.it_no_bairro_corresp IS NOT NULL OR loc_corresp.no_municipio IS NOT NULL THEN
                TRIM(
                    p.it_tx_logradouro_corresp
                    || CASE WHEN p.it_no_bairro_corresp IS NOT NULL THEN ', ' || p.it_no_bairro_corresp ELSE '' END
                    || CASE WHEN loc_corresp.no_municipio IS NOT NULL THEN ', ' || loc_corresp.no_municipio ELSE '' END
                    || CASE WHEN p.it_sg_uf IS NOT NULL THEN ', ' || p.it_sg_uf ELSE '' END
                )
        END AS endereco,
        NULLIF(TRIM(NVL(p.it_nu_ddd, '') || NVL(p.it_nu_telefone, '')), '') AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS telefone_nfe_nfce,
        COALESCE(
            NULLIF(TRIM(p.it_co_correio_eletronico), ''),
            NULLIF(TRIM(p.it_co_correio_eletro_corresp), '')
        ) AS email,
        CAST(NULL AS VARCHAR2(4000)) AS telefones_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS emails_por_fonte,
        CAST(NULL AS VARCHAR2(4000)) AS fontes_contato,
        CASE
            WHEN hs.it_in_ultima_fac = '9'
                 AND (hs.it_da_fim_part_societaria = '        ' OR hs.it_da_fim_part_societaria > TO_CHAR(SYSDATE, 'yyyymmdd'))
            THEN 'SOCIO ATUAL'
            ELSE 'SOCIO ANTIGO'
        END AS situacao_cadastral,
        'SOCIO' AS indicador_matriz_filial,
        'dossie_historico_socios.sql' AS origem_dado,
        'SITAFE.SITAFE_HISTORICO_SOCIO; SITAFE.SITAFE_PESSOA; SITAFE.SITAFE_TABELAS_CADASTRO; BI.DM_LOCALIDADE' AS tabela_origem,
        CASE
            WHEN hs.it_in_ultima_fac = '9'
                 AND (hs.it_da_fim_part_societaria = '        ' OR hs.it_da_fim_part_societaria > TO_CHAR(SYSDATE, 'yyyymmdd'))
            THEN 40
            ELSE 45
        END AS ordem_exibicao
    FROM parametros prm
    JOIN contribuinte c
        ON prm.cnpj_consultado = c.cnpj_consultado
    JOIN sitafe.sitafe_historico_socio hs
        ON hs.it_nu_inscricao_estadual = c.ie
    LEFT JOIN sitafe.sitafe_pessoa p
        ON p.gr_identificacao = hs.gr_identificacao
        AND p.it_in_ultima_situacao = '9'
    LEFT JOIN sitafe.sitafe_tabelas_cadastro tg
        ON p.it_co_logradouro = tg.it_co_logradouro
    LEFT JOIN bi.dm_localidade loc
        ON p.it_co_municipio = loc.co_municipio
    LEFT JOIN bi.dm_localidade loc_corresp
        ON p.it_co_municipio_corresp = loc_corresp.co_municipio
),
empresa_fac_atual AS (
    SELECT
        'EMPRESA_FAC_ATUAL' AS tipo_vinculo,
        prm.cnpj_consultado,
        prm.cnpj_raiz,
        prm.cnpj_consultado AS cpf_cnpj_referencia,
        COALESCE(TRIM(hf.nome), c.nome_referencia) AS nome_referencia,
        CAST(NULL AS VARCHAR2(20)) AS crc_contador,
        COALESCE(
            hf.logradouro || ', ' || hf.num || ', ' || hf.municipio || ', ' || hf.uf,
            hf.logradouro_corr || ', ' || hf.bairro_corr || ', ' || hf.municipio_corr || ', ' || hf.uf_corr
        ) AS endereco,
        COALESCE(NULLIF(TRIM(hf.telefone), ''), NULLIF(TRIM(hf.telefone_corr), '')) AS telefone,
        CAST(NULL AS VARCHAR2(20)) AS telefone_nfe_nfce,
        COALESCE(TRIM(hf.email), TRIM(hf.email_corr)) AS email,
        CASE
            WHEN COALESCE(NULLIF(TRIM(hf.telefone), ''), NULLIF(TRIM(hf.telefone_corr), '')) IS NOT NULL THEN
                'FAC atual: ' || COALESCE(NULLIF(TRIM(hf.telefone), ''), NULLIF(TRIM(hf.telefone_corr), ''))
        END AS telefones_por_fonte,
        CASE
            WHEN COALESCE(TRIM(hf.email), TRIM(hf.email_corr)) IS NOT NULL THEN
                'FAC atual: ' || COALESCE(TRIM(hf.email), TRIM(hf.email_corr))
        END AS emails_por_fonte,
        'FAC atual' AS fontes_contato,
        'FAC atual' AS situacao_cadastral,
        'EMPRESA' AS indicador_matriz_filial,
        'dossie_historico_fac.sql' AS origem_dado,
        'SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; SITAFE.SITAFE_PESSOA; SITAFE.SITAFE_TABELAS_CADASTRO; BI.DM_LOCALIDADE' AS tabela_origem,
        15 AS ordem_exibicao
    FROM parametros prm
    JOIN contribuinte c
        ON prm.cnpj_consultado = c.cnpj_consultado
    JOIN (
        SELECT *
        FROM (
            SELECT
                h.it_nu_inscricao_estadual,
                sp.it_no_pessoa AS nome,
                tg.it_no_logradouro AS logradouro,
                sp.it_ed_numero AS num,
                loc.no_municipio AS municipio,
                sp.it_sg_uf AS uf,
                TRIM(NVL(sp.it_nu_ddd, '') || NVL(sp.it_nu_telefone, '')) AS telefone,
                sp.it_co_correio_eletronico AS email,
                sp.it_co_correio_eletro_corresp AS email_corr,
                sp.it_tx_logradouro_corresp AS logradouro_corr,
                sp.it_no_bairro_corresp AS bairro_corr,
                loc_corresp.no_municipio AS municipio_corr,
                sp.it_sg_uf AS uf_corr,
                TRIM(NVL(sp.it_nu_ddd_corresp, '') || NVL(sp.it_nu_telefone_corresp, '')) AS telefone_corr,
                ROW_NUMBER() OVER (PARTITION BY h.it_nu_inscricao_estadual ORDER BY h.it_da_transacao DESC, h.it_nu_fac DESC) AS rn
            FROM contribuinte c
            JOIN sitafe.sitafe_historico_contribuinte h
                ON h.it_nu_inscricao_estadual = c.ie
            LEFT JOIN sitafe.sitafe_pessoa sp
                ON h.it_nu_fac = sp.it_nu_fac
            LEFT JOIN sitafe.sitafe_tabelas_cadastro tg
                ON sp.it_co_logradouro = tg.it_co_logradouro
            LEFT JOIN bi.dm_localidade loc
                ON sp.it_co_municipio = loc.co_municipio
            LEFT JOIN bi.dm_localidade loc_corresp
                ON sp.it_co_municipio_corresp = loc_corresp.co_municipio
        )
        WHERE rn = 1
    ) hf
        ON hf.it_nu_inscricao_estadual = c.ie
    WHERE COALESCE(
        hf.logradouro,
        hf.logradouro_corr,
        hf.telefone,
        hf.telefone_corr,
        hf.email,
        hf.email_corr
    ) IS NOT NULL
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
-- Etapa 1.1: E-mails de contadores extraidos de NFe/NFCe (apenas como destinatarios)
emails_contador_notas AS (
    SELECT
        documento,
        LISTAGG(email, ', ') WITHIN GROUP (ORDER BY email) AS email_nfe_nfce
    FROM (
        SELECT DISTINCT
            cp.cpf_contador AS documento,
            TRIM(n.email_dest) AS email
        FROM contador_priorizado cp
        JOIN bi.fato_nfe_detalhe n
            ON cp.cpf_contador = n.co_destinatario
        WHERE cp.rn = 1
          AND n.email_dest IS NOT NULL
          AND TRIM(n.email_dest) IS NOT NULL
          AND n.dhemi >= DATE '2020-01-01'
        UNION
        SELECT DISTINCT
            cp.cpf_contador AS documento,
            TRIM(n.email_dest) AS email
        FROM contador_priorizado cp
        JOIN bi.fato_nfce_detalhe n
            ON cp.cpf_contador = n.co_destinatario
        WHERE cp.rn = 1
          AND n.email_dest IS NOT NULL
          AND TRIM(n.email_dest) IS NOT NULL
    )
    WHERE LENGTH(documento) IN (11, 14)
      AND REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    GROUP BY documento
),
-- Etapa 1.2: E-mails de socios extraidos de NFe/NFCe (apenas como destinatarios)
emails_socios_notas AS (
    SELECT
        documento,
        LISTAGG(email, ', ') WITHIN GROUP (ORDER BY email) AS email_nfe_nfce
    FROM (
        SELECT DISTINCT
            s.cpf_cnpj_referencia AS documento,
            TRIM(n.email_dest) AS email
        FROM socios_historico s
        JOIN bi.fato_nfe_detalhe n
            ON s.cpf_cnpj_referencia = n.co_destinatario
        WHERE s.tipo_vinculo = 'SOCIO_ATUAL'
          AND n.email_dest IS NOT NULL
          AND TRIM(n.email_dest) IS NOT NULL
          AND n.dhemi >= DATE '2020-01-01'
        UNION
        SELECT DISTINCT
            s.cpf_cnpj_referencia AS documento,
            TRIM(n.email_dest) AS email
        FROM socios_historico s
        JOIN bi.fato_nfce_detalhe n
            ON s.cpf_cnpj_referencia = n.co_destinatario
        WHERE s.tipo_vinculo = 'SOCIO_ATUAL'
          AND n.email_dest IS NOT NULL
          AND TRIM(n.email_dest) IS NOT NULL
    )
    WHERE LENGTH(documento) IN (11, 14)
      AND REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    GROUP BY documento
),
-- Etapa 1.3: Endereços extraídos de NFe/NFCe (como emitente ou destinatário) para a Empresa
enderecos_empresa_notas AS (
    SELECT
        documento,
        SUBSTR(LISTAGG(endereco, ' | ') WITHIN GROUP (ORDER BY endereco), 1, 4000) AS enderecos_nfe_nfce
    FROM (
        SELECT DISTINCT
            prm.cnpj_consultado AS documento,
            UPPER(n.xlgr_emit) || ', ' || UPPER(n.nro_emit) ||
            CASE WHEN n.xcpl_emit IS NOT NULL THEN ' - ' || UPPER(n.xcpl_emit) ELSE '' END ||
            ' - ' || UPPER(n.xbairro_emit) || ' - ' || UPPER(n.xmun_emit) || '/' || UPPER(n.co_uf_emit) AS endereco
        FROM parametros prm
        JOIN bi.fato_nfe_detalhe n ON prm.cnpj_consultado = n.co_emitente
        WHERE n.xlgr_emit IS NOT NULL AND n.dhemi >= DATE '2020-01-01'
        UNION
        SELECT DISTINCT
            prm.cnpj_consultado AS documento,
            UPPER(n.xlgr_dest) || ', ' || UPPER(n.nro_dest) ||
            CASE WHEN n.xcpl_dest IS NOT NULL THEN ' - ' || UPPER(n.xcpl_dest) ELSE '' END ||
            ' - ' || UPPER(n.xbairro_dest) || ' - ' || UPPER(n.xmun_dest) || '/' || UPPER(n.co_uf_dest) AS endereco
        FROM parametros prm
        JOIN bi.fato_nfe_detalhe n ON prm.cnpj_consultado = n.co_destinatario
        WHERE n.xlgr_dest IS NOT NULL AND n.dhemi >= DATE '2020-01-01'
    )
    GROUP BY documento
),
-- Etapa 1.4: Telefones extraídos de NFe/NFCe para a Empresa
telefones_empresa_notas AS (
    SELECT
        documento,
        SUBSTR(LISTAGG(telefone, ', ') WITHIN GROUP (ORDER BY telefone), 1, 4000) AS telefone_nfe_nfce
    FROM (
        SELECT DISTINCT prm.cnpj_consultado AS documento, REGEXP_REPLACE(n.fone_emit, '[^0-9]', '') AS telefone
        FROM parametros prm JOIN bi.fato_nfe_detalhe n ON prm.cnpj_consultado = n.co_emitente WHERE n.fone_emit IS NOT NULL AND n.dhemi >= DATE '2020-01-01'
        UNION
        SELECT DISTINCT prm.cnpj_consultado AS documento, REGEXP_REPLACE(n.fone_dest, '[^0-9]', '') AS telefone
        FROM parametros prm JOIN bi.fato_nfe_detalhe n ON prm.cnpj_consultado = n.co_destinatario WHERE n.fone_dest IS NOT NULL AND n.dhemi >= DATE '2020-01-01'
        UNION
        SELECT DISTINCT prm.cnpj_consultado AS documento, REGEXP_REPLACE(n.fone_emit, '[^0-9]', '') AS telefone
        FROM parametros prm JOIN bi.fato_nfce_detalhe n ON prm.cnpj_consultado = n.co_emitente WHERE n.fone_emit IS NOT NULL
        UNION
        SELECT DISTINCT prm.cnpj_consultado AS documento, REGEXP_REPLACE(n.fone_dest, '[^0-9]', '') AS telefone
        FROM parametros prm JOIN bi.fato_nfce_detalhe n ON prm.cnpj_consultado = n.co_destinatario WHERE n.fone_dest IS NOT NULL
    )
    WHERE telefone IS NOT NULL AND LENGTH(telefone) >= 8
    GROUP BY documento
),
-- Etapa 1.5: E-mails extraídos de NFe/NFCe para a Empresa
emails_empresa_notas AS (
    SELECT
        documento,
        SUBSTR(LISTAGG(email, ', ') WITHIN GROUP (ORDER BY email), 1, 4000) AS email_nfe_nfce
    FROM (
        SELECT DISTINCT prm.cnpj_consultado AS documento, TRIM(n.email_dest) AS email
        FROM parametros prm JOIN bi.fato_nfe_detalhe n ON prm.cnpj_consultado = n.co_destinatario
        WHERE n.email_dest IS NOT NULL AND TRIM(n.email_dest) IS NOT NULL AND n.dhemi >= DATE '2020-01-01'
        UNION
        SELECT DISTINCT prm.cnpj_consultado AS documento, TRIM(n.email_dest) AS email
        FROM parametros prm JOIN bi.fato_nfce_detalhe n ON prm.cnpj_consultado = n.co_destinatario
        WHERE n.email_dest IS NOT NULL AND TRIM(n.email_dest) IS NOT NULL
    )
    WHERE REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
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
        CASE 
            WHEN ender_nfe.enderecos_nfe_nfce IS NOT NULL THEN
                c.endereco || ' | NFe/NFCe reconciliado: ' || ender_nfe.enderecos_nfe_nfce
            ELSE c.endereco
        END AS endereco,
        CAST(NULL AS VARCHAR2(20)) AS telefone,
        fone_nfe.telefone_nfe_nfce AS telefone_nfe_nfce,
        CAST(NULL AS VARCHAR2(70)) AS email,
        CAST(NULL AS VARCHAR2(4000)) AS telefones_por_fonte,
        CASE
            WHEN eml_nfe.email_nfe_nfce IS NOT NULL THEN 'NFe/NFCe reconciliado: ' || eml_nfe.email_nfe_nfce
            ELSE CAST(NULL AS VARCHAR2(4000))
        END AS emails_por_fonte,
        CASE
            WHEN ender_nfe.enderecos_nfe_nfce IS NOT NULL OR fone_nfe.telefone_nfe_nfce IS NOT NULL OR eml_nfe.email_nfe_nfce IS NOT NULL THEN 'NFe/NFCe'
            ELSE CAST(NULL AS VARCHAR2(4000))
        END AS fontes_contato,
        c.situacao_cadastral,
        'EMPRESA' AS indicador_matriz_filial,
        'dados_cadastrais.sql' AS origem_dado,
        'BI.DM_PESSOA; BI.DM_SITUACAO_CONTRIBUINTE' AS tabela_origem,
        10 AS ordem_exibicao
    FROM contribuinte c
    LEFT JOIN enderecos_empresa_notas ender_nfe ON c.cnpj_consultado = ender_nfe.documento
    LEFT JOIN telefones_empresa_notas fone_nfe ON c.cnpj_consultado = fone_nfe.documento
    LEFT JOIN emails_empresa_notas eml_nfe ON c.cnpj_consultado = eml_nfe.documento

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
        CASE
            WHEN ecn.email_nfe_nfce IS NOT NULL THEN
                COALESCE(cp.emails_por_fonte || ' | ', '') || 'NFe/NFCe reconciliado: ' || ecn.email_nfe_nfce
            ELSE cp.emails_por_fonte
        END AS emails_por_fonte,
        CASE
            WHEN ecn.email_nfe_nfce IS NOT NULL THEN
                COALESCE(cp.fontes_contato || ' | ', '') || 'NFe/NFCe'
            ELSE cp.fontes_contato
        END AS fontes_contato,
        cp.situacoes_vinculo AS situacao_cadastral,
        'CONTADOR' AS indicador_matriz_filial,
        cp.origem_dado,
        cp.tabela_origem,
        30 AS ordem_exibicao
    FROM parametros prm
    JOIN contador_priorizado cp
        ON cp.rn = 1
    LEFT JOIN fones_contador_notas fn
        ON cp.cpf_contador = fn.documento
    LEFT JOIN emails_contador_notas ecn
        ON cp.cpf_contador = ecn.documento

    UNION ALL

    SELECT * FROM empresa_fac_atual

    UNION ALL

    -- Etapa 1.3: Socios com emails de NFe/NFCe integrados
    SELECT
        s.tipo_vinculo,
        s.cnpj_consultado,
        s.cnpj_raiz,
        s.cpf_cnpj_referencia,
        s.nome_referencia,
        s.crc_contador,
        s.endereco,
        s.telefone,
        s.telefone_nfe_nfce,
        s.email,
        s.telefones_por_fonte,
        CASE
            WHEN esn.email_nfe_nfce IS NOT NULL THEN
                COALESCE(s.emails_por_fonte || ' | ', '') || 'NFe/NFCe reconciliado: ' || esn.email_nfe_nfce
            ELSE s.emails_por_fonte
        END AS emails_por_fonte,
        CASE
            WHEN esn.email_nfe_nfce IS NOT NULL THEN
                COALESCE(s.fontes_contato || ' | ', '') || 'NFe/NFCe'
            ELSE s.fontes_contato
        END AS fontes_contato,
        s.situacao_cadastral,
        s.indicador_matriz_filial,
        s.origem_dado,
        s.tabela_origem,
        s.ordem_exibicao
    FROM socios_historico s
    LEFT JOIN emails_socios_notas esn
        ON s.cpf_cnpj_referencia = esn.documento
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
