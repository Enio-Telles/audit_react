WITH PARAMETROS AS (
    SELECT 
        :CNPJ AS cnpj_filtro,
        TO_DATE(:data_inicial, 'DD/MM/YYYY') AS dt_ini_filtro,
        TO_DATE(:data_final,   'DD/MM/YYYY') AS dt_fim_filtro,
        /* Garante que se o parametro vier nulo, usa sysdate, senao usa a data informada */
        NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte
    FROM dual
),

ARQUIVOS_RANKING AS (SELECT
reg_0000.id as reg_0000_id,
reg_0000.cnpj,
reg_0000.cod_fin AS cod_fin_efd,
reg_0000.dt_ini,
reg_0000.dt_fin,
reg_0000.data_entrega,
       CASE 
          WHEN reg_0000.cod_fin = 0 THEN '0 - Remessa do arquivo original'
        WHEN reg_0000.cod_fin = 1 THEN '1 - Remessa do arquivo substituto'
         ELSE 'Outros'    
       END AS desc_finalidade,
      -- Colunas de controle trazidas do CTE Parametros para filtro posterior
        p.dt_corte, 
        p.dt_ini_filtro, 
        p.dt_fim_filtro,
        -- Lógica de Versionamento:
        -- Particiona por Empresa e Mês de Referência (dt_inicio)
        -- Ordena por Data de Entrega decrescente (o mais recente fica com rn=1)
        ROW_NUMBER() OVER (
            PARTITION BY reg_0000.cnpj, reg_0000.dt_ini 
            ORDER BY reg_0000.data_entrega DESC
        ) AS rn       
FROM sped.reg_0000 reg_0000
 JOIN PARAMETROS p ON reg_0000.cnpj = p.cnpj_filtro
     WHERE 
        
        reg_0000.data_entrega <= p.dt_corte -- Filtro de "Viagem no Tempo": Ignora retificações feitas após a data de corte
        AND reg_0000.dt_ini BETWEEN p.dt_ini_filtro AND p.dt_fim_filtro
        )

SELECT
    TO_CHAR(arq.dt_ini, 'MM/YYYY') AS periodo_efd,
    
    /* Tratamento de Nulos para evitar erros em somas ou relatórios */
    NVL(e110.vl_tot_debitos, 0)        AS vl_tot_debitos,
    NVL(e110.vl_aj_debitos, 0)         AS vl_aj_debitos,
    NVL(e110.vl_tot_aj_debitos, 0)     AS vl_tot_aj_debitos,
    NVL(e110.vl_estornos_cred, 0)      AS vl_estornos_cred,
    NVL(e110.vl_tot_creditos, 0)       AS vl_tot_creditos,
    NVL(e110.vl_aj_creditos, 0)        AS vl_aj_creditos,
    NVL(e110.vl_tot_aj_creditos, 0)    AS vl_tot_aj_creditos,
    NVL(e110.vl_estornos_deb, 0)       AS vl_estornos_deb,
    NVL(e110.vl_sld_credor_ant, 0)     AS vl_sld_credor_ant,
    NVL(e110.vl_sld_apurado, 0)        AS vl_sld_apurado,
    NVL(e110.vl_tot_ded, 0)            AS vl_tot_ded,
    NVL(e110.vl_icms_recolher, 0)      AS vl_icms_recolher,
    NVL(e110.vl_sld_credor_transportar, 0) AS vl_sld_credor_transportar,
    NVL(e110.deb_esp, 0)               AS deb_esp,
    arq.cod_fin_efd,
    arq.data_entrega                   AS Data_entrega_efd_periodo
    
FROM sped.reg_e110 e110
INNER JOIN ARQUIVOS_RANKING arq 
  ON e110.reg_0000_id = arq.reg_0000_id
WHERE arq.rn = 1 /* Filtra apenas a versão mais recente (Original ou Retificadora Final) */
ORDER BY arq.dt_ini ASC